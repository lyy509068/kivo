#define _GNU_SOURCE
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <stdint.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <bpf/libbpf.h>
#include <bpf/bpf.h>
#include <errno.h>
#include <fcntl.h>
#include <poll.h>
#include <signal.h>

/* ============================================================
 * 实验开关
 * ============================================================ */
#define ENABLE_REORDER    1   // 实验B/C：是否启用 TCP 序号排序
#define ENABLE_FORWARD    1   // 实验C：是否转发给从机
/* ============================================================ */

#define SLAVE_IP "192.168.88.130"
#define SLAVE_PORT 2000
#define MAX_PAYLOAD_SIZE 1024
#define RING_SIZE (64 * 1024 * 1024)
#define OOO_MAX 128

#define SEQ_GT(a,b)  ((int32_t)((a)-(b)) > 0)

struct event_t {
    uint32_t src_ip;
    uint16_t src_port;
    uint32_t seq;
    uint32_t payload_len;
    char payload[MAX_PAYLOAD_SIZE];
};

struct ooo_node {
    uint32_t seq;
    uint32_t len;
    int used;
    char data[MAX_PAYLOAD_SIZE];
};

/* 环形缓冲区 */
struct ring_buf {
    char *buf;
    size_t size;
    size_t head;
    size_t tail;
    size_t bytes;
};

int slave_sock = -1;
volatile int running = 1;

static struct ring_buf rb;
static struct ooo_node ooo_pool[OOO_MAX];
static uint32_t expected_seq = 0;
static int seq_initialized = 0;

static volatile uint64_t recv_event  = 0;
static volatile uint64_t recv_bytes  = 0;
static volatile uint64_t sent_bytes  = 0;
static volatile uint64_t ooo_stored  = 0;
static volatile uint64_t ooo_merged  = 0;

/* ---------- 环形缓冲区操作 ---------- */
static int rb_init(struct ring_buf *rb, size_t size) {
    rb->buf = malloc(size);
    if (!rb->buf) return -1;
    rb->size = size;
    rb->head = 0;
    rb->tail = 0;
    rb->bytes = 0;
    return 0;
}

static int rb_write(struct ring_buf *rb, const char *data, size_t len) {
    if (rb->bytes + len > rb->size) {
        size_t new_size = rb->size * 2;
        while (new_size < rb->bytes + len) new_size *= 2;
        char *new_buf = malloc(new_size);
        if (!new_buf) return -1;

        size_t pos = 0;
        size_t remaining = rb->bytes;
        size_t h = rb->head;
        while (remaining > 0) {
            size_t chunk = rb->size - h;
            if (chunk > remaining) chunk = remaining;
            memcpy(new_buf + pos, rb->buf + h, chunk);
            pos += chunk;
            h = (h + chunk) % rb->size;
            remaining -= chunk;
        }
        free(rb->buf);
        rb->buf = new_buf;
        rb->size = new_size;
        rb->head = 0;
        rb->tail = rb->bytes;
    }

    size_t space_to_end = rb->size - rb->tail;
    if (len <= space_to_end) {
        memcpy(rb->buf + rb->tail, data, len);
        rb->tail += len;
        if (rb->tail == rb->size) rb->tail = 0;
    } else {
        memcpy(rb->buf + rb->tail, data, space_to_end);
        size_t rest = len - space_to_end;
        memcpy(rb->buf, data + space_to_end, rest);
        rb->tail = rest;
    }
    rb->bytes += len;
    return 0;
}

static ssize_t rb_send(int fd) {
    if (rb.bytes == 0) return 0;

    size_t avail;
    if (rb.head < rb.tail) {
        avail = rb.tail - rb.head;
    } else if (rb.head > rb.tail) {
        avail = rb.size - rb.head;
    } else {
        return 0;
    }
    if (avail > rb.bytes) avail = rb.bytes;

    ssize_t sent = send(fd, rb.buf + rb.head, avail, MSG_NOSIGNAL);
    if (sent > 0) {
        rb.head += sent;
        if (rb.head == rb.size) rb.head = 0;
        rb.bytes -= sent;
        sent_bytes += sent;
        return sent;
    } else if (sent < 0) {
        if (errno == EAGAIN || errno == EWOULDBLOCK) {
            return 0;
        }
        running = 0;
        return -1;
    } else {
        running = 0;
        return -1;
    }
}

static void flush_all(void) {
    while (rb.bytes > 0 && running) {
        ssize_t ret = rb_send(slave_sock);
        if (ret <= 0) break;
    }
}

/* ---------- 乱序队列 ---------- */
#if ENABLE_REORDER
static int ooo_find_seq(uint32_t seq) {
    for (int i = 0; i < OOO_MAX; i++) {
        if (ooo_pool[i].used && ooo_pool[i].seq == seq) return i;
    }
    return -1;
}

static int ooo_find_free(void) {
    for (int i = 0; i < OOO_MAX; i++) {
        if (!ooo_pool[i].used) return i;
    }
    return -1;
}

static void ooo_insert(uint32_t seq, const char *data, uint32_t len) {
    if (ooo_find_seq(seq) >= 0) return;
    int idx = ooo_find_free();
    if (idx < 0) {
        memset(ooo_pool, 0, sizeof(ooo_pool));
        seq_initialized = 0;
        return;
    }
    ooo_pool[idx].seq = seq;
    ooo_pool[idx].len = len;
    memcpy(ooo_pool[idx].data, data, len);
    ooo_pool[idx].used = 1;
    ooo_stored++;
}

static void ooo_flush(void) {
    int merged;
    do {
        merged = 0;
        for (int i = 0; i < OOO_MAX; i++) {
            if (ooo_pool[i].used && ooo_pool[i].seq == expected_seq) {
                rb_write(&rb, ooo_pool[i].data, ooo_pool[i].len);
                expected_seq += ooo_pool[i].len;
                ooo_pool[i].used = 0;
                ooo_merged++;
                merged = 1;
                break;
            }
        }
    } while (merged);
}
#endif

/* ---------- TCP 重组 ---------- */
static void process_tcp_payload(uint32_t seq, const char *data, uint32_t len) {
#if ENABLE_REORDER
    if (!seq_initialized) {
        expected_seq = seq;
        seq_initialized = 1;
    }

    if (seq == expected_seq) {
        rb_write(&rb, data, len);
        expected_seq += len;
        ooo_flush();
    } else if (SEQ_GT(seq, expected_seq)) {
        ooo_insert(seq, data, len);
    } else {
        uint32_t end = seq + len;
        if (SEQ_GT(end, expected_seq)) {
            uint32_t offset = expected_seq - seq;
            uint32_t new_len = end - expected_seq;
            rb_write(&rb, data + offset, new_len);
            expected_seq += new_len;
            ooo_flush();
        }
    }
#else
    (void)seq;
    rb_write(&rb, data, len);
#endif
}

/* ---------- eBPF 回调 ---------- */
static int handle_event(void *ctx, void *data, size_t data_sz) {
    (void)ctx; (void)data_sz;
    struct event_t *e = data;
    if (e->payload_len == 0 || e->payload_len > MAX_PAYLOAD_SIZE)
        return 0;

    recv_event++;
    recv_bytes += e->payload_len;

    process_tcp_payload(e->seq, e->payload, e->payload_len);
    return 0;
}

/* ---------- 信号处理 ---------- */
static void sig_handler(int sig) {
    (void)sig;
    running = 0;
}

int main(void) {
    signal(SIGINT, sig_handler);
    signal(SIGTERM, sig_handler);

    rb_init(&rb, RING_SIZE);

#if ENABLE_FORWARD
    slave_sock = socket(AF_INET, SOCK_STREAM, 0);
    struct sockaddr_in sa = {.sin_family = AF_INET, .sin_port = htons(SLAVE_PORT)};
    inet_pton(AF_INET, SLAVE_IP, &sa.sin_addr);
    if (connect(slave_sock, (struct sockaddr *)&sa, sizeof(sa)) < 0) {
        perror("connect");
        return 1;
    }

    int flags = fcntl(slave_sock, F_GETFL, 0);
    fcntl(slave_sock, F_SETFL, flags | O_NONBLOCK);
    int sndbuf = 8 * 1024 * 1024;
    setsockopt(slave_sock, SOL_SOCKET, SO_SNDBUF, &sndbuf, sizeof(sndbuf));
#endif

    int map_fd = bpf_obj_get("/sys/fs/bpf/payload_ringbuf");
    if (map_fd < 0) { perror("bpf_obj_get"); return 1; }

    struct ring_buffer *ringbuf = ring_buffer__new(map_fd, handle_event, NULL, NULL);
    if (!ringbuf) { fprintf(stderr, "ring_buffer__new failed\n"); return 1; }

    printf("[Relay] Ready\n");
    printf("[Mode] reorder=%d forward=%d\n", ENABLE_REORDER, ENABLE_FORWARD);

    while (running) {
        ring_buffer__poll(ringbuf, 100);

#if ENABLE_FORWARD
        flush_all();
#else
        /* 实验A/B：不转发，只消费数据 */
        rb.bytes = 0;
        rb.head = 0;
        rb.tail = 0;
#endif
    }

    printf("\n[Relay] Final Stats:\n");
    printf("  recv_event: %lu\n", recv_event);
    printf("  recv_bytes: %lu\n", recv_bytes);
#if ENABLE_FORWARD
    printf("  sent_bytes: %lu\n", sent_bytes);
#endif
#if ENABLE_REORDER
    printf("  ooo_stored: %lu\n", ooo_stored);
    printf("  ooo_merged: %lu\n", ooo_merged);
#endif
    printf("  rb_left:    %zu\n", rb.bytes);

    ring_buffer__free(ringbuf);
    close(map_fd);
#if ENABLE_FORWARD
    close(slave_sock);
#endif
    free(rb.buf);
    return 0;
}