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

#define SLAVE_IP "192.168.88.130"
#define SLAVE_PORT 2000
#define MAX_PAYLOAD_SIZE 16384
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
    size_t head;   // 待发送数据起始
    size_t tail;   // 待写入位置
    size_t bytes;  // 有效字节数
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
        // 扩容：将旧数据整理到新缓冲区开头
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

    // 写入，处理跨尾部
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

/* 从环形缓冲区发送尽可能多的数据，返回发送的字节数（可能为0） */
static ssize_t rb_send(int fd) {
    if (rb.bytes == 0) return 0;

    // 计算当前连续可发送的最大长度
    size_t avail;
    if (rb.head < rb.tail) {
        avail = rb.tail - rb.head;
    } else if (rb.head > rb.tail) {
        avail = rb.size - rb.head;
    } else {
        // bytes 不为0，但 head==tail，不可能出现，除非 size 不对
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
            return 0;  // 缓冲区满，下次再发
        }
        running = 0;
        return -1;
    } else {
        running = 0;
        return -1;
    }
}

/* 批量发送所有待发送数据，直到 EAGAIN 或发送完毕 */
static void flush_all(void) {
    while (rb.bytes > 0 && running) {
        ssize_t ret = rb_send(slave_sock);
        if (ret <= 0) break;
    }
}

/* ---------- 乱序队列 ---------- */
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
        // 清空重新同步
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

/* ---------- TCP 重组 ---------- */
static void process_tcp_payload(uint32_t seq, const char *data, uint32_t len) {
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
}

/* ---------- eBPF 回调 ---------- */
static int handle_event(void *ctx, void *data, size_t data_sz) {
    (void)ctx; (void)data_sz;
    struct event_t *e = data;
    if (e->payload_len == 0 || e->payload_len > MAX_PAYLOAD_SIZE) return 0;

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

    int map_fd = bpf_obj_get("/sys/fs/bpf/payload_ringbuf");
    if (map_fd < 0) { perror("bpf_obj_get"); return 1; }

    struct ring_buffer *ringbuf = ring_buffer__new(map_fd, handle_event, NULL, NULL);
    if (!ringbuf) { fprintf(stderr, "ring_buffer__new failed\n"); return 1; }

    printf("[Relay] Ready\n");

    while (running) {
        // 消费一批事件（内部会多次调用 handle_event）
        ring_buffer__poll(ringbuf, 100);
        // 批量发送所有重组好的数据
        flush_all();
    }

    // 退出前尝试发送剩余数据
    while (rb.bytes > 0 && running) {
        flush_all();
    }

    printf("\n[Relay] Final Stats:\n");
    printf("  recv_event: %lu\n", recv_event);
    printf("  recv_bytes: %lu\n", recv_bytes);
    printf("  sent_bytes: %lu\n", sent_bytes);
    printf("  ooo_stored: %lu\n", ooo_stored);
    printf("  ooo_merged: %lu\n", ooo_merged);
    printf("  rb_left:    %zu\n", rb.bytes);

    ring_buffer__free(ringbuf);
    close(map_fd);
    close(slave_sock);
    free(rb.buf);
    return 0;
}