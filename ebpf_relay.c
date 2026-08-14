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

#define SLAVE_IP "192.168.37.129"
#define SLAVE_PORT 2000
#define MAX_PAYLOAD_SIZE 16384
#define REBUF_SIZE (64 * 1024 * 1024)
#define OOO_MAX 512   // 最大乱序缓存节点数

// TCP 序列号比较宏
#define SEQ_GT(a,b)  ((int32_t)((a)-(b)) > 0)
#define SEQ_GEQ(a,b) ((int32_t)((a)-(b)) >= 0)

// eBPF 传递的事件结构（与内核侧一致）
struct event_t {
    uint32_t src_ip;
    uint16_t src_port;
    uint32_t seq;
    uint32_t payload_len;
    char payload[0];
};

// 乱序包节点
struct ooo_node {
    uint32_t seq;
    uint32_t len;
    char data[MAX_PAYLOAD_SIZE];
    struct ooo_node *next;
};

// 全局状态
int slave_sock = -1;
volatile int running = 1;

// 待发送的连续字节流
static char *rebuf = NULL;
static int rebuf_len = 0;
static int rebuf_cap = REBUF_SIZE;

// TCP 重组状态
static uint32_t expected_seq = 0;
static int seq_initialized = 0;
static struct ooo_node *ooo_head = NULL;
static int ooo_cnt = 0;

// 统计
static volatile uint64_t recv_event   = 0;
static volatile uint64_t recv_bytes   = 0;
static volatile uint64_t sent_bytes   = 0;
static volatile uint64_t ooo_stored   = 0;
static volatile uint64_t ooo_merged   = 0;

// ------------------ 发送辅助 ------------------
int set_nonblocking(int fd) {
    int flags = fcntl(fd, F_GETFL, 0);
    if (flags < 0) return -1;
    return fcntl(fd, F_SETFL, flags | O_NONBLOCK);
}

// 尝试将 rebuf 中的数据通过 TCP 发送给从机，发不出去就保留
static void flush_rebuf(void) {
    while (rebuf_len > 0 && running) {
        ssize_t sent = send(slave_sock, rebuf, rebuf_len, MSG_NOSIGNAL);
        if (sent > 0) {
            sent_bytes += sent;
            memmove(rebuf, rebuf + sent, rebuf_len - sent);
            rebuf_len -= sent;
        } else if (sent < 0) {
            if (errno == EAGAIN || errno == EWOULDBLOCK) {
                break;  // 发送缓冲区满，等下次
            } else {
                fprintf(stderr, "[Relay] send error: %s\n", strerror(errno));
                running = 0;
                break;
            }
        } else {
            // sent == 0，连接关闭
            fprintf(stderr, "[Relay] slave closed connection\n");
            running = 0;
            break;
        }
    }
}

// 将数据追加到 rebuf
static void rebuf_append(const char *data, int len) {
    if (rebuf_len + len > rebuf_cap) {
        rebuf_cap *= 2;
        char *new_buf = realloc(rebuf, rebuf_cap);
        if (!new_buf) {
            fprintf(stderr, "[Relay] realloc failed\n");
            running = 0;
            return;
        }
        rebuf = new_buf;
    }
    memcpy(rebuf + rebuf_len, data, len);
    rebuf_len += len;
}

// ------------------ 乱序队列管理 ------------------
static void ooo_flush(void) {
    int merged;
    do {
        merged = 0;
        struct ooo_node *prev = NULL;
        struct ooo_node *cur = ooo_head;
        while (cur) {
            if (cur->seq == expected_seq) {
                // 接到预期序列号，追加并发送
                rebuf_append(cur->data, cur->len);
                expected_seq += cur->len;
                ooo_merged++;

                if (prev) prev->next = cur->next;
                else ooo_head = cur->next;
                struct ooo_node *tmp = cur;
                cur = cur->next;
                free(tmp);
                ooo_cnt--;
                merged = 1;

                // 追加后立即尝试发送
                flush_rebuf();
            } else {
                prev = cur;
                cur = cur->next;
            }
        }
    } while (merged);
}

static void ooo_insert(uint32_t seq, const char *data, uint32_t len) {
    // 去重
    for (struct ooo_node *cur = ooo_head; cur; cur = cur->next) {
        if (cur->seq == seq) return;
    }

    // 如果乱序队列过大，清空并重新同步
    if (ooo_cnt >= OOO_MAX) {
        fprintf(stderr, "[Relay] OOO queue overflow, resyncing\n");
        while (ooo_head) {
            struct ooo_node *tmp = ooo_head;
            ooo_head = ooo_head->next;
            free(tmp);
        }
        ooo_cnt = 0;
        seq_initialized = 0;
        return;
    }

    struct ooo_node *node = calloc(1, sizeof(*node));
    node->seq = seq;
    node->len = len;
    memcpy(node->data, data, len);

    // 按 seq 升序插入
    struct ooo_node *prev = NULL;
    struct ooo_node *cur = ooo_head;
    while (cur && SEQ_GEQ(seq, cur->seq)) {
        prev = cur;
        cur = cur->next;
    }
    node->next = cur;
    if (prev) prev->next = node;
    else ooo_head = node;
    ooo_cnt++;
    ooo_stored++;
}

// ------------------ TCP 重组处理 ------------------
static void process_tcp_payload(uint32_t seq, const char *data, uint32_t len) {
    if (!seq_initialized) {
        // 第一个包，直接作为起始序列号
        expected_seq = seq;
        seq_initialized = 1;
    }

    if (seq == expected_seq) {
        // 顺序正确，直接追加并发送
        rebuf_append(data, len);
        expected_seq += len;
        flush_rebuf();
        // 检查乱序队列是否有后续连续数据
        ooo_flush();
    } else if (SEQ_GT(seq, expected_seq)) {
        // 未来包，存入乱序队列
        ooo_insert(seq, data, len);
    } else {
        // 旧包或重叠包
        uint32_t end = seq + len;
        if (SEQ_GT(end, expected_seq)) {
            // 有部分新数据
            uint32_t offset = expected_seq - seq;
            uint32_t new_len = end - expected_seq;
            rebuf_append(data + offset, new_len);
            expected_seq += new_len;
            flush_rebuf();
            ooo_flush();
        }
        // 纯旧数据，忽略
    }
}

// ------------------ eBPF 事件回调 ------------------
static int handle_event(void *ctx, void *data, size_t data_sz) {
    struct event_t *e = (struct event_t *)data;
    if (e->payload_len == 0 || e->payload_len > MAX_PAYLOAD_SIZE) return 0;

    recv_event++;
    recv_bytes += e->payload_len;

    process_tcp_payload(e->seq, e->payload, e->payload_len);
    return 0;
}

// ------------------ 信号处理 ------------------
static void sig_handler(int sig) {
    running = 0;
}

// ------------------ 主函数 ------------------
int main(int argc, char **argv) {
    signal(SIGINT, sig_handler);
    signal(SIGTERM, sig_handler);

    rebuf = malloc(REBUF_SIZE);
    if (!rebuf) {
        perror("malloc rebuf");
        return 1;
    }

    printf("[Relay] Connecting to slave %s:%d...\n", SLAVE_IP, SLAVE_PORT);
    slave_sock = socket(AF_INET, SOCK_STREAM, 0);
    struct sockaddr_in sa = {.sin_family = AF_INET, .sin_port = htons(SLAVE_PORT)};
    inet_pton(AF_INET, SLAVE_IP, &sa.sin_addr);
    if (connect(slave_sock, (struct sockaddr *)&sa, sizeof(sa)) < 0) {
        perror("connect");
        free(rebuf);
        return 1;
    }
    set_nonblocking(slave_sock);
    int sndbuf = 8 * 1024 * 1024;
    setsockopt(slave_sock, SOL_SOCKET, SO_SNDBUF, &sndbuf, sizeof(sndbuf));

    int map_fd = bpf_obj_get("/sys/fs/bpf/payload_ringbuf");
    if (map_fd < 0) {
        perror("bpf_obj_get");
        close(slave_sock);
        free(rebuf);
        return 1;
    }

    struct ring_buffer *rb = ring_buffer__new(map_fd, handle_event, NULL, NULL);
    if (!rb) {
        fprintf(stderr, "ring_buffer__new failed\n");
        close(map_fd);
        close(slave_sock);
        free(rebuf);
        return 1;
    }

    printf("[Relay] Ready, polling events...\n");

    // 主循环：先发送待发数据，再 poll 新事件
    while (running) {
        // 有数据就尝试发送
        if (rebuf_len > 0) {
            flush_rebuf();
        }

        // poll eBPF 事件；如果有待发数据，使用短超时避免阻塞发送
        int timeout = rebuf_len > 0 ? 1 : 100;
        ring_buffer__poll(rb, timeout);
    }

    printf("\n[Relay] Final Stats:\n");
    printf("  recv_event: %lu\n", recv_event);
    printf("  recv_bytes: %lu\n", recv_bytes);
    printf("  sent_bytes: %lu\n", sent_bytes);
    printf("  ooo_stored: %lu\n", ooo_stored);
    printf("  ooo_merged: %lu\n", ooo_merged);
    printf("  rebuf_left: %d\n", rebuf_len);
    printf("  ooo_left:   %d\n", ooo_cnt);

    // 清理
    ring_buffer__free(rb);
    close(map_fd);
    close(slave_sock);
    free(rebuf);
    while (ooo_head) {
        struct ooo_node *tmp = ooo_head;
        ooo_head = ooo_head->next;
        free(tmp);
    }
    return 0;
}