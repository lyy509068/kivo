#define _GNU_SOURCE
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <bpf/libbpf.h>
#include <bpf/bpf.h>
#include <errno.h>
#include <fcntl.h>   
#include <poll.h>    

#define SLAVE_IP "192.168.37.129" 
#define SLAVE_PORT 2000           
#define MAX_PAYLOAD_SIZE 16384
#define REBUF_SIZE (32 * 1024 * 1024) 

struct event_t {
    __u32 payload_len;
    char payload[0];
};

int slave_sock = -1;
volatile int running = 1;

static char *rebuf = NULL;
static int rebuf_len = 0;
static int rebuf_cap = REBUF_SIZE;

int set_nonblocking(int fd) {
    int flags = fcntl(fd, F_GETFL, 0);
    if (flags < 0) return -1;
    return fcntl(fd, F_SETFL, flags | O_NONBLOCK);
}

// 解析完整 RESP 命令长度
static int find_resp_cmd(const char *buf, int len) {
    if (len < 4 || buf[0] != '*') return 0;
    const char *p = buf;
    const char *end = buf + len;

    const char *crlf = memmem(p, end - p, "\r\n", 2);
    if (!crlf) return 0;
    int argc = atoi(p + 1);
    p = crlf + 2;

    for (int i = 0; i < argc; i++) {
        if (p >= end || *p != '$') return 0;
        crlf = memmem(p, end - p, "\r\n", 2);
        if (!crlf) return 0;
        int arg_len = atoi(p + 1);
        p = crlf + 2;
        if (p + arg_len + 2 > end) return 0;
        if (p[arg_len] != '\r' || p[arg_len+1] != '\n') return 0;
        p += arg_len + 2;
    }
    return (int)(p - buf);
}

static int is_replication_command(const char *cmd_buf, int cmd_len) {
    if (cmd_len < 10) return 0;
    const char *p = cmd_buf + 1;
    const char *crlf = memmem(p, cmd_len - (p - cmd_buf), "\r\n", 2);
    if (!crlf) return 0;
    p = crlf + 2;
    
    if (*p != '$') return 0;
    crlf = memmem(p, cmd_len - (p - cmd_buf), "\r\n", 2);
    if (!crlf) return 0;
    int len = atoi(p + 1);
    p = crlf + 2;
    
    if (p + len > cmd_buf + cmd_len) return 0;
    if (len == 3 && strncmp(p, "SET", 3) == 0) return 1;
    if (len == 4) {
        if (strncmp(p, "RSET", 4) == 0) return 1;
        if (strncmp(p, "HSET", 4) == 0) return 1;
        if (strncmp(p, "SSET", 4) == 0) return 1;
    }
    return 0;
}

// 非阻塞网络发送函数
static int send_all_nonblock(int fd, const char *buf, int len) {
    int total_sent = 0;
    while (total_sent < len) {
        ssize_t sent = send(fd, buf + total_sent, len - total_sent, MSG_NOSIGNAL);
        if (sent < 0) {
            if (errno == EAGAIN || errno == EWOULDBLOCK) {
                break; // Socket 发送缓冲区满，返回已发送字节数
            }
            return -1; // 出错
        }
        total_sent += sent;
    }
    return total_sent;
}

// 直接将动态 Ring Buffer 数据存入重组缓冲区，减少中间队列开销
static int handle_event(void *ctx, void *data, size_t data_sz) {
    struct event_t *e = (struct event_t *)data;
    
    if (e->payload_len == 0 || e->payload_len > MAX_PAYLOAD_SIZE) {
        return 0;
    }

    if (rebuf_len + e->payload_len > rebuf_cap) {
        rebuf_cap *= 2;
        char *new_buf = (char*)realloc(rebuf, rebuf_cap);
        if (!new_buf) {
            fprintf(stderr, "[eBPF Relay] Out of memory!\n");
            return 0;
        }
        rebuf = new_buf;
    }

    memcpy(rebuf + rebuf_len, e->payload, e->payload_len);
    rebuf_len += e->payload_len;
    return 0;
}

int main(int argc, char **argv) {
    rebuf = (char*)malloc(REBUF_SIZE);
    if (!rebuf) return 1;

    printf("[eBPF Relay] Start transport\n");

    slave_sock = socket(AF_INET, SOCK_STREAM, 0);
    struct sockaddr_in slave_addr;
    memset(&slave_addr, 0, sizeof(slave_addr));
    slave_addr.sin_family = AF_INET;
    slave_addr.sin_port = htons(SLAVE_PORT);
    inet_pton(AF_INET, SLAVE_IP, &slave_addr.sin_addr);
    if (connect(slave_sock, (struct sockaddr *)&slave_addr, sizeof(slave_addr)) < 0) {
        perror("connect");
        free(rebuf);
        return 1;
    }
    set_nonblocking(slave_sock);

    int sndbuf = 8 * 1024 * 1024;
    setsockopt(slave_sock, SOL_SOCKET, SO_SNDBUF, &sndbuf, sizeof(sndbuf));

    int map_fd = bpf_obj_get("/sys/fs/bpf/payload_ringbuf");
    if (map_fd < 0) {
        perror("bpf_obj_get map failed");
        free(rebuf);
        close(slave_sock);
        return 1;
    }

    struct ring_buffer *rb = ring_buffer__new(map_fd, handle_event, NULL, NULL);
    if (!rb) {
        fprintf(stderr, "ring_buffer__new failed\n");
        free(rebuf);
        close(map_fd);
        close(slave_sock);
        return 1;
    }

    while (running) {
        // 1. 优先拉取内核 RingBuffer 数据填充 rebuf
        ring_buffer__poll(rb, 1);

        // 2. 解析并尽可能多地发送完整 RESP 指令
        while (rebuf_len > 0) {
            int cmd_len = find_resp_cmd(rebuf, rebuf_len);
            
            // 没找到完整命令
            if (cmd_len <= 0) {
                // 如果积压数据太大且没有合法 RESP 头，尝试自动容错修补错位
                if (rebuf_len > MAX_PAYLOAD_SIZE * 4) {
                    int next_star = -1;
                    for (int i = 1; i < rebuf_len; i++) {
                        if (rebuf[i] == '*') {
                            next_star = i;
                            break;
                        }
                    }
                    if (next_star > 0) {
                        memmove(rebuf, rebuf + next_star, rebuf_len - next_star);
                        rebuf_len -= next_star;
                        continue;
                    }
                }
                break;
            }

            // 找到了完整 RESP 指令
            if (is_replication_command(rebuf, cmd_len)) {
                int sent = send_all_nonblock(slave_sock, rebuf, cmd_len);
                if (sent < cmd_len) {
                    // 如果只发了一部分（缓冲区满），保留未发完的部分，跳出循环等待下次发送
                    if (sent > 0) {
                        memmove(rebuf, rebuf + sent, rebuf_len - sent);
                        rebuf_len -= sent;
                    }
                    usleep(100);
                    break; 
                }
                usleep(2);
            }

            // 发送完毕或非复制指令，从 rebuf 中安全移除
            memmove(rebuf, rebuf + cmd_len, rebuf_len - cmd_len);
            rebuf_len -= cmd_len;
        }
    }

    ring_buffer__free(rb);
    close(map_fd);
    close(slave_sock);
    free(rebuf);
    return 0;
}