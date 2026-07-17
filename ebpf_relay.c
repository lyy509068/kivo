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
#define MAX_PAYLOAD_SIZE 1024

struct event_t {
    char payload[MAX_PAYLOAD_SIZE];
    __u32 payload_len;
};


// 用户态背压缓冲区：用于暂存因从端阻塞而发送失败的数据
struct {
    struct event_t ev;
    __u32 sent_bytes; // 该网络包已经成功发送了多少字节
} pending_ctx = {0};

int slave_sock = -1;
int has_pending_data = 0; // 是否存在积压数据的标志位

// 设置 Socket 为非阻塞模式
int set_nonblocking(int fd) {
    int flags = fcntl(fd, F_GETFL, 0);
    if (flags < 0) return -1;
    return fcntl(fd, F_SETFL, flags | O_NONBLOCK);
}

static int handle_event(void *ctx, void *data, size_t data_sz) {
    struct event_t *e = data;

    if (slave_sock <= 0 || e->payload_len == 0) {
        return 0;
    }

    // 尝试非阻塞发送
    ssize_t sent = send(slave_sock, e->payload, e->payload_len, 0);
    
    if (sent < 0) {
        // 情况 A：从端缓冲区满了，网络发生拥堵 (EAGAIN / EWOULDBLOCK)
        if (errno == EAGAIN || errno == EWOULDBLOCK) {
            // 触发应用层背压：整包保存到暂存区
            memcpy(&pending_ctx.ev, e, sizeof(struct event_t));
            pending_ctx.sent_bytes = 0;
            has_pending_data = 1;
            
            // 关键：返回负数会终止当前的 ring_buffer__poll，不再从内核读新数据
            return -EAGAIN; 
        }
        // 情况 B：其他网络致命错误（如从端真的断开了）
        perror("[eBPF Relay] Send failed with fatal error");
        return -1;
    } 
    else if (sent < e->payload_len) {
        // 情况 C：TCP 缓冲区未满但不够大，只发出去了一部分数据 (Partial Send)
        memcpy(&pending_ctx.ev, e, sizeof(struct event_t));
        pending_ctx.sent_bytes = sent;
        has_pending_data = 1;
        
        return -EAGAIN; // 同样中止内核轮询
    }

    return 0;
}

int main(int argc, char **argv) {
    printf("[eBPF Relay] Starting standalone high-performance forwarder...\n");

    slave_sock = socket(AF_INET, SOCK_STREAM, 0);
    struct sockaddr_in slave_addr;
    memset(&slave_addr, 0, sizeof(slave_addr));
    slave_addr.sin_family = AF_INET;
    slave_addr.sin_port = htons(SLAVE_PORT);
    inet_pton(AF_INET, SLAVE_IP, &slave_addr.sin_addr);

    if (connect(slave_sock, (struct sockaddr *)&slave_addr, sizeof(slave_addr)) < 0) {
        perror("[eBPF Relay] Failed to connect to Slave");
        return 1;
    }
    
    // 成功建立连接后，立刻将其设为非阻塞模式
    if (set_nonblocking(slave_sock) < 0) {
        perror("[eBPF Relay] Failed to set non-blocking mode");
        close(slave_sock);
        return 1;
    }
    printf("[eBPF Relay] TCP connection to Slave established (Non-blocking mode).\n");

    int map_fd = bpf_obj_get("/sys/fs/bpf/payload_ringbuf");
    if (map_fd < 0) {
        fprintf(stderr, "[eBPF Relay] Error: Kernel map not found.\n");
        close(slave_sock);
        return 1;
    }

    struct ring_buffer *rb = ring_buffer__new(map_fd, handle_event, NULL, NULL);
    if (!rb) {
        fprintf(stderr, "Failed to create ring buffer\n");
        close(map_fd);
        close(slave_sock);
        return 1;
    }

    printf("[eBPF Relay] Listening and ready with Backpressure control...\n");

    // 核心状态机循环
    while (1) {
        if (!has_pending_data) {
            // 正常轮询内核 Ring Buffer 超时时间设为 10ms，提高系统对网络事件的响应灵敏度
            int err = ring_buffer__poll(rb, 10); 
            if (err == -EINTR) {
                break;
            }
            if (err < 0 && err != -EAGAIN) { 
                // 如果是因为 -EAGAIN 退出，属于预期的背压触发，不用报错
                fprintf(stderr, "Error polling ring buffer: %d\n", err);
                break;
            }
        } 
        else {
            // 背压触发！从端堵住了，有积压数据未发完，转为使用 poll() 监听从端 Socket 的可写事件
            struct pollfd pfd = {
                .fd = slave_sock,
                .events = POLLOUT, // 关注可写事件
            };

            // 阻塞等待 50ms 看从端缓冲区是否释放出空间
            int ret = poll(&pfd, 1, 50); 
            if (ret < 0) {
                if (errno == EINTR) continue;
                perror("Poll failed");
                break;
            }

            if (ret > 0 && (pfd.revents & POLLOUT)) {
                // 从端 Socket 重新可写了！尝试把剩余的数据赶快送走
                char *data_ptr = pending_ctx.ev.payload + pending_ctx.sent_bytes;
                __u32 to_send = pending_ctx.ev.payload_len - pending_ctx.sent_bytes;

                ssize_t sent = send(slave_sock, data_ptr, to_send, 0);
                if (sent > 0) {
                    pending_ctx.sent_bytes += sent;
                    // 检查这一包是否全部发完了
                    if (pending_ctx.sent_bytes >= pending_ctx.ev.payload_len) {
                        has_pending_data = 0; // 解除背压状态
                        printf("[eBPF Relay] Backpressure cleared. Resuming kernel poll.\n");
                    }
                } 
                else if (sent < 0) {
                    if (errno != EAGAIN && errno != EWOULDBLOCK) {
                        perror("[eBPF Relay] Connection lost during pending data flush");
                        break;
                    }
                    // 如果依然是 EAGAIN，说明从端吞吐依然很慢，不做任何处理，下一轮循环继续 poll 监听
                }
            }
        }
    }

    ring_buffer__free(rb);
    close(map_fd);
    close(slave_sock);
    printf("[eBPF Relay] Stopped.\n");
    return 0;
}