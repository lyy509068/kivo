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
#include <time.h>
#include <signal.h>

#define SLAVE_IP "192.168.37.129" 
#define SLAVE_PORT 2000           
#define MAX_PAYLOAD_SIZE 16384
#define REBUF_SIZE (32 * 1024 * 1024)
#define TARGET_COMMANDS 100000

struct event_t {
    __u64 seq_num;
    __u32 cpu_id;
    __u32 payload_len;
    char payload[0];
};

struct pkt_entry {
    __u64 seq_num;
    __u32 cpu_id;
    int data_len;
    char *data;
    int processed;
};

int slave_sock = -1;
volatile int running = 1;
volatile int draining = 0;

static char *rebuf = NULL;
static int rebuf_len = 0;
static int rebuf_cap = REBUF_SIZE;

#define SORT_WINDOW 1024
static __u64 next_expected_seq = 0;
static struct pkt_entry *sort_buf = NULL;
static int sort_buf_size = 0;

// 统计信息
static __u64 total_events = 0;
static __u64 total_bytes_intercepted = 0;  // 内核拦截总字节数
static __u64 total_commands_sent = 0;
static __u64 total_commands_parsed = 0;
static __u64 total_write_cmds = 0;
static __u64 duplicate_seq = 0;
static __u64 out_of_order = 0;
static __u64 max_seq_seen = 0;
static __u64 ringbuf_drops = 0;
static __u64 total_kernel_drops = 0;
static __u64 partial_sends = 0;
static __u64 real_failures = 0;

static __u64 last_seen_seq[SORT_WINDOW];
static int target_reached = 0;
static time_t start_time;

void read_all_counters() {
    __u32 key = 0;
    __u64 value = 0;
    int fd;
    
    fd = bpf_obj_get("/sys/fs/bpf/ringbuf_drop_counter");
    if (fd >= 0) {
        if (bpf_map_lookup_elem(fd, &key, &value) == 0)
            ringbuf_drops = value;
        close(fd);
    }
    
    fd = bpf_obj_get("/sys/fs/bpf/drop_counter");
    if (fd >= 0) {
        if (bpf_map_lookup_elem(fd, &key, &value) == 0)
            total_kernel_drops = value;
        close(fd);
    }
    
    // 读取内核事件计数器（总处理包数）
    fd = bpf_obj_get("/sys/fs/bpf/event_counter");
    if (fd >= 0) {
        if (bpf_map_lookup_elem(fd, &key, &value) == 0) {
            // 内核计数，仅供参考
        }
        close(fd);
    }
}

void signal_handler(int signo) {
    if (signo == SIGINT || signo == SIGTERM) {
        if (!draining) {
            printf("\n[信号] 收到停止信号，进入排空模式...\n");
            running = 0;
        }
    }
}

int set_nonblocking(int fd) {
    int flags = fcntl(fd, F_GETFL, 0);
    if (flags < 0) return -1;
    return fcntl(fd, F_SETFL, flags | O_NONBLOCK);
}

int reorder_empty() {
    for (int i = 0; i < sort_buf_size; i++)
        if (!sort_buf[i].processed) return 0;
    return 1;
}

// 格式化字节数
static void format_bytes(__u64 bytes, char *buf, int buf_size) {
    if (bytes < 1024) {
        snprintf(buf, buf_size, "%llu B", (unsigned long long)bytes);
    } else if (bytes < 1024 * 1024) {
        snprintf(buf, buf_size, "%.2f KB", bytes / 1024.0);
    } else if (bytes < 1024 * 1024 * 1024) {
        snprintf(buf, buf_size, "%.2f MB", bytes / (1024.0 * 1024.0));
    } else {
        snprintf(buf, buf_size, "%.2f GB", bytes / (1024.0 * 1024.0 * 1024.0));
    }
}

void print_stats() {
    static time_t last_print = 0;
    time_t now = time(NULL);
    
    if (now - last_print >= 2) {
        char bytes_str[32];
        format_bytes(total_bytes_intercepted, bytes_str, sizeof(bytes_str));
        
        printf("\n");
        printf("┌─────────────────────────────────────────┐\n");
        printf("│        eBPF 中继统计                    │\n");
        printf("├─────────────────────────────────────────┤\n");
        printf("│ 内核拦截事件:     %10llu            │\n", (unsigned long long)total_events);
        printf("│ 内核拦截字节:     %10s            │\n", bytes_str);
        printf("│ 解析命令总数:     %10llu            │\n", (unsigned long long)total_commands_parsed);
        printf("│ 识别写命令:       %10llu            │\n", (unsigned long long)total_write_cmds);
        printf("│                                         │\n");
        printf("│ ✅ 已发送:        %10llu / %d     │\n", 
               (unsigned long long)total_commands_sent, TARGET_COMMANDS);
        printf("│ ⏸️  部分发送:     %10llu            │\n", (unsigned long long)partial_sends);
        printf("│ ❌ 发送失败:      %10llu            │\n", (unsigned long long)real_failures);
        printf("│                                         │\n");
        printf("│ RingBuf丢弃:      %10llu", (unsigned long long)ringbuf_drops);
        if (ringbuf_drops > 0) printf(" ⚠️"); else printf(" ✅");
        printf("           │\n");
        printf("│ 重复序列号:       %10llu            │\n", (unsigned long long)duplicate_seq);
        printf("│ 乱序:             %10llu            │\n", (unsigned long long)out_of_order);
        printf("│                                         │\n");
        printf("│ 缓冲区:           %10d / %d     │\n", rebuf_len, rebuf_cap);
        
        time_t elapsed = now - start_time;
        printf("│ 运行时间:         %10lld 秒        │\n", (long long)elapsed);
        printf("│ 状态: %-32s │\n", 
               draining ? "排空中" : target_reached ? "已达到目标" : "运行中");
        printf("└─────────────────────────────────────────┘\n");
        
        last_print = now;
    }
}

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

static int is_write_command(const char *cmd, int len) {
    if (len == 3) {
        if (strncmp(cmd, "SET", 3) == 0) return 1;
        if (strncmp(cmd, "MOD", 3) == 0) return 1;
        if (strncmp(cmd, "DEL", 3) == 0) return 1;
    }
    if (len == 4) {
        if (strncmp(cmd, "RSET", 4) == 0) return 1;
        if (strncmp(cmd, "HSET", 4) == 0) return 1;
        if (strncmp(cmd, "SSET", 4) == 0) return 1;
        if (strncmp(cmd, "RMOD", 4) == 0) return 1;
        if (strncmp(cmd, "HMOD", 4) == 0) return 1;
        if (strncmp(cmd, "SMOD", 4) == 0) return 1;
        if (strncmp(cmd, "RDEL", 4) == 0) return 1;
        if (strncmp(cmd, "HDEL", 4) == 0) return 1;
        if (strncmp(cmd, "SDEL", 4) == 0) return 1;
    }
    return 0;
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
    return is_write_command(p, len);
}

int init_sort_buffer() {
    sort_buf_size = SORT_WINDOW;
    sort_buf = (struct pkt_entry *)calloc(sort_buf_size, sizeof(struct pkt_entry));
    if (!sort_buf) return -1;
    
    for (int i = 0; i < SORT_WINDOW; i++)
        last_seen_seq[i] = (__u64)-1;
    
    for (int i = 0; i < sort_buf_size; i++) {
        sort_buf[i].processed = 1;
        sort_buf[i].data = (char *)malloc(MAX_PAYLOAD_SIZE);
        if (!sort_buf[i].data) return -1;
    }
    return 0;
}

void free_sort_buffer() {
    if (sort_buf) {
        for (int i = 0; i < sort_buf_size; i++)
            if (sort_buf[i].data) free(sort_buf[i].data);
        free(sort_buf);
    }
}

int insert_to_sort_buffer(__u64 seq, __u32 cpu_id, const char *data, int len) {
    int check_idx = seq % SORT_WINDOW;
    if (last_seen_seq[check_idx] == seq) {
        duplicate_seq++;
        return -2;
    }
    last_seen_seq[check_idx] = seq;
    
    if (seq < next_expected_seq) {
        out_of_order++;
        return -1;
    }
    
    int idx = seq % sort_buf_size;
    
    if (!sort_buf[idx].processed && sort_buf[idx].seq_num == seq) {
        duplicate_seq++;
        return -2;
    }
    
    if (!sort_buf[idx].processed) {
        memcpy(rebuf + rebuf_len, sort_buf[idx].data, sort_buf[idx].data_len);
        rebuf_len += sort_buf[idx].data_len;
        sort_buf[idx].processed = 1;
    }
    
    sort_buf[idx].seq_num = seq;
    sort_buf[idx].cpu_id = cpu_id;
    sort_buf[idx].data_len = len;
    memcpy(sort_buf[idx].data, data, len);
    sort_buf[idx].processed = 0;
    
    if (seq > max_seq_seen) max_seq_seen = seq;
    
    return 1;
}

int extract_ordered_data() {
    int extracted = 0;
    int idx = next_expected_seq % sort_buf_size;
    
    while (!sort_buf[idx].processed && sort_buf[idx].seq_num == next_expected_seq) {
        if (rebuf_len + sort_buf[idx].data_len > rebuf_cap) {
            rebuf_cap *= 2;
            char *new_buf = (char*)realloc(rebuf, rebuf_cap);
            if (!new_buf) break;
            rebuf = new_buf;
        }
        
        memcpy(rebuf + rebuf_len, sort_buf[idx].data, sort_buf[idx].data_len);
        rebuf_len += sort_buf[idx].data_len;
        sort_buf[idx].processed = 1;
        next_expected_seq++;
        extracted++;
        idx = next_expected_seq % sort_buf_size;
    }
    return extracted;
}

static int try_send(int fd, const char *buf, int len) {
    ssize_t sent = send(fd, buf, len, MSG_NOSIGNAL | MSG_DONTWAIT);
    if (sent < 0) {
        if (errno == EAGAIN || errno == EWOULDBLOCK) return 0;
        return -1;
    }
    return (int)sent;
}

void process_commands() {
    int max_iter = 500;
    int offset = 0;
    static __u64 last_progress_print = 0;
    
    while (rebuf_len > 0 && max_iter-- > 0) {
        // 安全检查：offset不能越界
        if (offset < 0 || offset >= rebuf_len) {
            offset = 0;
            rebuf_len = 0;
            break;
        }
        
        int remaining = rebuf_len - offset;
        if (remaining <= 0) break;
        
        int cmd_len = find_resp_cmd(rebuf + offset, remaining);
        
        if (cmd_len <= 0) {
            if (offset > 0 && offset < rebuf_len) {
                int move_len = rebuf_len - offset;
                memmove(rebuf, rebuf + offset, move_len);
                rebuf_len = move_len;
            }
            break;
        }

        // 安全检查：cmd_len不能超出剩余数据
        if (cmd_len > remaining) break;

        total_commands_parsed++;
        
        if (is_replication_command(rebuf + offset, cmd_len)) {
            total_write_cmds++;
            
            int sent = try_send(slave_sock, rebuf + offset, cmd_len);
            
            if (sent == cmd_len) {
                total_commands_sent++;
                offset += cmd_len;
                
                if (!target_reached && total_commands_sent >= TARGET_COMMANDS) {
                    target_reached = 1;
                    printf("\n🎯 [达到目标] 已发送 %llu 条命令！进入排空模式...\n", 
                           (unsigned long long)total_commands_sent);
                    running = 0;
                    draining = 1;
                }
                
                if (total_commands_sent - last_progress_print >= 10000) {
                    char bytes_str[32];
                    format_bytes(total_bytes_intercepted, bytes_str, sizeof(bytes_str));
                    time_t elapsed = time(NULL) - start_time;
                    
                    printf("[%6llu/%d] 已发送 | 内核拦截: %s | 事件: %llu | 耗时: %lld秒\n",
                           (unsigned long long)total_commands_sent, TARGET_COMMANDS,
                           bytes_str,
                           (unsigned long long)total_events,
                           (long long)elapsed);
                    last_progress_print = total_commands_sent;
                }
                
                // 定期压缩（带安全检查）
                if (offset > REBUF_SIZE / 2 && offset < rebuf_len) {
                    int move_len = rebuf_len - offset;
                    memmove(rebuf, rebuf + offset, move_len);
                    rebuf_len = move_len;
                    offset = 0;
                }
                
            } else if (sent > 0) {
                offset += sent;
                partial_sends++;
                // 部分发送后暂停，保留下次继续
                if (offset < rebuf_len) {
                    int move_len = rebuf_len - offset;
                    memmove(rebuf, rebuf + offset, move_len);
                    rebuf_len = move_len;
                    offset = 0;
                }
                break;
            } else if (sent == 0) {
                // EAGAIN，压缩后暂停
                if (offset > 0 && offset < rebuf_len) {
                    int move_len = rebuf_len - offset;
                    memmove(rebuf, rebuf + offset, move_len);
                    rebuf_len = move_len;
                    offset = 0;
                }
                break;
            } else {
                real_failures++;
                offset += cmd_len;
            }
        } else {
            offset += cmd_len;
        }
        
        // 定期压缩（带安全检查）
        if (offset > REBUF_SIZE / 2 && offset < rebuf_len) {
            int move_len = rebuf_len - offset;
            memmove(rebuf, rebuf + offset, move_len);
            rebuf_len = move_len;
            offset = 0;
        }
    }
    
    // 最终压缩（带安全检查）
    if (offset > 0 && offset < rebuf_len) {
        int move_len = rebuf_len - offset;
        memmove(rebuf, rebuf + offset, move_len);
        rebuf_len = move_len;
    } else if (offset >= rebuf_len) {
        rebuf_len = 0;
    }
}

static int handle_event(void *ctx, void *data, size_t data_sz) {
    if (data_sz < sizeof(__u64) + sizeof(__u32) + sizeof(__u32)) return 0;
    
    __u64 seq = *(__u64 *)data;
    __u32 cpu_id = *(__u32 *)(data + sizeof(__u64));
    __u32 payload_len = *(__u32 *)(data + sizeof(__u64) + sizeof(__u32));
    char *payload = data + sizeof(__u64) + sizeof(__u32) + sizeof(__u32);
    
    if (payload_len == 0 || payload_len > MAX_PAYLOAD_SIZE) return 0;
    
    total_events++;
    total_bytes_intercepted += payload_len;  // 累加拦截字节数
    
    int ret = insert_to_sort_buffer(seq, cpu_id, payload, payload_len);
    if (ret >= 0) {
        extract_ordered_data();
    }
    
    return 0;
}

int main(int argc, char **argv) {
    signal(SIGINT, signal_handler);
    signal(SIGTERM, signal_handler);
    
    start_time = time(NULL);
    
    rebuf = (char*)malloc(REBUF_SIZE);
    if (!rebuf) return 1;
    
    if (init_sort_buffer() < 0) { free(rebuf); return 1; }

    slave_sock = socket(AF_INET, SOCK_STREAM, 0);
    struct sockaddr_in slave_addr;
    memset(&slave_addr, 0, sizeof(slave_addr));
    slave_addr.sin_family = AF_INET;
    slave_addr.sin_port = htons(SLAVE_PORT);
    inet_pton(AF_INET, SLAVE_IP, &slave_addr.sin_addr);
    if (connect(slave_sock, (struct sockaddr *)&slave_addr, sizeof(slave_addr)) < 0) {
        perror("connect");
        free(rebuf); free_sort_buffer();
        return 1;
    }
    set_nonblocking(slave_sock);

    int sndbuf = 8 * 1024 * 1024;
    setsockopt(slave_sock, SOL_SOCKET, SO_SNDBUF, &sndbuf, sizeof(sndbuf));

    int ringbuf_fd = bpf_obj_get("/sys/fs/bpf/payload_ringbuf");
    if (ringbuf_fd < 0) {
        perror("bpf_obj_get ringbuf failed");
        free(rebuf); free_sort_buffer(); close(slave_sock);
        return 1;
    }

    struct ring_buffer *rb = ring_buffer__new(ringbuf_fd, handle_event, NULL, NULL);
    if (!rb) {
        fprintf(stderr, "ring_buffer__new 失败\n");
        free(rebuf); free_sort_buffer();
        close(ringbuf_fd); close(slave_sock);
        return 1;
    }

    printf("┌─────────────────────────────────────────┐\n");
    printf("│     eBPF 中继已启动                     │\n");
    printf("│     目标: %d 条命令                     │\n", TARGET_COMMANDS);
    printf("│     RingBuf: 64MB                       │\n");
    printf("│     Ctrl+C 优雅退出                     │\n");
    printf("└─────────────────────────────────────────┘\n\n");

    // 添加超时检测
    time_t last_event_time = time(NULL);
    
    while (1) {
        int poll_ret = ring_buffer__poll(rb, 10);
        
        if (poll_ret > 0) {
            last_event_time = time(NULL);
        }
        
        process_commands();
        read_all_counters();
        
        // 30秒无新事件自动退出
        if (!draining && total_events > 0) {
            if (time(NULL) - last_event_time > 30) {
                printf("\n⏰ [超时] 30秒无新数据，自动排空...\n");
                running = 0;
            }
        }
        
        if (!running && !draining) draining = 1;
        
        if (draining) {
            if (rebuf_len == 0 && reorder_empty()) {
                ring_buffer__poll(rb, 100);
                process_commands();
                read_all_counters();
                
                if (rebuf_len == 0 && reorder_empty()) break;
            }
        }
        
        print_stats();
    }

    // 最终报告
    time_t elapsed = time(NULL) - start_time;
    char bytes_str[32];
    format_bytes(total_bytes_intercepted, bytes_str, sizeof(bytes_str));
    
    printf("\n");
    printf("╔══════════════════════════════════════════╗\n");
    printf("║           最终报告                      ║\n");
    printf("╠══════════════════════════════════════════╣\n");
    printf("║ 内核拦截事件:     %10llu          ║\n", (unsigned long long)total_events);
    printf("║ 内核拦截字节:     %10s          ║\n", bytes_str);
    printf("║ 解析命令总数:     %10llu          ║\n", (unsigned long long)total_commands_parsed);
    printf("║ 识别写命令:       %10llu          ║\n", (unsigned long long)total_write_cmds);
    printf("║ ✅ 已发送:        %10llu          ║\n", (unsigned long long)total_commands_sent);
    printf("║ ⏸️  部分发送:     %10llu          ║\n", (unsigned long long)partial_sends);
    printf("║ ❌ 发送失败:      %10llu          ║\n", (unsigned long long)real_failures);
    printf("╠══════════════════════════════════════════╣\n");
    printf("║ RingBuf丢弃:      %10llu          ║\n", (unsigned long long)ringbuf_drops);
    printf("║ 重复序列号:       %10llu          ║\n", (unsigned long long)duplicate_seq);
    printf("║ 乱序:             %10llu          ║\n", (unsigned long long)out_of_order);
    printf("║ 总耗时:           %10lld 秒       ║\n", (long long)elapsed);
    if (elapsed > 0)
        printf("║ 平均QPS:          %10.0f          ║\n", 
               (double)total_commands_sent / elapsed);
    printf("╚══════════════════════════════════════════╝\n");

    ring_buffer__free(rb);
    close(ringbuf_fd);
    close(slave_sock);
    free(rebuf);
    free_sort_buffer();
    
    return 0;
}