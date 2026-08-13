#define _GNU_SOURCE
#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <unistd.h>
#include <arpa/inet.h>
#include <sys/socket.h>
#include <sys/time.h>
#include <errno.h>

#define SERVER_PORT 2000
#define TOTAL_RECORDS 1000000
#define RECV_BUF_SIZE (2 * 1024 * 1024)
#define SEND_BUF_SIZE (64 * 1024)

#define ARRAY_RECORDS 1
#define RBTREE_RECORDS 333333
#define HASH_RECORDS 333333
#define SKIPLIST_RECORDS 333333

const char* SET_CMDS[] = {"", "SET", "RSET", "HSET", "SSET"};
const char* ENGINE_NAMES[] = {"", "Array", "RBTree", "Hash", "SkipList"};
const int ENGINE_RECORDS[] = {0, ARRAY_RECORDS, RBTREE_RECORDS, HASH_RECORDS, SKIPLIST_RECORDS};

typedef struct {
    double start_time;
    double end_time;
    long total_sent;
    long total_acked;
    int save_count;
    double last_save_time_ms;
} benchmark_stats_t;

static benchmark_stats_t stats = {0};

double get_time_sec() {
    struct timeval tv;
    gettimeofday(&tv, NULL);
    return tv.tv_sec + (double)tv.tv_usec / 1000000.0;
}

// 流式RESP解析器：解析+OK\r\n回复
int parse_resp_replies(const char *buf, int len, int *parsed_bytes) {
    int count = 0;
    int ptr = 0;

    while (ptr < len) {
        char type = buf[ptr];
        
        if (type == '+' || type == '-' || type == ':') {
            char *crlf = memmem(buf + ptr, len - ptr, "\r\n", 2);
            if (!crlf) break;
            ptr = (crlf - buf) + 2;
            count++;
        } else if (type == '$') {
            char *crlf1 = memmem(buf + ptr, len - ptr, "\r\n", 2);
            if (!crlf1) break;
            int str_len = atoi(buf + ptr + 1);
            if (str_len == -1) {
                ptr = (crlf1 - buf) + 2;
            } else {
                int total_len = (crlf1 - buf) + 2 + str_len + 2;
                if (ptr + total_len > len) break;
                ptr += total_len;
            }
            count++;
        } else {
            break;
        }
    }

    *parsed_bytes = ptr;
    return count;
}

// 构建SET命令
int build_set_cmd(char *buf, int engine_type, int global_index) {
    const char *cmd = SET_CMDS[engine_type];
    const char *engine_name = ENGINE_NAMES[engine_type];
    
    char key[32], val[64];
    sprintf(key, "key_%07d", global_index);
    sprintf(val, "val_%07d_%s", global_index, engine_name);
    
    return sprintf(buf, "*3\r\n$%zu\r\n%s\r\n$%zu\r\n%s\r\n$%zu\r\n%s\r\n",
                   strlen(cmd), cmd, strlen(key), key, strlen(val), val);
}

// 构建SAVE命令
int build_save_cmd(char *buf) {
    return sprintf(buf, "*1\r\n$4\r\nSAVE\r\n");
}

int connect_server() {
    int sock = socket(AF_INET, SOCK_STREAM, 0);
    if (sock < 0) return -1;
    
    struct sockaddr_in addr;
    addr.sin_family = AF_INET;
    addr.sin_port = htons(SERVER_PORT);
    inet_pton(AF_INET, "127.0.0.1", &addr.sin_addr);

    if (connect(sock, (struct sockaddr*)&addr, sizeof(addr)) == 0) {
        return sock;
    }
    close(sock);
    return -1;
}

// 发送并等待指定数量的响应（用于SAVE命令同步）
void send_and_wait_ack(int sock, char *send_buf, int send_len, 
                       char *recv_buf, int *recv_len, int expect_acks) {
    // 先发送
    send(sock, send_buf, send_len, 0);
    
    // 等待接收expect_acks个响应
    int acked = 0;
    while (acked < expect_acks) {
        int n = recv(sock, recv_buf + *recv_len, RECV_BUF_SIZE - *recv_len, 0);
        if (n <= 0) break;
        *recv_len += n;
        
        int parsed_bytes = 0;
        int count = parse_resp_replies(recv_buf, *recv_len, &parsed_bytes);
        acked += count;
        stats.total_acked += count;
        
        if (parsed_bytes > 0) {
            int remain = *recv_len - parsed_bytes;
            if (remain > 0) memmove(recv_buf, recv_buf + parsed_bytes, remain);
            *recv_len = remain;
        }
    }
}

// 执行SAVE命令
int execute_save(int sock, char *send_buf, char *recv_buf, int *recv_len) {
    double save_start = get_time_sec();
    
    // 构建SAVE命令
    int save_len = build_save_cmd(send_buf);
    
    // SAVE只有1个响应(+OK\r\n)
    send_and_wait_ack(sock, send_buf, save_len, recv_buf, recv_len, 1);
    
    stats.save_count++;
    stats.last_save_time_ms = (get_time_sec() - save_start) * 1000.0;
    
    return 0;
}

// 执行基准测试
int run_benchmark(int save_interval) {
    char *recv_buf = malloc(RECV_BUF_SIZE);
    char *send_buf = malloc(SEND_BUF_SIZE);
    if (!recv_buf || !send_buf) { printf("OOM\n"); return -1; }
    
    int recv_buf_len = 0;
    int sent_cnt = 0;
    int acked_cnt = 0;
    long last_save_at = 0;
    
    printf("\nSAVE interval: %d records, Total SAVEs: %d\n", 
           save_interval, TOTAL_RECORDS / save_interval);
    
    memset(&stats, 0, sizeof(stats));
    
    int sock = connect_server();
    if (sock < 0) {
        fprintf(stderr, "Cannot connect to server!\n");
        free(recv_buf);
        free(send_buf);
        return -1;
    }
    
    stats.total_sent = 0;
    stats.total_acked = 0;
    stats.start_time = get_time_sec();

    for (int engine_type = 1; engine_type <= 4; engine_type++) {
        const char *engine_name = ENGINE_NAMES[engine_type];
        int records_to_insert = ENGINE_RECORDS[engine_type];
        
        printf("  [%s] %d records...", engine_name, records_to_insert);
        fflush(stdout);
        
        double engine_start_time = get_time_sec();
        int engine_sent_start = sent_cnt;
        
        // 逐条发送该引擎的所有记录
        while (acked_cnt - engine_sent_start < records_to_insert) {
            // 构建一条SET命令并直接发送
            char cmd[256];
            int cmd_len = build_set_cmd(cmd, engine_type, sent_cnt);
            
            if (send(sock, cmd, cmd_len, 0) < 0) {
                perror("send");
                goto cleanup;
            }
            sent_cnt++;
            stats.total_sent++;
            
            // 等待接收1个响应
            int acked = 0;
            while (acked < 1) {
                int n = recv(sock, recv_buf + recv_buf_len, RECV_BUF_SIZE - recv_buf_len, 0);
                if (n <= 0) {
                    if (n == 0) printf("\nServer closed connection.\n");
                    else perror("recv");
                    goto cleanup;
                }
                
                recv_buf_len += n;
                
                int parsed_bytes = 0;
                int count = parse_resp_replies(recv_buf, recv_buf_len, &parsed_bytes);
                acked += count;
                acked_cnt += count;
                stats.total_acked += count;
                
                if (parsed_bytes > 0) {
                    int remain = recv_buf_len - parsed_bytes;
                    if (remain > 0) memmove(recv_buf, recv_buf + parsed_bytes, remain);
                    recv_buf_len = remain;
                }
            }
            
            // 检查是否需要SAVE
            if (save_interval > 0 && (acked_cnt - last_save_at) >= save_interval) {
                execute_save(sock, send_buf, recv_buf, &recv_buf_len);
                last_save_at = acked_cnt;
            }
        }
        
        double engine_time = get_time_sec() - engine_start_time;
        printf(" %.2fs\n", engine_time);
    }
    
    // 最后执行一次SAVE
    if (acked_cnt - last_save_at > 0) {
        execute_save(sock, send_buf, recv_buf, &recv_buf_len);
    }
    
    stats.end_time = get_time_sec();
    
cleanup:
    close(sock);
    
    double total_time = stats.end_time - stats.start_time;
    double qps = stats.total_acked / total_time;
    
    printf("  Total time: %.2fs, QPS: %.0f, Last SAVE: %.2fms\n", 
           total_time, qps, stats.last_save_time_ms);
    
    free(recv_buf);
    free(send_buf);
    return 0;
}

int main(int argc, char *argv[]) {
    setvbuf(stdout, NULL, _IONBF, 0);

    int save_interval = 0;
    
    if (argc >= 2) {
        save_interval = atoi(argv[1]);
        if (save_interval < 0) {
            printf("Usage: %s <save_interval>\n", argv[0]);
            printf("  1000000 100000 10000 1000\n");
            return 1;
        }
    } else {
        printf("Usage: %s <save_interval>\n", argv[0]);
        return 1;
    }
    
    if (run_benchmark(save_interval) < 0) {
        return 1;
    }
    
    return 0;
}