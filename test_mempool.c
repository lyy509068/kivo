#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <sys/socket.h>
#include <arpa/inet.h>
#include <sys/wait.h>
#include <sys/time.h>
#include <time.h>
#include <signal.h>
#include <errno.h>
#include <stdbool.h>

#define SERVER_IP "127.0.0.1"
#define SERVER_PORT 2000
#define BATCH_SIZE 100

// RESP 命令映射
const char* SET_CMDS[] = {"", "SET", "RSET", "HSET", "SSET"};
const char* GET_CMDS[] = {"", "GET", "RGET", "HGET", "SGET"};
const char* DEL_CMDS[] = {"", "DEL", "RDEL", "HDEL", "SDEL"};

// 自动获取服务器 PID
pid_t get_server_pid() {
    FILE *fp = popen("pgrep -x server", "r");
    if (!fp) return -1;
    pid_t pid;
    if (fscanf(fp, "%d", &pid) != 1) {
        pclose(fp);
        return -1;
    }
    pclose(fp);
    return pid;
}

// 获取服务端物理与虚拟内存
void get_server_memory(pid_t pid, long *vmsize, long *vmrss) {
    char path[256];
    snprintf(path, sizeof(path), "/proc/%d/status", pid);
    FILE *fp = fopen(path, "r");
    *vmsize = 0; *vmrss = 0;
    if (!fp) return;

    char line[256];
    while (fgets(line, sizeof(line), fp)) {
        if (strncmp(line, "VmSize:", 7) == 0) sscanf(line + 7, "%ld", vmsize);
        else if (strncmp(line, "VmRSS:", 6) == 0) sscanf(line + 6, "%ld", vmrss);
    }
    fclose(fp);
}

// 记录量化日志
void log_results(const char* strategy, int engine, long total_ops, long time_ms, 
                 long qps, long d_vmsize, long d_vmrss, long batch_size) {
    FILE *fp = fopen("mempool_random_test.log", "a");
    if (!fp) return;
    fprintf(fp, "[Mixed Strategy: %s | Engine: %d | Batch: %ld] TotalOps: %ld | Time: %ld ms | QPS: %ld | VmSize+: %+ld kB | VmRSS+: %+ld kB\n\n",
            strategy, engine, batch_size, total_ops, time_ms, qps, d_vmsize, d_vmrss);
    fclose(fp);
}

// 构建 RESP 格式请求
int build_resp_cmd(char *buf, const char *cmd, const char *key, const char *val) {
    if (val) {
        return sprintf(buf, "*3\r\n$%zu\r\n%s\r\n$%zu\r\n%s\r\n$%zu\r\n%s\r\n",
                       strlen(cmd), cmd, strlen(key), key, strlen(val), val);
    } else {
        return sprintf(buf, "*2\r\n$%zu\r\n%s\r\n$%zu\r\n%s\r\n",
                       strlen(cmd), cmd, strlen(key), key);
    }
}

/* =========================================================================
 * 【核心状态机】：流式解析 RESP 协议响应数量（完美处理 TCP 粘包与分包）
 * ========================================================================= */
int parse_resp_count(const char *buf, int len, int *parsed_bytes) {
    int count = 0;
    int i = 0;

    while (i < len) {
        if (buf[i] == '+' || buf[i] == '-' || buf[i] == ':') {
            // 单行回复: 状态(+)、错误(-)、整数(:) -> 寻找到 \r\n 结束
            char *p = memchr(buf + i, '\n', len - i);
            if (!p) break; // 未收全，留到下次
            i = (p - buf) + 1;
            count++;
        } 
        else if (buf[i] == '$') {
            // 多行块字符串: $长度\r\n数据\r\n
            char *p1 = memchr(buf + i, '\n', len - i);
            if (!p1) break; // 第一行长度未收全
            
            int str_len = atoi(buf + i + 1);
            if (str_len == -1) {
                // $-1\r\n 代表 Null Bulk String (GET 未命中)
                i = (p1 - buf) + 1;
                count++;
            } else {
                // 计算该响应的总期望物理长度
                int first_line_len = (p1 - (buf + i)) + 1;
                int total_expected = i + first_line_len + str_len + 2; // +2 是数据末尾的 \r\n
                if (len < total_expected) break; // 数据体未收全，挂起等待下一次 recv
                
                i = total_expected;
                count++;
            }
        } 
        else {
            // 协议错位异常保护，跳过单字节防止死循环
            i++;
        }
    }
    *parsed_bytes = i; // 返回本次成功消耗掉的字节数
    return count;
}

int main(int argc, char *argv[]) {
    srand(time(NULL));
    
    if (argc < 4) {
        fprintf(stderr, "Usage: %s <strategy: 0|1|2> <engine_type: 1-4> <total_ops>\n", argv[0]);
        fprintf(stderr, "Strategies: 0=Glibc, 1=Jemalloc, 2=Custom Mempool\n");
        fprintf(stderr, "Engine:     1=Array, 2=RBTree, 3=Hash, 4=SkipList\n");
        return 1;
    }

    int strategy = atoi(argv[1]);
    int engine_type = atoi(argv[2]);
    long total_ops = atol(argv[3]);

    const char* strategy_names[] = {"Glibc_Malloc", "Jemalloc", "Custom_Mempool"};

    pid_t server_pid = get_server_pid();
    if (server_pid <= 0) {
        fprintf(stderr, "Server process not found. Please start it first.\n");
        return 1;
    }

    int sock = socket(AF_INET, SOCK_STREAM, 0);
    if (sock < 0) { perror("socket failed"); return 1; }
    
    struct sockaddr_in addr = { .sin_family = AF_INET, .sin_port = htons(SERVER_PORT) };
    inet_pton(AF_INET, SERVER_IP, &addr.sin_addr);
    
    int retry = 0;
    while (connect(sock, (struct sockaddr*)&addr, sizeof(addr)) < 0 && retry < 10) {
        usleep(100000);
        retry++;
    }
    if (retry >= 10) {
        fprintf(stderr, "Connection failed after 10 retries\n");
        close(sock);
        return 1;
    }
    printf("[Client] Connected to server (PID: %d) successfully!\n", server_pid);

    long init_vmsize = 0, init_vmrss = 0;
    get_server_memory(server_pid, &init_vmsize, &init_vmrss);
    
    long max_key_index = 0; 
    char send_buf[512], key[64], val[64];
    
    // 动态大缓冲区管理 Pipeline
    char *batch_buf = malloc(65536);
    char *stream_buf = malloc(65536); // 用户态 TCP 流式接收残余缓冲区
    if (!batch_buf || !stream_buf) {
        fprintf(stderr, "Memory allocation failed for buffers\n");
        return 1;
    }
    
    int batch_len = 0;
    int batch_count = 0;
    int stream_len = 0; // 接收缓冲区当前残留的字节数
    
    long count_set = 0, count_get = 0, count_del = 0, count_mod = 0;

    printf("[START] Testing %s on Engine %d (%ld Ops, Pipeline Batch: %d)...\n", 
           strategy_names[strategy], engine_type, total_ops, BATCH_SIZE);
           
    struct timeval tv_begin, tv_end;
    gettimeofday(&tv_begin, NULL);

    for (long i = 0; i < total_ops; i++) {
        int prob = rand() % 100;
        int len = 0;

        if (prob < 60) { 
            max_key_index++;
            sprintf(key, "rand_key_%08ld", max_key_index);
            sprintf(val, "value_bytes_%04d", rand() % 1000);
            len = build_resp_cmd(send_buf, SET_CMDS[engine_type], key, val);
            count_set++;
        } 
        else if (prob < 75) { 
            long target_idx = (max_key_index > 0) ? (rand() % max_key_index + 1) : 0;
            sprintf(key, "rand_key_%08ld", target_idx);
            len = build_resp_cmd(send_buf, GET_CMDS[engine_type], key, NULL);
            count_get++;
        } 
        else if (prob < 95) { 
            long target_idx = (max_key_index > 0) ? (rand() % max_key_index + 1) : 0;
            sprintf(key, "rand_key_%08ld", target_idx);
            len = build_resp_cmd(send_buf, DEL_CMDS[engine_type], key, NULL);
            count_del++;
        } 
        else { 
            long target_idx = (max_key_index > 0) ? (rand() % max_key_index + 1) : 0;
            sprintf(key, "rand_key_%08ld", target_idx);
            sprintf(val, "modified_value_padding_longer_bytes_%04d", rand() % 1000);
            len = build_resp_cmd(send_buf, SET_CMDS[engine_type], key, val);
            count_mod++;
        }

        memcpy(batch_buf + batch_len, send_buf, len);
        batch_len += len;
        batch_count++;
        
        // 触发 Pipeline 批次发送
        if (batch_count >= BATCH_SIZE || i == total_ops - 1) {
            if (send(sock, batch_buf, batch_len, 0) < 0) {
                perror("send failed");
                break;
            }
            
            int resp_received = 0;
            // 只要当前批次没收满，持续深入 I/O 读循环
            while (resp_received < batch_count) {
                int n = recv(sock, stream_buf + stream_len, 65536 - stream_len, 0);
                if (n < 0) {
                    perror("recv failed");
                    goto out;
                } else if (n == 0) {
                    fprintf(stderr, "Connection closed by server\n");
                    goto out;
                }
                stream_len += n;

                // 精准状态机解析当前缓冲区内包含了多少个完整 RESP 报文
                int parsed_bytes = 0;
                int ready_count = parse_resp_count(stream_buf, stream_len, &parsed_bytes);
                
                resp_received += ready_count;

                if (parsed_bytes > 0) {
                    // 将已被消费的数据移出缓冲区（处理半包/残留问题）
                    memmove(stream_buf, stream_buf + parsed_bytes, stream_len - parsed_bytes);
                    stream_len -= parsed_bytes;
                }
            }
            
            batch_len = 0;
            batch_count = 0;
        }

        if (i > 0 && i % 50000 == 0) {
            printf("  -> Progress: %ld / %ld ops finished.\n", i, total_ops);
        }
    }

out:
    gettimeofday(&tv_end, NULL);

    long time_ms = (tv_end.tv_sec - tv_begin.tv_sec) * 1000 + (tv_end.tv_usec - tv_begin.tv_usec) / 1000;
    if (time_ms == 0) time_ms = 1;
    long qps = total_ops * 1000 / time_ms;

    long cur_vmsize = 0, cur_vmrss = 0;
    get_server_memory(server_pid, &cur_vmsize, &cur_vmrss);

    printf("\n============================= TEST REPORT =============================\n");
    printf("Strategy: %s | Engine Type: %d\n", strategy_names[strategy], engine_type);
    printf("Executed Ops  : SET:%ld | GET:%ld | DEL:%ld | MOD:%ld (Total:%ld)\n", count_set, count_get, count_del, count_mod, total_ops);
    printf("Time Cost     : %ld ms\n", time_ms);
    printf("Throughput    : %ld QPS\n", qps);
    printf("Memory Delta  : VmSize: %+ld kB | VmRSS: %+ld kB\n", cur_vmsize - init_vmsize, cur_vmrss - init_vmrss);
    printf("=======================================================================\n");

    log_results(strategy_names[strategy], engine_type, total_ops, time_ms, qps, 
                cur_vmsize - init_vmsize, cur_vmrss - init_vmrss, BATCH_SIZE);

    free(batch_buf);
    free(stream_buf);
    close(sock);
    return 0;
}
//LD_PRELOAD=/usr/lib/x86_64-linux-gnu/libjemalloc.so.2 ./server 2000
// sudo LD_PRELOAD=/usr/lib/x86_64-linux-gnu/libjemalloc.so.2 ./server config.conf