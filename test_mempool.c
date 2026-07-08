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

#define SERVER_IP "127.0.0.1"
#define SERVER_PORT 2000
#define BATCH_SIZE 2

// RESP 命令映射
const char* SET_CMDS[] = {"", "SET", "RSET", "HSET", "SSET"};
const char* GET_CMDS[] = {"", "GET", "RGET", "HGET", "SGET"};
const char* DEL_CMDS[] = {"", "DEL", "RDEL", "HDEL", "SDEL"};

// 自动获取服务器 PID
pid_t get_server_pid() {
    fprintf(stderr, "[DEBUG] Getting server PID...\n");
    FILE *fp = popen("pgrep -x server", "r");
    if (!fp) {
        fprintf(stderr, "[DEBUG] popen failed\n");
        return -1;
    }
    pid_t pid;
    if (fscanf(fp, "%d", &pid) != 1) {
        fprintf(stderr, "[DEBUG] No server process found\n");
        pclose(fp);
        return -1;
    }
    pclose(fp);
    fprintf(stderr, "[DEBUG] Server PID: %d\n", pid);
    return pid;
}

// 获取服务端物理与虚拟内存
void get_server_memory(pid_t pid, long *vmsize, long *vmrss) {
    char path[256];
    snprintf(path, sizeof(path), "/proc/%d/status", pid);
    FILE *fp = fopen(path, "r");
    *vmsize = 0; *vmrss = 0;
    if (!fp) {
        fprintf(stderr, "[DEBUG] Cannot open /proc/%d/status\n", pid);
        return;
    }

    char line[256];
    while (fgets(line, sizeof(line), fp)) {
        if (strncmp(line, "VmSize:", 7) == 0) sscanf(line + 7, "%ld", vmsize);
        else if (strncmp(line, "VmRSS:", 6) == 0) sscanf(line + 6, "%ld", vmrss);
    }
    fclose(fp);
    fprintf(stderr, "[DEBUG] Memory: VmSize=%ld kB, VmRSS=%ld kB\n", *vmsize, *vmrss);
}

// 记录量化日志
void log_results(const char* strategy, int engine, long total_ops, long time_ms, 
                 long qps, long d_vmsize, long d_vmrss, long batch_size) {
    FILE *fp = fopen("mempool_random_test.log", "a");
    if (!fp) return;
    fprintf(fp, "[Mixed Strategy: %s | Engine: %d | Batch: %ld] TotalOps: %ld | Time: %ld ms | QPS: %ld | VmSize+: %+ld kB | VmRSS+: %+ld kB\n\n",
            strategy, engine, batch_size, total_ops, time_ms, qps, d_vmsize, d_vmrss);
    fclose(fp);
    fprintf(stderr, "[DEBUG] Results written to mempool_random_test.log\n");
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

int main(int argc, char *argv[]) {
    fprintf(stderr, "[DEBUG] Program started\n");
    srand(time(NULL));
    
    if (argc < 4) {
        fprintf(stderr, "Usage: %s <strategy: 0|1|2> <engine_type: 1-4> <total_ops>\n", argv[0]);
        fprintf(stderr, "Strategies: 0=Glibc, 1=Jemalloc, 2=Custom Mempool\n");
        fprintf(stderr, "Engine:     1=Array, 2=RBTree, 3=Hash, 4=SkipList\n");
        fprintf(stderr, "Example:    %s 2 3 100000\n", argv[0]);
        fprintf(stderr, "\nBefore running, start server manually:\n");
        fprintf(stderr, "  For Glibc:    ./server 2000\n");
        fprintf(stderr, "  For Jemalloc: LD_PRELOAD=/usr/lib/x86_64-linux-gnu/libjemalloc.so.2 ./server 2000\n");
        fprintf(stderr, "  For Mempool:  ./server 2000\n");
        return 1;
    }

    int strategy = atoi(argv[1]);
    int engine_type = atoi(argv[2]);
    long total_ops = atol(argv[3]);
    fprintf(stderr, "[DEBUG] Strategy=%d, Engine=%d, TotalOps=%ld\n", strategy, engine_type, total_ops);

    const char* strategy_names[] = {"Glibc_Malloc", "Jemalloc", "Custom_Mempool"};

    // 自动获取服务器 PID
    pid_t server_pid = get_server_pid();
    if (server_pid <= 0) {
        fprintf(stderr, "Server not found. Please start it first.\n");
        fprintf(stderr, "  For Glibc:    ./server 2000\n");
        fprintf(stderr, "  For Jemalloc: LD_PRELOAD=/usr/lib/x86_64-linux-gnu/libjemalloc.so.2 ./server 2000\n");
        fprintf(stderr, "  For Mempool:  ./server 2000\n");
        return 1;
    }

    // 建立 TCP 连接
    fprintf(stderr, "[DEBUG] Creating socket...\n");
    int sock = socket(AF_INET, SOCK_STREAM, 0);
    if (sock < 0) {
        fprintf(stderr, "[DEBUG] socket failed\n");
        return 1;
    }
    fprintf(stderr, "[DEBUG] Socket created: %d\n", sock);
    
    struct sockaddr_in addr = { .sin_family = AF_INET, .sin_port = htons(SERVER_PORT) };
    inet_pton(AF_INET, SERVER_IP, &addr.sin_addr);
    
    fprintf(stderr, "[DEBUG] Connecting to %s:%d...\n", SERVER_IP, SERVER_PORT);
    int retry = 0;
    while (connect(sock, (struct sockaddr*)&addr, sizeof(addr)) < 0 && retry < 10) {
        fprintf(stderr, "[DEBUG] Connection attempt %d failed, retrying...\n", retry + 1);
        usleep(100000);
        retry++;
    }
    if (retry >= 10) {
        fprintf(stderr, "Connection failed after 10 retries\n");
        close(sock);
        return 1;
    }
    fprintf(stderr, "[DEBUG] Connected successfully\n");

    // 采集初始内存
    fprintf(stderr, "[DEBUG] Getting initial memory stats...\n");
    long init_vmsize = 0, init_vmrss = 0;
    get_server_memory(server_pid, &init_vmsize, &init_vmrss);
    fprintf(stderr, "[DEBUG] Initial: VmSize=%ld, VmRSS=%ld\n", init_vmsize, init_vmrss);
    
    // 测试状态
    long max_key_index = 0; 
    char send_buf[512], recv_buf[8192], key[64], val[64];
    char *batch_buf = malloc(65536);
    if (!batch_buf) {
        fprintf(stderr, "[DEBUG] malloc failed for batch_buf\n");
        close(sock);
        return 1;
    }
    fprintf(stderr, "[DEBUG] batch_buf allocated\n");
    
    int batch_len = 0;
    int batch_count = 0;
    
    long count_set = 0, count_get = 0, count_del = 0, count_mod = 0;

    fprintf(stderr, "[DEBUG] Starting operations...\n");
    struct timeval tv_begin, tv_end;
    gettimeofday(&tv_begin, NULL);

    for (long i = 0; i < total_ops; i++) {
        if (i % 10000 == 0) {
            fprintf(stderr, "[DEBUG] Progress: %ld / %ld\n", i, total_ops);
        }
        
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

        // Pipeline 批量发送接收
        memcpy(batch_buf + batch_len, send_buf, len);
        batch_len += len;
        batch_count++;
        
        if (batch_count >= BATCH_SIZE || i == total_ops - 1) {
            fprintf(stderr, "[DEBUG] Sending batch: %d commands, %d bytes\n", batch_count, batch_len);
            ssize_t sent = send(sock, batch_buf, batch_len, 0);
            if (sent < 0) {
                fprintf(stderr, "[DEBUG] send failed: %s\n", strerror(errno));
                break;
            }
            fprintf(stderr, "[DEBUG] Sent %zd bytes\n", sent);
            
            fprintf(stderr, "[DEBUG] Receiving %d responses...\n", batch_count);
            int total_recv = 0;
            while (total_recv < batch_count) {
                int n = recv(sock, recv_buf, sizeof(recv_buf) - 1, 0);
                if (n < 0) {
                    fprintf(stderr, "[DEBUG] recv failed: %s\n", strerror(errno));
                    break;
                } else if (n == 0) {
                    fprintf(stderr, "[DEBUG] Connection closed by server\n");
                    break;
                }
                recv_buf[n] = '\0';
                total_recv++;
            }
            fprintf(stderr, "[DEBUG] Received %d responses\n", total_recv);
            
            batch_len = 0;
            batch_count = 0;
        }
    }
    gettimeofday(&tv_end, NULL);
    fprintf(stderr, "[DEBUG] Operations completed\n");

    // 计算性能指标
    long time_ms = (tv_end.tv_sec - tv_begin.tv_sec) * 1000 + 
                   (tv_end.tv_usec - tv_begin.tv_usec) / 1000;
    if (time_ms == 0) time_ms = 1;
    long qps = total_ops * 1000 / time_ms;
    fprintf(stderr, "[DEBUG] Time: %ld ms, QPS: %ld\n", time_ms, qps);

    fprintf(stderr, "[DEBUG] Getting final memory stats...\n");
    long cur_vmsize = 0, cur_vmrss = 0;
    get_server_memory(server_pid, &cur_vmsize, &cur_vmrss);
    fprintf(stderr, "[DEBUG] Final: VmSize=%ld, VmRSS=%ld\n", cur_vmsize, cur_vmrss);

    // 写日志
    fprintf(stderr, "[DEBUG] Writing results to log...\n");
    log_results(strategy_names[strategy], engine_type, total_ops, time_ms, qps, 
                cur_vmsize - init_vmsize, cur_vmrss - init_vmrss, BATCH_SIZE);

    // 关闭 fd
    fprintf(stderr, "[DEBUG] Cleaning up...\n");
    free(batch_buf);
    close(sock);
    fprintf(stderr, "[DEBUG] Done\n");

    return 0;
}