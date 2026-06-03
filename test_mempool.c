#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <sys/socket.h>
#include <arpa/inet.h>
#include <sys/wait.h>
#include <sys/time.h>
#include <time.h>

#define SERVER_IP "127.0.0.1"
#define SERVER_PORT 2000

// RESP 命令映射
const char* SET_CMDS[] = {"", "SET", "RSET", "HSET", "SSET"};
const char* GET_CMDS[] = {"", "GET", "RGET", "HGET", "SGET"};
const char* DEL_CMDS[] = {"", "DEL", "RDEL", "HDEL", "SDEL"};

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
void log_results(const char* strategy, int engine, long total_ops, long time_ms, long qps, long d_vmsize, long d_vmrss) {
    FILE *fp = fopen("mempool_random_test.log", "a");
    if (!fp) return;
    fprintf(fp, "[Mixed Strategy: %s | Engine: %d] TotalOps: %ld | Time: %ld ms | QPS: %ld | VmSize+: %+ld kB | VmRSS+: %+ld kB\n\n",
            strategy, engine, total_ops, time_ms, qps, d_vmsize, d_vmrss);
    fclose(fp);
    printf(">>> Metrics successfully appended to mempool_random_test.log\n");
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
    srand(time(NULL));
    if (argc < 4) {
        fprintf(stderr, "Usage: %s <strategy: 0|1|2> <engine_type: 1-4> <total_ops>\n", argv[0]);
        fprintf(stderr, "Strategies: 0=Glibc, 1=Jemalloc, 2=Custom Mempool\n");
        fprintf(stderr, "Engine:     1=Array, 2=RBTree, 3=Hash, 4=SkipList\n");
        fprintf(stderr, "Example:    %s 2 3 200000 (Custom Mempool, Hash, 20W Operations)\n", argv[0]);
        return 1;
    }

    int strategy = atoi(argv[1]);
    int engine_type = atoi(argv[2]);
    long total_ops = atol(argv[3]);

    const char* strategy_names[] = {"Glibc_Malloc", "Jemalloc", "Custom_Mempool"};
    const char* server_path = "./server"; // 服务端可执行程序路径

    // 1. Fork 并按策略注入环境变量拉起服务器
    pid_t server_pid = fork();
    if (server_pid < 0) { perror("Fork failed"); return 1; }
    
    if (server_pid == 0) {
        if (strategy == 1) {
            setenv("LD_PRELOAD", "/usr/lib/x86_64-linux-gnu/libjemalloc.so.2", 1);
        } else {
            unsetenv("LD_PRELOAD"); 
        }
        execl(server_path, server_path, "2000", (char *)NULL);
        perror("execl failed"); exit(1);
    }
    
    printf("[Client] Spawned server (PID: %d). Initializing...\n", server_pid);
    usleep(600000); 

    // 2. 建立 TCP 连接
    int sock = socket(AF_INET, SOCK_STREAM, 0);
    struct sockaddr_in addr = { .sin_family = AF_INET, .sin_port = htons(SERVER_PORT) };
    inet_pton(AF_INET, SERVER_IP, &addr.sin_addr);
    if (connect(sock, (struct sockaddr*)&addr, sizeof(addr)) < 0) {
        fprintf(stderr, "❌ Connection failed!\n"); return 1;
    }
    printf("Connected to Server successfully!\n\n");

    // 3. 采集初始内存
    long init_vmsize = 0, init_vmrss = 0;
    get_server_memory(server_pid, &init_vmsize, &init_vmrss);
    
    // 为了使随机 GET 和 DEL 能命中已有数据，客户端需要维护一个合理的 Key 范围状态计数
    long max_key_index = 0; 
    char send_buf[512], recv_buf[512], key[64], val[64];
    
    long count_set = 0, count_get = 0, count_del = 0, count_mod = 0;

    printf("[🧪 START] Executing %ld Mixed Operations (60%% SET, 15%% GET, 20%% DEL, 5%% MOD)...\n", total_ops);
    
    struct timeval tv_begin, tv_end;
    gettimeofday(&tv_begin, NULL);

    for (long i = 0; i < total_ops; i++) {
        int prob = rand() % 100; // 0 - 99 随机数
        int len = 0;

        if (prob < 60) { 
            max_key_index++;
            sprintf(key, "rand_key_%08ld", max_key_index);
            sprintf(val, "value_bytes_%04d", rand() % 1000); // 随机生成内容
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
            // ----- 5% 概率: MOD (修改/覆盖已有 Key，触发 Realloc) -----
            long target_idx = (max_key_index > 0) ? (rand() % max_key_index + 1) : 0;
            sprintf(key, "rand_key_%08ld", target_idx);
            // 故意生成比原先更长或不同的数据，逼迫底层的 realloc 做迁移
            sprintf(val, "modified_value_padding_longer_bytes_%04d", rand() % 1000);
            len = build_resp_cmd(send_buf, SET_CMDS[engine_type], key, val);
            count_mod++;
        }

        send(sock, send_buf, len, 0);
        recv(sock, recv_buf, sizeof(recv_buf), 0); // 吞掉响应响应，保持 socket 清洁

        if (i > 0 && i % 50000 == 0) {
            printf("  -> Progress: %ld / %ld ops finished.\n", i, total_ops);
        }
    }
    gettimeofday(&tv_end, NULL);

    // 4. 计算吞吐指标与增量内存
    long time_ms = (tv_end.tv_sec - tv_begin.tv_sec) * 1000 + (tv_end.tv_usec - tv_begin.tv_usec) / 1000;
    if (time_ms == 0) time_ms = 1;
    long qps = total_ops * 1000 / time_ms;

    long cur_vmsize = 0, cur_vmrss = 0;
    get_server_memory(server_pid, &cur_vmsize, &cur_vmrss);

    // 5. 输出本次测试的量化报告
    printf("\n============================= TEST REPORT =============================\n");
    printf("Strategy: %s | Engine Type: %d\n", strategy_names[strategy], engine_type);
    printf("Executed Ops  : SET:%ld | GET:%ld | DEL:%ld | MOD:%ld (Total:%ld)\n", count_set, count_get, count_del, count_mod, total_ops);
    printf("Time Cost     : %ld ms\n", time_ms);
    printf("Throughput    : %ld QPS\n", qps);
    printf("Memory Delta  : VmSize: %+ld kB | VmRSS: %+ld kB\n", cur_vmsize - init_vmsize, cur_vmrss - init_vmrss);
    printf("=======================================================================\n");

    // 写入持久化日志文件供对比
    log_results(strategy_names[strategy], engine_type, total_ops, time_ms, qps, cur_vmsize - init_vmsize, cur_vmrss - init_vmrss);

    // 6. 安全优雅地关闭服务端
    printf("[Clean] Sending SHUTDOWN to release server context...\n");
    int len = sprintf(send_buf, "*1\r\n$8\r\nSHUTDOWN\r\n");
    send(sock, send_buf, len, 0);
    recv(sock, recv_buf, sizeof(recv_buf), 0);

    close(sock);
    waitpid(server_pid, NULL, 0); 
    printf("🏆 Test case finished successfully for %s.\n\n", strategy_names[strategy]);

    return 0;
}