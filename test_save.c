#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <unistd.h>
#include <arpa/inet.h>
#include <sys/socket.h>
#include <sys/time.h>

#define SERVER_PORT 2000
#define TOTAL_RECORDS 1000000  // 总共100万条记录

// 修改数据分布：Array 1万条，其他三种各33万条
#define ARRAY_RECORDS 10000
#define RBTREE_RECORDS 330000
#define HASH_RECORDS 330000
#define SKIPLIST_RECORDS 330000

// 验证总数：10000 + 330000 + 330000 + 330000 = 1000000

const char* SET_CMDS[] = {"", "SET", "RSET", "HSET", "SSET"};
const char* ENGINE_NAMES[] = {"", "Array", "RBTree", "Hash", "SkipList"};

// 每个引擎的记录数配置
const int ENGINE_RECORDS[] = {0, ARRAY_RECORDS, RBTREE_RECORDS, HASH_RECORDS, SKIPLIST_RECORDS};

// 时间统计结构
typedef struct {
    struct timeval start_time;
    struct timeval end_time;
    long total_requests;
    long total_save_time_us;  // SAVE总耗时(微秒)
    int save_count;
    long engine_requests[5];  // 每个引擎的请求数
    double engine_time[5];    // 每个引擎的耗时
} benchmark_stats_t;

static benchmark_stats_t stats = {0};

// 获取当前时间(微秒)
long get_time_us() {
    struct timeval tv;
    gettimeofday(&tv, NULL);
    return tv.tv_sec * 1000000L + tv.tv_usec;
}

// 计算时间差(微秒)
long time_diff_us(struct timeval *start, struct timeval *end) {
    return (end->tv_sec - start->tv_sec) * 1000000L + 
           (end->tv_usec - start->tv_usec);
}

int build_resp_request(char *buf, const char *cmd, const char *key, const char *val) {
    if (val) {
        return sprintf(buf, "*3\r\n$%zu\r\n%s\r\n$%zu\r\n%s\r\n$%zu\r\n%s\r\n", 
                       strlen(cmd), cmd, strlen(key), key, strlen(val), val);
    } else {
        return sprintf(buf, "*1\r\n$%zu\r\n%s\r\n", strlen(cmd), cmd);
    }
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

int send_all(int sock, const char *buf, int len) {
    int total_sent = 0;
    while (total_sent < len) {
        int sent = send(sock, buf + total_sent, len - total_sent, 0);
        if (sent <= 0) return -1;
        total_sent += sent;
    }
    return total_sent;
}

int recv_all(int sock, char *buf, int len) {
    int total_recv = 0;
    while (total_recv < len) {
        int r = recv(sock, buf + total_recv, len - total_recv, 0);
        if (r <= 0) return -1;
        total_recv += r;
    }
    return total_recv;
}

// 执行SAVE命令并测量时间
int execute_save(int sock, char *send_buf, char *recv_buf) {
    struct timeval save_start, save_end;
    
    gettimeofday(&save_start, NULL);
    
    int send_len = build_resp_request(send_buf, "SAVE", NULL, NULL);
    
    if (send_all(sock, send_buf, send_len) < 0) {
        printf("   Failed to send SAVE command!\n");
        return -1;
    }
    
    memset(recv_buf, 0, 1024);
    if (recv_all(sock, recv_buf, 5) < 0 || strstr(recv_buf, "OK") == NULL) {
        printf("   SAVE command failed! Got: [%s]\n", recv_buf);
        return -1;
    }
    
    gettimeofday(&save_end, NULL);
    
    long save_time = time_diff_us(&save_start, &save_end);
    stats.total_save_time_us += save_time;
    stats.save_count++;
    
    return 0;
}

// 插入单条记录
int insert_record(int sock, char *send_buf, char *recv_buf, 
                  int engine_type, int global_index) {
    const char *cmd = SET_CMDS[engine_type];
    const char *engine_name = ENGINE_NAMES[engine_type];
    
    char key[32], val[64];
    sprintf(key, "key_%07d", global_index);  // 改为7位，支持100万
    sprintf(val, "val_%07d_%s", global_index, engine_name);
    
    int send_len = build_resp_request(send_buf, cmd, key, val);

    if (send_all(sock, send_buf, send_len) < 0) {
        return -1;
    }
    
    memset(recv_buf, 0, 1024);
    if (recv_all(sock, recv_buf, 5) < 0 || strstr(recv_buf, "OK") == NULL) {
        return -1;
    }
    
    stats.total_requests++;
    stats.engine_requests[engine_type]++;
    return 0;
}

// 执行基准测试
int run_benchmark(int save_interval) {
    char send_buf[1024], recv_buf[1024];
    
    printf("\n══════════════════════════════════════════════════════════\n");
    printf("  BENCHMARK: SAVE every %d records                        \n", save_interval);
    printf("══════════════════════════════════════════════════════════\n");
    
    // 重置统计
    memset(&stats, 0, sizeof(stats));
    
    int sock = connect_server();
    if (sock < 0) {
        fprintf(stderr, " Fatal: Cannot connect to kvstore!\n");
        return -1;
    }
    
    gettimeofday(&stats.start_time, NULL);
    
    int global_index = 0;
    long last_save_at = 0;
    struct timeval engine_start, engine_end;
    
    // 按顺序插入不同数量的记录到各个引擎
    for (int engine_type = 1; engine_type <= 4; engine_type++) {
        const char *engine_name = ENGINE_NAMES[engine_type];
        int records_to_insert = ENGINE_RECORDS[engine_type];
        
        printf("  [%s] Inserting %d records...\n", engine_name, records_to_insert);
        printf("  Key range: key_%07d to key_%07d\n", global_index, global_index + records_to_insert - 1);
        
        gettimeofday(&engine_start, NULL);
        
        for (int i = 0; i < records_to_insert; i++) {
            // 插入记录
            if (insert_record(sock, send_buf, recv_buf, engine_type, global_index) < 0) {
                printf("   Failed at index: %d\n", global_index);
                close(sock);
                return -1;
            }
            
            // 检查是否需要执行SAVE
            if (save_interval > 0 && (global_index - last_save_at) >= save_interval) {
                if (execute_save(sock, send_buf, recv_buf) < 0) {
                    close(sock);
                    return -1;
                }
                last_save_at = global_index;
            }
            
            global_index++;
        }
        
        gettimeofday(&engine_end, NULL);
        stats.engine_time[engine_type] = time_diff_us(&engine_start, &engine_end) / 1000000.0;
        
        printf("  [%s]  Completed in %.2f seconds\n", engine_name, stats.engine_time[engine_type]);
    }
    
    // 最后执行一次SAVE确保数据落盘
    if (save_interval > 0) {
        printf("\n   Final SAVE operation...\n");
        execute_save(sock, send_buf, recv_buf);
    }
    
    gettimeofday(&stats.end_time, NULL);
    
    close(sock);
    
    // 计算并显示结果
    long total_time_us = time_diff_us(&stats.start_time, &stats.end_time);
    double total_time_sec = total_time_us / 1000000.0;
    double qps = stats.total_requests / total_time_sec;
    
    printf("\n  ═══════════════════════════════════════════\n");
    printf("          BENCHMARK RESULTS                 \n");
    printf("  ═══════════════════════════════════════════\n");
    printf("   Total records inserted:  %10d     \n", TOTAL_RECORDS);
    printf("   Total SAVE operations:   %10d     \n", stats.save_count);
    printf("   Total time:              %10.2f sec   \n", total_time_sec);
    printf("   Overall QPS:             %10.0f       \n", qps);
    printf("  ═══════════════════════════════════════════\n");
    
    return 0;
}

int main(int argc, char *argv[]) {
    setvbuf(stdout, NULL, _IONBF, 0);

    int save_interval = 0;  // 0表示只在最后SAVE一次
    
    if (argc >= 2) {
        save_interval = atoi(argv[1]);
        if (save_interval < 0) {
            printf("Usage: %s [save_interval]\n", argv[0]);
            printf("  save_interval: number of records between SAVEs\n");
            printf("                 0 = SAVE only at the end\n");
            printf("                 Examples: 1000000, 100000, 10000, 1000\n");
            return 1;
        }
    }
    
    printf("══════════════════════════════════════════════════════════\n");
    printf("     KVSTORE SAVE PERFORMANCE BENCHMARK                  \n");
    printf("     Total Records: 1,000,000                           \n");
    printf("     Array: 10K | RBTree: 330K | Hash: 330K | Skip: 330K\n");
    printf("══════════════════════════════════════════════════════════\n");
    
    if (save_interval == 0) {
        printf("[CONFIG] SAVE only at the end (1 SAVE total)\n");
    } else {
        printf("[CONFIG] SAVE every %d records (~%d SAVEs total)\n", 
               save_interval, TOTAL_RECORDS / save_interval);
    }
    
    // 执行基准测试
    if (run_benchmark(save_interval) < 0) {
        return 1;
    }
    
    printf("\n Benchmark completed successfully!\n");
    return 0;
}