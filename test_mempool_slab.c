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
#define BATCH_SIZE 1
#define MAX_KEY_LEN 64
#define MAX_VAL_LEN 128
#define SEND_BUF_SIZE 65536
#define RECV_BUF_SIZE 65536

// RESP 命令映射
const char* SET_CMDS[] = {"", "SET", "RSET", "HSET", "SSET"};
const char* GET_CMDS[] = {"", "GET", "RGET", "HGET", "SGET"};
const char* DEL_CMDS[] = {"", "DEL", "RDEL", "HDEL", "SDEL"};

const char* STRATEGY_NAMES[] = {"Glibc_Malloc", "Jemalloc", "Custom_Mempool"};
const char* ENGINE_NAMES[] = {"", "Array", "RBTree", "Hash", "SkipList"};

// 可配置的 key/value 长度
static size_t g_key_len = 31;
static size_t g_val_len = 31;

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

// RESP 协议解析状态机
int parse_resp_count(const char *buf, int len, int *parsed_bytes) {
    int count = 0;
    int i = 0;

    while (i < len) {
        if (buf[i] == '+' || buf[i] == '-' || buf[i] == ':') {
            char *p = memchr(buf + i, '\n', len - i);
            if (!p) break;
            i = (p - buf) + 1;
            count++;
        } 
        else if (buf[i] == '$') {
            char *p1 = memchr(buf + i, '\n', len - i);
            if (!p1) break;
            
            int str_len = atoi(buf + i + 1);
            if (str_len == -1) {
                i = (p1 - buf) + 1;
                count++;
            } else {
                int first_line_len = (p1 - (buf + i)) + 1;
                int total_expected = i + first_line_len + str_len + 2;
                if (len < total_expected) break;
                i = total_expected;
                count++;
            }
        } 
        else {
            i++;
        }
    }
    *parsed_bytes = i;
    return count;
}

// 发送并接收批次响应
int send_and_receive_batch(int sock, char *batch_buf, int batch_len, int batch_count,
                           char *stream_buf, int *stream_len) {
    if (send(sock, batch_buf, batch_len, 0) < 0) {
        return -1;
    }
    
    int resp_received = 0;
    while (resp_received < batch_count) {
        int n = recv(sock, stream_buf + (*stream_len), RECV_BUF_SIZE - (*stream_len), 0);
        if (n <= 0) return -1;
        *stream_len += n;

        int parsed_bytes = 0;
        int ready_count = parse_resp_count(stream_buf, *stream_len, &parsed_bytes);
        resp_received += ready_count;

        if (parsed_bytes > 0) {
            memmove(stream_buf, stream_buf + parsed_bytes, *stream_len - parsed_bytes);
            *stream_len -= parsed_bytes;
        }
    }
    return 0;
}

// 检查文件是否为空
int is_file_empty(const char *filename) {
    FILE *fp = fopen(filename, "r");
    if (!fp) return 1;
    
    fseek(fp, 0, SEEK_END);
    long size = ftell(fp);
    fclose(fp);
    
    return (size == 0);
}

// 写入表头
void write_header_if_needed(const char *filename, long total_ops) {
    if (!is_file_empty(filename)) {
        return;
    }
    
    FILE *fp = fopen(filename, "w");
    if (!fp) return;
    
    fprintf(fp, "# MEMORY POOL 3-PHASE BENCHMARK RESULTS\n");
    fprintf(fp, "# Test: SET:60%% GET:15%% DEL:10%% MOD:15%% (All Engines Mixed)\n");
    fprintf(fp, "# Key/Value Length: %zu/%zu bytes\n", g_key_len, g_val_len);
    fprintf(fp, "# Legend: 1=Initial  2=Peak  3=Clean\n");
    fprintf(fp, "#\n");
    fprintf(fp, "%-14s | %8s | %8s | %8s | %8s | %8s | %8s | %8s | %8s | %8s\n",
            "Strategy", 
            "VmSz1", "VmSz2", "VmSz3",
            "VmRSS1", "VmRSS2", "VmRSS3",
            "Time_ms", "QPS", "Ops");
    fprintf(fp, "----------------|----------|----------|----------|----------|----------|----------|----------|----------|----------\n");
    
    fclose(fp);
}

// 追加测试结果
void append_result(const char *filename, int strategy,
                   long vmsize1, long vmsize2, long vmsize3,
                   long vmrss1, long vmrss2, long vmrss3,
                   long time_ms, long qps, long total_ops) {
    FILE *fp = fopen(filename, "a");
    if (!fp) return;
    
    fprintf(fp, "%-14s | %8ld | %8ld | %8ld | %8ld | %8ld | %8ld | %8ld | %8ld | %8ld\n",
            STRATEGY_NAMES[strategy],
            vmsize1, vmsize2, vmsize3,
            vmrss1, vmrss2, vmrss3,
            time_ms, qps, total_ops);
    
    fclose(fp);
}

// 生成固定长度 key
void make_key(char *key, size_t size, int engine_type, long index) {
    const char *prefix = "";
    switch (engine_type) {
        case 1: prefix = "A"; break;
        case 2: prefix = "R"; break;
        case 3: prefix = "H"; break;
        case 4: prefix = "S"; break;
    }

    int written = snprintf(key, size, "%s%010ld", prefix, index);
    while (written < (int)g_key_len && (size_t)written < size - 1) {
        key[written++] = 'x';
    }
    key[written] = '\0';
}

// 生成固定长度 value
void make_value(char *val, size_t size) {
    static const char chars[] = "abcdefghijklmnopqrstuvwxyz0123456789";
    size_t len = g_val_len;
    if (len >= size) len = size - 1;
    for (size_t i = 0; i < len; i++) {
        val[i] = chars[rand() % (sizeof(chars)-1)];
    }
    val[len] = '\0';
}

// 单次测试
int run_single_test(int strategy, long total_ops, const char *log_filename) {
    pid_t server_pid = get_server_pid();
    if (server_pid <= 0) {
        printf("  [ERROR] Server not found\n");
        return -1;
    }

    int sock = socket(AF_INET, SOCK_STREAM, 0);
    if (sock < 0) {
        printf("  [ERROR] Socket creation failed\n");
        return -1;
    }
    
    struct sockaddr_in addr = { .sin_family = AF_INET, .sin_port = htons(SERVER_PORT) };
    inet_pton(AF_INET, SERVER_IP, &addr.sin_addr);
    
    if(connect(sock, (struct sockaddr*)&addr, sizeof(addr)) < 0) {
        printf("  [ERROR] Connection failed\n");
        close(sock);
        return -1;
    }

    long init_vmsize, init_vmrss;
    get_server_memory(server_pid, &init_vmsize, &init_vmrss);
    printf("  Phase1 Init:   VmSize=%ld KB, VmRSS=%ld KB\n", init_vmsize, init_vmrss);

    char *batch_buf = malloc(SEND_BUF_SIZE);
    char *stream_buf = malloc(RECV_BUF_SIZE);
    if (!batch_buf || !stream_buf) {
        printf("  [ERROR] Buffer allocation failed\n");
        close(sock);
        return -1;
    }
    
    int batch_len = 0, batch_count = 0, stream_len = 0;
    long max_key_index[5] = {0, 0, 0, 0, 0};
    
    printf("  Phase2 Load:   ");
    fflush(stdout);
    struct timeval tv_begin, tv_end;
    gettimeofday(&tv_begin, NULL);

    for (long i = 0; i < total_ops; i++) {
        int prob = rand() % 100;
        int engine_type;
        int eng_rand = rand() % 100;
        if (eng_rand < 2) {
            engine_type = 1;
        } else if (eng_rand < 35) {
            engine_type = 2;
        } else if (eng_rand < 68) {
            engine_type = 3;
        } else {
            engine_type = 4;
        }
        char send_buf[512], key[MAX_KEY_LEN], val[MAX_VAL_LEN];
        int len = 0;

        if (prob < 60) {
            max_key_index[engine_type]++;
            make_key(key, sizeof(key), engine_type, max_key_index[engine_type]);
            make_value(val, sizeof(val));
            len = build_resp_cmd(send_buf, SET_CMDS[engine_type], key, val);
        } 
        else if (prob < 75) {
            if (max_key_index[engine_type] > 0) {
                make_key(key, sizeof(key), engine_type, (rand() % max_key_index[engine_type]) + 1);
                len = build_resp_cmd(send_buf, GET_CMDS[engine_type], key, NULL);
            } else { i--; continue; }
        } 
        else if (prob < 85) {
            if (max_key_index[engine_type] > 0) {
                make_key(key, sizeof(key), engine_type, (rand() % max_key_index[engine_type]) + 1);
                len = build_resp_cmd(send_buf, DEL_CMDS[engine_type], key, NULL);
            } else { i--; continue; }
        } 
        else {
            if (max_key_index[engine_type] > 0) {
                make_key(key, sizeof(key), engine_type, (rand() % max_key_index[engine_type]) + 1);
                make_value(val, sizeof(val));
                len = build_resp_cmd(send_buf, SET_CMDS[engine_type], key, val);
            } else { i--; continue; }
        }

        memcpy(batch_buf + batch_len, send_buf, len);
        batch_len += len;
        batch_count++;
        
        if (batch_count >= BATCH_SIZE || batch_len > SEND_BUF_SIZE - 512 || i == total_ops - 1) {
            if (send_and_receive_batch(sock, batch_buf, batch_len, batch_count,
                                       stream_buf, &stream_len) < 0) {
                printf("  [ERROR] Communication error at op %ld\n", i);
                free(batch_buf); free(stream_buf); close(sock);
                return -1;
            }
            batch_len = 0;
            batch_count = 0;
        }

        if ((i + 1) % (total_ops / 10) == 0) {
            printf(".");
            fflush(stdout);
        }
    }
    printf(" done\n");

    gettimeofday(&tv_end, NULL);
    
    long peak_vmsize, peak_vmrss;
    get_server_memory(server_pid, &peak_vmsize, &peak_vmrss);
    printf("  Phase2 Peak:   VmSize=%ld KB, VmRSS=%ld KB\n", peak_vmsize, peak_vmrss);

    printf("  Phase3 Clean:  ");
    fflush(stdout);
    batch_len = 0;
    batch_count = 0;
    
    for (int eng = 1; eng <= 4; eng++) {
        for (long i = 1; i <= max_key_index[eng]; i++) {
            char send_buf[512], key[MAX_KEY_LEN];
            make_key(key, sizeof(key), eng, i);
            int len = build_resp_cmd(send_buf, DEL_CMDS[eng], key, NULL);
            
            memcpy(batch_buf + batch_len, send_buf, len);
            batch_len += len;
            batch_count++;
            
            if (batch_count >= BATCH_SIZE || (eng == 4 && i == max_key_index[eng])) {
                send_and_receive_batch(sock, batch_buf, batch_len, batch_count,
                                      stream_buf, &stream_len);
                batch_len = 0;
                batch_count = 0;
            }
        }
        printf(".");
        fflush(stdout);
    }
    printf(" done\n");

    printf("  Sending MEMTRIM... ");
    fflush(stdout);
    const char *trim_cmd = "*1\r\n$7\r\nMEMTRIM\r\n";
    if (send(sock, trim_cmd, strlen(trim_cmd), 0) < 0) {
        printf("send failed\n");
    } else {
        char resp_buf[64];
        int n = recv(sock, resp_buf, sizeof(resp_buf) - 1, 0);
        if (n > 0) {
            resp_buf[n] = '\0';
            printf("OK\n");
        } else {
            printf("no response\n");
        }
    }

    sleep(2);
    
    long clean_vmsize, clean_vmrss;
    get_server_memory(server_pid, &clean_vmsize, &clean_vmrss);
    printf("  Phase3 Clean:  VmSize=%ld KB, VmRSS=%ld KB\n", clean_vmsize, clean_vmrss);

    printf("  Key distribution: Array=%ld, RBTree=%ld, Hash=%ld, SkipList=%ld\n",
           max_key_index[1], max_key_index[2], max_key_index[3], max_key_index[4]);

    long time_ms = (tv_end.tv_sec - tv_begin.tv_sec) * 1000 + 
                   (tv_end.tv_usec - tv_begin.tv_usec) / 1000;
    if (time_ms == 0) time_ms = 1;
    long qps = total_ops * 1000 / time_ms;
    printf("  Result:        Time=%ld ms, QPS=%ld\n\n", time_ms, qps);

    append_result(log_filename, strategy,
                  init_vmsize, peak_vmsize, clean_vmsize,
                  init_vmrss, peak_vmrss, clean_vmrss,
                  time_ms, qps, total_ops);

    free(batch_buf);
    free(stream_buf);
    close(sock);
    return 0;
}

int main(int argc, char *argv[]) {
    srand(time(NULL));
    
    if (argc < 3) {
        printf("Usage: %s <strategy> <mode> [ops]\n", argv[0]);
        printf("  strategy: 0=Glibc  1=Jemalloc  2=Mempool\n");
        printf("  mode:     1=key/value 31 bytes, 2=key/value 33 bytes\n");
        printf("  ops:      total operations (default: 1000000)\n");
        return 1;
    }

    long total_ops = (argc >= 4) ? atol(argv[3]) : 1000000;
    int mode = atoi(argv[2]);

    if (mode == 1) {
        g_key_len = 31;
        g_val_len = 31;
    } else if (mode == 2) {
        g_key_len = 33;
        g_val_len = 33;
    } else {
        printf("Invalid mode: %d (must be 1 or 2)\n", mode);
        return 1;
    }

    const char *log_filename = "mempool_results.txt";
    write_header_if_needed(log_filename, total_ops);

    int strategies[3] = {0, 1, 2};
    int strat_count = 3;

    if (strcmp(argv[1], "all") != 0) {
        strategies[0] = atoi(argv[1]);
        strat_count = 1;
    }

    printf("\n");
    printf("========================================\n");
    printf("  Memory Pool Benchmark (Mixed Engines)\n");
    printf("  Key/Value Length: %zu/%zu bytes\n", g_key_len, g_val_len);
    printf("  Total tests: %d\n", strat_count);
    printf("  Log file: %s\n", log_filename);
    printf("========================================\n\n");

    for (int s = 0; s < strat_count; s++) {
        printf("[%d/%d] %s (All Engines Mixed)\n", 
               s + 1, strat_count,
               STRATEGY_NAMES[strategies[s]]);
        
        int ret = run_single_test(strategies[s], total_ops, log_filename);
        if (ret < 0) {
            printf("  *** TEST FAILED ***\n\n");
        }
        
        if (s < strat_count - 1) {
            sleep(1);
        }
    }
    
    printf("========================================\n");
    printf("  All tests completed!\n");
    printf("  Results appended to: %s\n", log_filename);
    printf("========================================\n");
    
    return 0;
}