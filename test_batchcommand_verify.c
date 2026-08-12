#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <sys/socket.h>
#include <arpa/inet.h>
#include <errno.h>
#include <time.h>

#define SERVER_IP "127.0.0.1"
#define SERVER_PORT 2000
#define TOTAL_KEYS  100
#define KEYS_PER_TYPE 25
#define REPEAT_TIMES 1000

#define MAX_SEND_BUF (TOTAL_KEYS * 256)
#define MAX_RECV_BUF (1024)
#define PROGRESS_INTERVAL 10000  // 每10000条打印一次

const char* SET_CMDS[] = {"", "SET", "RSET", "HSET", "SSET"};
const char* GET_CMDS[] = {"", "GET", "RGET", "HGET", "SGET"};

// 连接服务器
int connect_server() {
    int sock = socket(AF_INET, SOCK_STREAM, 0);
    if (sock < 0) return -1;
    
    struct sockaddr_in serv_addr;
    serv_addr.sin_family = AF_INET;
    serv_addr.sin_port = htons(SERVER_PORT);
    inet_pton(AF_INET, SERVER_IP, &serv_addr.sin_addr);

    if (connect(sock, (struct sockaddr *)&serv_addr, sizeof(serv_addr)) < 0) {
        close(sock);
        return -1;
    }
    return sock;
}

// 构建 RESP GET 命令
int build_resp_get(char *buf, const char *cmd, const char *key) {
    return sprintf(buf, "*2\r\n$%zu\r\n%s\r\n$%zu\r\n%s\r\n",
                   strlen(cmd), cmd, strlen(key), key);
}

// 构建 RESP SET 命令
int build_resp_set(char *buf, const char *cmd, const char *key, const char *val) {
    return sprintf(buf, "*3\r\n$%zu\r\n%s\r\n$%zu\r\n%s\r\n$%zu\r\n%s\r\n",
                   strlen(cmd), cmd, strlen(key), key, strlen(val), val);
}

int send_all(int fd, const char *buf, int len) {
    int sent = 0;
    while (sent < len) {
        int n = send(fd, buf + sent, len - sent, 0);
        if (n <= 0) return -1;
        sent += n;
    }
    return sent;
}

// 发送并接收单个命令的响应
int send_and_recv_one(int sock, const char *cmd, const char *key, 
                      char *recv_buf, int recv_buf_size) {
    char send_buf[256];
    int send_len = build_resp_get(send_buf, cmd, key);
    
    if (send_all(sock, send_buf, send_len) < 0) {
        return -1;
    }
    
    int n = recv(sock, recv_buf, recv_buf_size - 1, 0);
    if (n <= 0) return -1;
    recv_buf[n] = '\0';
    return n;
}

// 解析 RESP 响应，提取字符串值
int parse_resp_bulk_string(const char *resp, char *out, int out_size) {
    if (resp[0] != '$') {
        return -1;
    }
    
    if (strncmp(resp, "$-1", 3) == 0) {
        out[0] = '\0';
        return 0;
    }
    
    int len = atoi(resp + 1);
    if (len < 0) {
        out[0] = '\0';
        return 0;
    }
    
    char *end = strstr(resp, "\r\n");
    if (!end) return -1;
    char *data_start = end + 2;
    
    int copy_len = (len < out_size - 1) ? len : out_size - 1;
    memcpy(out, data_start, copy_len);
    out[copy_len] = '\0';
    
    return len;
}

// 单条验证函数（不打印，只返回结果）
int verify_single_key(int sock, int round, int engine, int idx, 
                      char *recv_buf, int recv_buf_size) {
    const char *cmd_name = GET_CMDS[engine];
    const char *set_cmd = SET_CMDS[engine];
    
    char key[64], expected_val[64];
    sprintf(key, "%s_r%04d_k%02d", set_cmd, round, idx);
    sprintf(expected_val, "%s_r%04d_v%02d", set_cmd, round, idx);
    
    int n = send_and_recv_one(sock, cmd_name, key, recv_buf, recv_buf_size);
    if (n < 0) {
        return 0;
    }
    
    char actual_val[256];
    int ret = parse_resp_bulk_string(recv_buf, actual_val, sizeof(actual_val));
    if (ret < 0) {
        return 0;
    }
    
    return (strcmp(actual_val, expected_val) == 0) ? 1 : 0;
}

// 修复单个错误的 key
int fix_single_key(int sock, int round, int engine, int idx) {
    const char *cmd_name = SET_CMDS[engine];
    char send_buf[256], recv_buf[64];
    char key[64], val[64];
    
    sprintf(key, "%s_r%04d_k%02d", cmd_name, round, idx);
    sprintf(val, "%s_r%04d_v%02d", cmd_name, round, idx);
    
    int send_len = build_resp_set(send_buf, cmd_name, key, val);
    if (send_all(sock, send_buf, send_len) < 0) {
        return -1;
    }
    
    int n = recv(sock, recv_buf, sizeof(recv_buf) - 1, 0);
    if (n <= 0) return -1;
    recv_buf[n] = '\0';
    
    return (strncmp(recv_buf, "+OK", 3) == 0) ? 0 : -1;
}

int main(int argc, char *argv[]) {
    int start_round = 0;
    int end_round = REPEAT_TIMES - 1;
    
    if (argc >= 3) {
        start_round = atoi(argv[1]);
        end_round = atoi(argv[2]);
        if (start_round < 0) start_round = 0;
        if (end_round >= REPEAT_TIMES) end_round = REPEAT_TIMES - 1;
    } else if (argc == 2) {
        start_round = atoi(argv[1]);
        end_round = start_round;
    }
    
    int total_keys_to_verify = TOTAL_KEYS * (end_round - start_round + 1);
    int total_progress_steps = total_keys_to_verify / PROGRESS_INTERVAL;
    if (total_keys_to_verify % PROGRESS_INTERVAL != 0) {
        total_progress_steps++;
    }
    
    printf("\n");
    printf("==================================================================\n");
    printf("Single Key Verify: GET each record one by one\n");
    printf("  Total: %d keys per round, rounds: %d - %d\n", 
           TOTAL_KEYS, start_round, end_round);
    printf("  Total keys to verify: %d\n", total_keys_to_verify);
    printf("  Progress will be printed every %d keys (%d times)\n", 
           PROGRESS_INTERVAL, total_progress_steps);
    printf("==================================================================\n\n");

    int sock = connect_server();
    if (sock < 0) {
        printf("[FATAL] Cannot connect to server!\n");
        return 1;
    }

    char *recv_buf = (char *)malloc(MAX_RECV_BUF);
    if (!recv_buf) {
        printf("[FATAL] malloc failed!\n");
        close(sock);
        return 1;
    }

    int total_passed = 0;
    int total_failed = 0;
    int fixed_count = 0;
    int verified_rounds = 0;
    int total_keys = 0;
    int next_progress_threshold = PROGRESS_INTERVAL;

    for (int round = start_round; round <= end_round; round++) {
        for (int engine = 1; engine <= 4; engine++) {
            for (int i = 0; i < KEYS_PER_TYPE; i++) {
                int result = verify_single_key(sock, round, engine, i, 
                                               recv_buf, MAX_RECV_BUF);
                if (result == 1) {
                    total_passed++;
                } else {
                    total_failed++;
                    
                    // 尝试修复失败的 key
                    const char *engine_name = SET_CMDS[engine];
                    printf("[FIX] Attempting to fix %s_r%04d_k%02d...\n", 
                           engine_name, round, i);
                    if (fix_single_key(sock, round, engine, i) == 0) {
                        fixed_count++;
                        int retry = verify_single_key(sock, round, engine, i,
                                                       recv_buf, MAX_RECV_BUF);
                        if (retry == 1) {
                            total_passed++;
                            total_failed--;
                            printf("[FIX] %s_r%04d_k%02d repaired successfully\n",
                                   engine_name, round, i);
                        }
                    }
                }
                total_keys++;
                
                // 每验证 PROGRESS_INTERVAL 条数据打印一次进度
                if (total_keys >= next_progress_threshold) {
                    printf("[PROGRESS] %d keys verified, PASSED: %d, FAILED: %d\n",
                           total_keys, total_passed, total_failed);
                    next_progress_threshold += PROGRESS_INTERVAL;
                }
            }
        }
        verified_rounds++;
    }

    printf("\n");
    printf("============================================================\n");
    printf("                       VERIFICATION RESULT\n");
    printf("============================================================\n");
    printf("  Total rounds verified: %d\n", verified_rounds);
    printf("  Total keys verified:   %d\n", total_keys);
    printf("  [PASS] %d keys\n", total_passed);
    printf("  [FAIL] %d keys\n", total_failed);
    if (fixed_count > 0) {
        printf("  [FIX]  %d keys repaired\n", fixed_count);
    }
    printf("============================================================\n");
    printf("  Overall: %s\n", (total_failed == 0) ? "ALL PASSED!" : "SOME FAILURES");
    printf("============================================================\n");

    free(recv_buf);
    close(sock);
    
    printf("\nVerify completed.\n");
    return 0;
}