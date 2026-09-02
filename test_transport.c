#define _GNU_SOURCE
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <sys/socket.h>
#include <arpa/inet.h>
#include <sys/time.h>
#include <errno.h>

#define SERVER_IP "192.168.88.128"
#define SERVER_PORT 2000
#define TOTAL_COMMANDS 100000

#define RECV_BUF_SIZE (2 * 1024 * 1024)
#define SEND_BUF_SIZE (64 * 1024)

const char* SET_CMDS[] = {"", "SET", "RSET", "HSET", "SSET"};

double get_time_sec() {
    struct timeval tv;
    gettimeofday(&tv, NULL);
    return tv.tv_sec + (double)tv.tv_usec / 1000000.0;
}

// 流式RESP解析器：解析+OK\r\n回复，统计数量
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

int connect_server() {
    int sock = socket(AF_INET, SOCK_STREAM, 0);
    struct sockaddr_in serv_addr;
    serv_addr.sin_family = AF_INET;
    serv_addr.sin_port = htons(SERVER_PORT);
    inet_pton(AF_INET, SERVER_IP, &serv_addr.sin_addr);
    if (connect(sock, (struct sockaddr *)&serv_addr, sizeof(serv_addr)) < 0) return -1;
    return sock;
}

int build_resp_set(char *buf, const char *cmd, const char *key, const char *val) {
    return sprintf(buf, "*3\r\n$%zu\r\n%s\r\n$%zu\r\n%s\r\n$%zu\r\n%s\r\n",
                   strlen(cmd), cmd, strlen(key), key, strlen(val), val);
}

int main() {
    int sock = connect_server();
    if (sock < 0) {
        printf("[FATAL] Cannot connect to server!\n");
        return -1;
    }

    char *recv_buf = malloc(RECV_BUF_SIZE);
    char *send_buf = malloc(SEND_BUF_SIZE);
    if (!recv_buf || !send_buf) {
        printf("[FATAL] malloc failed!\n");
        close(sock);
        return -1;
    }

    // 固定引擎轮转：SET → RSET → HSET → SSET
    const char *eng_cmds[] = {"SET", "RSET", "HSET", "SSET"};

    printf("QPS Test (single-command mode): %d commands (fixed order: SET → RSET → HSET → SSET)\n", TOTAL_COMMANDS);

    int recv_buf_len = 0;
    int acked_cnt = 0;
    long eng_counts[5] = {0};

    double start_time = get_time_sec();

    for (int sent_cnt = 0; sent_cnt < TOTAL_COMMANDS; sent_cnt++) {
        // 1. 构造单条命令（固定轮转引擎）
        int eng_idx = sent_cnt % 4;          // 0=SET, 1=RSET, 2=HSET, 3=SSET
        int eng = eng_idx + 1;               // 1-4

        char key[32], val[32];
        sprintf(key, "key_%06d", sent_cnt);
        sprintf(val, "value_%06d", sent_cnt);

        char cmd[256];
        int cmd_len = build_resp_set(cmd, eng_cmds[eng_idx], key, val);

        // 2. 发送命令
        if (send(sock, cmd, cmd_len, 0) < 0) {
            perror("send");
            goto cleanup;
        }

        // 3. 接收并解析响应（直到收到至少一条完整回复）
        while (1) {
            int n = recv(sock, recv_buf + recv_buf_len, RECV_BUF_SIZE - recv_buf_len, 0);
            if (n <= 0) {
                if (n == 0) printf("\nServer closed connection.\n");
                else perror("recv");
                goto cleanup;
            }
            recv_buf_len += n;

            int parsed_bytes = 0;
            int count = parse_resp_replies(recv_buf, recv_buf_len, &parsed_bytes);
            if (count > 0) {
                // 成功解析出至少一条回复（正常情况下应为1）
                acked_cnt += count;
                // 移除已解析的数据
                if (parsed_bytes > 0) {
                    memmove(recv_buf, recv_buf + parsed_bytes, recv_buf_len - parsed_bytes);
                    recv_buf_len -= parsed_bytes;
                }
                eng_counts[eng]++;   // 记录该引擎的成功次数
                break;               // 跳出接收循环，继续下一条命令
            }
            // 否则数据不完整，继续接收
        }

        // 打印进度
        if (acked_cnt % 10000 == 0 && acked_cnt > 0) {
            double elapsed = get_time_sec() - start_time;
            printf("  [%d/%d] QPS: %.0f\n", acked_cnt, TOTAL_COMMANDS, acked_cnt / elapsed);
        }
    }

    double end_time = get_time_sec();
    double elapsed = end_time - start_time;

    printf("\n=== QPS Test Complete ===\n");
    printf("Total:       %d commands\n", TOTAL_COMMANDS);
    printf("Time:        %.3f seconds\n", elapsed);
    printf("Average QPS: %.0f\n", TOTAL_COMMANDS / elapsed);
    printf("\nEngine distribution (fixed rotation):\n");
    printf("  SET  (Array):    %ld (%.1f%%)\n", eng_counts[1], eng_counts[1] * 100.0 / TOTAL_COMMANDS);
    printf("  RSET (RBTree):   %ld (%.1f%%)\n", eng_counts[2], eng_counts[2] * 100.0 / TOTAL_COMMANDS);
    printf("  HSET (Hash):     %ld (%.1f%%)\n", eng_counts[3], eng_counts[3] * 100.0 / TOTAL_COMMANDS);
    printf("  SSET (SkipList): %ld (%.1f%%)\n", eng_counts[4], eng_counts[4] * 100.0 / TOTAL_COMMANDS);

cleanup:
    free(recv_buf);
    free(send_buf);
    close(sock);
    return 0;
}