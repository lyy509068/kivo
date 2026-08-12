#define _GNU_SOURCE
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <sys/socket.h>
#include <arpa/inet.h>
#include <errno.h>

#define SERVER_IP "192.168.37.128"
#define SERVER_PORT 2000

// 验证的 key 范围：从 0 到 TOTAL_VERIFY - 1
#define TOTAL_VERIFY 100000

const char* GET_CMDS[] = {"", "GET", "RGET", "HGET", "SGET"};

int connect_server() {
    int sock = socket(AF_INET, SOCK_STREAM, 0);
    struct sockaddr_in serv_addr;
    serv_addr.sin_family = AF_INET;
    serv_addr.sin_port = htons(SERVER_PORT);
    inet_pton(AF_INET, SERVER_IP, &serv_addr.sin_addr);
    if (connect(sock, (struct sockaddr *)&serv_addr, sizeof(serv_addr)) < 0) return -1;
    return sock;
}

int build_resp_get(char *buf, const char *cmd, const char *key) {
    return sprintf(buf, "*2\r\n$%zu\r\n%s\r\n$%zu\r\n%s\r\n",
                   strlen(cmd), cmd, strlen(key), key);
}

// 流式RESP解析器：解析Bulk String回复
// 返回 0 成功，-1 失败
int parse_resp_bulk(const char *buf, int buflen, char **out_val, int *out_val_len) {
    if (buflen < 1) return -1;
    if (buf[0] == '-') return -1;
    
    if (buf[0] == '$') {
        char *crlf1 = strstr(buf, "\r\n");
        if (!crlf1) return -1;
        
        int str_len = atoi(buf + 1);
        if (str_len == -1) return -1;  // nil
        
        char *data_start = crlf1 + 2;
        int total_needed = (crlf1 - buf) + 2 + str_len + 2;
        if (buflen < total_needed) return -1;
        
        *out_val = data_start;
        *out_val_len = str_len;
        return 0;
    }
    
    return -1;
}

// 接收完整的一个 RESP 回复
int recv_resp(int sock, char *buf, int bufsize) {
    int total = 0;
    while (total < bufsize) {
        int n = recv(sock, buf + total, bufsize - total, 0);
        if (n <= 0) return -1;
        total += n;
        buf[total] = '\0';
        
        // 检查是否收到完整回复
        if (buf[0] == '-') {
            if (strstr(buf, "\r\n")) return total;
        } else if (buf[0] == '+') {
            if (strstr(buf, "\r\n")) return total;
        } else if (buf[0] == '$') {
            char *crlf1 = strstr(buf, "\r\n");
            if (!crlf1) continue;
            int str_len = atoi(buf + 1);
            if (str_len == -1) return total;
            int needed = (crlf1 - buf) + 2 + str_len + 2;
            if (total >= needed) return total;
        }
    }
    return -1;
}

int main() {
    int sock = connect_server();
    if (sock < 0) {
        printf("[FATAL] Cannot connect to server!\n");
        return -1;
    }

    // 固定引擎轮转（与测试客户端一致）
    const char *get_cmds[] = {"GET", "RGET", "HGET", "SGET"};

    char send_buf[256];
    char recv_buf[1024];

    printf("Verifying %d keys (fixed order: GET → RGET → HGET → SGET)...\n", TOTAL_VERIFY);

    long long ok_count = 0;
    long long fail_count = 0;
    long long not_found = 0;

    for (int i = 0; i < TOTAL_VERIFY; i++) {
        int eng_idx = i % 4;       // 0=GET, 1=RGET, 2=HGET, 3=SGET
        const char *get_cmd = get_cmds[eng_idx];
        
        char key[32];
        sprintf(key, "key_%06d", i);
        
        int send_len = build_resp_get(send_buf, get_cmd, key);
        if (send(sock, send_buf, send_len, 0) != send_len) {
            printf("[FATAL] Send failed at i=%d\n", i);
            goto cleanup;
        }
        
        int recv_len = recv_resp(sock, recv_buf, sizeof(recv_buf));
        if (recv_len <= 0) {
            printf("[FATAL] Recv failed at i=%d\n", i);
            goto cleanup;
        }
        
        char *val;
        int val_len;
        if (parse_resp_bulk(recv_buf, recv_len, &val, &val_len) != 0) {
            not_found++;
            printf("[NOT_FOUND] key=%s\n", key);
        } else {
            char expected[32];
            int expected_len = sprintf(expected, "value_%06d", i);
            
            if (val_len == expected_len && memcmp(val, expected, val_len) == 0) {
                ok_count++;
            } else {
                fail_count++;
                printf("[FAIL] key=%s, expected='%s', got='%.*s'\n",
                       key, expected, val_len, val);
            }
        }

        if ((i + 1) % 10000 == 0) {
            printf("  Verified: %d keys (ok=%lld, fail=%lld, not_found=%lld)\n",
                   i + 1, ok_count, fail_count, not_found);
        }
    }

    printf("\n=== Verification Complete ===\n");
    printf("Total:  %d\n", TOTAL_VERIFY);
    printf("OK:     %lld\n", ok_count);
    printf("FAIL:   %lld\n", fail_count);
    printf("NOT_FOUND: %lld\n", not_found);
    
    if (fail_count == 0 && not_found == 0) {
        printf("\n✓ All %d keys verified successfully!\n", TOTAL_VERIFY);
    } else {
        printf("\n✗ Verification failed!\n");
    }

cleanup:
    close(sock);
    return (fail_count > 0 || not_found > 0) ? 1 : 0;
}