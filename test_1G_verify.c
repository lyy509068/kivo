#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <sys/socket.h>
#include <arpa/inet.h>

#define SERVER_IP "127.0.0.1"
#define SERVER_PORT 2000

const char* FILES[] = {"",
    "本地文件1.txt", "本地文件2.txt", "本地文件3.txt",
    "本地文件4.txt", "本地文件5.txt"
};

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

char* read_file(const char *filename, size_t *out_len) {
    FILE *fp = fopen(filename, "rb");
    if (!fp) {
        *out_len = 0;
        return NULL;
    }

    fseek(fp, 0, SEEK_END);
    *out_len = ftell(fp);
    fseek(fp, 0, SEEK_SET);

    char *buf = (char *)malloc(*out_len ? *out_len : 1);
    if (!buf) {
        fclose(fp);
        *out_len = 0;
        return NULL;
    }

    size_t n = fread(buf, 1, *out_len, fp);
    if (n != *out_len) {
        free(buf);
        fclose(fp);
        *out_len = 0;
        return NULL;
    }

    fclose(fp);
    return buf;
}

// 构建 GET 命令（需要知道用哪个引擎）
int build_resp_get(char *buf, const char *cmd, const char *key) {
    int cmd_len = strlen(cmd);
    int key_len = strlen(key);
    int total = 0;
    
    total += sprintf(buf + total, "*2\r\n$%d\r\n", cmd_len);
    memcpy(buf + total, cmd, cmd_len); total += cmd_len;
    total += sprintf(buf + total, "\r\n$%d\r\n", key_len);
    memcpy(buf + total, key, key_len); total += key_len;
    memcpy(buf + total, "\r\n", 2); total += 2;
    return total;
}

// 解析 RESP Bulk String 返回值
// 成功返回：$<len>\r\n<data>\r\n
// 失败返回：$-1\r\n（key 不存在）
int parse_resp_bulk(char *buf, int buflen, char **out_data, int *out_len) {
    if (buflen < 1) return -1;
    
    if (buf[0] == '-') {
        // 错误或不存在
        return -1;
    }
    
    if (buf[0] == '$') {
        int len = atoi(buf + 1);
        if (len == -1) return -1;  // nil
        
        char *crlf = strstr(buf, "\r\n");
        if (!crlf) return -1;
        crlf += 2;  // 跳过第一行 \r\n
        
        *out_data = crlf;
        *out_len = len;
        return 0;
    }
    
    return -1;
}

int recv_all(int sock, char *buf, int bufsize) {
    int total = 0;
    while (total < bufsize) {
        int n = recv(sock, buf + total, bufsize - total, 0);
        if (n <= 0) return -1;
        total += n;
        // 检查是否收到完整 RESP 响应（以 \r\n 结尾）
        if (total >= 2 && buf[total - 2] == '\r' && buf[total - 1] == '\n') {
            // 对于 Bulk String，需要检查是否收完数据部分
            if (buf[0] == '$') {
                int len = atoi(buf + 1);
                if (len == -1) break;  // $-1\r\n
                int expected = (strstr(buf, "\r\n") - buf) + 2 + len + 2;
                if (total >= expected) break;
            } else {
                break;
            }
        }
    }
    return total;
}

int main() {
    // 加载预期文件内容
    size_t file_len[6] = {0};
    char *file_data[6] = {NULL};
    for (int i = 1; i <= 5; i++) {
        file_data[i] = read_file(FILES[i], &file_len[i]);
        if (!file_data[i]) {
            printf("[FATAL] Cannot read %s\n", FILES[i]);
            return -1;
        }
        printf("Loaded %s: %zu bytes\n", FILES[i], file_len[i]);
    }

    int sock = connect_server();
    if (sock < 0) {
        printf("[FATAL] Cannot connect to server!\n");
        return -1;
    }

    // 固定规则（和测试客户端一致）
    const char *get_cmds[]  = {"GET", "RGET", "HGET", "SGET"};
    const int file_order[] = {1, 2, 3, 4, 5};

    printf("\nVerifying ~1GB inserted data...\n");

    char send_buf[256];
    char recv_buf[256 * 1024];
    char key[32];
    long long total_cmds = 0;
    long long ok_count = 0;
    long long fail_count = 0;

    // 验证命令数 = 插入时的命令数（计算方式相同）
    // 这里保守验证 1GB 的 key，直接循环直到第一个不存在的 key
    while (1) {
        int eng_idx = total_cmds % 4;
        int file_idx = total_cmds % 5;
        
        sprintf(key, "file_%06lld", total_cmds);
        
        const char *get_cmd = get_cmds[eng_idx];
        int f_idx = file_order[file_idx];
        
        int send_len = build_resp_get(send_buf, get_cmd, key);
        if (send(sock, send_buf, send_len, 0) != send_len) {
            printf("[FATAL] Send failed at cmd=%lld\n", total_cmds);
            goto cleanup;
        }
        
        int recv_len = recv_all(sock, recv_buf, sizeof(recv_buf));
        if (recv_len <= 0) {
            printf("[FATAL] Recv failed at cmd=%lld\n", total_cmds);
            goto cleanup;
        }
        
        char *data;
        int data_len;
        if (parse_resp_bulk(recv_buf, recv_len, &data, &data_len) != 0) {
            // key 不存在，说明数据已经验证完了
            printf("\nKey %s not found. Verification ends.\n", key);
            break;
        }
        
        // 比对文件内容
        if (data_len == (int)file_len[f_idx] && 
            memcmp(data, file_data[f_idx], data_len) == 0) {
            ok_count++;
        } else {
            fail_count++;
            printf("[FAIL] key=%s expected %zu bytes, got %d bytes\n",
                   key, file_len[f_idx], data_len);
        }
        
        total_cmds++;

        if (total_cmds % 10000 == 0) {
            printf("  Verified: %lld keys (ok=%lld, fail=%lld)\n",
                   total_cmds, ok_count, fail_count);
        }
    }

    printf("\n=== Verification Complete ===\n");
    printf("Total verified: %lld\n", total_cmds);
    printf("OK:  %lld\n", ok_count);
    printf("FAIL: %lld\n", fail_count);
    
    if (fail_count == 0) {
        printf("\n✓ All data verified successfully!\n");
    } else {
        printf("\n✗ %lld keys have mismatched data!\n", fail_count);
    }

cleanup:
    close(sock);
    for (int i = 1; i <= 5; i++) free(file_data[i]);
    return (fail_count > 0) ? 1 : 0;
}