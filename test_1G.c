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
#define TARGET_SIZE (1024LL * 1024 * 1024)  // 1GB

const char* CMDS[] = {"", "SET", "RSET", "HSET", "SSET"};
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
    if (!fp) { *out_len = 0; return NULL; }

    if (fseek(fp, 0, SEEK_END) != 0) {
        fclose(fp);
        *out_len = 0;
        return NULL;
    }
    long len = ftell(fp);
    if (len < 0) {
        fclose(fp);
        *out_len = 0;
        return NULL;
    }
    *out_len = (size_t)len;

    if (fseek(fp, 0, SEEK_SET) != 0) {
        fclose(fp);
        *out_len = 0;
        return NULL;
    }

    char *buf = (char *)malloc(*out_len);
    if (!buf) {
        fclose(fp);
        *out_len = 0;
        return NULL;
    }

    size_t nread = fread(buf, 1, *out_len, fp);
    if (nread != *out_len) {
        free(buf);
        fclose(fp);
        *out_len = 0;
        return NULL;
    }

    fclose(fp);
    return buf;
}

int build_resp_set(char *buf, const char *cmd, const char *key,
                    const char *val, int val_len) {
    int cmd_len = strlen(cmd);
    int key_len = strlen(key);
    int total = 0;
    
    total += sprintf(buf + total, "*3\r\n$%d\r\n", cmd_len);
    memcpy(buf + total, cmd, cmd_len); total += cmd_len;
    total += sprintf(buf + total, "\r\n$%d\r\n", key_len);
    memcpy(buf + total, key, key_len); total += key_len;
    total += sprintf(buf + total, "\r\n$%d\r\n", val_len);
    memcpy(buf + total, val, val_len); total += val_len;
    memcpy(buf + total, "\r\n", 2); total += 2;
    return total;
}

int check_reply(int sock, const char *expect) {
    char buf[16];
    int n = recv(sock, buf, sizeof(buf) - 1, 0);
    if (n <= 0) return -1;
    buf[n] = '\0';
    return (strncmp(buf, expect, strlen(expect)) == 0) ? 0 : -1;
}

int main() {
    // 加载 5 个文件
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

    printf("\nInserting ~1GB data in fixed order...\n");

    char send_buf[256 * 1024];
    char key[32];
    long long total_bytes = 0;
    long long total_cmds = 0;
    
    // 固定轮转引擎：SET → RSET → HSET → SSET（每 4 个一轮）
    const char *eng_cmds[] = {"SET", "RSET", "HSET", "SSET"};
    // 固定轮转文件：1→2→3→4→5→1→2...（每 5 个一轮）
    const int file_order[] = {1, 2, 3, 4, 5};

    while (total_bytes < TARGET_SIZE) {
        int eng_idx = total_cmds % 4;         // 引擎索引 0-3
        int file_idx = total_cmds % 5;         // 文件索引 0-4
        
        sprintf(key, "file_%06lld", total_cmds);
        
        const char *cmd = eng_cmds[eng_idx];
        int f_idx = file_order[file_idx];
        
        int len = build_resp_set(send_buf, cmd, key,
                                  file_data[f_idx], file_len[f_idx]);
        
        if (send(sock, send_buf, len, 0) != len) {
            printf("[FATAL] Send failed at cmd=%lld\n", total_cmds);
            goto cleanup;
        }
        
        if (check_reply(sock, "+OK") != 0) {
            printf("[FATAL] Bad reply at cmd=%lld\n", total_cmds);
            goto cleanup;
        }
        
        total_bytes += len;
        total_cmds++;

        if (total_cmds % 10000 == 0) {
            printf("  Progress: %.2f MB, %lld commands\n",
                   total_bytes / (1024.0 * 1024.0), total_cmds);
        }
    }

    printf("\n=== Insert Complete ===\n");
    printf("Total bytes:   %.2f MB\n", total_bytes / (1024.0 * 1024.0));
    printf("Total commands: %lld\n", total_cmds);

cleanup:
    close(sock);
    for (int i = 1; i <= 5; i++) free(file_data[i]);
    return 0;
}