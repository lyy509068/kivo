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
#define MAX_FILE_SIZE (128 * 1024)          // 最大文件 128KB

const char* CMDS[] = {"", "SET", "RSET", "HSET", "SSET"};
const char* FILES[] = {"", 
    "本地文件1.txt", "本地文件2.txt", "本地文件3.txt", 
    "本地文件4.txt", "本地文件5.txt"
};

int connect_server() {
    int sock = socket(AF_INET, SOCK_STREAM, 0);
    struct sockaddr_in serv_addr;
    serv_addr.sin_family = AF_INET;
    serv_addr.sin_port = htons(SERVER_PORT);
    inet_pton(AF_INET, SERVER_IP, &serv_addr.sin_addr);
    if (connect(sock, (struct sockaddr *)&serv_addr, sizeof(serv_addr)) < 0) return -1;
    return sock;
}

// 读取文件内容
char* read_file(const char *filename, size_t *out_len) {
    FILE *fp = fopen(filename, "rb");
    if (!fp) { *out_len = 0; return NULL; }
    
    fseek(fp, 0, SEEK_END);
    *out_len = ftell(fp);
    fseek(fp, 0, SEEK_SET);
    
    char *buf = (char *)malloc(*out_len);
    fread(buf, 1, *out_len, fp);
    fclose(fp);
    return buf;
}

// 构建 RESP SET 命令（二进制安全，value 可能含 \0）
int build_resp_raw(char *buf, const char *cmd, const char *key,
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

// 检查 +OK 回复
int check_ok(int sock) {
    char buf[8];
    int n = recv(sock, buf, 5, 0);
    if (n != 5) return -1;
    return (strncmp(buf, "+OK\r\n", 5) == 0) ? 0 : -1;
}

// 按比例选引擎：1% array, 33% rbtree, 33% hash, 33% skiplist
int pick_engine() {
    int r = rand() % 100;
    if (r < 1)  return 1;
    if (r < 34) return 2;
    if (r < 67) return 3;
    return 4;
}

int main() {
    srand(time(NULL));

    // 预加载 5 个文件
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

    printf("\nInserting ~1GB data (1%% array, 33%% rbtree, 33%% hash, 33%% skiplist)...\n");

    char send_buf[256 * 1024];  // 256KB 发送缓冲区（够装最大文件+头）
    char key[32];
    long long total_bytes = 0;
    long long total_cmds = 0;
    long long eng_counts[5] = {0};

    while (total_bytes < TARGET_SIZE) {
        int eng = pick_engine();
        int file_idx = (rand() % 5) + 1;  // 随机选文件 1-5
        
        sprintf(key, "k%lld_f%d", total_cmds, file_idx);  // 每个 key 唯一
        
        int len = build_resp_raw(send_buf, CMDS[eng], key,
                                  file_data[file_idx], file_len[file_idx]);
        
        if (send(sock, send_buf, len, 0) != len) {
            printf("[FATAL] Send failed\n");
            goto cleanup;
        }
        
        if (check_ok(sock) != 0) {
            printf("[FATAL] Bad reply at cmd=%lld\n", total_cmds);
            goto cleanup;
        }
        
        total_bytes += len;
        total_cmds++;
        eng_counts[eng]++;

        if (total_cmds % 10000 == 0) {
            printf("  Progress: %.2f MB, %lld commands...\n",
                   total_bytes / (1024.0 * 1024.0), total_cmds);
        }
    }

    printf("\n=== Insert Complete ===\n");
    printf("Total bytes:   %.2f MB\n", total_bytes / (1024.0 * 1024.0));
    printf("Total commands: %lld\n", total_cmds);
    printf("Array   (SET ): %lld (%.1f%%)\n", eng_counts[1], eng_counts[1] * 100.0 / total_cmds);
    printf("Rbtree  (RSET): %lld (%.1f%%)\n", eng_counts[2], eng_counts[2] * 100.0 / total_cmds);
    printf("Hash    (HSET): %lld (%.1f%%)\n", eng_counts[3], eng_counts[3] * 100.0 / total_cmds);
    printf("Skiplist(SSET): %lld (%.1f%%)\n", eng_counts[4], eng_counts[4] * 100.0 / total_cmds);

cleanup:
    close(sock);
    for (int i = 1; i <= 5; i++) free(file_data[i]);
    return 0;
}