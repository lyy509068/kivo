#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <sys/socket.h>
#include <arpa/inet.h>
#include <errno.h>
#include <sys/time.h>

#define SERVER_IP "127.0.0.1"
#define SERVER_PORT 2000
#define MAX_SEND_BUF (10 * 1024 * 1024)
#define MAX_RECV_BUF (64 * 1024)

const char* SET_CMDS[] = {"", "SET", "RSET", "HSET", "SSET"};

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

int send_all(int fd, const char *buf, int len) {
    int sent = 0;
    while (sent < len) {
        int n = send(fd, buf + sent, len - sent, 0);
        if (n <= 0) return -1;
        sent += n;
    }
    return sent;
}

int recv_all(int fd, char *buf, int len, int timeout_sec) {
    struct timeval tv;
    tv.tv_sec = timeout_sec;
    tv.tv_usec = 0;
    setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));

    int received = 0;
    while (received < len) {
        int n = recv(fd, buf + received, len - received, 0);
        if (n <= 0) {
            printf("[TIMEOUT] received=%d/%d\n", received, len);
            return -1;
        }
        received += n;
    }
    return received;
}

// 读取整个文件到内存
char* read_file(const char *filename, size_t *out_len) {
    FILE *fp = fopen(filename, "rb");  // 二进制模式
    if (!fp) {
        printf("Cannot open: %s\n", filename);
        return NULL;
    }
    
    fseek(fp, 0, SEEK_END);
    *out_len = ftell(fp);
    fseek(fp, 0, SEEK_SET);
    
    char *buf = (char *)malloc(*out_len);
    if (!buf) {
        fclose(fp);
        return NULL;
    }
    
    size_t read_len = fread(buf, 1, *out_len, fp);
    fclose(fp);
    
    if (read_len != *out_len) {
        printf("fread partial: %zu/%zu\n", read_len, *out_len);
        *out_len = read_len;
    }
    return buf;
}

// 构建 RESP SET（二进制安全）
int build_resp_set_raw(char *buf, const char *cmd, const char *key, 
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

// 构建 RESP GET
int build_resp_get(char *buf, const char *cmd, const char *key) {
    int cmd_len = strlen(cmd);
    int key_len = strlen(key);
    return sprintf(buf, "*2\r\n$%d\r\n%s\r\n$%d\r\n%s\r\n",
                   cmd_len, cmd, key_len, key);
}

// 解析 GET 的 RESP 回复头，返回 body_len，并把 body 开始位置写入 *out_body_start
// 只解析缓冲区内的头部，body 可能跨越多次 recv
int parse_get_reply_header(char *buf, int buf_len, char **out_body_start, int *out_body_len) {
    if (buf_len < 4) return -1;
    if (buf[0] != '$') return -1;
    
    // 找第一个 \r\n
    char *crlf = strstr(buf, "\r\n");
    if (!crlf) return -1;
    
    int data_len = atoi(buf + 1);
    *out_body_len = data_len;
    *out_body_start = crlf + 2;
    return 0;
}

// 测试单个文件：SET → GET → 比对
int test_file(int sock, const char *set_cmd, const char *get_cmd, 
              const char *filename, int file_index) {
    
    printf("File %d: %s\n", file_index, filename);

    // 1. 读取文件
    size_t file_len = 0;
    char *file_data = read_file(filename, &file_len);
    if (!file_data) return -1;
    
    printf("File size: %zu bytes\n", file_len);

    // 2. 构造 key
    char key[32];
    snprintf(key, sizeof(key), "file_%d", file_index);

    // 3. SET
    char *send_buf = (char *)malloc(MAX_SEND_BUF);
    int cmd_len = build_resp_set_raw(send_buf, set_cmd, key, file_data, file_len);
    printf("SET: key=%s, cmd_len=%d\n", key, cmd_len);

    if (send_all(sock, send_buf, cmd_len) < 0) {
        printf("SET send failed\n");
        free(send_buf); free(file_data);
        return -1;
    }

    char reply[256];
    memset(reply, 0, sizeof(reply));
    if (recv_all(sock, reply, 5, 10) < 0) {
        printf("SET reply timeout\n");
        free(send_buf); free(file_data);
        return -1;
    }

    if (strncmp(reply, "+OK\r\n", 5) != 0) {
        printf("SET failed: %.*s\n", 32, reply);
        free(send_buf); free(file_data);
        return -1;
    }
    printf("SET OK\n");

    // 4. GET
    cmd_len = build_resp_get(send_buf, get_cmd, key);
    printf("GET: key=%s\n", key);

    if (send_all(sock, send_buf, cmd_len) < 0) {
        printf("GET send failed\n");
        free(send_buf); free(file_data);
        return -1;
    }

    // 5. 接收 GET 回复
    // 先收一个较大的头部
    memset(reply, 0, sizeof(reply));
    if (recv_all(sock, reply, 32, 10) < 0) {
        printf("GET reply header timeout\n");
        free(send_buf); free(file_data);
        return -1;
    }

    char *body_start = NULL;
    int body_len = 0;
    if (parse_get_reply_header(reply, 32, &body_start, &body_len) < 0) {
        printf("GET reply parse error: %s\n", reply);
        free(send_buf); free(file_data);
        return -1;
    }

    if (body_len == -1) {
        printf("Key not found (nil bulk string)\n");
        free(send_buf); free(file_data);
        return -1;
    }

    printf("GET reply: body_len=%d (expected %zu)\n", body_len, file_len);

    // 6. 接收完整 body
    char *received_body = (char *)malloc(body_len);
    int already_in_header = 32 - (body_start - reply);
    
    if (already_in_header > 0 && already_in_header <= body_len) {
        memcpy(received_body, body_start, already_in_header);
    }
    
    int remaining = body_len - already_in_header;
    if (remaining > 0) {
        if (recv_all(sock, received_body + already_in_header, remaining, 10) < 0) {
            printf("GET body timeout (remaining=%d)\n", remaining);
            free(received_body); free(send_buf); free(file_data);
            return -1;
        }
    }
    
    // 接收末尾的 \r\n
    char tail[2];
    if (recv_all(sock, tail, 2, 5) < 0) {
        printf("GET tail timeout\n");
        free(received_body); free(send_buf); free(file_data);
        return -1;
    }

    // 7. 逐字节比对
    if (body_len != (int)file_len) {
        printf("LENGTH MISMATCH: sent=%zu, got=%d\n", file_len, body_len);
        free(received_body); free(send_buf); free(file_data);
        return -1;
    }

    int mismatch_count = 0;
    for (int i = 0; i < body_len; i++) {
        if (received_body[i] != file_data[i]) {
            mismatch_count++;
            if (mismatch_count <= 10) {
                printf("  Byte %d: sent=0x%02x('%c'), got=0x%02x('%c')\n",
                       i, 
                       (unsigned char)file_data[i], 
                       file_data[i] >= 32 ? file_data[i] : '?',
                       (unsigned char)received_body[i],
                       received_body[i] >= 32 ? received_body[i] : '?');
            }
        }
    }

    if (mismatch_count == 0) {
        printf("DATA MATCH: All %d bytes verified!\n", body_len);
        printf("--------------------------------------------------\n");
    } else {
        printf("DATA MISMATCH: %d bytes differ out of %d\n", mismatch_count, body_len);
    }

    free(received_body);
    free(send_buf);
    free(file_data);
    return (mismatch_count == 0) ? 0 : -1;
}

int main(int argc, char *argv[]) {
    printf("=================================================\n");
    printf("        KVS File Insert & Verify Test            \n");
    printf("=================================================\n");

    if (argc < 2) {
        printf("Usage: %s <engine_type>\n", argv[0]);
        printf("  1: Array  2: RBTree  3: Hash  4: SkipList\n");
        return 1;
    }

    int engine_type = atoi(argv[1]);
    if (engine_type < 1 || engine_type > 4) {
        printf("Invalid engine type!\n");
        return 1;
    }

    const char *set_cmd = SET_CMDS[engine_type];
    char get_cmd[8];
    if (strcmp(set_cmd, "SET") == 0) strcpy(get_cmd, "GET");
    else if (strcmp(set_cmd, "RSET") == 0) strcpy(get_cmd, "RGET");
    else if (strcmp(set_cmd, "HSET") == 0) strcpy(get_cmd, "HGET");
    else strcpy(get_cmd, "SGET");

    printf("Engine: %s / %s\n\n", set_cmd, get_cmd);

    int sock = connect_server();
    if (sock < 0) {
        printf("Cannot connect to server!\n");
        return 1;
    }

    int passed = 0, failed = 0;
    char filename[64];

    for (int i = 1; i <= 5; i++) {
        snprintf(filename, sizeof(filename), "本地文件%d.txt", i);
        
        int ret = test_file(sock, set_cmd, get_cmd, filename, i);
        if (ret == 0) {
            passed++;
        } else {
            failed++;
        }
    }

    close(sock);

    printf("Results: %d passed, %d failed\n", passed, failed);

    return (failed > 0) ? 1 : 0;
}