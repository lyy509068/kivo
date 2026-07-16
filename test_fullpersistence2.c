#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <unistd.h>
#include <arpa/inet.h>
#include <sys/socket.h>

#define SERVER_PORT 2000
#define TOTAL_RECORDS 100000   
#define RECORDS_PER_ENGINE 25000
#define SNAPSHOT_FILE "kvstore.snap"

const char* GET_CMDS[] = {"", "GET", "RGET", "HGET", "SGET"};
const char* ENGINE_NAMES[] = {"", "Array", "RBTree", "Hash", "SkipList"};

int build_resp_request(char *buf, const char *cmd, const char *key) {
    return sprintf(buf, "*2\r\n$%zu\r\n%s\r\n$%zu\r\n%s\r\n", 
                   strlen(cmd), cmd, strlen(key), key);
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

// 解析RESP响应，提取值部分
int parse_resp_value(const char *resp, char *value, int max_len) {
    // 处理简单的 "$<len>\r\n<value>\r\n" 格式
    const char *ptr = resp;
    
    // 跳过状态响应 (+OK\r\n 等)
    if (*ptr == '+') {
        strncpy(value, ptr, max_len);
        return 0;
    }
    
    // 跳过错误响应 (-ERR\r\n 等)
    if (*ptr == '-') {
        strncpy(value, ptr, max_len);
        return -1;
    }
    
    // 处理批量字符串响应 ($<len>\r\n<value>\r\n)
    if (*ptr == '$') {
        ptr++; // 跳过 '$'
        int len = atoi(ptr);
        if (len < 0) {
            strcpy(value, "(nil)");
            return -1;
        }
        
        // 跳过长度数字和\r\n
        while (*ptr != '\r') ptr++;
        ptr += 2; // 跳过 \r\n
        
        // 复制值
        int copy_len = len < max_len - 1 ? len : max_len - 1;
        strncpy(value, ptr, copy_len);
        value[copy_len] = '\0';
        return 0;
    }
    
    return -1;
}

int main() {
    setvbuf(stdout, NULL, _IONBF, 0);

    char send_buf[1024], recv_buf[4096];
    int send_len;

    printf("======================================================\n");
    printf("[STAGE 2] RECONNECTING & VERIFYING SNAPSHOT (ALL ENGINES)\n");
    printf("======================================================\n\n");

    int sock = connect_server();
    if (sock < 0) {
        printf("[FATAL] Cannot connect to server. Did you remember to restart it manually?\n");
        return 1;
    }

    int global_index = 0;
    int total_verified = 0;
    int total_errors = 0;

    // 依次验证四个引擎的数据
    for (int engine_type = 1; engine_type <= 4; engine_type++) {
        const char *get_cmd = GET_CMDS[engine_type];
        const char *engine_name = ENGINE_NAMES[engine_type];
        
        printf("\n[ENGINE %d: %s] Verifying %d records using [%s]...\n", 
               engine_type, engine_name, RECORDS_PER_ENGINE, get_cmd);
        printf("  Key range: key_%06d to key_%06d\n", global_index, global_index + RECORDS_PER_ENGINE - 1);

        int engine_verified = 0;
        int engine_errors = 0;

        for (int i = 0; i < RECORDS_PER_ENGINE; i++) {
            char key[32], expected_val[64];
            sprintf(key, "key_%06d", global_index);
            sprintf(expected_val, "val_%06d_%s", global_index, engine_name);

            send_len = build_resp_request(send_buf, get_cmd, key);
            if (send_all(sock, send_buf, send_len) < 0) {
                printf("   Server disconnected during send at index: %d\n", global_index);
                close(sock);
                return 1;
            }
            
            memset(recv_buf, 0, sizeof(recv_buf));
            int rlen = recv(sock, recv_buf, sizeof(recv_buf) - 1, 0);
            if (rlen <= 0) {
                printf("   Server disconnected during recv at index: %d\n", global_index);
                close(sock);
                return 1;
            }
            recv_buf[rlen] = '\0';
            
            // 解析响应获取实际值
            char actual_val[256];
            if (parse_resp_value(recv_buf, actual_val, sizeof(actual_val)) == 0) {
                if (strcmp(actual_val, expected_val) == 0) {
                    engine_verified++;
                } else {
                    engine_errors++;
                    if (engine_errors <= 5) {  // 只显示前5个错误
                        printf("   Data Mismatch at %s!\n", key);
                        printf("     Expected: %s\n", expected_val);
                        printf("     Got:      %s\n", actual_val);
                        printf("     Raw RESP: %s\n", recv_buf);
                    }
                }
            } else {
                engine_errors++;
                if (engine_errors <= 5) {
                    printf("   Failed to parse response at %s: %s\n", key, recv_buf);
                }
            }

            if ((i + 1) % 5000 == 0) {
                printf("  -> Progress: %d/%d records verified...\n", i + 1, RECORDS_PER_ENGINE);
            }
            
            global_index++;
        }

        printf("[ENGINE %d: %s] Verified: %d/%d, Errors: %d\n", 
               engine_type, engine_name, engine_verified, RECORDS_PER_ENGINE, engine_errors);
        
        total_verified += engine_verified;
        total_errors += engine_errors;
    }

    if (total_errors == 0) {
        printf("[RESULT] ALL %d RECORDS VERIFIED SUCCESSFULLY FROM SNAPSHOT!\n", total_verified);
    } else {
        printf("[RESULT]  VERIFICATION FAILED! %d errors found.\n", total_errors);
    }

    // 验证通过，删除快照文件
    if (total_errors == 0) {
        printf("\n[PURGE] Purging snapshot file: %s...\n", SNAPSHOT_FILE);
        if (remove(SNAPSHOT_FILE) == 0) {
            printf("[Success] %s deleted successfully.\n", SNAPSHOT_FILE);
        } else {
            printf("[Warning] Snapshot file not found or couldn't be deleted.\n");
        }
    } else {
        printf("\n[WARNING] Snapshot file NOT deleted due to verification errors.\n");
        printf("  File kept for debugging: %s\n", SNAPSHOT_FILE);
    }

    // 断开连接
    close(sock);
    
    return total_errors == 0 ? 0 : 1;
}