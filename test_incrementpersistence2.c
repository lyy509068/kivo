#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <sys/socket.h>
#include <arpa/inet.h>

#define SERVER_IP "127.0.0.1"
#define SERVER_PORT 2000
#define PER_ENGINE 25000
#define AOF_FILE "kvstore.aof" 

const char* GET_CMDS[] = {"", "GET", "RGET", "HGET", "SGET"};

int connect_server() {
    int sock = socket(AF_INET, SOCK_STREAM, 0);
    struct sockaddr_in serv_addr;
    serv_addr.sin_family = AF_INET;
    serv_addr.sin_port = htons(SERVER_PORT);
    inet_pton(AF_INET, SERVER_IP, &serv_addr.sin_addr);

    if (connect(sock, (struct sockaddr *)&serv_addr, sizeof(serv_addr)) < 0) {
        return -1;
    }
    return sock;
}

int build_resp_cmd(char *buf, const char *cmd, const char *key) {
    return sprintf(buf, "*2\r\n$%zu\r\n%s\r\n$%zu\r\n%s\r\n",
                   strlen(cmd), cmd, strlen(key), key);
}

int main() {
    char send_buf[256], recv_buf[256], key[32], val[32];
    int len;

    int sock = connect_server();
    if (sock < 0) {
        printf("[FATAL] Cannot connect to server. Did you manually restart it?\n");
        return 1;
    }

    int total = 0;
    int mismatch = 0;
    int key_index = 0;  // 全局递增的 key 编号
    
    for (int engine = 1; engine <= 4; engine++) {
        const char* get_cmd = GET_CMDS[engine];
        printf("\n[VERIFY] Engine %d [%s] verifying %d records...\n", engine, get_cmd, PER_ENGINE);
        
        for (int i = 0; i < PER_ENGINE; i++) {
            sprintf(key, "key_%06d", key_index);
            sprintf(val, "val_%06d", key_index);
            key_index++;
            
            len = build_resp_cmd(send_buf, get_cmd, key);
            send(sock, send_buf, len, 0);
            
            memset(recv_buf, 0, sizeof(recv_buf));
            int rlen = recv(sock, recv_buf, sizeof(recv_buf) - 1, 0);    
            
            if (rlen <= 0 || strstr(recv_buf, val) == NULL) {
                printf("\n[FATAL] Data mismatch at %s! Expected: %s, Got: [%s]\n", key, val, recv_buf);
                mismatch++;
                if (mismatch > 10) {
                    printf("  ... too many errors, aborting.\n");
                    close(sock);
                    return 1;
                }
            }
            total++;
            
            if (total % 20000 == 0) {
                printf("  -> Verified %d records...\n", total);
            }
        }
        
        if (mismatch == 0) {
            printf("[%s] %d records verified.\n", get_cmd, PER_ENGINE);
        }
    }

    if (mismatch > 0) {
        printf("\n[FAILED] %d mismatches found out of %d records!\n", mismatch, total);
        close(sock);
        return 1;
    }

    printf("\n[VERIFY] ALL 100,000 RECORDS VERIFIED! AOF DATA IS 100%% CONSISTENT.\n");

    // 验证成功，删除 AOF 文件
    printf("[PURGE] Purging old AOF file: %s...\n", AOF_FILE);
    if (remove(AOF_FILE) == 0) {
        printf("  [Success] AOF file successfully deleted.\n");
    } else {
        printf("  [Warning] AOF file could not be removed or doesn't exist.\n");
    }
    
    close(sock);
    printf("[TEST PASS FOR ALL ENGINES]\n\n");
    return 0;
}