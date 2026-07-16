#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <sys/socket.h>
#include <arpa/inet.h>

#define SERVER_IP "127.0.0.1"
#define SERVER_PORT 2000
#define TOTAL_RECORDS 100000
#define PER_ENGINE 25000

const char* SET_CMDS[] = {"", "SET", "RSET", "HSET", "SSET"};

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

int build_resp_cmd(char *buf, const char *cmd, const char *key, const char *val) {
    return sprintf(buf, "*3\r\n$%zu\r\n%s\r\n$%zu\r\n%s\r\n$%zu\r\n%s\r\n",
                   strlen(cmd), cmd, strlen(key), key, strlen(val), val);
}

int main() {
    char send_buf[256], recv_buf[256], key[32], val[32];
    int len;

    int sock = connect_server();
    if (sock < 0) {
        printf("[FATAL] Cannot connect to server. Please start the server manually first!\n");
        return 1;
    }

    int total = 0;
    int key_index = 0;  // 全局递增的 key 编号
    
    for (int engine = 1; engine <= 4; engine++) {
        const char* set_cmd = SET_CMDS[engine];
        printf("\n[PUSH] Engine %d [%s] inserting %d records...\n", engine, set_cmd, PER_ENGINE);
        
        for (int i = 0; i < PER_ENGINE; i++) {
            sprintf(key, "key_%06d", key_index);
            sprintf(val, "val_%06d", key_index);
            key_index++;
            
            len = build_resp_cmd(send_buf, set_cmd, key, val);
            send(sock, send_buf, len, 0);
            
            // 阻塞接收服务器的 +OK\r\n
            memset(recv_buf, 0, sizeof(recv_buf));
            int total_recv = 0;
            while (total_recv < 5) {
                int r = recv(sock, recv_buf + total_recv, 5 - total_recv, 0);
                if (r <= 0) {
                    printf("\n[FATAL] Server disconnected during push! (engine=%d, index=%d)\n", engine, i);
                    close(sock);
                    return 1;
                }
                total_recv += r;
            }
            total++;
            
            if (total % 20000 == 0) {
                printf("  -> Inserted %d records...\n", total);
            }
        }
        printf("[%s] %d records inserted.\n", set_cmd, PER_ENGINE);
    }

    printf("\n[PUSH] All 100,000 records pushed successfully.\n");
    close(sock);
    
    return 0;
}