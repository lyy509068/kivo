#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <sys/socket.h>
#include <arpa/inet.h>

#define SERVER_IP "192.168.37.129"
#define SERVER_PORT 2000
#define TOTAL_RECORDS 30000  // 1.5w × 2 轮

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

int main(int argc, char *argv[]) {
    int sock = connect_server();
    if (sock < 0) {
        printf("[FATAL] Cannot connect to slave!\n");
        return -1;
    }

    printf("Verifying 120000 records (all 4 engines, 2 rounds)...\n");

    char send_buf[256], recv_buf[512], key[32], expected_val[64];
    int total = 0;

    for (int round = 1; round <= 2; round++) {
        int start_idx = (round == 1) ? 0 : 15000;
        for (int i = start_idx; i < start_idx + 15000; i++) {
            sprintf(key, "key_%06d", i);
            sprintf(expected_val, "value_%06d", i);

            for (int eng = 1; eng <= 4; eng++) {
                const char *cmd = GET_CMDS[eng];

                int len = build_resp_get(send_buf, cmd, key);
                send(sock, send_buf, len, 0);

                memset(recv_buf, 0, sizeof(recv_buf));
                int rlen = recv(sock, recv_buf, sizeof(recv_buf) - 1, 0);
                if (rlen <= 0) {
                    printf("[FATAL] Disconnected at %s/%s\n", cmd, key);
                    close(sock); return -1;
                }
                if (strstr(recv_buf, expected_val) == NULL) {
                    printf("[MISMATCH] %s %s: expected [%s], got [%s]\n",
                           cmd, key, expected_val, recv_buf);
                    close(sock); return -1;
                }
                total++;
            }
        }
        printf("  Round %d verified.\n", round);
    }

    printf("ALL %d RECORDS VERIFIED SUCCESSFULLY!\n", total);
    close(sock);
    return 0;
}