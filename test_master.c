#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <sys/socket.h>
#include <arpa/inet.h>

#define SERVER_IP "192.168.88.128"
#define SERVER_PORT 2000
#define RECORDS_PER_ENGINE 15000  // 每种引擎 1.5w 条

const char* SET_CMDS[] = {"", "SET", "RSET", "HSET", "SSET"};

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

int check_ok_reply(int sock) {
    char recv_buf[64];
    memset(recv_buf, 0, sizeof(recv_buf));
    int r = recv(sock, recv_buf, sizeof(recv_buf) - 1, 0);
    if (r <= 0) return -1;
    if (strstr(recv_buf, "+OK") != NULL) return 0;
    return -2;
}

int main(int argc, char *argv[]) {
    if (argc < 2) {
        printf("Usage: %s <round: 1 or 2>\n", argv[0]);
        return 1;
    }

    int round = atoi(argv[1]);
    if (round != 1 && round != 2) {
        printf("Invalid round. Use 1 or 2.\n");
        return 1;
    }

    int start_idx = (round == 1) ? 0 : RECORDS_PER_ENGINE;

    int sock = connect_server();
    if (sock < 0) {
        printf("[FATAL] Cannot connect to server!\n");
        return -1;
    }

    printf("Round %d: Inserting records %d~%d for all 4 engines...\n",
           round, start_idx, start_idx + RECORDS_PER_ENGINE - 1);

    char send_buf[512], key[32], val[32];

    for (int eng = 1; eng <= 4; eng++) {
        const char *cmd = SET_CMDS[eng];
        printf("  [%s] starting...\n", cmd);

        for (int i = start_idx; i < start_idx + RECORDS_PER_ENGINE; i++) {
            sprintf(key, "key_%06d", i);
            sprintf(val, "value_%06d", i);

            int len = build_resp_set(send_buf, cmd, key, val);
            send(sock, send_buf, len, 0);

            if (check_ok_reply(sock) != 0) {
                printf("[FATAL] Failed at %s key=%s\n", cmd, key);
                close(sock); return -1;
            }
        }
        printf("  [%s] done.\n", cmd);
    }

    printf("Round %d complete: %d records inserted.\n",
           round, RECORDS_PER_ENGINE * 4);
    close(sock);
    return 0;
}