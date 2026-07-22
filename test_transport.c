#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <sys/socket.h>
#include <arpa/inet.h>
#include <time.h>

#define SERVER_IP "192.168.37.128"
#define SERVER_PORT 2000
#define TOTAL_COMMANDS 100000  // 总共发 10 万条命令
#define PRINT_INTERVAL 10000   // 每 1 万条打印一次

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

int main() {
    srand(time(NULL));

    int sock = connect_server();
    if (sock < 0) {
        printf("[FATAL] Cannot connect to server!\n");
        return -1;
    }

    printf("QPS Test: %d commands (1%% array, 33%% rbtree, 33%% hash, 33%% skiplist)\n", TOTAL_COMMANDS);

    char send_buf[512], key[32], val[32];
    long long eng_counts[5] = {0};

    struct timespec t_start, t_end;
    clock_gettime(CLOCK_MONOTONIC, &t_start);

    for (int i = 0; i < TOTAL_COMMANDS; i++) {
        // 按比例选引擎：1% array, 33% rbtree, 33% hash, 33% skiplist
        int r = rand() % 100;
        int eng;
        if (r < 1)       eng = 1;   // SET
        else if (r < 34) eng = 2;   // RSET
        else if (r < 67) eng = 3;   // HSET
        else             eng = 4;   // SSET

        sprintf(key, "newkey_%06d", i);
        sprintf(val, "value_%06d", i);

        int len = build_resp_set(send_buf, SET_CMDS[eng], key, val);
        send(sock, send_buf, len, 0);

        if (check_ok_reply(sock) != 0) {
            printf("[FATAL] Failed at %s key=%s\n", SET_CMDS[eng], key);
            close(sock);
            return -1;
        }

        eng_counts[eng]++;

        if ((i + 1) % PRINT_INTERVAL == 0) {
            clock_gettime(CLOCK_MONOTONIC, &t_end);
            double elapsed = (t_end.tv_sec - t_start.tv_sec) + 
                             (t_end.tv_nsec - t_start.tv_nsec) / 1e9;
            printf("  [%d/%d] QPS: %.0f\n", i + 1, TOTAL_COMMANDS, (i + 1) / elapsed);
        }
    }

    clock_gettime(CLOCK_MONOTONIC, &t_end);
    double elapsed = (t_end.tv_sec - t_start.tv_sec) + 
                     (t_end.tv_nsec - t_start.tv_nsec) / 1e9;

    printf("\n=== QPS Test Complete ===\n");
    printf("Total:       %d commands\n", TOTAL_COMMANDS);
    printf("Time:        %.3f seconds\n", elapsed);
    printf("Average QPS: %.0f\n", TOTAL_COMMANDS / elapsed);

    close(sock);
    return 0;
}