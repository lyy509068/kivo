#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <sys/socket.h>
#include <arpa/inet.h>
#include <errno.h>

#define SERVER_IP "127.0.0.1"
#define SERVER_PORT 2000
#define TOTAL_KEYS  100
#define KEYS_PER_TYPE 25
#define REPEAT_TIMES 1000

#define MAX_SEND_BUF (TOTAL_KEYS * 256)
#define MAX_RECV_BUF (TOTAL_KEYS * 256)

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

int build_resp_cmd(char *buf, const char *cmd, const char *key, const char *val) {
    return sprintf(buf, "*3\r\n$%zu\r\n%s\r\n$%zu\r\n%s\r\n$%zu\r\n%s\r\n",
                   strlen(cmd), cmd, strlen(key), key, strlen(val), val);
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

int recv_all(int fd, char *buf, int len) {
    int received = 0;
    while (received < len) {
        int n = recv(fd, buf + received, len - received, 0);
        if (n <= 0) return -1;
        received += n;
    }
    return received;
}

// 构建一轮的命令（key带轮次号）
int build_round_commands(char *send_buf, int round) {
    int total_len = 0;
    
    for (int engine = 1; engine <= 4; engine++) {
        const char *cmd = SET_CMDS[engine];
        
        for (int i = 0; i < KEYS_PER_TYPE; i++) {
            char key[64], val[64];
            sprintf(key, "%s_r%04d_k%02d", cmd, round, i);
            sprintf(val, "%s_r%04d_v%02d", cmd, round, i);
            
            int cmd_len = build_resp_cmd(send_buf + total_len, cmd, key, val);
            total_len += cmd_len;
        }
    }
    
    return total_len;
}

int verify_all_replies(int sock, char *recv_buf, int recv_buf_size, int round) {
    int expected_total_bytes = TOTAL_KEYS * 5;
    
    if (recv_buf_size < expected_total_bytes) {
        printf("[ROUND %d] recv_buf too small!\n", round);
        return -1;
    }
    
    if (recv_all(sock, recv_buf, expected_total_bytes) < 0) {
        printf("[ROUND %d] Failed to receive all replies (expected %d bytes)\n",
               round, expected_total_bytes);
        return -1;
    }
    
    for (int i = 0; i < TOTAL_KEYS; i++) {
        char *reply = recv_buf + i * 5;
        if (memcmp(reply, "+OK\r\n", 5) != 0) {
            char bad_reply[32] = {0};
            memcpy(bad_reply, reply, 20);
            for (int j = 0; j < 20; j++) {
                if (bad_reply[j] == '\r') bad_reply[j] = 'R';
                else if (bad_reply[j] == '\n') bad_reply[j] = 'N';
            }
            
            int engine_idx = i / KEYS_PER_TYPE + 1;
            int cmd_idx = i % KEYS_PER_TYPE;
            printf("[ROUND %d][ENGINE %d][CMD %d] "
                   "expected '+OK\\r\\n' but got '%s'\n",
                   round, engine_idx, cmd_idx, bad_reply);
            return -1;
        }
    }
    
    return 0;
}

int main() {
    printf("\n");
    printf("==================================================================\n");
    printf("Pipeline Test: Insert 100 unique records per round\n");
    printf("  SET: 25 | RSET: 25 | HSET: 25 | SSET: 25\n");
    printf("  Repeat: %d rounds | Total: %d operations\n",
           REPEAT_TIMES, TOTAL_KEYS * REPEAT_TIMES);
    printf("==================================================================\n\n");

    int sock = connect_server();
    if (sock < 0) {
        printf("[FATAL] Cannot connect to server!\n");
        return 1;
    }

    char *send_buf = (char *)malloc(MAX_SEND_BUF);
    char *recv_buf = (char *)malloc(MAX_RECV_BUF);
    if (!send_buf || !recv_buf) {
        printf("[FATAL] malloc failed!\n");
        close(sock);
        return 1;
    }

    int total_errors = 0;
    int completed_rounds = 0;

    for (int round = 0; round < REPEAT_TIMES; round++) {
        // 每轮构建新的命令（key带轮次号）
        int send_len = build_round_commands(send_buf, round);
        
        if (send_all(sock, send_buf, send_len) < 0) {
            printf("[ROUND %d] send failed!\n", round);
            total_errors++;
            break;
        }
        
        if (verify_all_replies(sock, recv_buf, MAX_RECV_BUF, round) < 0) {
            total_errors++;
            break;
        }
        
        completed_rounds++;
        
        if ((round + 1) % 100 == 0) {
            printf("  Completed %d rounds (%d operations)...\n",
                   round + 1, (round + 1) * TOTAL_KEYS);
        }
    }

    printf("\n--------------------------------------------------------\n");
    if (total_errors == 0) {
        printf("[RESULT] ALL %d rounds PASSED!\n", completed_rounds);
        printf("         Total operations: %d\n", completed_rounds * TOTAL_KEYS);
    } else {
        printf("[RESULT] %d errors detected.\n", total_errors);
        printf("         Only %d rounds completed.\n", completed_rounds);
    }
    printf("--------------------------------------------------------\n");

    free(send_buf);
    free(recv_buf);
    close(sock);
    
    printf("\nTest completed.\n");
    return 0;
}