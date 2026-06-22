#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <sys/socket.h>
#include <arpa/inet.h>

#define SERVER_IP "127.0.0.1"
#define SERVER_PORT 2000
#define TOTAL_RECORDS 100000

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
    if (val) {
        return sprintf(buf, "*3\r\n$%zu\r\n%s\r\n$%zu\r\n%s\r\n$%zu\r\n%s\r\n",
                       strlen(cmd), cmd, strlen(key), key, strlen(val), val);
    } else {
        return sprintf(buf, "*1\r\n$%zu\r\n%s\r\n", strlen(cmd), cmd);
    }
}

int main(int argc, char *argv[]) {
    if (argc < 2) {
        printf("Usage: %s <engine_type>\n", argv[0]);
        return 1;
    }
    int engine_type = atoi(argv[1]);
    if (engine_type < 1 || engine_type > 4) {
        printf("Invalid engine type! 1:Array, 2:RBTree, 3:Hash, 4:SkipList\n");
        return 1;
    }

    const char* set_cmd = SET_CMDS[engine_type];
    char send_buf[256], recv_buf[256], key[32], val[32];
    int len;

    printf("==================================================\n");
    printf("🚀 [STAGE 1] PUSHING DATA TO SERVER (ENGINE %d)\n", engine_type);
    printf("==================================================\n");

    int sock = connect_server();
    if (sock < 0) {
        printf("❌ [FATAL] Cannot connect to server. Please start the server manually first!\n");
        return 1;
    }

    printf("[PUSH] Pushing %d records using [%s]...\n", TOTAL_RECORDS, set_cmd);
    for (int i = 0; i < TOTAL_RECORDS; i++) {
        sprintf(key, "key_%06d", i);
        sprintf(val, "val_%06d", i);
        len = build_resp_cmd(send_buf, set_cmd, key, val);
        send(sock, send_buf, len, 0);
        
        // 阻塞接收服务器的 +OK\r\n
        memset(recv_buf, 0, sizeof(recv_buf));
        int total_recv = 0;
        while (total_recv < 5) {
            int r = recv(sock, recv_buf + total_recv, 5 - total_recv, 0);
            if (r <= 0) {
                printf("\n❌ [FATAL] Server disconnected during push at index %d!\n", i);
                close(sock);
                return 1;
            }
            total_recv += r;
        }
        
        if (i % 20000 == 0) printf("  -> Inserted %d records...\n", i);
    }
    printf("✅ [PUSH] All 100,000 records pushed successfully.\n");

    close(sock);
    
    return 0;
}