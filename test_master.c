#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <sys/socket.h>
#include <arpa/inet.h>

#define SERVER_IP "192.168.92.128"
#define MASTER_PORT 2000
#define BATCH_SIZE 50000

const char* SET_CMDS[] = {"", "SET", "RSET", "HSET", "SSET"};

int connect_server() {
    int sock = socket(AF_INET, SOCK_STREAM, 0);
    struct sockaddr_in serv_addr;
    serv_addr.sin_family = AF_INET;
    serv_addr.sin_port = htons(MASTER_PORT);
    inet_pton(AF_INET, SERVER_IP, &serv_addr.sin_addr);

    if (connect(sock, (struct sockaddr *)&serv_addr, sizeof(serv_addr)) < 0) {
        return -1;
    }
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
    
    if (strstr(recv_buf, "+OK") != NULL) {
        return 0; // 成功
    }
    return -2; // 回复不是 OK
}

int main(int argc, char *argv[]) {
    if (argc < 3) {
        printf("Usage: %s <engine_type> <mode: 1 or 2>\n", argv[0]);
        printf("Engine type: 1=Array, 2=RBTree, 3=Hash, 4=SkipList\n");
        return 1;
    }

    int engine_type = atoi(argv[1]);
    int mode = atoi(argv[2]);
    if (engine_type < 1 || engine_type > 4 || (mode != 1 && mode != 2)) {
        printf("❌ Invalid arguments.\n");
        return 1;
    }

    const char* set_cmd = SET_CMDS[engine_type];
    char send_buf[512], key[32], val[32];
    
    int sock = connect_server();
    if (sock < 0) {
        printf("❌ [FATAL] Cannot connect to Master Server on port %d!\n", MASTER_PORT);
        return -1;
    }

    int start_idx = (mode == 1) ? 0 : BATCH_SIZE;
    int end_idx = (mode == 1) ? BATCH_SIZE : (BATCH_SIZE * 2);

    printf("🚀 [MASTER CLIENT] Mode %d triggered. Injecting records from %d to %d using [%s]...\n", 
            mode, start_idx, end_idx - 1, set_cmd);

    for (int i = start_idx; i < end_idx; i++) {
        // 确保模式 1 和模式 2 的数据完全不同
        sprintf(key, "repl_%06d", i);
        sprintf(val, "value_mode%d_%06d", mode, i);
        
        int len = build_resp_set(send_buf, set_cmd, key, val);
        send(sock, send_buf, len, 0);

        int ret = check_ok_reply(sock);
        if (ret == -1) {
            printf("\n❌ [FATAL] Master disconnected at index %d\n", i);
            close(sock);
            return -1;
        } else if (ret == -2) {
            printf("\n❌ [FATAL] Master reply mismatch at index %d! Expected +OK\n", i);
            close(sock);
            return -1;
        }

        if (i > start_idx && (i - start_idx) % 10000 == 0) {
            usleep(20000); 
            printf("  -> Progress: Injected %d records...\n", i - start_idx);
        }
    }

    printf("✅ [MASTER CLIENT] Mode %d: All %d records verified and successfully pushed!\n", mode, BATCH_SIZE);
    close(sock);
    return 0;
}