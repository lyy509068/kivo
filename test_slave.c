#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <sys/socket.h>
#include <arpa/inet.h>

#define SERVER_IP "127.0.0.1"
#define SLAVE_PORT 2000 // 连向你的从端服务器端口
#define TOTAL_RECORDS 100000
#define BATCH_SIZE 50000

const char* GET_CMDS[] = {"", "GET", "RGET", "HGET", "SGET"};

int connect_slave() {
    int sock = socket(AF_INET, SOCK_STREAM, 0);
    struct sockaddr_in serv_addr;
    serv_addr.sin_family = AF_INET;
    serv_addr.sin_port = htons(SLAVE_PORT);
    inet_pton(AF_INET, SERVER_IP, &serv_addr.sin_addr);

    if (connect(sock, (struct sockaddr *)&serv_addr, sizeof(serv_addr)) < 0) {
        return -1;
    }
    return sock;
}

int build_resp_get(char *buf, const char *cmd, const char *key) {
    return sprintf(buf, "*2\r\n$%zu\r\n%s\r\n$%zu\r\n%s\r\n",
                   strlen(cmd), cmd, strlen(key), key);
}

int main(int argc, char *argv[]) {
    if (argc < 2) {
        printf("Usage: %s <engine_type>\n", argv[0]);
        printf("Engine type: 1=Array, 2=RBTree, 3=Hash, 4=SkipList\n");
        return 1;
    }

    int engine_type = atoi(argv[1]);
    if (engine_type < 1 || engine_type > 4) {
        printf("❌ Invalid engine type.\n");
        return 1;
    }

    const char* get_cmd = GET_CMDS[engine_type];
    char send_buf[256], recv_buf[512], key[32], expected_val[32];

    int sock = connect_slave();
    if (sock < 0) {
        printf("❌ [FATAL] Cannot connect to Slave Server on port %d!\n", SLAVE_PORT);
        return -1;
    }

    printf("🚀 [SLAVE CLIENT] Starting full cross-check for %d records from SLAVE node...\n", TOTAL_RECORDS);

    for (int i = 0; i < TOTAL_RECORDS; i++) {
        sprintf(key, "repl_%06d", i);
        
        // 根据索引还原当时 master_client 写入的期望值规则
        int origin_mode = (i < BATCH_SIZE) ? 1 : 2;
        sprintf(expected_val, "value_mode%d_%06d", origin_mode, i);

        int len = build_resp_get(send_buf, get_cmd, key);
        send(sock, send_buf, len, 0);

        memset(recv_buf, 0, sizeof(recv_buf));
        int rlen = recv(sock, recv_buf, sizeof(recv_buf) - 1, 0);

        if (rlen <= 0) {
            printf("\n❌ [FATAL] Slave disconnected or read error at %s\n", key);
            close(sock);
            return -1;
        }

        // 严格匹配回复内容中是否包含预期的 value 字符串
        if (strstr(recv_buf, expected_val) == NULL) {
            printf("\n❌ [FATAL] DATA MISMATCH at %s!\n", key);
            printf("           Expected Substring: [%s]\n", expected_val);
            printf("           Slave Node Returned: [%s]\n", recv_buf);
            close(sock);
            return -1;
        }

        if (i > 0 && i % 20000 == 0) {
            printf("  -> Progress: Successfully verified %d records from Slave...\n", i);
        }
    }

    printf("\n🏆🏆🏆 [FINAL REPLICATION TEST RESULT: PASS] 🏆🏆🏆\n");
    printf("✅ ALL %d RECORDS ARE 100%% CONSISTENT ON SLAVE NODE!\n\n", TOTAL_RECORDS);

    close(sock);
    return 0;
}