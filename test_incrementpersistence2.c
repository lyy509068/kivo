#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <sys/socket.h>
#include <arpa/inet.h>

#define SERVER_IP "127.0.0.1"
#define SERVER_PORT 2000
#define TOTAL_RECORDS 100000
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
    if (key) {
        return sprintf(buf, "*2\r\n$%zu\r\n%s\r\n$%zu\r\n%s\r\n",
                       strlen(cmd), cmd, strlen(key), key);
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

    const char* get_cmd = GET_CMDS[engine_type];
    char send_buf[256], recv_buf[256], key[32], val[32];
    int len;

    printf("==================================================\n");
    printf("🚀 [STAGE 2] VERIFYING DATA (ENGINE %d)\n", engine_type);
    printf("==================================================\n");

    int sock = connect_server();
    if (sock < 0) {
        printf("❌ [FATAL] Cannot connect to server. Did you manually restart it?\n");
        return 1;
    }

    printf("[VERIFY] Verifying %d records using [%s]...\n", TOTAL_RECORDS, get_cmd);
    for (int i = 0; i < TOTAL_RECORDS; i++) {
        sprintf(key, "key_%06d", i);
        sprintf(val, "val_%06d", i);
        
        len = build_resp_cmd(send_buf, get_cmd, key);
        send(sock, send_buf, len, 0);
        
        memset(recv_buf, 0, sizeof(recv_buf));
        int rlen = recv(sock, recv_buf, sizeof(recv_buf) - 1, 0);    
        
        if (rlen <= 0 || strstr(recv_buf, val) == NULL) {
            printf("\n❌ [FATAL] Data mismatch at %s! Expected: %s, Got: [%s]\n", key, val, recv_buf);
            close(sock);
            return 1;
        }

        if (i > 0 && i % 20000 == 0) printf("  -> Verified %d records...\n", i);
    }
    printf("✅ [VERIFY] ALL 100,000 RECORDS VERIFIED! AOF DATA IS 100%% CONSISTENT.\n");

    // 校验成功，抹除 AOF 日志残留文件
    printf("[PURGE] Purging old AOF file: %s...\n", AOF_FILE);
    if (remove(AOF_FILE) == 0) {
        printf("  🎉 [Success] AOF file successfully deleted.\n");
    } else {
        printf("  ⚠️ [Warning] AOF file could not be removed or doesn't exist.\n");
    }
    
    close(sock);
    printf("🏆🏆🏆 [TEST PASS FOR ENGINE %d] 🏆🏆🏆\n\n", engine_type);
    return 0;
}