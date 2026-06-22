#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <unistd.h>
#include <arpa/inet.h>
#include <sys/socket.h>

#define SERVER_PORT 2000
#define TOTAL_RECORDS 100000   
#define SNAPSHOT_FILE "kvstore.snap"

const char* GET_CMDS[] = {"", "GET", "RGET", "HGET", "SGET"};

int build_resp_request(char *buf, const char *cmd, const char *key) {
    return sprintf(buf, "*2\r\n$%zu\r\n%s\r\n$%zu\r\n%s\r\n", 
                   strlen(cmd), cmd, strlen(key), key);
}

int connect_server() {
    int sock = socket(AF_INET, SOCK_STREAM, 0);
    if (sock < 0) return -1;
    
    struct sockaddr_in addr;
    addr.sin_family = AF_INET;
    addr.sin_port = htons(SERVER_PORT);
    inet_pton(AF_INET, "127.0.0.1", &addr.sin_addr);

    if (connect(sock, (struct sockaddr*)&addr, sizeof(addr)) == 0) {
        return sock;
    }
    close(sock);
    return -1;
}

int send_all(int sock, const char *buf, int len) {
    int total_sent = 0;
    while (total_sent < len) {
        int sent = send(sock, buf + total_sent, len - total_sent, 0);
        if (sent <= 0) return -1;
        total_sent += sent;
    }
    return total_sent;
}

int main(int argc, char *argv[]) {
    setvbuf(stdout, NULL, _IONBF, 0);

    if (argc < 2) {
        printf("Usage: %s <Engine_Type> (1:Array, 2:RBTree, 3:Hash, 4:SkipList)\n", argv[0]);
        return 1;
    }

    int engine_type = atoi(argv[1]);
    if (engine_type < 1 || engine_type > 4) {
        printf("❌ Invalid engine type!\n");
        return 1;
    }

    char send_buf[1024], recv_buf[1024];
    int send_len;

    printf("======================================================\n");
    printf("🚀 [STAGE 2] RECONNECTING & VERIFYING SNAPSHOT (ENGINE %d)\n", engine_type);
    printf("======================================================\n\n");

    int sock = connect_server();
    if (sock < 0) {
        printf("❌ [FATAL] Cannot connect to server. Did you remember to restart it manually?\n");
        return 1;
    }

    const char *get_cmd = GET_CMDS[engine_type];
    printf("[VERIFY] Verifying %d records using [%s]...\n", TOTAL_RECORDS, get_cmd);

    for (int i = 0; i < TOTAL_RECORDS; i++) {
        char key[32], expected_val[32];
        sprintf(key, "key_%06d", i);
        sprintf(expected_val, "val_%06d", i);

        send_len = build_resp_request(send_buf, get_cmd, key);
        if (send_all(sock, send_buf, send_len) < 0) {
            printf("❌ Server disconnected during send at index: %d\n", i);
            close(sock);
            return 1;
        }
        
        memset(recv_buf, 0, sizeof(recv_buf));
        int rlen = recv(sock, recv_buf, sizeof(recv_buf) - 1, 0);
        if (rlen <= 0) {
            printf("❌ Server disconnected during recv at index: %d\n", i);
            close(sock);
            return 1;
        }
        recv_buf[rlen] = '\0';
        
        if (strstr(recv_buf, expected_val) == NULL) {
            printf("❌ Data Mismatch/Lost at %s! Expected: %s, Got: [%s]\n", key, expected_val, recv_buf);
            close(sock);
            return 1;
        }

        if (i > 0 && i % 20000 == 0) {
            printf("  -> Progress: Verified %d records successfully...\n", i);
        }
    }
    printf("✅ [VERIFY] ALL 100,000 RECORDS VERIFIED FROM SNAPSHOT!\n");

    // ✨ 强校验通过，抹除快照文件，不留痕迹
    printf("\n[PURGE] Purging snapshot file: %s...\n", SNAPSHOT_FILE);
    if (remove(SNAPSHOT_FILE) == 0) {
        printf("  🎉 [Success] %s deleted successfully.\n", SNAPSHOT_FILE);
    } else {
        printf("  ⚠️ [Warning] Snapshot file not found or couldn't be deleted.\n");
    }

    // ✨ 直接断开连接，不写 shutdown
    close(sock);
    printf("🔌 Disconnected. 🏆 [SNAPSHOT TEST PASS FOR ENGINE %d] 🏆\n\n", engine_type);
    return 0;
}