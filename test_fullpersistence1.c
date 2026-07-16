#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <unistd.h>
#include <arpa/inet.h>
#include <sys/socket.h>

#define SERVER_PORT 2000
#define TOTAL_RECORDS 100000   
#define RECORDS_PER_ENGINE 25000

#define SNAP_TYPE_ARRAY    1
#define SNAP_TYPE_RBTREE   2
#define SNAP_TYPE_HASH     3
#define SNAP_TYPE_SKIPLIST 4

const char* SET_CMDS[] = {"", "SET", "RSET", "HSET", "SSET"};
const char* ENGINE_NAMES[] = {"", "Array", "RBTree", "Hash", "SkipList"};

int build_resp_request(char *buf, const char *cmd, const char *key, const char *val) {
    if (val) {
        return sprintf(buf, "*3\r\n$%zu\r\n%s\r\n$%zu\r\n%s\r\n$%zu\r\n%s\r\n", 
                       strlen(cmd), cmd, strlen(key), key, strlen(val), val);
    } else {
        return sprintf(buf, "*1\r\n$%zu\r\n%s\r\n", strlen(cmd), cmd);
    }
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

int recv_all(int sock, char *buf, int len) {
    int total_recv = 0;
    while (total_recv < len) {
        int r = recv(sock, buf + total_recv, len - total_recv, 0);
        if (r <= 0) return -1;
        total_recv += r;
    }
    return total_recv;
}

int main() {
    setvbuf(stdout, NULL, _IONBF, 0);

    char send_buf[1024], recv_buf[1024];
    int send_len;

    printf("======================================================\n");
    printf("[STAGE 1] PUSHING DATA TO ALL ENGINES & CREATING SNAPSHOT\n");
    printf("======================================================\n\n");

    int sock = connect_server();
    if (sock < 0) {
        fprintf(stderr, "Fatal: Cannot connect to kvstore. Please start server manually!\n");
        return 1;
    }

    int global_index = 0;
    
    // 依次向四个引擎插入数据
    for (int engine_type = 1; engine_type <= 4; engine_type++) {
        const char *cmd = SET_CMDS[engine_type];
        const char *engine_name = ENGINE_NAMES[engine_type];
        
        printf("\n[ENGINE %d: %s] Injecting %d records using command [%s]...\n", 
               engine_type, engine_name, RECORDS_PER_ENGINE, cmd);
        printf("  Key range: key_%06d to key_%06d\n", global_index, global_index + RECORDS_PER_ENGINE - 1);

        for (int i = 0; i < RECORDS_PER_ENGINE; i++) {
            char key[32], val[32];
            sprintf(key, "key_%06d", global_index);
            sprintf(val, "val_%06d_%s", global_index, engine_name);
            send_len = build_resp_request(send_buf, cmd, key, val);

            if (send_all(sock, send_buf, send_len) < 0) {
                printf("  Server disconnected during send at index: %d\n", global_index);
                close(sock);
                return 1;
            }
            
            memset(recv_buf, 0, sizeof(recv_buf));
            if (recv_all(sock, recv_buf, 5) < 0 || strstr(recv_buf, "OK") == NULL) {
                printf("  Unexpected server reply at index: %d! (Recv: %s)\n", global_index, recv_buf);
                close(sock);
                return 1;
            }

            if ((i + 1) % 5000 == 0) {
                printf("  -> Progress: %d/%d records injected...\n", i + 1, RECORDS_PER_ENGINE);
            }
            
            global_index++;
        }
        printf("[ENGINE %d: %s] Successfully injected %d records.\n", 
               engine_type, engine_name, RECORDS_PER_ENGINE);
    }

    printf("\n======================================================\n");
    printf("[SUMMARY] Total records injected: %d\n", global_index);
    printf("  - Array (SET):    %d records (key_000000 - key_024999)\n", RECORDS_PER_ENGINE);
    printf("  - RBTree (RSET):  %d records (key_025000 - key_049999)\n", RECORDS_PER_ENGINE);
    printf("  - Hash (HSET):    %d records (key_050000 - key_074999)\n", RECORDS_PER_ENGINE);
    printf("  - SkipList (SSET): %d records (key_075000 - key_099999)\n", RECORDS_PER_ENGINE);
    printf("======================================================\n\n");

    // 发送 SAVE 快照落盘命令
    printf("[SAVE] Sending RESP 'SAVE' Command to trigger snapshot dump...\n");
    send_len = build_resp_request(send_buf, "SAVE", NULL, NULL); 
    
    if (send_all(sock, send_buf, send_len) < 0) {
        printf("  Failed to send SAVE command!\n");
        close(sock);
        return 1;
    }
    
    memset(recv_buf, 0, sizeof(recv_buf));
    if (recv_all(sock, recv_buf, 5) < 0 || strstr(recv_buf, "OK") == NULL) {
        printf("  SAVE command failed! Got: [%s]\n", recv_buf);
        close(sock);
        return 1;
    }
    printf("[Server RESP ACK]: %s", recv_buf);
    printf("[SNAPSHOT] Snapshot created successfully!\n\n");

    // 直接断开连接
    close(sock);
    printf("======================================================\n");
    printf("[COMPLETE] All operations finished successfully!\n");
    printf("======================================================\n");
    return 0;
}