#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <unistd.h>
#include <arpa/inet.h>
#include <sys/socket.h>

#define SERVER_PORT 2000
#define TOTAL_RECORDS 100000   

#define SNAP_TYPE_ARRAY    1
#define SNAP_TYPE_RBTREE   2
#define SNAP_TYPE_HASH     3
#define SNAP_TYPE_SKIPLIST 4

const char* SET_CMDS[] = {"", "SET", "RSET", "HSET", "SSET"};

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

int main(int argc, char *argv[]) {
    setvbuf(stdout, NULL, _IONBF, 0);

    if (argc < 2) {
        printf("Usage: %s <Engine_Type> (1:Array, 2:RBTree, 3:Hash, 4:SkipList)\n", argv[0]);
        return 1;
    }

    int engine_type = atoi(argv[1]);
    if (engine_type < 1 || engine_type > 4) {
        printf("Invalid engine type!\n");
        return 1;
    }

    char send_buf[1024], recv_buf[1024];
    int send_len;

    printf("======================================================\n");
    printf("[STAGE 1] PUSHING DATA & CREATING SNAPSHOT (ENGINE %d)\n", engine_type);
    printf("======================================================\n\n");

    int sock = connect_server();
    if (sock < 0) {
        fprintf(stderr, "Fatal: Cannot connect to kvstore. Please start server manually!\n");
        return 1;
    }

    const char *cmd = SET_CMDS[engine_type];
    printf("[PUSH] Injecting %d records using command [%s]...\n", TOTAL_RECORDS, cmd);

    for (int i = 0; i < TOTAL_RECORDS; i++) {
        char key[32], val[32];
        sprintf(key, "key_%06d", i);
        sprintf(val, "val_%06d", i);
        send_len = build_resp_request(send_buf, cmd, key, val);

        if (send_all(sock, send_buf, send_len) < 0) {
            printf("Server disconnected during send at index: %d\n", i);
            close(sock);
            return 1;
        }
        
        memset(recv_buf, 0, sizeof(recv_buf));
        if (recv_all(sock, recv_buf, 5) < 0 || strstr(recv_buf, "OK") == NULL) {
            printf("Unexpected server reply at index: %d! (Recv: %s)\n", i, recv_buf);
            close(sock);
            return 1;
        }

        if (i > 0 && i % 20000 == 0) {
            printf("  -> Progress: Injected and verified %d records...\n", i);
        }
    }
    printf("[PUSH] Successfully injected and verified %d records.\n", TOTAL_RECORDS);

    // 发送 SAVE 快照落盘命令
    printf("\n[SAVE] Sending RESP 'SAVE' Command to trigger snapshot dump...\n");
    send_len = build_resp_request(send_buf, "SAVE", NULL, NULL); 
    send_all(sock, send_buf, send_len);
    memset(recv_buf, 0, sizeof(recv_buf));
    
    memset(recv_buf, 0, sizeof(recv_buf));
    if (recv_all(sock, recv_buf, 5) < 0 || strstr(recv_buf, "OK") == NULL) {
        printf("SAVE command failed! Got: [%s]\n", recv_buf);
        close(sock);
        return 1;
    }
    printf("[Server RESP ACK]: %s", recv_buf); 

    // 直接断开连接
    close(sock);
    return 0;
}