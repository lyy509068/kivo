#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <unistd.h>
#include <arpa/inet.h>
#include <sys/time.h>
#include <sys/wait.h>
#include <sys/socket.h>
#include <stdint.h>
#include <signal.h>

#define SERVER_PORT 2000
#define TOTAL_RECORDS 100    

#define SNAP_TYPE_ARRAY    1
#define SNAP_TYPE_RBTREE   2
#define SNAP_TYPE_HASH     3
#define SNAP_TYPE_SKIPLIST 4

int build_resp_request_ttl(char *buf, const char *cmd, const char *key, const char *val, int expire_sec) {
    if (val && expire_sec > 0) {
        char exp_str[16];
        sprintf(exp_str, "%d", expire_sec);
        return sprintf(buf, "*5\r\n$%zu\r\n%s\r\n$%zu\r\n%s\r\n$%zu\r\n%s\r\n$2\r\nEX\r\n$%zu\r\n%s\r\n", 
                       strlen(cmd), cmd, strlen(key), key, strlen(val), val, strlen(exp_str), exp_str);
    } else if (val) {
        return sprintf(buf, "*3\r\n$%zu\r\n%s\r\n$%zu\r\n%s\r\n$%zu\r\n%s\r\n", 
                       strlen(cmd), cmd, strlen(key), key, strlen(val), val);
    } else if (key) {
        return sprintf(buf, "*2\r\n$%zu\r\n%s\r\n$%zu\r\n%s\r\n", 
                       strlen(cmd), cmd, strlen(key), key);
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

    for (int retry = 0; retry < 10; retry++) {
        if (connect(sock, (struct sockaddr*)&addr, sizeof(addr)) == 0) {
            return sock;
        }
        usleep(200000); 
    }
    close(sock);
    return -1;
}

int 

send_all(int sock, const char *buf, int len) {
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

int run_ttl_testcase(int engine_type, const char *engine_name, int ttl_sec, int sleep_sec) {
    char send_buf[1024];
    char recv_buf[1024];
    int send_len;

    printf("\n======================================================\n");
    printf("  TTL TEST: %s | TTL=%ds | Sleep=%ds\n", engine_name, ttl_sec, sleep_sec);
    printf("======================================================\n\n");

    int sock = connect_server();
    if (sock < 0) {
        fprintf(stderr, "Fatal: Cannot connect to kvstore on port %d\n", SERVER_PORT);
        return 1;
    }

    const char *set_cmd, *get_cmd;
    switch (engine_type) {
        case SNAP_TYPE_ARRAY:    set_cmd = "SET";  get_cmd = "GET";  break;
        case SNAP_TYPE_RBTREE:   set_cmd = "RSET"; get_cmd = "RGET"; break;
        case SNAP_TYPE_HASH:     set_cmd = "HSET"; get_cmd = "HGET"; break;
        case SNAP_TYPE_SKIPLIST: set_cmd = "SSET"; get_cmd = "SGET"; break;
        default: close(sock); return 1;
    }

    // PHASE 1: 写入带 TTL 的数据
    printf("=== PHASE 1: Injecting %d records with %ds TTL ===\n", TOTAL_RECORDS, ttl_sec);
    for (int i = 0; i < TOTAL_RECORDS; i++) {
        char key[32], val[32];
        sprintf(key, "ttlkey_%04d", i);
        sprintf(val, "ttlval_%04d", i);
        
        send_len = build_resp_request_ttl(send_buf, set_cmd, key, val, ttl_sec);
        if (send_all(sock, send_buf, send_len) < 0) {
            printf("Server disconnected at index: %d\n", i);
            close(sock); return 1;
        }
        memset(recv_buf, 0, sizeof(recv_buf));
        if (recv_all(sock, recv_buf, 5) < 0 || strstr(recv_buf, "OK") == NULL) {
            printf("Bad reply at index: %d\n", i);
            close(sock); return 1;
        }
    }
    printf("[Client] %d items injected.\n", TOTAL_RECORDS);

    // PHASE 2: 立即验证数据存在
    printf("\n=== PHASE 2: Immediate check (should EXIST) ===\n");
    for (int i = 0; i < TOTAL_RECORDS; i++) {
        char key[32], expected_val[32];
        sprintf(key, "ttlkey_%04d", i);
        sprintf(expected_val, "ttlval_%04d", i);

        send_len = build_resp_request_ttl(send_buf, get_cmd, key, NULL, 0);
        send_all(sock, send_buf, send_len);
        memset(recv_buf, 0, sizeof(recv_buf));
        int rlen = recv(sock, recv_buf, sizeof(recv_buf) - 1, 0);
        if (rlen <= 0 || strstr(recv_buf, expected_val) == NULL) {
            printf("[FAIL] Data missing too early at index: %d\n", i);
            close(sock); return 1;
        }
    }
    printf("[Client] All records exist.\n");

    // PHASE 3: 等待过期
    printf("\n=== PHASE 3: Sleeping %ds for expiration... ===\n", sleep_sec);
    sleep(sleep_sec);

    // PHASE 4: 验证数据已过期
    printf("\n=== PHASE 4: Post-expire check (should NOT exist) ===\n");
    for (int i = 0; i < TOTAL_RECORDS; i++) {
        char key[32];
        sprintf(key, "ttlkey_%04d", i);

        send_len = build_resp_request_ttl(send_buf, get_cmd, key, NULL, 0);
        send_all(sock, send_buf, send_len);
        memset(recv_buf, 0, sizeof(recv_buf));
        int rlen = recv(sock, recv_buf, sizeof(recv_buf) - 1, 0);
        if (rlen <= 0 || strncmp(recv_buf, "$-1\r\n", 5) != 0) {
            printf("[FAIL] Key '%s' should be expired! Got: [%s]\n", key, recv_buf);
            close(sock); return 1;
        }
    }

    printf("\n[PASS] TTL test OK for %s!\n", engine_name);
    close(sock);
    return 0;
}

int main(int argc, char *argv[]) {
    setvbuf(stdout, NULL, _IONBF, 0);

    int engine = 1;
    int ttl_sec = 3;
    int sleep_sec = 4;

    if (argc >= 2) engine = atoi(argv[1]);
    if (argc >= 3) ttl_sec = atoi(argv[2]);
    if (argc >= 4) sleep_sec = atoi(argv[3]);

    if (engine < 1 || engine > 4) {
        printf("Usage: %s <engine> [ttl_sec] [sleep_sec]\n", argv[0]);
        printf("  1:Array  2:RBTree  3:Hash  4:SkipList\n");
        printf("  Default: ttl=3s sleep=5s\n");
        return 1;
    }

    const char *names[] = {"", "Array", "Rbtree", "Hash", "SkipList"};
    return run_ttl_testcase(engine, names[engine], ttl_sec, sleep_sec);
}