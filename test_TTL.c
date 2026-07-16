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

// 定义引擎信息结构
typedef struct {
    int type;
    const char *name;
    const char *set_cmd;
    const char *get_cmd;
} engine_info_t;

int run_all_ttl_testcase(int ttl_sec, int sleep_sec) {
    char send_buf[1024];
    char recv_buf[1024];
    int send_len;

    // 定义所有引擎
    engine_info_t engines[] = {
        {SNAP_TYPE_ARRAY,    "Array",    "SET",  "GET"},
        {SNAP_TYPE_RBTREE,   "RBTree",   "RSET", "RGET"},
        {SNAP_TYPE_HASH,     "Hash",     "HSET", "HGET"},
        {SNAP_TYPE_SKIPLIST, "SkipList", "SSET", "SGET"}
    };
    int engine_count = sizeof(engines) / sizeof(engines[0]);

    printf("\n======================================================\n");
    printf("  TTL TEST: ALL ENGINES | TTL=%ds | Sleep=%ds\n", ttl_sec, sleep_sec);
    printf("======================================================\n\n");

    int sock = connect_server();
    if (sock < 0) {
        fprintf(stderr, "Fatal: Cannot connect to kvstore on port %d\n", SERVER_PORT);
        return 1;
    }

    // PHASE 1: 向所有引擎写入带 TTL 的数据
    printf("=== PHASE 1: Injecting %d records per engine with %ds TTL ===\n", TOTAL_RECORDS, ttl_sec);
    int total_injected = 0;
    
    for (int e = 0; e < engine_count; e++) {
        printf("  [%s] Injecting...\n", engines[e].name);
        for (int i = 0; i < TOTAL_RECORDS; i++) {
            char key[64], val[64];
            sprintf(key, "%s_ttlkey_%04d", engines[e].name, i);
            sprintf(val, "%s_ttlval_%04d", engines[e].name, i);
            
            send_len = build_resp_request_ttl(send_buf, engines[e].set_cmd, key, val, ttl_sec);
            if (send_all(sock, send_buf, send_len) < 0) {
                printf("[FAIL] Server disconnected at engine %s, index: %d\n", engines[e].name, i);
                close(sock); return 1;
            }
            memset(recv_buf, 0, sizeof(recv_buf));
            if (recv_all(sock, recv_buf, 5) < 0 || strstr(recv_buf, "OK") == NULL) {
                printf("[FAIL] Bad reply at engine %s, index: %d\n", engines[e].name, i);
                close(sock); return 1;
            }
            total_injected++;
        }
        printf("  [%s] Done.\n", engines[e].name);
    }
    printf("[Client] Total %d items injected across all engines.\n", total_injected);

    // PHASE 2: 立即验证所有引擎的数据存在
    printf("\n=== PHASE 2: Immediate check (should ALL EXIST) ===\n");
    int immediate_failures = 0;
    
    for (int e = 0; e < engine_count; e++) {
        int engine_fails = 0;
        for (int i = 0; i < TOTAL_RECORDS; i++) {
            char key[64], expected_val[64];
            sprintf(key, "%s_ttlkey_%04d", engines[e].name, i);
            sprintf(expected_val, "%s_ttlval_%04d", engines[e].name, i);

            send_len = build_resp_request_ttl(send_buf, engines[e].get_cmd, key, NULL, 0);
            send_all(sock, send_buf, send_len);
            memset(recv_buf, 0, sizeof(recv_buf));
            int rlen = recv(sock, recv_buf, sizeof(recv_buf) - 1, 0);
            if (rlen <= 0 || strstr(recv_buf, expected_val) == NULL) {
                if (engine_fails == 0) {
                    printf("[FAIL] %s: Data missing too early at index: %d\n", engines[e].name, i);
                }
                engine_fails++;
            }
        }
        if (engine_fails == 0) {
            printf("  [%s] All %d records exist. \n", engines[e].name, TOTAL_RECORDS);
        } else {
            printf("  [%s] %d/%d records missing! \n", engines[e].name, engine_fails, TOTAL_RECORDS);
            immediate_failures += engine_fails;
        }
    }
    
    if (immediate_failures > 0) {
        printf("[FAIL] Immediate check failed with %d missing records.\n", immediate_failures);
        close(sock); return 1;
    }
    printf("[Client] All records across all engines exist.\n");

    // PHASE 3: 等待过期
    printf("\n=== PHASE 3: Sleeping %ds for expiration... ===\n", sleep_sec);
    sleep(sleep_sec);

    // PHASE 4: 验证所有引擎的数据已过期
    printf("\n=== PHASE 4: Post-expire check (should ALL BE EXPIRED) ===\n");
    int expire_failures = 0;
    
    for (int e = 0; e < engine_count; e++) {
        int engine_fails = 0;
        for (int i = 0; i < TOTAL_RECORDS; i++) {
            char key[64];
            sprintf(key, "%s_ttlkey_%04d", engines[e].name, i);

            send_len = build_resp_request_ttl(send_buf, engines[e].get_cmd, key, NULL, 0);
            send_all(sock, send_buf, send_len);
            memset(recv_buf, 0, sizeof(recv_buf));
            int rlen = recv(sock, recv_buf, sizeof(recv_buf) - 1, 0);
            if (rlen <= 0 || strncmp(recv_buf, "$-1\r\n", 5) != 0) {
                if (engine_fails < 5) {  // 只显示前5个失败
                    printf("[FAIL] %s: Key '%s' should be expired! Got: [%.20s]\n", 
                           engines[e].name, key, recv_buf);
                }
                engine_fails++;
            }
        }
        if (engine_fails == 0) {
            printf("  [%s] All %d records expired correctly. \n", engines[e].name, TOTAL_RECORDS);
        } else {
            printf("  [%s] %d/%d records still exist! \n", engines[e].name, engine_fails, TOTAL_RECORDS);
            expire_failures += engine_fails;
        }
    }
    
    if (expire_failures > 0) {
        printf("\n[FAIL] TTL test failed with %d unexpired records.\n", expire_failures);
        close(sock); return 1;
    }

    printf("\n[PASS] TTL test OK for ALL engines!\n");
    close(sock);
    return 0;
}

int main(int argc, char *argv[]) {
    setvbuf(stdout, NULL, _IONBF, 0);

    int ttl_sec = 3;
    int sleep_sec = 4;

    if (argc >= 2) ttl_sec = atoi(argv[1]);
    if (argc >= 3) sleep_sec = atoi(argv[2]);

    printf("TTL Test Configuration:\n");
    printf("  TTL: %d seconds\n", ttl_sec);
    printf("  Sleep: %d seconds\n", sleep_sec);
    printf("  Records per engine: %d\n", TOTAL_RECORDS);
    printf("  Total records: %d\n\n", TOTAL_RECORDS * 4);

    return run_all_ttl_testcase(ttl_sec, sleep_sec);
}