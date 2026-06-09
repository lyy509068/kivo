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

// 💡 升级版 RESP 打包函数：支持追加 "EX" 和 "秒数" 参数
int build_resp_request_ttl(char *buf, const char *cmd, const char *key, const char *val, int expire_sec) {
    if (val && expire_sec > 0) {
        // 当需要设置超时时，参数个数变为 5 个：CMD key val EX seconds
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

// 核心 TTL 测试逻辑
int run_ttl_testcase(int engine_type, const char *engine_name) {
    char send_buf[1024];
    char recv_buf[1024];
    int send_len;

    printf("\n======================================================\n");
    printf("    ⏱️  STARTING TIMEOUT EXPIRE TEST FOR: %s\n", engine_name);
    printf("======================================================\n\n");

    int sock = connect_server();
    if (sock < 0) {
        fprintf(stderr, "Fatal: Cannot connect to kvstore on port %d\n", SERVER_PORT);
        return 1;
    }

    const char *set_cmd = "SET";
    const char *get_cmd = "GET";
    switch (engine_type) {
        case SNAP_TYPE_ARRAY:    set_cmd = "SET";  get_cmd = "GET";  break;
        case SNAP_TYPE_RBTREE:   set_cmd = "RSET"; get_cmd = "RGET"; break;
        case SNAP_TYPE_HASH:     set_cmd = "HSET"; get_cmd = "HGET"; break;
        case SNAP_TYPE_SKIPLIST: set_cmd = "SSET"; get_cmd = "SGET"; break;
        default:
            printf("❌ Unknown engine type: %d\n", engine_type);
            close(sock);
            return 1;
    }

    // -------------------------------------------------------------
    // PHASE 1: 写入带有 3 秒超时的 KV 数据
    // -------------------------------------------------------------
    printf("=== PHASE 1: Injecting Records with TTL (3 Seconds) ===\n");
    for (int i = 0; i < TOTAL_RECORDS; i++) {
        char key[32], val[32];
        sprintf(key, "ttlkey_%04d", i);
        sprintf(val, "ttlval_%04d", i);
        
        // 💡 传入 3，代表设置 3 秒超时
        send_len = build_resp_request_ttl(send_buf, set_cmd, key, val, 3);

        if (send_all(sock, send_buf, send_len) < 0) {
            printf("❌ Server disconnected during injection at index: %d\n", i);
            close(sock);
            return 1;
        }
        
        memset(recv_buf, 0, sizeof(recv_buf));
        if (recv_all(sock, recv_buf, 5) < 0 || strstr(recv_buf, "OK") == NULL) {
            printf("❌ Bad reply during injection at index: %d! Got: [%s]\n", i, recv_buf);
            close(sock);
            return 1;
        }
    }
    printf("[Client] Successfully injected %d items with 3s expiration.\n", TOTAL_RECORDS);

    // -------------------------------------------------------------
    // PHASE 2: 立即读取，验证到期前数据“必须存在”
    // -------------------------------------------------------------
    printf("\n=== PHASE 2: Immediate Fetching (Should Exist) ===\n");
    for (int i = 0; i < TOTAL_RECORDS; i++) {
        char key[32], expected_val[32];
        sprintf(key, "ttlkey_%04d", i);
        sprintf(expected_val, "ttlval_%04d", i);

        send_len = build_resp_request_ttl(send_buf, get_cmd, key, NULL, 0);
        send_all(sock, send_buf, send_len);
        
        memset(recv_buf, 0, sizeof(recv_buf));
        int rlen = recv(sock, recv_buf, sizeof(recv_buf) - 1, 0);
        if (rlen <= 0) {
            printf("❌ Server disconnected during immediate check\n");
            close(sock);
            return 1;
        }
        recv_buf[rlen] = '\0';
        
        if (strstr(recv_buf, expected_val) == NULL) {
            printf("❌ [FAIL] Data missing too early at index: %d! Got: %s\n", i, recv_buf);
            close(sock);
            return 1;
        }
    }
    printf("[Client] Verified: All records safely accessible within life cycle.\n");

    // -------------------------------------------------------------
    // PHASE 3: 跨越时间窗口，等待服务端主动剔除或惰性删除
    // -------------------------------------------------------------
    printf("\n=== PHASE 3: Sleeping 4.5 seconds to cross expire threshold... ===\n");
    usleep(4500000); // 睡 4.5 秒，确保全面超过 3 秒的生命周期

    // -------------------------------------------------------------
    // PHASE 4: 再次读取，验证到期后数据“必须消失”
    // -------------------------------------------------------------
    printf("\n=== PHASE 4: Post-Expire Fetching (Should NOT Exist) ===\n");
    for (int i = 0; i < TOTAL_RECORDS; i++) {
        char key[32];
        sprintf(key, "ttlkey_%04d", i);

        send_len = build_resp_request_ttl(send_buf, get_cmd, key, NULL, 0);
        send_all(sock, send_buf, send_len);
        
        memset(recv_buf, 0, sizeof(recv_buf));
        int rlen = recv(sock, recv_buf, sizeof(recv_buf) - 1, 0);
        if (rlen <= 0) {
            printf("❌ Server disconnected during post-expire check\n");
            close(sock);
            return 1;
        }
        recv_buf[rlen] = '\0';
        
        // 💡 核心校验点：过期的 Key 返回值必须是 Redis 格式的 "$-1\r\n"
        if (strncmp(recv_buf, "$-1\r\n", 5) != 0) {
            printf("❌ [CRITICAL ERROR] Key '%s' should be expired, but server replied: [%s]\n", key, recv_buf);
            close(sock);
            return 1;
        }
    }

    printf("\n🏆🏆🏆 [FINAL RESULT: PASS] TTL logic works perfectly for %s! 🏆🏆🏆\n", engine_name);
    close(sock);
    return 0;
}

int main(int argc, char *argv[]) {
    setvbuf(stdout, NULL, _IONBF, 0);

    if (argc < 2) {
        printf("Usage: %s <Engine_Type>\n", argv[0]);
        printf("  1 : Array\n  2 : RB-Tree\n  3 : Hash\n  4 : SkipList\n");
        return 1;
    }

    int choice = atoi(argv[1]);
    switch (choice) {
        case 1: run_ttl_testcase(SNAP_TYPE_ARRAY, "Array"); break;
        case 2: run_ttl_testcase(SNAP_TYPE_RBTREE, "Rbtree"); break;
        case 3: run_ttl_testcase(SNAP_TYPE_HASH, "Hash"); break;
        case 4: run_ttl_testcase(SNAP_TYPE_SKIPLIST, "SkipList"); break;
        default: printf("❌ Invalid type!\n"); return 1;
    }
    return 0;
}