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
#define TOTAL_RECORDS 1000    
#define SNAPSHOT_FILE "kvstore.snap"

#define SNAP_TYPE_ARRAY    1
#define SNAP_TYPE_RBTREE   2
#define SNAP_TYPE_HASH     3
#define SNAP_TYPE_SKIPLIST 4

// 辅助网络与协议打包函数
int build_resp_request(char *buf, const char *cmd, const char *key, const char *val) {
    if (val) {
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

// 健壮的安全发送函数：确保完整发送 len 字节
int send_all(int sock, const char *buf, int len) {
    int total_sent = 0;
    while (total_sent < len) {
        int sent = send(sock, buf + total_sent, len - total_sent, 0);
        if (sent <= 0) return -1;
        total_sent += sent;
    }
    return total_sent;
}

// 健壮的固定长度接收函数：防止 TCP 半包
int recv_all(int sock, char *buf, int len) {
    int total_recv = 0;
    while (total_recv < len) {
        int r = recv(sock, buf + total_recv, len - total_recv, 0);
        if (r <= 0) return -1;
        total_recv += r;
    }
    return total_recv;
}

// 核心测试用例逻辑
int run_testcase(int engine_type, const char *engine_name) {
    char send_buf[1024];
    char recv_buf[1024];
    int send_len;
    pid_t server_pid = -1; // 记录重启后的子进程 PID

    printf("\n======================================================\n");
    printf("    🚀 STARTING FULL PERSISTENCE TEST FOR: %s\n", engine_name);
    printf("======================================================\n\n");

    // 连接服务器
    printf("=== PHASE 1: Spawning Server & Injecting Records (%s) ===\n", engine_name);
    int sock = connect_server();
    if (sock < 0) {
        fprintf(stderr, "Fatal: Cannot connect to kvstore on port %d\n", SERVER_PORT);
        return 1;
    }

    // 判定插入命令
    const char *cmd = "SET";
    switch (engine_type) {
        case SNAP_TYPE_ARRAY:    cmd = "SET";  break;
        case SNAP_TYPE_RBTREE:   cmd = "RSET"; break;
        case SNAP_TYPE_HASH:     cmd = "HSET"; break;
        case SNAP_TYPE_SKIPLIST: cmd = "SSET"; break;
        default:
            printf("❌ Unknown engine type: %d\n", engine_type);
            close(sock);
            return 1;
    }

    printf("[Client] Selected command prefix '%s' for engine type %d.\n", cmd, engine_type);

    // 1. 插入数据并严格验证回复
    for (int i = 0; i < TOTAL_RECORDS; i++) {
        char key[32], val[32];
        sprintf(key, "key_%06d", i);
        sprintf(val, "val_%06d", i);
        send_len = build_resp_request(send_buf, cmd, key, val);

        if (send_len <= 0) {
            printf("❌ Failed to build RESP request at index: %d\n", i);
            close(sock);
            return 1;
        }

        // 发送 RESP 报文
        if (send_all(sock, send_buf, send_len) < 0) {
            printf("❌ Server disconnected during send at index: %d\n", i);
            close(sock);
            return 1;
        }
        
        // 接收并验证写操作回复：严格接收 5 字节的 "+OK\r\n"
        memset(recv_buf, 0, sizeof(recv_buf));
        if (recv_all(sock, recv_buf, 5) < 0) {
            printf("❌ Server disconnected or error at index: %d\n", i);
            close(sock);
            return 1;
        }

        if (strstr(recv_buf, "OK") == NULL) {
            printf("❌ Unexpected server reply at index: %d! Expected [+OK\\r\\n], Got: [%s]\n", i, recv_buf);
            close(sock);
            return 1;
        }

        // 每隔 20000 条打印一次进度
        if (i > 0 && i % 20000 == 0) {
            printf("  -> Progress: Injected and verified %d records...\n", i);
        }
    }
    printf("[Client] Successfully injected %d SET items and verified all OK.\n", TOTAL_RECORDS);

    // 2. 发送 SAVE 命令并严格验证回复
    printf("\n=== PHASE 2: Sending RESP 'SAVE' Command ===\n");
    send_len = build_resp_request(send_buf, "SAVE", NULL, NULL); 
    send_all(sock, send_buf, send_len);
    
    memset(recv_buf, 0, sizeof(recv_buf));
    if (recv_all(sock, recv_buf, 5) < 0 || strstr(recv_buf, "OK") == NULL) {
        printf("❌ SAVE command failed or invalid response! Got: [%s]\n", recv_buf);
        close(sock);
        return 1;
    }
    printf("[Server RESP ACK]: %s\n", recv_buf); 
    
    // 3. 发送 SHUTDOWN 命令关闭服务器并验证回复
    printf("\n=== PHASE 3: Sending RESP 'SHUTDOWN' Command ===\n");
    send_len = build_resp_request(send_buf, "SHUTDOWN", NULL, NULL);
    send_all(sock, send_buf, send_len);

    memset(recv_buf, 0, sizeof(recv_buf));
    if (recv_all(sock, recv_buf, 5) < 0 || strstr(recv_buf, "OK") == NULL) {
        printf("❌ SHUTDOWN failed or ungraceful! Expected [+OK\\r\\n], Got: [%s]\n", recv_buf);
        close(sock);
        return 1;
    }
    printf("  🎉 [Success] Server acknowledged SHUTDOWN perfectly!\n");
    close(sock); 
    sleep(3);

    // 4. 重启服务器
    printf("\n=== PHASE 4: Restarting Server & Reloading Dump file ===\n");
    server_pid = fork();
    if (server_pid == 0) {
        execl("./server", "./server", "2000", (char*)NULL);
        perror("❌ [Fatal Child Error] execl failed to boot server");
        exit(1);
    }
    
    printf("[Client] Server subprocess forked (PID: %d). Waiting 3 seconds for load...\n", server_pid);
    sleep(3);

    // 5. 重新连接服务器
    printf("\n=== PHASE 5: Re-fetching Records to verify Integrity ===\n");
    sock = connect_server();
    if (sock < 0) {
        printf("❌ [FAILURE] Server failed to reboot after recovery.\n");
        return 1;
    }

    // 判定查询命令
    const char *get_cmd = "GET"; 
    switch (engine_type) {
        case SNAP_TYPE_ARRAY:    get_cmd = "GET";  break;
        case SNAP_TYPE_RBTREE:   get_cmd = "RGET"; break;
        case SNAP_TYPE_HASH:     get_cmd = "HGET"; break;
        case SNAP_TYPE_SKIPLIST: get_cmd = "SGET"; break;
        default:
            printf("❌ Unknown engine type for verification: %d\n", engine_type);
            close(sock);
            return 1;
    }

    printf("[Validator] Selected validation command '%s' for engine type %d.\n", get_cmd, engine_type);

    // 6. 发送 GET 命令并严格验证包含预期的 Value
    for (int i = 0; i < TOTAL_RECORDS; i++) {
        char key[32], expected_val[32];
        sprintf(key, "key_%06d", i);
        sprintf(expected_val, "val_%06d", i);

        send_len = build_resp_request(send_buf, get_cmd, key, NULL);
        
        if (send_all(sock, send_buf, send_len) < 0) {
            printf("❌ Server disconnected during verification send at index: %d\n", i);
            close(sock);
            return 1;
        }
        
        // 接收并验证 GET 的具体数据
        memset(recv_buf, 0, sizeof(recv_buf));
        int rlen = recv(sock, recv_buf, sizeof(recv_buf) - 1, 0);
        if (rlen <= 0) {
            printf("❌ Server disconnected during verification recv at index: %d\n", i);
            close(sock);
            return 1;
        }
        recv_buf[rlen] = '\0';
        
        // 验证：收到的原始 RESP 流里必须包含预期的 "val_0000xx"
        if (strstr(recv_buf, expected_val) == NULL) {
            printf("❌ Data Mismatch/Lost at index: %d | Expected: %s | Recv Raw: %s\n", i, expected_val, recv_buf);
            close(sock);
            return 1;
        }

        if (i > 0 && i % 20000 == 0) {
            printf("  -> Progress: Verified %d records successfully...\n", i);
        }
    }

    // 通过全部强校验，判定最终 PASS
    printf("\n🏆🏆🏆 [FINAL RESULT: PASS] All %d %s items persistent, safe, and fully matched! 🏆🏆🏆\n", TOTAL_RECORDS, engine_name);

    // 7. 测试完毕，清理现场，安全SHUTDOWN
    printf("\n=== PHASE 6: Cleaning up testing server ===\n");
    send_len = build_resp_request(send_buf, "SHUTDOWN", NULL, NULL);
    if (send_all(sock, send_buf, send_len) < 0) {
        printf("❌ SHUTDOWN clean send failed\n");
    } else {
        memset(recv_buf, 0, sizeof(recv_buf));
        int rlen = recv(sock, recv_buf, 5, 0);
        if (rlen > 0 && strstr(recv_buf, "OK") != NULL) {
            printf("✅ Final SHUTDOWN successful, server closing...\n");
        } else {
            printf("❌ Final SHUTDOWN failed or ungraceful (Recv: %s)\n", recv_buf);
        }
    }
    
    close(sock);

    // 回收后台的子进程
    if (server_pid > 0) {
        int status;
        for (int k = 0; k < 10; k++) {
            if (waitpid(server_pid, &status, WNOHANG) > 0) {
                server_pid = -1;
                break;
            }
            usleep(200000);
        }
        if (server_pid > 0) {
            kill(server_pid, SIGKILL);
            waitpid(server_pid, &status, 0);
            printf("[System] Server subprocess forced killed and reaped.\n");
        }
    }

    return 0;
}

// 各引擎独立测试入口
void testcase_array() {
    run_testcase(SNAP_TYPE_ARRAY, "Array");
}

void testcase_rbtree() {
    run_testcase(SNAP_TYPE_RBTREE, "RB-Tree");
}

void testcase_hash() {
    run_testcase(SNAP_TYPE_HASH, "Hash");
}

void testcase_skiptable() {
    run_testcase(SNAP_TYPE_SKIPLIST, "SkipList");
}

// 主函数：根据输入参数执行相应的测试
int main(int argc, char *argv[]) {
    // 关闭标准输出的缓冲，确保打印实时输出
    setvbuf(stdout, NULL, _IONBF, 0);

    if (argc < 2) {
        printf("Usage: %s <Engine_Type>\n", argv[0]);
        printf("  1 : Array\n");
        printf("  2 : RB-Tree\n");
        printf("  3 : Hash\n");
        printf("  4 : SkipList\n");
        printf("Example: ./test_fullpersistence 1\n");
        return 1;
    }

    int choice = atoi(argv[1]);
    
    switch (choice) {
        case 1:
            testcase_array();
            break;
        case 2:
            testcase_rbtree();
            break;
        case 3:
            testcase_hash();
            break;
        case 4:
            testcase_skiptable();
            break;
        default:
            printf("❌ Invalid engine type! Please enter 1, 2, 3, or 4.\n");
            return 1;
    }

    return 0;
}