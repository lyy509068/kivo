#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <unistd.h>
#include <arpa/inet.h>
#include <sys/time.h>
#include <sys/wait.h>
#include <sys/socket.h>
#include <stdint.h>

#define SERVER_PORT 2000
#define TOTAL_RECORDS 100000    
#define SNAPSHOT_FILE "kvstore.snap"
#define EXPECTED_SNAP "expected.snap"

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
    return -1;
}

// 根据不同的引擎类型生成预期快照，默认永不过期
void generate_expected_snapshot_file(int engine_type) {
    FILE *fp = fopen(EXPECTED_SNAP, "wb");
    if (!fp) {
        perror("Failed to create expected snapshot template");
        return;
    }

    int type = engine_type; // 动态写入对应的引擎类型标识
    int64_t expire_time = 0;    

    char key[32];
    char val[32];

    for (int i = 0; i < TOTAL_RECORDS; i++) {
        int key_len = sprintf(key, "key_%06d", i);
        int val_len = sprintf(val, "val_%06d", i);

        fwrite(&type, sizeof(int), 1, fp);
        fwrite(&expire_time, sizeof(int64_t), 1, fp);
        fwrite(&key_len, sizeof(int), 1, fp);
        fwrite(key, 1, key_len, fp);
        fwrite(&val_len, sizeof(int), 1, fp);
        fwrite(val, 1, val_len, fp);
    }
    fclose(fp);
    printf("[Validator] 'expected.snap' built perfectly with %d records (Type: %d).\n", TOTAL_RECORDS, engine_type);
}

int compare_snapshot_files() {
    FILE *f1 = fopen(EXPECTED_SNAP, "rb");
    FILE *f2 = fopen(SNAPSHOT_FILE, "rb");
    if (!f1 || !f2) {
        if (f1) fclose(f1);
        if (f2) fclose(f2);
        return -1;
    }

    char buf1[4096], buf2[4096];
    size_t r1, r2;
    int match = 1;

    while (1) {
        r1 = fread(buf1, 1, sizeof(buf1), f1);
        r2 = fread(buf2, 1, sizeof(buf2), f2);

        if (r1 != r2 || memcmp(buf1, buf2, r1) != 0) {
            match = 0; 
            break;
        }
        if (r1 == 0) break; 
    }

    fclose(f1);
    fclose(f2);
    return match;
}


// 核心测试用例逻辑
int run_testcase(int engine_type, const char *engine_name) {
    char send_buf[1024];
    char recv_buf[1024];
    int send_len;

    printf("\n======================================================\n");
    printf("   🚀 STARTING FULL PERSISTENCE TEST FOR: %s\n", engine_name);
    printf("======================================================\n\n");
    //连接服务器
    printf("=== PHASE 1: Spawning Server & Injecting 10W Records (%s) ===\n", engine_name);
    int sock = connect_server();
    if (sock < 0) {
        fprintf(stderr, "Fatal: Cannot connect to kvstore on port %d\n", SERVER_PORT);
        return 1;
    }
    //插入数据
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

    // 插入数据
    for (int i = 0; i < TOTAL_RECORDS; i++) {
        char key[32], val[32];
        sprintf(key, "key_%06d", i);
        sprintf(val, "val_%06d", i);
        send_len = build_resp_request(send_buf, cmd, key, val);

        if (send_len <= 0) {
            printf("❌ Failed to build RESP request at index: %d\n", i);
            continue;
        }

        // 发送 RESP 报文
        int total_sent = 0;
        while (total_sent < send_len) {
            int sent = send(sock, send_buf + total_sent, send_len - total_sent, 0);
            if (sent <= 0) {
                printf("❌ Server disconnected during send at index: %d\n", i);
                close(sock);
                return 1;
            }
            total_sent += sent;
        }
        
        // 接收服务端的 RESP 响应
        int rlen = recv(sock, recv_buf, sizeof(recv_buf) - 1, 0);
        if (rlen <= 0 && strstr(recv_buf, "OK\r\n") == NULL) {
            printf("❌ Server disconnected at index: %d\n", i);
            close(sock);
            return 1;
        }    
    }
    printf("[Client] Successfully injected 100,000 SET items via RESP.\n");
    //发送命令
    printf("\n=== PHASE 2: Sending RESP 'SAVE' Command ===\n");
    send_len = build_resp_request(send_buf, "SAVE", NULL, NULL); 
    send(sock, send_buf, send_len, 0);
    
    memset(recv_buf, 0, sizeof(recv_buf));
    recv(sock, recv_buf, sizeof(recv_buf) - 1, 0);
    printf("[Server RESP ACK]: %s", recv_buf); 
    
    //对比文件
    printf("\n=== PHASE 3: Validating disk serialization raw bytes ===\n");
    generate_expected_snapshot_file(engine_type);
    
    if (compare_snapshot_files() == 1) {
        printf("👉 [SUCCESS] %s file verification matching byte-by-byte with expected snapshot!\n", engine_name);
    } else {
        printf("⚠️ [WARNING] Mismatch detected. (Note: Hash/Trees reorder elements naturally, so binary match may fail, passing to recovery check...)\n");
        return 1;
    }
    //关闭服务器
    printf("\n=== PHASE 4: Sending RESP 'SHUTDOWN' Command ===\n");
    send_len = build_resp_request(send_buf, "SHUTDOWN", NULL, NULL);
    send(sock, send_buf, send_len, 0);

    // 增加关闭检验，等服务端把回复冲刷出来
    memset(recv_buf, 0, sizeof(recv_buf));
    int shutdown_rlen = recv(sock, recv_buf, sizeof(recv_buf) - 1, 0);
    if (shutdown_rlen > 0 && strstr(recv_buf, "OK") != NULL) {
        printf("  🎉 [Success] Server acknowledged PHASE 4 SHUTDOWN perfectly!\n");
    } else {
        printf("  ⚠️ [Warning] Server disconnected or returned ungracefully during PHASE 4.\n");
    }

    close(sock); 
    printf("[System] SHUTDOWN command dispatched. Server closed ports gracefully.\n");
    sleep(3);

    //重新打开服务器
    printf("\n=== PHASE 5: Restarting Server & Reloading Dump file ===\n");
    pid_t pid = fork();
    if (pid == 0) {
        execl("./server", "./server", "2000", (char*)NULL);
        perror("❌ [Fatal Child Error] execl failed to boot server");
        exit(1);
    }
    
    printf("[Client] Server subprocess forked (PID: %d). Waiting 3 seconds for load...\n", pid);
    sleep(3);
    //重新连接服务器
    printf("\n=== PHASE 6: Re-fetching 10W Records to verify Integrity ===\n");
    sock = connect_server();
    if (sock < 0) {
        printf("❌ [FAILURE] Server failed to reboot after recovery.\n");
        return 1;
    }
    const char *get_cmd = "GET"; // 默认或者 ARRAY
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

    // 发送命令与数据校验
    int integrity_check = 1;
    for (int i = 0; i < TOTAL_RECORDS; i++) {
        char key[32], expected_val[32];
        sprintf(key, "key_%06d", i);
        sprintf(expected_val, "val_%06d", i);

        send_len = build_resp_request(send_buf, get_cmd, key, NULL);
        
        // 防止大批量连续请求时发送缓冲区满
        int total_sent = 0;
        while (total_sent < send_len) {
            int sent = send(sock, send_buf + total_sent, send_len - total_sent, 0);
            if (sent <= 0) {
                printf("❌ Server disconnected during verification send at index: %d\n", i);
                integrity_check = 0;
                break;
            }
            total_sent += sent;
        }
        if (!integrity_check) break;
        
        // 接收服务端返回的 RESP 报文
        memset(recv_buf, 0, sizeof(recv_buf));
        int rlen = recv(sock, recv_buf, sizeof(recv_buf) - 1, 0);
        
        if (rlen <= 0) {
            integrity_check = 0;
            printf("❌ Server disconnected during verification recv at index: %d\n", i);
            break;
        }
        
        // 确保字符串安全截断
        recv_buf[rlen] = '\0';
        
        // 使用 strstr 检查返回的 RESP 原始数据流中是否包含预期的 "val_0000xx"
        if (strstr(recv_buf, expected_val) == NULL) {
            integrity_check = 0;
            printf("❌ Data Lost/Corrupted at index: %d | Expected: %s | Recv Raw: %s\n", i, expected_val, recv_buf);
            break;
        }
    }
    //验证正确性
    if (integrity_check) {
        printf("\n🏆🏆🏆 [FINAL RESULT: PASS] All 100,000 %s items persistent, safe, and fully matched! 🏆🏆🏆\n", engine_name);
    } else {
        printf("\n❌ [FINAL RESULT: FAIL] Data inconsistency or RESP format error detected after reboot.\n");
    }
    remove(SNAPSHOT_FILE);
    remove(EXPECTED_SNAP);
    // 测试完毕，清理测试服务器
    send_len = build_resp_request(send_buf, "SHUTDOWN", NULL, NULL);
    if (send(sock, send_buf, send_len, 0) < 0) {
        printf("❌ SHUTDOWN send failed\n");
    } else {
        memset(recv_buf, 0, sizeof(recv_buf));
         int rlen = recv(sock, recv_buf, sizeof(recv_buf) - 1, 0);
        
        if (rlen > 0 && strstr(recv_buf, "OK") != NULL) {
            printf("✅ SHUTDOWN successful, server closing...\n");
        } else {
            printf("❌ SHUTDOWN failed or ungraceful (Recv: %s)\n", recv_buf);
        }
    }
    close(sock);
    
    return 0;
}


// 针对不同引擎的独立测试入口

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


// 主函数：通过运行参数选择测试对象

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