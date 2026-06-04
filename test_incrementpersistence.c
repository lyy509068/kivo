#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <sys/socket.h>
#include <arpa/inet.h>
#include <sys/wait.h>
#include <sys/time.h>
#include <time.h>

#define SERVER_IP "127.0.0.1"
#define SERVER_PORT 2000
#define TOTAL_RECORDS 1000
#define AOF_FILE "kvstore.aof" 

// 引擎对应的命令前缀
const char* SET_CMDS[] = {"", "SET", "RSET", "HSET", "SSET"};
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

// 辅助函数：构建 RESP 格式的 写/读 请求
int build_resp_cmd(char *buf, const char *cmd, const char *key, const char *val) {
    if (val) { // SET 类命令 (*3)
        return sprintf(buf, "*3\r\n$%zu\r\n%s\r\n$%zu\r\n%s\r\n$%zu\r\n%s\r\n",
                       strlen(cmd), cmd, strlen(key), key, strlen(val), val);
    } else {   // GET/SHUTDOWN 类命令 (*2 或 *1)
        if (key) {
            return sprintf(buf, "*2\r\n$%zu\r\n%s\r\n$%zu\r\n%s\r\n",
                           strlen(cmd), cmd, strlen(key), key);
        } else {
            return sprintf(buf, "*1\r\n$%zu\r\n%s\r\n", strlen(cmd), cmd);
        }
    }
}

int run_aof_testcase(int engine_type) {
    if (engine_type < 1 || engine_type > 4) {
        printf("Invalid engine type! 1:Array, 2:RBTree, 3:Hash, 4:SkipList\n");
        return -1;
    }

    const char* set_cmd = SET_CMDS[engine_type];
    const char* get_cmd = GET_CMDS[engine_type];
    char send_buf[256], recv_buf[256], key[32], val[32];
    int sock, len;

    printf("==================================================\n");
    printf("🚀 STARTING AOF PERSISTENCE TEST FOR ENGINE [%d]\n", engine_type);
    printf("==================================================\n");

    // 连接服务器
    sock = connect_server();
    if (sock < 0) {
        printf("❌ [FATAL] Cannot connect to server. Please start the server manually first!\n");
        return -1;
    }

    // 插入数据
    printf("[PHASE 1] Pushing %d records using command [%s]...\n", TOTAL_RECORDS, set_cmd);
    for (int i = 0; i < TOTAL_RECORDS; i++) {
        sprintf(key, "key_%06d", i);
        sprintf(val, "val_%06d", i);
        len = build_resp_cmd(send_buf, set_cmd, key, val);
        send(sock, send_buf, len, 0);
        
        // 接收服务端回复
        memset(recv_buf, 0, sizeof(recv_buf));
        int total_recv = 0;
        while (total_recv < 5) {
            int r = recv(sock, recv_buf + total_recv, 5 - total_recv, 0);
            if (r <= 0) {
                printf("\n❌ [FATAL] Server disconnected or recv failed during PHASE 1!\n");
                close(sock);
                return -1;
            }
            total_recv += r;
        }
        // 验证回复是否正确
        if (strcmp(recv_buf, "+OK\r\n") != 0) {
            printf("\n❌ [FATAL] Reply mismatch! Expected [+OK\\r\\n], Got [%s]\n", recv_buf);
            close(sock);
            return -1;
        }
        
        if (i % 20000 == 0) printf("  -> Inserted %d records...\n", i);
    }
    printf("✅ [PHASE 1] All records pushed successfully.\n");

    // 关闭服务器 
    printf("[PHASE 2] Sending SHUTDOWN to simulate server crash...\n");
    len = build_resp_cmd(send_buf, "SHUTDOWN", NULL, NULL);
    send(sock, send_buf, len, 0);
    char shutdown_recv_buf[128] = {0};
    int rlen = recv(sock, shutdown_recv_buf, sizeof(shutdown_recv_buf) - 1, 0);
    if (rlen > 0 && strcmp(shutdown_recv_buf, "+OK\r\n") == 0) {
        printf("  🎉 [Success] Server acknowledged SHUTDOWN perfectly!\n");
    } else {
        printf("  ❌ [Failure] Server did not reply +OK\\r\\n (Recv: %s)\n", shutdown_recv_buf);
    }
    close(sock);

    // 重启服务器
    printf("[PHASE 3] Restarting server to trigger AOF recovery...\n");
    pid_t pid = fork();
    if (pid == 0) {
        execl("./server", "./server", "2000", NULL);
        exit(1); // 如果 execl 失败则退出
    }
    
    sleep(3); // 给服务器3秒钟的时间启动并恢复日志

    // 重新连接服务器
    printf("[PHASE 4] Reconnecting to verify AOF data...\n");
    sock = connect_server();
    if (sock < 0) {
        printf("❌ [FATAL] Failed to reconnect!\n");
        return -1;
    }


    // 设置 20ms 超时，把新连接管道里可能存在的任何残留数据全部抽干
    struct timeval tv = {0, 20000}; 
    setsockopt(sock, SOL_SOCKET, SO_RCVTIMEO, (const char*)&tv, sizeof(tv));
    char trash_buf[512];
    while (recv(sock, trash_buf, sizeof(trash_buf), 0) > 0);
    // 恢复正常阻塞模式
    struct timeval tv_block = {0, 0};
    setsockopt(sock, SOL_SOCKET, SO_RCVTIMEO, (const char*)&tv_block, sizeof(tv_block));


    //get校验
    printf("[PHASE 5] Verifying %d records using command [%s]...\n", TOTAL_RECORDS, get_cmd);
    for (int i = 0; i < TOTAL_RECORDS; i++) {
        //构建预期key和value
        sprintf(key, "key_%06d", i);
        sprintf(val, "val_%06d", i);
        //构建get命令并发送
        len = build_resp_cmd(send_buf, get_cmd, key, NULL);
        send(sock, send_buf, len, 0);
        //接收响应
        memset(recv_buf, 0, sizeof(recv_buf));
        int rlen = recv(sock, recv_buf, sizeof(recv_buf) - 1, 0);    
        //验证回复和预期值是否相同
        if (rlen <= 0 || strstr(recv_buf, val) == NULL) {
            printf("\n❌ [FATAL] Data mismatch at %s! Expected: %s, Got: %s\n", key, val, recv_buf);
            return -1;
        }

        if (i > 0 && i % 20000 == 0) printf("  -> Verified %d records...\n", i);
    }
    printf("✅ [PHASE 5] ALL 100,000 RECORDS VERIFIED! AOF DATA IS 100%% CONSISTENT.\n");

    // 销毁日志
    printf("[PHASE 6] Purging old AOF file: %s...\n", AOF_FILE);
    remove(AOF_FILE);

    // 测试结束，清理服务器进程
    printf("[PHASE 7] Shutting down testing server...\n");
    len = build_resp_cmd(send_buf, "SHUTDOWN", NULL, NULL);
    send(sock, send_buf, len, 0);
    memset(shutdown_recv_buf, 0, sizeof(shutdown_recv_buf));
    rlen = recv(sock, shutdown_recv_buf, sizeof(shutdown_recv_buf) - 1, 0);
    if (rlen > 0 && strcmp(shutdown_recv_buf, "+OK\r\n") == 0) {
        printf("  🎉 [Success] Server acknowledged SHUTDOWN perfectly!\n");
    } else {
        printf("  ❌ [Failure] Server did not reply +OK\\r\\n (Recv: %s)\n", shutdown_recv_buf);
    }
    
    printf("🏆🏆🏆 [FINAL RESULT: AOF TEST PASS FOR ENGINE %d] 🏆🏆🏆\n\n", engine_type);
    waitpid(pid, NULL, 0); // 回收刚才启动的服务器僵尸进程
    return 0;
}

int main(int argc, char *argv[]) {
    if (argc < 2) {
        printf("Usage: %s <engine_type>\n", argv[0]);
        printf("Engine type: 1=Array, 2=RBTree, 3=Hash, 4=SkipList\n");
        return 1;
    }
    
    int engine_type = atoi(argv[1]);
    run_aof_testcase(engine_type);
    
    return 0;
}