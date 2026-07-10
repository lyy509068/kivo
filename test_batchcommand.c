#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <sys/socket.h>
#include <arpa/inet.h>
#include <errno.h>

#define SERVER_IP "127.0.0.1"
#define SERVER_PORT 2000
#define BATCH_SIZE  100        // 每批发送100条命令
#define TOTAL_BATCHES 1000    // 共发1000批 = 10万条
#define MAX_SEND_BUF (BATCH_SIZE * 256)
#define MAX_RECV_BUF (BATCH_SIZE * 256)

const char* SET_CMDS[] = {"", "SET", "RSET", "HSET", "SSET"};

int connect_server() {
    int sock = socket(AF_INET, SOCK_STREAM, 0);
    if (sock < 0) return -1;
    
    struct sockaddr_in serv_addr;
    serv_addr.sin_family = AF_INET;
    serv_addr.sin_port = htons(SERVER_PORT);
    inet_pton(AF_INET, SERVER_IP, &serv_addr.sin_addr);

    if (connect(sock, (struct sockaddr *)&serv_addr, sizeof(serv_addr)) < 0) {
        close(sock);
        return -1;
    }
    return sock;
}

// 拼一个 RESP SET 命令
int build_resp_cmd(char *buf, const char *cmd, const char *key, const char *val) {
    return sprintf(buf, "*3\r\n$%zu\r\n%s\r\n$%zu\r\n%s\r\n$%zu\r\n%s\r\n",
                   strlen(cmd), cmd, strlen(key), key, strlen(val), val);
}

// 可靠发送全部数据
int send_all(int fd, const char *buf, int len) {
    int sent = 0;
    while (sent < len) {
        int n = send(fd, buf + sent, len - sent, 0);
        if (n <= 0) return -1;
        sent += n;
    }
    return sent;
}

// 可靠接收指定长度数据
int recv_all(int fd, char *buf, int len) {
    int received = 0;
    while (received < len) {
        int n = recv(fd, buf + received, len - received, 0);
        if (n <= 0) return -1;
        received += n;
    }
    return received;
}

// 批量发送所有命令
int send_batch_commands(int sock, const char *set_cmd, char *send_buf, 
                        int batch_idx, int start_idx) {
    int total_len = 0;
    
    for (int i = 0; i < BATCH_SIZE; i++) {
        int idx = start_idx + i;
        char key[32], val[32];
        sprintf(key, "key_%06d", idx);
        sprintf(val, "val_%06d", idx);
        
        int cmd_len = build_resp_cmd(send_buf + total_len, set_cmd, key, val);
        total_len += cmd_len;
    }
    
    return send_all(sock, send_buf, total_len);
}

// 接收并校验 BATCH_SIZE 条 "+OK\r\n" 回复
int verify_batch_replies(int sock, char *recv_buf, int recv_buf_size,
                         int batch_idx, int start_idx) {
    // 需要接收的总字节数：BATCH_SIZE * 5（"+OK\r\n" = 5字节）
    int expected_total_bytes = BATCH_SIZE * 5;
    
    if (recv_buf_size < expected_total_bytes) {
        printf("[BATCH %d] recv_buf too small!\n", batch_idx);
        return -1;
    }
    
    // 阻塞接收全部回复
    if (recv_all(sock, recv_buf, expected_total_bytes) < 0) {
        printf("[BATCH %d] Failed to receive all replies (expected %d bytes)\n",
               batch_idx, expected_total_bytes);
        return -1;
    }
    
    // 逐条校验每条回复是否为 "+OK\r\n"
    for (int i = 0; i < BATCH_SIZE; i++) {
        char *reply = recv_buf + i * 5;
        if (memcmp(reply, "+OK\r\n", 5) != 0) {
            // 打印出错详情（安全截断）
            char bad_reply[32] = {0};
            memcpy(bad_reply, reply, 20);
            // 把 \r\n 替换成可见字符方便调试
            for (int j = 0; j < 20; j++) {
                if (bad_reply[j] == '\r') bad_reply[j] = 'R';
                else if (bad_reply[j] == '\n') bad_reply[j] = 'N';
            }
            printf("[BATCH %d][CMD %d] index=%d key=key_%06d "
                   "expected '+OK\\r\\n' but got '%s'\n",
                   batch_idx, i, start_idx + i, start_idx + i, bad_reply);
            return -1;
        }
    }
    
    return 0;
}

void test_engine(int engine_type) {
    const char *set_cmd = SET_CMDS[engine_type];
    
    printf("\n");
    printf("==================================================================\n");
    printf("Testing Engine %d [%s] — Batch Mode\n", engine_type, set_cmd);
    printf("   Batch size: %d | Total batches: %d | Total: %d records\n",
           BATCH_SIZE, TOTAL_BATCHES, BATCH_SIZE * TOTAL_BATCHES);
    printf("==================================================================\n");

    int sock = connect_server();
    if (sock < 0) {
        printf("[FATAL] Cannot connect to server!\n");
        return;
    }

    // 分配大缓冲区
    char *send_buf = (char *)malloc(MAX_SEND_BUF);
    char *recv_buf = (char *)malloc(MAX_RECV_BUF);
    if (!send_buf || !recv_buf) {
        printf("[FATAL] malloc failed!\n");
        close(sock);
        return;
    }

    int total_errors = 0;
    int record_idx = 0;

    for (int batch = 0; batch < TOTAL_BATCHES; batch++) {
        int start_idx = record_idx;
        
        // 批量发送100条命令
        if (send_batch_commands(sock, set_cmd, send_buf, batch, start_idx) < 0) {
            printf("[BATCH %d] send failed!\n", batch);
            total_errors++;
            break;
        }
        
        // 批量接收并校验100条回复
        if (verify_batch_replies(sock, recv_buf, MAX_RECV_BUF, batch, start_idx) < 0) {
            total_errors++;
            // 校验失败后连接状态可能混乱，中止测试
            break;
        }
        
        record_idx += BATCH_SIZE;
        
        // 每100批打印一次进度
        if ((batch + 1) % 100 == 0) {
            printf("  Completed %d batches (%d records)...\n",
                   batch + 1, record_idx);
        }
    }

    printf("\n--------------------------------------------------------\n");
    if (total_errors == 0) {
        printf("[Engine %d] ALL %d records PASSED!\n", 
               engine_type, record_idx);
    } else {
        printf("[Engine %d] %d errors detected. Only %d records processed.\n",
               engine_type, total_errors, record_idx);
    }
    printf("--------------------------------------------------------\n");

    free(send_buf);
    free(recv_buf);
    close(sock);
}

int main(int argc, char *argv[]) {

    if (argc >= 2) {
        // 指定引擎
        int engine_type = atoi(argv[1]);
        if (engine_type < 1 || engine_type > 4) {
            printf("Usage: %s [engine_type]\n", argv[0]);
            printf("  1: Array  2: RBTree  3: Hash  4: SkipList\n");
            printf("  (no arg = test all 4 engines)\n");
            return 1;
        }
        test_engine(engine_type);
    } else {
        // 测试全部4种引擎
        for (int eng = 1; eng <= 4; eng++) {
            test_engine(eng);
        }
    }

    printf("\nAll tests completed.\n");
    return 0;
}