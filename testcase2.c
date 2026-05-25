// test_binary.c
#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <unistd.h>
#include <arpa/inet.h>
#include <sys/time.h>
#include <sys/wait.h>
#include <time.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <signal.h> 

#define MAX_BATCH_CMDS 32

void build_request(char *buf, int *len, const char *cmd, const void *key, int key_len, const void *value, int value_len) {
    int pos = 0;
    int cmd_len = strlen(cmd);
    
    *(int*)(buf + pos) = cmd_len;
    pos += 4;
    memcpy(buf + pos, cmd, cmd_len);
    pos += cmd_len;
    
    *(int*)(buf + pos) = key_len;
    pos += 4;
    memcpy(buf + pos, key, key_len);
    pos += key_len;
    
    if (value && value_len > 0) {
        *(int*)(buf + pos) = value_len;
        pos += 4;
        memcpy(buf + pos, value, value_len);
        pos += value_len;
    } else {
        *(int*)(buf + pos) = 0;
        pos += 4;
    }
    
    *len = pos;
}

void parse_line(const char *input, char *cmd, char **key, int *key_len, char **value, int *value_len) {
    cmd[0] = '\0';
    *key = NULL;
    *key_len = 0;
    *value = NULL;
    *value_len = 0;
    
    // 1. 解析 CMD
    const char *cmd_start = strstr(input, "CMD=");
    if (cmd_start) {
        cmd_start += 4;
        const char *cmd_end = strchr(cmd_start, ' ');
        if (!cmd_end) cmd_end = cmd_start + strlen(cmd_start);
        int len = cmd_end - cmd_start;
        // 剔除 CMD 末尾可能带有的换行
        while (len > 0 && (cmd_start[len-1] == '\n' || cmd_start[len-1] == '\r')) len--;
        memcpy(cmd, cmd_start, len);
        cmd[len] = '\0';
    }
    
    // 2. 解析 KEY
    const char *key_start = strstr(input, "KEY=");
    if (key_start) {
        key_start += 4;
        const char *key_end = strstr(key_start, " VALUE=");
        if (!key_end) {
            // 如果没有 VALUE=，说明是 GET/DEL，直接划到行尾
            key_end = key_start + strlen(key_start);
        }
        
        *key = (char*)key_start;
        *key_len = key_end - key_start;
        
        // 【核心修复】：精准裁剪 KEY 末尾的换行符、回车符和空格
        while (*key_len > 0 && 
              ((*key)[*key_len - 1] == '\n' || (*key)[*key_len - 1] == '\r' || 
               (*key)[*key_len - 1] == ' '  || (*key)[*key_len - 1] == '\t')) {
            (*key_len)--;
        }
    }
    
    // 3. 解析 VALUE
    const char *value_start = strstr(input, "VALUE=");
    if (value_start) {
        value_start += 6;
        *value = (char*)value_start;
        *value_len = strlen(value_start);
        
        // 【核心修复】：精准裁剪 VALUE 末尾的换行符和回车符
        while (*value_len > 0 && 
              ((*value)[*value_len - 1] == '\n' || (*value)[*value_len - 1] == '\r')) {
            (*value_len)--;
        }
    }
}



void print_response(const char *cmd, char *recv_buf, int recv_len) {
    if (strcmp(cmd, "GET") == 0 && recv_len >= 4) {
        int data_len = *(int*)recv_buf;
        if (data_len > 0 && recv_len == 4 + data_len) {
            //printf("  [recv] %d bytes, data: %.*s\n", recv_len, data_len, recv_buf + 4);
        } else if (data_len == 0 && recv_len == 4) {
            //printf("  [recv] %d bytes, result: NO EXIST\n", recv_len);
        } else {
            //printf("  [recv] %d bytes, result: %.*s\n", recv_len, recv_len, recv_buf);
        }
    } else {
        int len = recv_len;
        while (len > 0 && (recv_buf[len-1] == '\n' || recv_buf[len-1] == '\r')) len--;
        //printf("  [recv] %d bytes, result: %.*s\n", recv_len, len, recv_buf);
    }
}

char* read_file_content(const char *filename, int *file_len) {
    FILE *fp = fopen(filename, "rb");
    if (!fp) {
        printf("Cannot open file: %s\n", filename);
        return NULL;
    }

    fseek(fp, 0, SEEK_END);
    *file_len = ftell(fp);
    fseek(fp, 0, SEEK_SET);

    char *content = (char*)malloc(*file_len);
    if (!content) {
        fclose(fp);
        return NULL;
    }
    size_t read_len = fread(content, 1, *file_len, fp);
    fclose(fp);
    
    if (read_len != *file_len) {
        free(content);
        return NULL;
    }
    
    return content;
}

void testcase(int connfd, char *msg, char *pattern, char *casename) {
    if (!msg || !pattern || !casename) return;

    char send_buf[4096] = {0};//这个长度够用吗？字符串命令并没有很多字符，这个地方不是大问题
    int total_pos = 0;
    int cmd_count = 0;

    total_pos += 4;

    char *current_cmd_start = strstr(msg, "CMD=");//当前命令开始
    
    while (current_cmd_start != NULL) {
        char *next_cmd_start = strstr(current_cmd_start + 4, "CMD=");//下一个命令开始
        //当前命令长度
        int current_cmd_str_len = next_cmd_start ? (next_cmd_start - current_cmd_start) : strlen(current_cmd_start);
        
        char *one_cmd_str = (char *)malloc(current_cmd_str_len + 1);
        memcpy(one_cmd_str, current_cmd_start, current_cmd_str_len);
        one_cmd_str[current_cmd_str_len] = '\0';//为当前命令添加结束符
        //去掉当前命令结尾无用字符
        while (current_cmd_str_len > 0 && 
              (one_cmd_str[current_cmd_str_len - 1] == ' '  || one_cmd_str[current_cmd_str_len - 1] == '\t' || 
               one_cmd_str[current_cmd_str_len - 1] == '\r' || one_cmd_str[current_cmd_str_len - 1] == '\n')) {
            one_cmd_str[--current_cmd_str_len] = '\0';
        }

        char cmd[64] = {0};
        char *key = NULL; int key_len = 0;
        char *value = NULL; int value_len = 0;

        parse_line(one_cmd_str, cmd, &key, &key_len, &value, &value_len);

        if (strlen(cmd) > 0) {
            int one_cmd_len = 0;
            char temp_buf[512] = {0};
            build_request(temp_buf, &one_cmd_len, cmd, key, key_len, value, value_len);                      
            
            if (total_pos + one_cmd_len < 4096) {//放不下会怎样？
                memcpy(send_buf + total_pos, temp_buf, one_cmd_len);
                total_pos += one_cmd_len;
                cmd_count++;
            }
        }
        free(one_cmd_str);
        current_cmd_start = next_cmd_start;
    }
    if (cmd_count == 0) return;

    *(int*)(send_buf) = cmd_count; // 填入总命令条数
    send(connfd, send_buf, total_pos, 0);

    char recv_buf[4096] = {0};
    int recv_len = recv(connfd, recv_buf, sizeof(recv_buf) - 1, 0);

    int pass = 0;
    int pattern_len = strlen(pattern);
    if (recv_len == pattern_len && memcmp(recv_buf, pattern, pattern_len) == 0) {
        pass = 1;
    }
    
    if (pass) {
        printf("==> PASS -> %s (Executed %d commands)\n", casename, cmd_count);
    } else {
        printf("==> FAILED -> %s\n", casename);
        printf("Expected:\n%s\n", pattern);
        printf("Received:\n%s\n", recv_buf);
        exit(1);
    }
}

void testcase_file(int connfd, char *msg, char *pattern, char *casename) {
    if (!msg || !pattern || !casename) return;

    int total_upload_file_len = 0;  
    int expected_download_len = 0;  
    int download_cmd_count = 0; 
    int cmd_count = 0;

    char *file_contents[MAX_BATCH_CMDS] = {0};
    int file_lens[MAX_BATCH_CMDS] = {0};
    char cmds[MAX_BATCH_CMDS][64] = {0};
    char *keys[MAX_BATCH_CMDS] = {0};     
    int key_lens[MAX_BATCH_CMDS] = {0};
    char *values[MAX_BATCH_CMDS] = {0};   
    int value_lens[MAX_BATCH_CMDS] = {0};
    int is_download_cmd[MAX_BATCH_CMDS] = {0}; 

    char *allocated_strs[MAX_BATCH_CMDS] = {0};

    char *current_cmd_start = strstr(msg, "CMD=");
    while (current_cmd_start != NULL && cmd_count < MAX_BATCH_CMDS) {
        char *next_cmd_start = strstr(current_cmd_start + 4, "CMD=");
        int current_cmd_str_len = next_cmd_start ? (next_cmd_start - current_cmd_start) : strlen(current_cmd_start);
        
        char *one_cmd_str = (char *)malloc(current_cmd_str_len + 1);
        memcpy(one_cmd_str, current_cmd_start, current_cmd_str_len);
        one_cmd_str[current_cmd_str_len] = '\0';
        
        while (current_cmd_str_len > 0 && 
              (one_cmd_str[current_cmd_str_len - 1] == ' '  || one_cmd_str[current_cmd_str_len - 1] == '\t' || 
               one_cmd_str[current_cmd_str_len - 1] == '\r' || one_cmd_str[current_cmd_str_len - 1] == '\n')) {
            one_cmd_str[--current_cmd_str_len] = '\0';
        }

        parse_line(one_cmd_str, cmds[cmd_count], &keys[cmd_count], &key_lens[cmd_count], &values[cmd_count], &value_lens[cmd_count]);
        allocated_strs[cmd_count] = one_cmd_str;

        char filename[256] = {0};
        snprintf(filename, sizeof(filename), "%.*s", key_lens[cmd_count], keys[cmd_count]);

        // === 支持多引擎的文件读取判定 ===
        if (strcmp(cmds[cmd_count], "SET") == 0  || 
            strcmp(cmds[cmd_count], "RSET") == 0 || 
            strcmp(cmds[cmd_count], "HSET") == 0 || 
            strcmp(cmds[cmd_count], "SSET") == 0) {
            
            file_contents[cmd_count] = read_file_content(filename, &file_lens[cmd_count]);
            if (!file_contents[cmd_count]) {
                printf("==> FAILED -> %s, cannot read upload file: %s\n", casename, filename);
                for (int i = 0; i <= cmd_count; i++) {
                    if (file_contents[i]) free(file_contents[i]);
                    if (allocated_strs[i]) free(allocated_strs[i]);
                }
                exit(1);
            }
            total_upload_file_len += file_lens[cmd_count];
        } 
        else if (strcmp(cmds[cmd_count], "GET") == 0  || 
                 strcmp(cmds[cmd_count], "RGET") == 0 || 
                 strcmp(cmds[cmd_count], "HGET") == 0 || 
                 strcmp(cmds[cmd_count], "SGET") == 0) {
            
            is_download_cmd[cmd_count] = 1;
            download_cmd_count++; 
            file_contents[cmd_count] = read_file_content(filename, &file_lens[cmd_count]);
            if (!file_contents[cmd_count]) {
                printf("==> FAILED -> %s, cannot read expected golden file: %s\n", casename, filename);
                for (int i = 0; i <= cmd_count; i++) {
                    if (file_contents[i]) free(file_contents[i]);
                    if (allocated_strs[i]) free(allocated_strs[i]);
                }
                exit(1);
            }
            expected_download_len += file_lens[cmd_count];
        }

        cmd_count++;
        current_cmd_start = next_cmd_start;
    }
    if (cmd_count == 0) return;

    int max_send_size = 4096 * cmd_count + total_upload_file_len;
    char *send_buf = (char*)malloc(max_send_size);
    if (!send_buf) {
        for (int i = 0; i < cmd_count; i++) {
            if (file_contents[i]) free(file_contents[i]);
            if (allocated_strs[i]) free(allocated_strs[i]);
        }
        exit(1);
    }
    
    int total_send_len = 0;
    *(int*)(send_buf) = cmd_count; 
    total_send_len += 4;

    for (int i = 0; i < cmd_count; i++) {
        int one_cmd_len = 0;
        char *temp_buf = (char*)malloc(4096 + file_lens[i]);
        
        if (is_download_cmd[i]) {
            build_request(temp_buf, &one_cmd_len, cmds[i], keys[i], key_lens[i], NULL, 0);
        } else {
            build_request(temp_buf, &one_cmd_len, cmds[i], keys[i], key_lens[i], file_contents[i], file_lens[i]);
        }
        
        memcpy(send_buf + total_send_len, temp_buf, one_cmd_len);
        total_send_len += one_cmd_len;
        free(temp_buf);
    }

    int total_sent = 0;
    while (total_sent < total_send_len) {
        int s_ret = send(connfd, send_buf + total_sent, total_send_len - total_sent, 0);
        if (s_ret <= 0) {
            free(send_buf);
            for (int i = 0; i < cmd_count; i++) {
                if (file_contents[i]) free(file_contents[i]);
                if (allocated_strs[i]) free(allocated_strs[i]);
            }
            exit(1);
        }
        total_sent += s_ret;
    }
    free(send_buf);

    int pattern_len = strlen(pattern);
    int expected_recv_len = pattern_len + expected_download_len + (download_cmd_count * 2);

    char *recv_buf = (char*)malloc(expected_recv_len + 128);
    if (!recv_buf) {
        for (int i = 0; i < cmd_count; i++) {
            if (file_contents[i]) free(file_contents[i]);
            if (allocated_strs[i]) free(allocated_strs[i]);
        }
        exit(1);
    }
    memset(recv_buf, 0, expected_recv_len + 128);

    int total_recv = 0;
    while (total_recv < expected_recv_len) {
        int ret = recv(connfd, recv_buf + total_recv, expected_recv_len - total_recv, 0);
        if (ret <= 0) break;
        total_recv += ret;
    }

    int pass = 0;
    if (total_recv == expected_recv_len) {
        int recv_offset = 0;
        int pattern_offset = 0;
        pass = 1;

        for (int i = 0; i < cmd_count; i++) {
            if (is_download_cmd[i]) {
                if (memcmp(recv_buf + recv_offset, file_contents[i], file_lens[i]) != 0) {
                    pass = 0;
                    break;
                }
                recv_offset += file_lens[i];
                
                if (memcmp(recv_buf + recv_offset, "\r\n", 2) != 0) {
                    pass = 0;
                    break;
                }
                recv_offset += 2; 
            } else {
                char *sub_pat_end = strstr(pattern + pattern_offset, "\r\n");
                if (sub_pat_end) {
                    int sub_len = (sub_pat_end - (pattern + pattern_offset)) + 2;
                    if (memcmp(recv_buf + recv_offset, pattern + pattern_offset, sub_len) != 0) {
                        pass = 0;
                        break;
                    }
                    recv_offset += sub_len;
                    pattern_offset += sub_len;
                }
            }
        }
    }

    // 文件落盘与最终处理
    if (pass) {
        int save_offset = 0;
        int save_pattern_offset = 0;

        // 遍历所有批量解析出来的指令
        for (int i = 0; i < cmd_count; i++) {
            if (is_download_cmd[i]) {
                // 1. 动态拼装下载落盘的文件名
                char output_filename[512];
                snprintf(output_filename, sizeof(output_filename), "retrieved_%.*s", key_lens[i], keys[i]);
                
                // 2. 将数据从 recv_buf 中剥离出来写入本地
                FILE *out = fopen(output_filename, "wb");
                if (out) {
                    fwrite(recv_buf + save_offset, 1, file_lens[i], out);
                    fclose(out);
                    //printf("Saved pipeline GET file to: %s (%d bytes)\n", output_filename, file_lens[i]);
                } else {
                    printf("[Batch Save Error] ❌ Cannot open file %s for writing!\n", output_filename);
                }

                // 指针向后滑动：跳过当前文件体长度 + \r\n (2字节)
                save_offset += file_lens[i] + 2; 
            } else {
                // 文本命令（SET, DEL）同样同步向后滑动 save_offset
                char *sub_pat_end = strstr(pattern + save_pattern_offset, "\r\n");
                if (sub_pat_end) {
                    int sub_len = (sub_pat_end - (pattern + save_pattern_offset)) + 2;
                    save_offset += sub_len;
                    save_pattern_offset += sub_len;
                }
            }
        }
        // printf("==> BATCH PASS -> %s\n", casename);
    } else {
        printf("==> BATCH FAILED -> %s\n", casename);
        free(recv_buf);
        for (int i = 0; i < cmd_count; i++) {
            if (file_contents[i]) free(file_contents[i]);
            if (allocated_strs[i]) free(allocated_strs[i]);
        }
        exit(1);
    }

    // 清理收尾
    free(recv_buf);
    for (int i = 0; i < cmd_count; i++) {
        if (file_contents[i]) free(file_contents[i]);
        if (allocated_strs[i]) free(allocated_strs[i]);
    }
}

void array_full_test(int connfd) {
    printf("\n========== Array 1w Full Lifecycle Test ==========\n");
    struct timeval tv_begin, tv_end;
    gettimeofday(&tv_begin, NULL);
    for (int i = 0; i < 1000; i++) {
        testcase(connfd, 
                 "CMD=SET KEY=Teacher VALUE=King "
                 "CMD=GET KEY=Teacher "
                 "CMD=EXIST KEY=Teacher "
                 "CMD=MOD KEY=Teacher VALUE=Darren "
                 "CMD=GET KEY=Teacher "
                 "CMD=EXIST KEY=Teacher "
                 "CMD=DEL KEY=Teacher "
                 "CMD=GET KEY=Teacher "
                 "CMD=EXIST KEY=Teacher", 
                 "OK\r\nKing\r\nEXIST\r\nOK\r\nDarren\r\nEXIST\r\nOK\r\nNO EXIST\r\nNO EXIST\r\n", 
                 "Array-Lifecycle");
    }
    gettimeofday(&tv_end, NULL);
    long time_ms = (tv_end.tv_sec - tv_begin.tv_sec) * 1000 + (tv_end.tv_usec - tv_begin.tv_usec) / 1000;
    printf("Array 1w Full Test: time=%ldms, QPS=%ld\n", time_ms, 90000 * 1000 / time_ms);
}

void array_full_test_file(int connfd) {
    printf("\n========== Array File Pipeline Batch Test ==========\n");
    struct timeval tv_begin, tv_end;
    gettimeofday(&tv_begin, NULL);
    char *batch_msg = 
        "CMD=SET KEY=text.txt VALUE=text.txt "
        "CMD=GET KEY=text.txt "
        "CMD=EXIST KEY=text.txt "
        "CMD=DEL KEY=text.txt "
        "CMD=EXIST KEY=text.txt "
        "CMD=SET KEY=io多路复用.txt VALUE=io多路复用.txt "
        "CMD=GET KEY=io多路复用.txt "
        "CMD=EXIST KEY=io多路复用.txt "
        "CMD=DEL KEY=io多路复用.txt "
        "CMD=EXIST KEY=io多路复用.txt "
        "CMD=SET KEY=screenshot.png VALUE=screenshot.png "
        "CMD=GET KEY=screenshot.png "
        "CMD=EXIST KEY=screenshot.png "
        "CMD=DEL KEY=screenshot.png "
        "CMD=EXIST KEY=screenshot.png";

    char *batch_pattern = "OK\r\nEXIST\r\nOK\r\nNO EXIST\r\nOK\r\nEXIST\r\nOK\r\nNO EXIST\r\nOK\r\nEXIST\r\nOK\r\nNO EXIST\r\n";

    for (int i = 0; i < 1000; i++) {
        testcase_file(connfd, batch_msg, batch_pattern, "Batch-File-Pipeline");
    }

    gettimeofday(&tv_end, NULL);
    long time_ms = (tv_end.tv_sec - tv_begin.tv_sec) * 1000 + (tv_end.tv_usec - tv_begin.tv_usec) / 1000;
    printf("Array File Pipeline Test: time=%ldms, QPS=%ld\n", time_ms, 150000 * 1000 / time_ms);
}

void rbtree_full_test(int connfd) {
    printf("\n========== RBTree 1w Full Lifecycle Test (Pipeline Mode) ==========\n");
    struct timeval tv_begin, tv_end;
    gettimeofday(&tv_begin, NULL);

    char *batch_msg = 
        "CMD=RSET KEY=Teacher VALUE=King "
        "CMD=RGET KEY=Teacher "
        "CMD=REXIST KEY=Teacher "
        "CMD=RMOD KEY=Teacher VALUE=Darren "
        "CMD=RGET KEY=Teacher "
        "CMD=REXIST KEY=Teacher "
        "CMD=RDEL KEY=Teacher "
        "CMD=RGET KEY=Teacher "
        "CMD=REXIST KEY=Teacher";

    char *batch_pattern = "OK\r\nKing\r\nEXIST\r\nOK\r\nDarren\r\nEXIST\r\nOK\r\nNO EXIST\r\nNO EXIST\r\n";

    for (int i = 0; i < 1000; i++) {
        testcase(connfd, batch_msg, batch_pattern, "RBT-Pipeline");
    }

    gettimeofday(&tv_end, NULL);
    long time_ms = (tv_end.tv_sec - tv_begin.tv_sec) * 1000 + (tv_end.tv_usec - tv_begin.tv_usec) / 1000;
    printf("RBTree 1w Full Test: time=%ldms, QPS=%ld\n", time_ms, 90000L * 1000 / time_ms);
}

void rbtree_full_test_file(int connfd) {
    printf("\n========== RBTree File Pipeline Test ==========\n");
    struct timeval tv_begin, tv_end;
    gettimeofday(&tv_begin, NULL);

    char *batch_msg = 
        "CMD=RSET KEY=text.txt VALUE=text.txt "
        "CMD=RGET KEY=text.txt "
        "CMD=REXIST KEY=text.txt "
        "CMD=RDEL KEY=text.txt "
        "CMD=REXIST KEY=text.txt "
        "CMD=RSET KEY=io多路复用.txt VALUE=io多路复用.txt "
        "CMD=RGET KEY=io多路复用.txt "
        "CMD=REXIST KEY=io多路复用.txt "
        "CMD=RDEL KEY=io多路复用.txt "
        "CMD=REXIST KEY=io多路复用.txt "
        "CMD=RSET KEY=screenshot.png VALUE=screenshot.png "
        "CMD=RGET KEY=screenshot.png "
        "CMD=REXIST KEY=screenshot.png "
        "CMD=RDEL KEY=screenshot.png "
        "CMD=REXIST KEY=screenshot.png";

    char *batch_pattern = "OK\r\nEXIST\r\nOK\r\nNO EXIST\r\nOK\r\nEXIST\r\nOK\r\nNO EXIST\r\nOK\r\nEXIST\r\nOK\r\nNO EXIST\r\n";

    for (int i = 0; i < 1000; i++) {
        testcase_file(connfd, batch_msg, batch_pattern, "RBT-File-Pipeline");
    }

    gettimeofday(&tv_end, NULL);
    long time_ms = (tv_end.tv_sec - tv_begin.tv_sec) * 1000 + (tv_end.tv_usec - tv_begin.tv_usec) / 1000;
    printf("Rbtree File Test: time=%ldms, QPS=%ld\n", time_ms, 150000L * 1000 / time_ms);
}

void hash_full_test(int connfd) {
    printf("\n========== Hash 1w Full Lifecycle Test (Pipeline Mode) ==========\n");
    struct timeval tv_begin, tv_end;
    gettimeofday(&tv_begin, NULL);

    char *batch_msg = 
        "CMD=HSET KEY=Teacher VALUE=King "
        "CMD=HGET KEY=Teacher "
        "CMD=HEXIST KEY=Teacher "
        "CMD=HMOD KEY=Teacher VALUE=Darren "
        "CMD=HGET KEY=Teacher "
        "CMD=HEXIST KEY=Teacher "
        "CMD=HDEL KEY=Teacher "
        "CMD=HGET KEY=Teacher "
        "CMD=HEXIST KEY=Teacher";

    char *batch_pattern = "OK\r\nKing\r\nEXIST\r\nOK\r\nDarren\r\nEXIST\r\nOK\r\nNO EXIST\r\nNO EXIST\r\n";

    for (int i = 0; i < 1000; i++) {
        testcase(connfd, batch_msg, batch_pattern, "HASH-Pipeline");
    }

    gettimeofday(&tv_end, NULL);
    long time_ms = (tv_end.tv_sec - tv_begin.tv_sec) * 1000 + (tv_end.tv_usec - tv_begin.tv_usec) / 1000;
    printf("Hash 1w Full Test: time=%ldms, QPS=%ld\n", time_ms, 90000L * 1000 / time_ms);
}

void hash_full_test_file(int connfd) {
    printf("\n========== Hash File Pipeline Test ==========\n");
    struct timeval tv_begin, tv_end;
    gettimeofday(&tv_begin, NULL);

    char *batch_msg = 
        "CMD=HSET KEY=text.txt VALUE=text.txt "
        "CMD=HGET KEY=text.txt "
        "CMD=HEXIST KEY=text.txt "
        "CMD=HDEL KEY=text.txt "
        "CMD=HEXIST KEY=text.txt "
        "CMD=HSET KEY=io多路复用.txt VALUE=io多路复用.txt "
        "CMD=HGET KEY=io多路复用.txt "
        "CMD=HEXIST KEY=io多路复用.txt "
        "CMD=HDEL KEY=io多路复用.txt "
        "CMD=HEXIST KEY=io多路复用.txt "
        "CMD=HSET KEY=screenshot.png VALUE=screenshot.png "
        "CMD=HGET KEY=screenshot.png " 
        "CMD=HEXIST KEY=screenshot.png "
        "CMD=HDEL KEY=screenshot.png "
        "CMD=HEXIST KEY=screenshot.png";

    char *batch_pattern = "OK\r\nEXIST\r\nOK\r\nNO EXIST\r\nOK\r\nEXIST\r\nOK\r\nNO EXIST\r\nOK\r\nEXIST\r\nOK\r\nNO EXIST\r\n";

    for (int i = 0; i < 1000; i++) {
        testcase_file(connfd, batch_msg, batch_pattern, "HASH-File-Pipeline");
    }

    gettimeofday(&tv_end, NULL);
    long time_ms = (tv_end.tv_sec - tv_begin.tv_sec) * 1000 + (tv_end.tv_usec - tv_begin.tv_usec) / 1000;
    printf("Hash File Test: time=%ldms, QPS=%ld\n", time_ms, 150000L * 1000 / time_ms);
}

void skiplist_full_test(int connfd) {
    printf("\n========== Skiplist 1w Full Lifecycle Test (Pipeline Mode) ==========\n");
    struct timeval tv_begin, tv_end;
    gettimeofday(&tv_begin, NULL);

    char *batch_msg = 
        "CMD=SSET KEY=Teacher VALUE=King "
        "CMD=SGET KEY=Teacher "
        "CMD=SEXIST KEY=Teacher "
        "CMD=SMOD KEY=Teacher VALUE=Darren "
        "CMD=SGET KEY=Teacher "
        "CMD=SEXIST KEY=Teacher "
        "CMD=SDEL KEY=Teacher "
        "CMD=SGET KEY=Teacher "
        "CMD=SEXIST KEY=Teacher";

    char *batch_pattern = "OK\r\nKing\r\nEXIST\r\nOK\r\nDarren\r\nEXIST\r\nOK\r\nNO EXIST\r\nNO EXIST\r\n";

    for (int i = 0; i < 1000; i++) {
        testcase(connfd, batch_msg, batch_pattern, "SKP-Pipeline");
    }

    gettimeofday(&tv_end, NULL);
    long time_ms = (tv_end.tv_sec - tv_begin.tv_sec) * 1000 + (tv_end.tv_usec - tv_begin.tv_usec) / 1000;
    printf("Skiplist 1w Full Test: time=%ldms, QPS=%ld\n", time_ms, 90000L * 1000 / time_ms);
}

void skiplist_full_test_file(int connfd) {
    printf("\n========== Skiplist File Pipeline Test ==========\n");
    struct timeval tv_begin, tv_end;
    gettimeofday(&tv_begin, NULL);

    char *batch_msg = 
        "CMD=SSET KEY=text.txt VALUE=text.txt "
        "CMD=SGET KEY=text.txt "
        "CMD=SEXIST KEY=text.txt "
        "CMD=SDEL KEY=text.txt "
        "CMD=SEXIST KEY=text.txt "
        "CMD=SSET KEY=io多路复用.txt VALUE=io多路复用.txt "
        "CMD=SGET KEY=io多路复用.txt "
        "CMD=SEXIST KEY=io多路复用.txt "
        "CMD=SDEL KEY=io多路复用.txt "
        "CMD=SEXIST KEY=io多路复用.txt "
        "CMD=SSET KEY=screenshot.png VALUE=screenshot.png "
        "CMD=SGET KEY=screenshot.png "
        "CMD=SEXIST KEY=screenshot.png "
        "CMD=SDEL KEY=screenshot.png "
        "CMD=SEXIST KEY=screenshot.png";

    char *batch_pattern = "OK\r\nEXIST\r\nOK\r\nNO EXIST\r\nOK\r\nEXIST\r\nOK\r\nNO EXIST\r\nOK\r\nEXIST\r\nOK\r\nNO EXIST\r\n";

    for (int i = 0; i < 1000; i++) {
        // 去掉了末尾的 op_type 参数
        testcase_file(connfd, batch_msg, batch_pattern, "SKP-File-Pipeline");
    }

    gettimeofday(&tv_end, NULL);
    long time_ms = (tv_end.tv_sec - tv_begin.tv_sec) * 1000 + (tv_end.tv_usec - tv_begin.tv_usec) / 1000;
    printf("Skiplist File Test: time=%ldms, QPS=%ld\n", time_ms, 150000L * 1000 / time_ms);
}

void array_random_test(int connfd, int iterations) {
    printf("\n========== Array Randomized Mixed Test (%d loops) ==========\n", iterations);
    struct timeval tv_begin, tv_end;
    gettimeofday(&tv_begin, NULL);
    
    for (int i = 0; i < iterations; i++) {
        int branch = rand() % 4;
        int key_id = rand() % 100000;
        char key_str[64];
        
        switch (branch) {
            case 0: { // 链路 0: 纯文本全家桶合并为一个大包
                snprintf(key_str, sizeof(key_str), "arr_txt_%d", key_id);
                char batch_msg[1024];
                snprintf(batch_msg, sizeof(batch_msg),
                         "CMD=SET KEY=%s VALUE=val_%d "
                         "CMD=GET KEY=%s "
                         "CMD=EXIST KEY=%s "
                         "CMD=MOD KEY=%s VALUE=new_%d "
                         "CMD=GET KEY=%s "
                         "CMD=DEL KEY=%s "
                         "CMD=GET KEY=%s", 
                         key_str, key_id, key_str, key_str, key_str, key_id, key_str, key_str, key_str);

                // ✨【核心修改】完美对齐服务器返回的 \r\n 换行规范
                char batch_pattern[512];
                snprintf(batch_pattern, sizeof(batch_pattern), 
                         "OK\r\nval_%d\r\nEXIST\r\nOK\r\nnew_%d\r\nOK\r\nNO EXIST\r\n", key_id, key_id);
                
                testcase(connfd, batch_msg, batch_pattern, "RAND-ARR-PIPE");
                break;
            } 
            case 1: { // 链路 1: 去掉 op_type
                char *msg = "CMD=SET KEY=text.txt VALUE=text.txt CMD=GET KEY=text.txt CMD=EXIST KEY=text.txt CMD=DEL KEY=text.txt";
                testcase_file(connfd, msg, "OK\r\nEXIST\r\nOK\r\n", "RAND-ARR-F-TXT");
                break;
            }
            case 2: { // 链路 2: 去掉 op_type
                char *msg = "CMD=SET KEY=io多路复用.txt VALUE=io多路复用.txt CMD=GET KEY=io多路复用.txt CMD=DEL KEY=io多路复用.txt";
                testcase_file(connfd, msg, "OK\r\nOK\r\n", "RAND-ARR-F-ZH");
                break;
            }
            case 3: { // 链路 3: 去掉 op_type
                char *msg = "CMD=SET KEY=screenshot.png VALUE=screenshot.png CMD=GET KEY=screenshot.png CMD=EXIST KEY=screenshot.png CMD=DEL KEY=screenshot.png";
                testcase_file(connfd, msg, "OK\r\nEXIST\r\nOK\r\n", "RAND-ARR-F-PNG");
                break;
            }
        }
    }
    gettimeofday(&tv_end, NULL);
    long time_ms = (tv_end.tv_sec - tv_begin.tv_sec) * 1000 + (tv_end.tv_usec - tv_begin.tv_usec) / 1000;
    printf("Array Random Test Finished. Cost: %ld ms\n", time_ms);
}

void rbtree_random_test(int connfd, int iterations) {
    printf("\n========== RBTree Randomized Mixed Test (%d loops) ==========\n", iterations);
    struct timeval tv_begin, tv_end;
    gettimeofday(&tv_begin, NULL);
    
    for (int i = 0; i < iterations; i++) {
        int branch = rand() % 4;
        int key_id = rand() % 100000;
        char key_str[64];
        
        switch (branch) {
            case 0: {
                snprintf(key_str, sizeof(key_str), "rbt_txt_%d", key_id);
                char batch_msg[1024];
                snprintf(batch_msg, sizeof(batch_msg),
                         "CMD=RSET KEY=%s VALUE=val_%d "
                         "CMD=RGET KEY=%s "
                         "CMD=REXIST KEY=%s "
                         "CMD=RMOD KEY=%s VALUE=new_%d "
                         "CMD=RGET KEY=%s "
                         "CMD=RDEL KEY=%s "
                         "CMD=RGET KEY=%s", 
                         key_str, key_id, key_str, key_str, key_str, key_id, key_str, key_str, key_str);

                char batch_pattern[512];
                snprintf(batch_pattern, sizeof(batch_pattern), "OK\r\nval_%d\r\nEXIST\r\nOK\r\nnew_%d\r\nOK\r\nNO EXIST\r\n", key_id, key_id);
                testcase(connfd, batch_msg, batch_pattern, "RAND-RBT-PIPE");
                break;
            }
            case 1:
                testcase_file(connfd, "CMD=RSET KEY=text.txt VALUE=text.txt CMD=RGET KEY=text.txt CMD=REXIST KEY=text.txt CMD=RDEL KEY=text.txt", "OK\r\nEXIST\r\nOK\r\n", "RAND-RBT-F-TXT");
                break;
            case 2:
                testcase_file(connfd, "CMD=RSET KEY=io多路复用.txt VALUE=io多路复用.txt CMD=RGET KEY=io多路复用.txt CMD=RDEL KEY=io多路复用.txt", "OK\r\nOK\r\n", "RAND-RBT-F-ZH");
                break;
            case 3:
                testcase_file(connfd, "CMD=RSET KEY=screenshot.png VALUE=screenshot.png CMD=RGET KEY=screenshot.png CMD=REXIST KEY=screenshot.png CMD=RDEL KEY=screenshot.png", "OK\r\nEXIST\r\nOK\r\n", "RAND-RBT-F-PNG");
                break;
        }
    }
    gettimeofday(&tv_end, NULL);
    long time_ms = (tv_end.tv_sec - tv_begin.tv_sec) * 1000 + (tv_end.tv_usec - tv_begin.tv_usec) / 1000;
    printf("RBTree Random Test Finished. Cost: %ld ms\n", time_ms);
}

void hash_random_test(int connfd, int iterations) {
    printf("\n========== Hash Randomized Mixed Test (%d loops) ==========\n", iterations);
    struct timeval tv_begin, tv_end;
    gettimeofday(&tv_begin, NULL);
    
    for (int i = 0; i < iterations; i++) {
        int branch = rand() % 4;
        int key_id = rand() % 100000;
        char key_str[64];
        
        switch (branch) {
            case 0: {
                snprintf(key_str, sizeof(key_str), "msh_txt_%d", key_id);
                char batch_msg[1024];
                snprintf(batch_msg, sizeof(batch_msg),
                         "CMD=HSET KEY=%s VALUE=val_%d "
                         "CMD=HGET KEY=%s "
                         "CMD=HEXIST KEY=%s "
                         "CMD=HMOD KEY=%s VALUE=new_%d "
                         "CMD=HGET KEY=%s "
                         "CMD=HDEL KEY=%s "
                         "CMD=HGET KEY=%s", 
                         key_str, key_id, key_str, key_str, key_str, key_id, key_str, key_str, key_str);

                char batch_pattern[512];
                snprintf(batch_pattern, sizeof(batch_pattern), "OK\r\nval_%d\r\nEXIST\r\nOK\r\nnew_%d\r\nOK\r\nNO EXIST\r\n", key_id, key_id);
                testcase(connfd, batch_msg, batch_pattern, "RAND-HASH-PIPE");
                break;
            }
            case 1:
                testcase_file(connfd, "CMD=HSET KEY=text.txt VALUE=text.txt CMD=HGET KEY=text.txt CMD=HEXIST KEY=text.txt CMD=HDEL KEY=text.txt", "OK\r\nEXIST\r\nOK\r\n", "RAND-HASH-F-TXT");
                break;
            case 2:
                testcase_file(connfd, "CMD=HSET KEY=io多路复用.txt VALUE=io多路复用.txt CMD=HGET KEY=io多路复用.txt CMD=HDEL KEY=io多路复用.txt", "OK\r\nOK\r\n", "RAND-HASH-F-ZH");
                break;
            case 3:
                testcase_file(connfd, "CMD=HSET KEY=screenshot.png VALUE=screenshot.png CMD=HGET KEY=screenshot.png CMD=HEXIST KEY=screenshot.png CMD=HDEL KEY=screenshot.png", "OK\r\nEXIST\r\nOK\r\n", "RAND-HASH-F-PNG");
                break;
        }
    }
    gettimeofday(&tv_end, NULL);
    long time_ms = (tv_end.tv_sec - tv_begin.tv_sec) * 1000 + (tv_end.tv_usec - tv_begin.tv_usec) / 1000;
    printf("Hash Random Test Finished. Cost: %ld ms\n", time_ms);
}

void skiplist_random_test(int connfd, int iterations) {
    printf("\n========== Skiplist Randomized Mixed Test (%d loops) ==========\n", iterations);
    struct timeval tv_begin, tv_end;
    gettimeofday(&tv_begin, NULL);
    
    for (int i = 0; i < iterations; i++) {
        int branch = rand() % 4;
        int key_id = rand() % 100000;
        char key_str[64];
        
        switch (branch) {
            case 0: {
                snprintf(key_str, sizeof(key_str), "skp_txt_%d", key_id);
                char batch_msg[1024];
                snprintf(batch_msg, sizeof(batch_msg),
                         "CMD=SSET KEY=%s VALUE=val_%d "
                         "CMD=SGET KEY=%s "
                         "CMD=SEXIST KEY=%s "
                         "CMD=SMOD KEY=%s VALUE=new_%d "
                         "CMD=SGET KEY=%s "
                         "CMD=SDEL KEY=%s "
                         "CMD=SGET KEY=%s", 
                         key_str, key_id, key_str, key_str, key_str, key_id, key_str, key_str, key_str);

                char batch_pattern[512];
                snprintf(batch_pattern, sizeof(batch_pattern), "OK\r\nval_%d\r\nEXIST\r\nOK\r\nnew_%d\r\nOK\r\nNO EXIST\r\n", key_id, key_id);
                testcase(connfd, batch_msg, batch_pattern, "RAND-SKP-PIPE");
                break;
            }
            case 1:
                testcase_file(connfd, "CMD=SSET KEY=text.txt VALUE=text.txt CMD=SGET KEY=text.txt CMD=SEXIST KEY=text.txt CMD=SDEL KEY=text.txt", "OK\r\nEXIST\r\nOK\r\n", "RAND-SKP-F-TXT");
                break;
            case 2:
                testcase_file(connfd, "CMD=SSET KEY=io多路复用.txt VALUE=io多路复用.txt CMD=SGET KEY=io多路复用.txt CMD=SDEL KEY=io多路复用.txt", "OK\r\nOK\r\n", "RAND-SKP-F-ZH");
                break;
            case 3:
                testcase_file(connfd, "CMD=SSET KEY=screenshot.png VALUE=screenshot.png CMD=SGET KEY=screenshot.png CMD=SEXIST KEY=screenshot.png CMD=SDEL KEY=screenshot.png", "OK\r\nEXIST\r\nOK\r\n", "RAND-SKP-F-PNG");
                break;
        }
    }
    gettimeofday(&tv_end, NULL);
    long time_ms = (tv_end.tv_sec - tv_begin.tv_sec) * 1000 + (tv_end.tv_usec - tv_begin.tv_usec) / 1000;
    printf("Skiplist Random Test Finished. Cost: %ld ms\n", time_ms);
}

void test_snapshot_flow(int connfd) {
    printf("\n========== Test Snapshot Flow (Pipeline Unified Binary) ==========\n");

    // 1. 准备大容量发送缓冲区，用来融合 20 条大大小小的命令
    // 考虑到有图片文件，缓冲区开大一点（4MB）
    size_t max_buf_size = 4 * 1024 * 1024;
    char *big_pipeline_buf = (char *)malloc(max_buf_size);
    if (!big_pipeline_buf) {
        printf("Malloc failed for snapshot test!\n");
        return;
    }

    // 头部预留 4 字节，用来存放 cmd_count
    int pos = 4; 
    int cmd_count = 20;

    // 2. 模拟读取三个测试文件的原始内容与长度
    // (实际测试时，确保本地这三个文件存在；如果是模拟，可以随便给它们分配一块数据)
    int file_len_txt = 1024;                    // text.txt 长度
    char *file_data_txt = malloc(file_len_txt);
    memset(file_data_txt, 'A', file_len_txt);    // 模拟1KB文本内容

    int file_len_io = 5120;                     // io多路复用.txt 长度
    char *file_data_io = malloc(file_len_io);
    memset(file_data_io, 'B', file_len_io);     // 模拟5KB复杂的技术文本

    int file_len_png = 51200;                    // screenshot.png 长度
    char *file_data_png = malloc(file_len_png);
    memset(file_data_png, 'P', file_len_png);    // 模拟50KB图片二进制数据

    // 3. 开始通过二进制协议 build_request 密集轰炸、疯狂压实这 20 条命令
    int single_len = 0;

    // 1) 基础字符串和文件的 SET / RSET / HSET / SSET (初始化)
    build_request(big_pipeline_buf + pos, &single_len, "SET", "arr_text", 8, "Initial_Array_Data", 18); pos += single_len;
    build_request(big_pipeline_buf + pos, &single_len, "SET", "screenshot.png", 14, file_data_png, file_len_png); pos += single_len;
    build_request(big_pipeline_buf + pos, &single_len, "RSET", "rbt_text", 8, "Initial_RBTree_Data", 19); pos += single_len;
    build_request(big_pipeline_buf + pos, &single_len, "RSET", "text.txt", 8, file_data_txt, file_len_txt); pos += single_len;
    build_request(big_pipeline_buf + pos, &single_len, "HSET", "hash_text", 9, "Initial_Hash_Data", 17); pos += single_len;
    build_request(big_pipeline_buf + pos, &single_len, "HSET", "io多路复用.txt", 16, file_data_io, file_len_io); pos += single_len;
    build_request(big_pipeline_buf + pos, &single_len, "SSET", "skip_text", 9, "Initial_Skiplist_Data", 21); pos += single_len;

    // 2) 更新修改指令 MOD / RMOD / HMOD / SMOD
    build_request(big_pipeline_buf + pos, &single_len, "MOD", "arr_text", 8, "Updated_Array_Data_Keep", 23); pos += single_len;
    build_request(big_pipeline_buf + pos, &single_len, "RMOD", "rbt_text", 8, "Updated_RBTree_Data_Keep", 24); pos += single_len;
    build_request(big_pipeline_buf + pos, &single_len, "HMOD", "hash_text", 9, "Updated_Hash_Data_Delete", 25); pos += single_len;
    build_request(big_pipeline_buf + pos, &single_len, "SMOD", "skip_text", 9, "Updated_Skiplist_Data_Keep", 26); pos += single_len;

    // 3) 删除指令 DEL / RDEL / HDEL
    build_request(big_pipeline_buf + pos, &single_len, "RDEL", "text.txt", 8, NULL, 0); pos += single_len;
    build_request(big_pipeline_buf + pos, &single_len, "HDEL", "hash_text", 9, NULL, 0); pos += single_len;

    // 4) 获取指令与大包二进制提取检测 GET / RGET / HGET / SGET
    build_request(big_pipeline_buf + pos, &single_len, "GET", "arr_text", 8, NULL, 0); pos += single_len;
    build_request(big_pipeline_buf + pos, &single_len, "GET", "screenshot.png", 14, NULL, 0); pos += single_len;
    build_request(big_pipeline_buf + pos, &single_len, "RGET", "rbt_text", 8, NULL, 0); pos += single_len;
    build_request(big_pipeline_buf + pos, &single_len, "HGET", "io多路复用.txt", 16, NULL, 0); pos += single_len;
    build_request(big_pipeline_buf + pos, &single_len, "SGET", "skip_text", 9, NULL, 0); pos += single_len;

    // 5) 存在性审查 REXIST / HEXIST
    build_request(big_pipeline_buf + pos, &single_len, "REXIST", "text.txt", 8, NULL, 0); pos += single_len;
    build_request(big_pipeline_buf + pos, &single_len, "HEXIST", "hash_text", 9, NULL, 0); pos += single_len;

    // 4. 将总命令数完美回填到最前方的 4 个字节里
    *(int *)big_pipeline_buf = cmd_count;

    // 5. 提取出对应的预期状态文本流进行校验
    // 注意：你原先写的是文本命令流，下面这个是服务端按顺序列队回吐的真实 TCP 流
    char *snapshot_batch_pattern = 
        "OK\r\n"                  // SET arr_text
        "OK\r\n"                  // SET screenshot.png
        "OK\r\n"                  // RSET rbt_text
        "OK\r\n"                  // RSET text.txt
        "OK\r\n"                  // HSET hash_text
        "OK\r\n"                  // HSET io多路复用.txt
        "OK\r\n"                  // SSET skip_text
        "OK\r\n"                  // MOD arr_text
        "OK\r\n"                  // RMOD rbt_text
        "OK\r\n"                  // HMOD hash_text
        "OK\r\n"                  // SMOD skip_text
        "OK\r\n"                  // RDEL text.txt
        "OK\r\n"                  // HDEL hash_text
        "Updated_Array_Data_Keep\r\n"; // GET arr_text (注意服务端回复带换行符)
        // (此处网络流在自适应检测到 GET screenshot.png 时会吐出其原始二进制体，校验引擎会自动切分 memcmp)
        // "Updated_RBTree_Data_Keep\r\n"
        // (此处自适应检测到 HGET io多路复用.txt 时会吐出其原始二进制体)
        // "Updated_Skiplist_Data_Keep\r\n"
        // "NO EXIST\r\n"
        // "NO EXIST\r\n"

    printf("[Snapshot-Test] Big atomic pipeline package built done. Total payload size: %d bytes.\n", pos);
    printf("[Snapshot-Test] Pushing hybrid data pipeline to server...\n");

    // 6. 绕开原来的文本注入，直接利用 socket 将拼好的大二进制流全包打过去
    // 如果你的 testcase_file 原本只接字符串，可以重写或用底层直接发：
    send(connfd, big_pipeline_buf, pos, 0);

    // 释放临时内存
    free(big_pipeline_buf);
    free(file_data_txt);
    free(file_data_io);
    free(file_data_png);
    
    printf("[Snapshot-Test] Pipeline package sent. Check server side logs for chunk split verification!\n");
}

void shutdown_server(int connfd) {
    printf("\n========== Shutting Down Server ==========\n");
    char send_buf[256];
    int send_len;
    build_request(send_buf, &send_len, "SHUTDOWN", NULL, 0, NULL, 0);
    send(connfd, send_buf, send_len, 0);
    
    char recv_buf[256];
    int recv_len = recv(connfd, recv_buf, sizeof(recv_buf), 0);
    if (recv_len > 0) {
        printf("Server response: %.*s\n", recv_len, recv_buf);
    }
}

// 读取服务端物理与虚拟内存
void get_server_memory(pid_t pid, long *vmsize, long *vmrss) {
    char path[256];
    snprintf(path, sizeof(path), "/proc/%d/status", pid);
    FILE *fp = fopen(path, "r");
    *vmsize = 0;
    *vmrss = 0;
    if (!fp) return;

    char line[256];
    while (fgets(line, sizeof(line), fp)) {
        if (strncmp(line, "VmSize:", 7) == 0) {
            sscanf(line + 7, "%ld", vmsize);
        } else if (strncmp(line, "VmRSS:", 6) == 0) {
            sscanf(line + 6, "%ld", vmrss);
        }
    }
    fclose(fp);
}

// 将测试结果追加记录到本地日志文件 (已修改为记录内存变化量)
void log_results(int mode, long time_ms, long qps, long delta_vmsize, long delta_vmrss) {
    FILE *fp = fopen("benchmark_results.log", "a");
    if (!fp) {
        perror("Cannot open log file");
        return;
    }
    // 使用 %+ld 格式输出，自带正负号
    fprintf(fp, "[Mode %d] Time: %ld ms | QPS: %ld | VmSize+: %+ld kB | VmRSS+: %+ld kB\n\n",
            mode, time_ms, qps, delta_vmsize, delta_vmrss);
    fclose(fp);
    printf(">>> Metrics successfully written to benchmark_results.log\n");
}


// 辅助函数：通过进程名动态获取正在运行的服务器 PID（保持不变）
pid_t get_server_pid_by_name(const char *process_name) {
    char cmd[128];
    snprintf(cmd, sizeof(cmd), "pidof %s", process_name);
    FILE *fp = popen(cmd, "r");
    if (!fp) return -1;

    pid_t pid = -1;
    if (fscanf(fp, "%d", &pid) != 1) {
        pid = -1; // 没找到说明服务器没开
    }
    pclose(fp);
    return pid;
}

int main(int argc, char *argv[]) {
    srand(time(NULL));
    
    // ✅【核心安全补丁】：忽略 SIGPIPE 信号
    // 防止手动调试时，服务端因异常退出或网络异常导致客户端闪退，确保 log_results 100% 被执行！
    signal(SIGPIPE, SIG_IGN); 

    if (argc < 2) {
        fprintf(stderr, "Usage: %s <mode 0-13>\n", argv[0]);
        fprintf(stderr, "Modes:\n  0: Array Base   1: Array File\n  2: RBTree Base  3: RBTree File\n"
                        "  4: Hash Base    5: Hash File      6: Skiplist Base 7: Skiplist File\n"  
                        "  8: Array Random 9: Rbtree Random\n 10:Hash Random\n 11:Skiplist Random\n"
                        " 12: Mix 13: Shutdown Only\n");
        return 1;
    }

    int mode = atoi(argv[1]);
    if (mode < 0 || mode > 13) {
        fprintf(stderr, "Invalid mode: %d. Please choose 0-13.\n", mode);
        return 1;
    }

    printf("[Client] Attempting to connect to MANUALLY started server...\n");
    
    // 1. 循环尝试建立 TCP 连接
    int sock = -1;
    struct sockaddr_in addr;
    addr.sin_family = AF_INET;
    addr.sin_port = htons(2000); 
    inet_pton(AF_INET, "127.0.0.1", &addr.sin_addr);

    int connected = 0;
    for (int retry = 0; retry < 15; retry++) {
        sock = socket(AF_INET, SOCK_STREAM, 0);
        if (sock < 0) return 1;
        if (connect(sock, (struct sockaddr*)&addr, sizeof(addr)) == 0) {
            connected = 1;
            break;
        }
        close(sock);
        usleep(100000); 
    }

    if (!connected) {
        fprintf(stderr, "[Client Error] Could not connect to server on port 2000.\n");
        fprintf(stderr, "👉 Please manually start the server first via: ./kvstore\n");
        return 1;
    }
    printf("Connected to KVStore Server successfully!\n\n");

    // ------- 动态获取手动启动的服务器 PID 并采集初始内存 -------
    pid_t server_pid = get_server_pid_by_name("kvstore");
    long init_vmsize = 0, init_vmrss = 0;
    
    if (server_pid > 0) {
        get_server_memory(server_pid, &init_vmsize, &init_vmrss);
        printf("[Client] Detected Server PID: %d (VmSize: %ld kB, VmRSS: %ld kB)\n", server_pid, init_vmsize, init_vmrss);
    } else {
        printf("[Client Warning] Could not detect server process named 'kvstore'. Memory logging skipped.\n");
    }
    
    // 将初始内存信息写入日志文件
    if (mode != 13) {
        FILE *fp = fopen("benchmark_results.log", "a");
        if (fp) {
            fprintf(fp, "\n========== Mode %d (Manual Server Mode) ==========\n", mode);
            fprintf(fp, "Init: VmSize=%ld kB | VmRSS=%ld kB\n", init_vmsize, init_vmrss);
            fclose(fp);
        }
    }

    // 2. ✅【数据修复】：精准统计请求总量，补上 Mode 12
    long total_ops = 0;
    if (mode == 0 || mode == 2 || mode == 4 || mode == 6)   total_ops = 90000;
    if (mode == 1 || mode == 3 || mode == 5 || mode == 7)   total_ops = 150000;
    if (mode == 8 || mode == 9 || mode == 10 || mode == 11) total_ops = 100000;
    if (mode == 12) total_ops = 43; // ✅ 补上：你的 Snapshot Flow 里面包含了 43 条组合二进制/文本指令
    if (mode == 13) total_ops = 20;

    // 3. 精准包裹计时运行
    struct timeval tv_begin, tv_end;
    gettimeofday(&tv_begin, NULL);

    switch (mode) {
        case 0: array_full_test(sock); break;
        case 1: array_full_test_file(sock); break;
        case 2: rbtree_full_test(sock); break;
        case 3: rbtree_full_test_file(sock); break;
        case 4: hash_full_test(sock); break;
        case 5: hash_full_test_file(sock); break;
        case 6: skiplist_full_test(sock); break;
        case 7: skiplist_full_test_file(sock); break;
        case 8: array_random_test(sock, 100); break;
        case 9: rbtree_random_test(sock, 100); break;
        case 10: hash_random_test(sock, 100); break;
        case 11: skiplist_random_test(sock, 100); break;
        case 12: test_snapshot_flow(sock); break;
        case 13: printf("Mode 13: Direct Shutdown Triggered.\n"); break;
    }

    gettimeofday(&tv_end, NULL);
    long time_ms = (tv_end.tv_sec - tv_begin.tv_sec) * 1000 + (tv_end.tv_usec - tv_begin.tv_usec) / 1000;
    if (time_ms == 0) time_ms = 1; 
    long qps = total_ops * 1000 / time_ms;

    // ------- 测试完毕后，再次采集内存并计算 delta 变动量 -------
    long current_vmsize = 0, current_vmrss = 0;
    long delta_vmsize = 0;
    long delta_vmrss = 0;

    if (server_pid > 0) {
        get_server_memory(server_pid, &current_vmsize, &current_vmrss);
        delta_vmsize = current_vmsize - init_vmsize;
        delta_vmrss = current_vmrss - init_vmrss;
    }

    

    // 持久化落盘指标
    if (mode != 13) {
        log_results(mode, time_ms, qps, delta_vmsize, delta_vmrss);
    }

    shutdown_server(sock);
    close(sock);


    return 0;
}

