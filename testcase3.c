#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <unistd.h>
#include <arpa/inet.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <sys/time.h>

#define MAX_BATCH_CMDS 40
// 结束符
#define FILE_EOF_MARKER "---KV_FILE_EOF_BOUNDARY---"

char* find_marker(const char* haystack, int haystack_len, const char* needle, int needle_len) {
    if (!haystack || haystack_len < needle_len || needle_len == 0) return NULL;
    for (int i = 0; i <= haystack_len - needle_len; i++) {
        if (haystack[i] == needle[0] && memcmp(haystack + i, needle, needle_len) == 0) {
            return (char*)(haystack + i);
        }
    }
    return NULL;
}

char* read_file_content(const char *filename, int *file_len) {
    FILE *fp = fopen(filename, "rb");
    if (!fp) {
        printf("Cannot open file: %s\n", filename);
        return NULL;
    }
    fseek(fp, 0, SEEK_END);
    int actual_len = ftell(fp);
    fseek(fp, 0, SEEK_SET);

    int marker_len = strlen(FILE_EOF_MARKER);
    *file_len = actual_len + marker_len; 

    char *content = (char*)malloc(*file_len);
    if (!content) {
        fclose(fp);
        return NULL;
    }
    size_t read_len = fread(content, 1, actual_len, fp);
    fclose(fp);
    
    if (read_len != actual_len) {
        free(content);
        return NULL;
    }
    
    // 把结束符硬塞到文件二进制数据的最末尾
    memcpy(content + actual_len, FILE_EOF_MARKER, marker_len);
    return content;
}

void build_request(char *buf, int *len, const char *cmd, const void *key, int key_len, const void *value, int value_len) {
    int pos = 0;
    int cmd_len = strlen(cmd);
    
    *(int*)(buf + pos) = cmd_len; pos += 4;
    memcpy(buf + pos, cmd, cmd_len); pos += cmd_len;
    
    *(int*)(buf + pos) = key_len; pos += 4;
    memcpy(buf + pos, key, key_len); pos += key_len;
    
    if (value && value_len > 0) {
        *(int*)(buf + pos) = value_len; pos += 4;
        memcpy(buf + pos, value, value_len); pos += value_len;
    } else {
        *(int*)(buf + pos) = 0; pos += 4;
    }
    *len = pos;
}

void parse_line(const char *input, char *cmd, char **key, int *key_len, char **value, int *value_len, char *file_path) {
    cmd[0] = '\0'; *key = NULL; *key_len = 0; *value = NULL; *value_len = 0; file_path[0] = '\0';
    
    const char *cmd_start = strstr(input, "CMD=");
    if (cmd_start) {
        cmd_start += 4;
        const char *cmd_end = strchr(cmd_start, ' ');
        if (!cmd_end) cmd_end = cmd_start + strlen(cmd_start);
        int len = cmd_end - cmd_start;
        while (len > 0 && (cmd_start[len-1] == '\n' || cmd_start[len-1] == '\r')) len--;
        memcpy(cmd, cmd_start, len);
        cmd[len] = '\0';
    }
    
    const char *key_start = strstr(input, "KEY=");
    if (key_start) {
        key_start += 4;
        const char *val_start = strstr(key_start, " VALUE=");
        const char *file_start = strstr(key_start, " FILE=");
        
        const char *key_end = input + strlen(input);
        if (val_start && (!file_start || val_start < file_start)) key_end = val_start;
        else if (file_start) key_end = file_start;
        
        *key = (char*)key_start;
        *key_len = key_end - key_start;
        
        while (*key_len > 0 && 
              ((*key)[*key_len - 1] == '\n' || (*key)[*key_len - 1] == '\r' || 
               (*key)[*key_len - 1] == ' '  || (*key)[*key_len - 1] == '\t')) {
            (*key_len)--;
        }
        
        if (val_start) {
            val_start += 7;
            *value = (char*)val_start;
            *value_len = strlen(val_start); 
            while (*value_len > 0 && ((*value)[*value_len - 1] == '\n' || (*value)[*value_len - 1] == '\r')) {
                (*value_len)--;
            }
        }
        else if (file_start) {
            file_start += 6;
            strncpy(file_path, file_start, 255);
            int flen = strlen(file_path);
            while (flen > 0 && (file_path[flen-1] == '\n' || file_path[flen-1] == '\r' || file_path[flen-1] == ' ' || file_path[flen-1] == '\t')) {
                file_path[--flen] = '\0';
            }
        }
    }
}

int main() {
    printf("KVStore Interactive Test Client\n");
    printf("Connect to server on port 2000\n");

    int sock = socket(AF_INET, SOCK_STREAM, 0);
    if (sock < 0) return 1;

    struct sockaddr_in addr;
    addr.sin_family = AF_INET;
    addr.sin_port = htons(2000); 
    inet_pton(AF_INET, "127.0.0.1", &addr.sin_addr);

    if (connect(sock, (struct sockaddr*)&addr, sizeof(addr)) < 0) {
        printf("Connect failed\n");
        close(sock);
        return 1;
    }
    printf("Connected to server\n\n");
    
    while (1) {
        int input_cap = 1024;
        int input_len = 0;
        char *total_input_buf = (char*)malloc(input_cap);
        if (!total_input_buf) {
            printf("Memory allocation failed!\n");
            break;
        }
        total_input_buf[0] = '\0';

        printf("Input commands (type END to send, 0 to exit):\n");

        while (1) {
            char line_buf[1024] = {0}; 
            if (!fgets(line_buf, sizeof(line_buf), stdin)) break;

            char check_buf[1024];
            strcpy(check_buf, line_buf);
            int clen = strlen(check_buf);
            while(clen > 0 && (check_buf[clen-1] == '\n' || check_buf[clen-1] == '\r' || check_buf[clen-1] == ' ')) check_buf[--clen] = '\0';

            if (strcmp(check_buf, "0") == 0 && input_len == 0) {
                free(total_input_buf);
                return close(sock), 0;
            }
            if (strcmp(check_buf, "END") == 0 || strcmp(check_buf, "end") == 0) break;

            int line_len = strlen(line_buf);
            while (input_len + line_len >= input_cap) {
                input_cap *= 2;
                char *new_buf = (char*)realloc(total_input_buf, input_cap);
                if (!new_buf) {
                    printf("Out of memory during input expansion!\n");
                    free(total_input_buf);
                    return 1;
                }
                total_input_buf = new_buf;
            }
            
            strcpy(total_input_buf + input_len, line_buf);
            input_len += line_len;
        }

        if (input_len == 0) {
            free(total_input_buf);
            continue;
        }

        char **input_lines = NULL;
        int cmd_count = 0;
        int cmd_cap = 4;
        input_lines = (char**)malloc(cmd_cap * sizeof(char*));

        char *ptr = total_input_buf;
        char *next_cmd = NULL;

        while (ptr && *ptr != '\0') {
            if (strncmp(ptr, "CMD=", 4) == 0) next_cmd = strstr(ptr + 4, "CMD=");
            else next_cmd = strstr(ptr, "CMD=");

            int sub_len = next_cmd ? (next_cmd - ptr) : (int)strlen(ptr);

            if (cmd_count >= cmd_cap) {
                cmd_cap *= 2;
                input_lines = (char**)realloc(input_lines, cmd_cap * sizeof(char*));
            }

            input_lines[cmd_count] = (char*)malloc(sub_len + 1);
            memcpy(input_lines[cmd_count], ptr, sub_len);
            input_lines[cmd_count][sub_len] = '\0';
            cmd_count++;

            ptr = next_cmd;
        }

        free(total_input_buf); 

        char cmds[MAX_BATCH_CMDS][64] = {0}; 
        char file_paths[MAX_BATCH_CMDS][256] = {0};
        char *upload_contents[MAX_BATCH_CMDS] = {0};
        int file_lens[MAX_BATCH_CMDS] = {0};
        
        int exact_send_size = 4; 

        printf("\nParsing commands...\n");
        for (int i = 0; i < cmd_count; i++) {
            char *key, *value;
            int key_len, value_len;
            parse_line(input_lines[i], cmds[i], &key, &key_len, &value, &value_len, file_paths[i]);

            if (strlen(file_paths[i]) > 0) {
                if (strstr(cmds[i], "SET") || strstr(cmds[i], "MOD")) {
                    upload_contents[i] = read_file_content(file_paths[i], &file_lens[i]);
                    if (!upload_contents[i]) { printf("File read error\n"); break; }
                    value = upload_contents[i];
                    value_len = file_lens[i];
                }
            }
            
            exact_send_size += (4 + (int)strlen(cmds[i]) + 4 + key_len + 4 + value_len);
            printf("Cmd %d: %s key=%.*s (%d bytes)\n", i + 1, cmds[i], key_len, key, value_len);
        }

        char *send_buf = (char*)malloc(exact_send_size);
        int total_send_len = 0;
        *(int*)(send_buf) = cmd_count; total_send_len += 4;

        for (int i = 0; i < cmd_count; i++) {
            char *key, *value;
            int key_len, value_len;
            parse_line(input_lines[i], cmds[i], &key, &key_len, &value, &value_len, file_paths[i]);
            if (upload_contents[i]) { value = upload_contents[i]; value_len = file_lens[i]; }

            int one_cmd_len = 0;
            char *temp_buf = (char*)malloc(4 + (int)strlen(cmds[i]) + 4 + key_len + 4 + value_len);
            build_request(temp_buf, &one_cmd_len, cmds[i], key, key_len, value, value_len);

            memcpy(send_buf + total_send_len, temp_buf, one_cmd_len);
            total_send_len += one_cmd_len;
            free(temp_buf);
        }

        send(sock, send_buf, total_send_len, 0);
        printf("\nSent %d bytes\n", total_send_len);

        // ==========================================================
        // 接收响应流
        // ==========================================================
        int max_recv = 16 * 1024 * 1024; 
        char *recv_buf = (char*)calloc(1, max_recv); 
        if (!recv_buf) {
            printf("Failed to allocate receive buffer.\n");
            goto CLEANUP; 
        }

        int total_recv_len = 0;
        printf("Receiving data from server...\n");

        struct timeval timeout;
        timeout.tv_sec = 1;
        timeout.tv_usec = 0;
        setsockopt(sock, SOL_SOCKET, SO_RCVTIMEO, &timeout, sizeof(timeout));

        int curr_recv = recv(sock, recv_buf, max_recv - 1, 0);
        if (curr_recv <= 0) {
            printf("Server disconnected or initial recv timeout.\n");
            free(recv_buf);
            break;
        }
        total_recv_len += curr_recv;

        while (1) {
            char tmp_chunk[8192];
            int next_recv = recv(sock, tmp_chunk, sizeof(tmp_chunk), MSG_DONTWAIT);
            if (next_recv > 0) {
                if (total_recv_len + next_recv < max_recv - 1) {
                    memcpy(recv_buf + total_recv_len, tmp_chunk, next_recv);
                    total_recv_len += next_recv;
                } else break;
            } else {
                usleep(20000); 
                next_recv = recv(sock, tmp_chunk, sizeof(tmp_chunk), MSG_DONTWAIT);
                if (next_recv > 0) {
                    if (total_recv_len + next_recv < max_recv - 1) {
                        memcpy(recv_buf + total_recv_len, tmp_chunk, next_recv);
                        total_recv_len += next_recv;
                    }
                } else break; 
            }
        }

        timeout.tv_sec = 0; timeout.tv_usec = 0;
        setsockopt(sock, SOL_SOCKET, SO_RCVTIMEO, &timeout, sizeof(timeout));

        printf("Server Response: Total Received %d bytes\n", total_recv_len);

        char *parse_ptr = recv_buf;
        char *buf_end = recv_buf + total_recv_len;

        printf("\n--- 执行回执明细 ---\n");
        for (int i = 0; i < cmd_count; i++) {
            if (parse_ptr >= buf_end) {
                //printf("Cmd %d [%s]: 警告，服务器响应流提前结束，无对应回执。\n", i + 1, cmds[i]);
                break;
            }

            char *current_key = NULL; char *dummy_val; int dummy_klen, dummy_vlen; char dummy_path[256];
            parse_line(input_lines[i], cmds[i], &current_key, &dummy_klen, &dummy_val, &dummy_vlen, dummy_path);

            int is_file_key = 0;
            if (current_key && dummy_klen > 0) {
                char *tmp_key = (char*)malloc(dummy_klen + 1);
                memcpy(tmp_key, current_key, dummy_klen);
                tmp_key[dummy_klen] = '\0';
                if (strstr(tmp_key, ".txt") || strstr(tmp_key, ".png") || strstr(tmp_key, ".jpg") || 
                    strstr(tmp_key, ".jpeg") || strstr(tmp_key, ".gif") || strstr(tmp_key, ".bin")) {
                    is_file_key = 1;
                }
                free(tmp_key);
            }

            int should_download = 0;
            if (strstr(cmds[i], "GET") != NULL) {
                if (strncmp(parse_ptr, "NO EXIST", 8) != 0) {
                    if (strlen(file_paths[i]) > 0 || is_file_key) {
                        should_download = 1;
                    }
                }
            }

            #if 0 
            {//根据这个分支修改
                // 【场景 A】：纯文本命令回执 (如 SET 返回的 OK)
                char *next_line = strchr(parse_ptr, '\n');
                int line_len = next_line ? (next_line - parse_ptr + 1) : (buf_end - parse_ptr);

                printf("Cmd %d [%s]: ", i + 1, cmds[i]);
                for (int j = 0; j < line_len; j++) {
                    if (parse_ptr[j] >= 32 && parse_ptr[j] <= 126) putchar(parse_ptr[j]);
                    else if (parse_ptr[j] == '\n' || parse_ptr[j] == '\r') putchar(parse_ptr[j]);
                }
                
                parse_ptr += line_len; 
            }
            #endif
            if (!should_download) {
                // ==========================================================
                // 【场景 A】：纯文本/长字符串命令回执 (兼容老版本服务器)
                // ==========================================================
                
                // 1. 探测这一段文本的结束边界：
                // 如果是批量命令中的后续命令，可能后面还粘着别的数据。
                // 我们通过扫描下一个 "CMD=" 或者探测整个接收流的末尾 (buf_end) 来确定边界。
                char *next_cmd_pos = NULL;
                if (i < cmd_count - 1) {
                    // 寻找下一个响应包的边界（假设后续包以 CMD= 对应回复或者新行开始）
                    // 最稳妥的方法是，文本回执通常以换行结束，或者直接推到流末尾
                    next_cmd_pos = strstr(parse_ptr + 1, "Cmd ");
                }
                
                int text_len = next_cmd_pos ? (next_cmd_pos - parse_ptr) : (buf_end - parse_ptr);

                // 2. 去除末尾可能存在的尾随换行符，让打印更好看
                while (text_len > 0 && (parse_ptr[text_len - 1] == '\n' || parse_ptr[text_len - 1] == '\r')) {
                    text_len--;
                }

                //printf("Cmd %d [%s]: ", i + 1, cmds[i]);
                
                if (text_len > 0) {
                    // ==========================================================
                    // 核心修改点：彻底抛弃老代码的 for 循环逐字节 [32-126] 过滤！
                    // 使用 %.*s 直接把指定长度的原始内存流（包括中文、长文本）打印出来
                    // ==========================================================
                    printf("%.*s\n", text_len, parse_ptr);
                } else {
                    printf("(Empty)\n");
                }

                // 3. 解析指针向后推进，对齐到下一个回执的开头
                parse_ptr += text_len;
                while (parse_ptr < buf_end && (*parse_ptr == '\n' || *parse_ptr == '\r')) {
                    parse_ptr++; // 跳过可能残余的换行符
                }
            } 
            else {
                // 【场景 B】：文件接收逻辑
                // 【核心修复点】：服务器 GET 成功时直接返回原始二进制，无额外前缀文本头。
                // 故必须将 header_offset 锁死为 0，防止误切 PNG 文件的头部标识字节！
                int header_offset = 0;

                char *file_data_start = parse_ptr + header_offset;
                char *file_data_end = buf_end;

                // 核心边界探测：扫描我们在 SET 阶段植入的文件结束符
                int marker_len = strlen(FILE_EOF_MARKER);
                char *marker_ptr = find_marker(file_data_start, buf_end - file_data_start, FILE_EOF_MARKER, marker_len);

                int pure_file_bytes = 0;
                if (marker_ptr) {
                    file_data_end = marker_ptr; 
                    pure_file_bytes = file_data_end - file_data_start; 
                } else {
                    printf("Cmd %d [%s]: ⚠️ 警告，未找到结束边界符！\n", i + 1, cmds[i]);
                    pure_file_bytes = buf_end - file_data_start; 
                }

                char save_path[512];
                if (strlen(file_paths[i]) > 0) snprintf(save_path, sizeof(save_path), "download_%s", file_paths[i]);
                else {
                    char orig_name[256] = {0}; memcpy(orig_name, current_key, dummy_klen);
                    snprintf(save_path, sizeof(save_path), "download_%s", orig_name);
                }

                if (pure_file_bytes >= 0) {
                    FILE *out = fopen(save_path, "wb");
                    if (out) {
                        if (pure_file_bytes > 0) {
                            fwrite(file_data_start, 1, pure_file_bytes, out);
                        }
                        fclose(out);
                        printf("Cmd %d [%s]: 💾 成功依据结束符剥离，文件已独立下载 -> %s (%d 字节)\n", 
                               i + 1, cmds[i], save_path, pure_file_bytes);
                    }
                }

                // 解析指针向后跳过：文件体 + 结束符本身
                if (marker_ptr) {
                    parse_ptr = marker_ptr + marker_len; 
                } else {
                    parse_ptr = buf_end; 
                }
                
                while (parse_ptr < buf_end && (*parse_ptr == '\n' || *parse_ptr == '\r')) {
                    parse_ptr++; 
                }
            }
        }
        printf("--- 回执解析完毕 ---\n\n");

        if (recv_buf) { free(recv_buf); recv_buf = NULL; }

    CLEANUP:
        if (send_buf) { free(send_buf); send_buf = NULL; }
        
        for(int i = 0; i < cmd_count; i++) {
            if (input_lines && input_lines[i]) { 
                free(input_lines[i]); 
                input_lines[i] = NULL; 
            }
            if (upload_contents[i]) { 
                free(upload_contents[i]); 
                upload_contents[i] = NULL; 
            }
        }
        if (input_lines) { free(input_lines); input_lines = NULL; }
        
    } // end of while(1)
    
    close(sock);
    return 0;
}