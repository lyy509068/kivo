#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <arpa/inet.h>
#include <sys/socket.h>
#include <termios.h>

#define MAX_COMMANDS 1024
#define BUFFER_SIZE 65536 // 用于标准输入的初始读取缓冲区

const char* kvs_status_str(int status) {
    switch (status) {
        case 0: return "OK";
        case 1: return "EXIST";
        case 2: return "NO_EXIST";
        case 3: return "ERROR";
        case 4: return "PARSE_ERROR";
        case 5: return "UNKNOWN_COMMAND";
        case 6: return "SHUTDOWN";
        case 7: return "GET_OK";
        default: return "UNKNOWN_STATUS";
    }
}

/*
 * 函数功能：解析具有转义字符的字符串，去除可能存在的外层双引号，
 * 将字符字面量转换为真实的单字节控制字符，并返回转换后纯数据的实际字节长度。
 */
int unescape_string(const char *src, char *dest) {
    int i = 0, j = 0;
    int len = strlen(src);
    int start = 0, end = len;
    
    if (len >= 2 && src[0] == '"' && src[len - 1] == '"') {
        start = 1;
        end = len - 1;
    }
    
    for (i = start; i < end; i++) {
        if (src[i] == '\\' && i + 1 < end) {
            i++;
            if (src[i] == 'n') dest[j++] = '\n';
            else if (src[i] == 'r') dest[j++] = '\r';
            else if (src[i] == 't') dest[j++] = '\t';
            else if (src[i] == '"') dest[j++] = '"';
            else if (src[i] == '\\') dest[j++] = '\\';
            else dest[j++] = src[i];
        } else {
            dest[j++] = src[i];
        }
    }
    dest[j] = '\0';
    return j;
}

/*
 * 函数功能：从输入行中切分并提取下一个 Token，能够自动识别并保持双引号边界。
 */
const char* extract_token(const char *str, char *token_buf) {
    while (*str == ' ' || *str == '\t') {
        str++;
    }
    if (*str == '\0') return NULL;
    
    int i = 0;
    if (*str == '"') {
        token_buf[i++] = *str++;
        while (*str != '\0') {
            if (*str == '\\' && *(str + 1) != '\0') {
                token_buf[i++] = *str++;
                token_buf[i++] = *str++;
            } else if (*str == '"') {
                token_buf[i++] = *str++;
                break;
            } else {
                token_buf[i++] = *str++;
            }
        }
    } else {
        while (*str != '\0' && *str != ' ' && *str != '\t') {
            token_buf[i++] = *str++;
        }
    }
    token_buf[i] = '\0';
    return str;
}

/*
 * 函数功能：[动态内存升级版] 
 * 将单行文本拆分为独立的命令，并根据实际长度动态分配内存，彻底消除4096截断。
 */
int parse_single_command(const char *line, char **cmd, int *cmd_len, char **key, int *key_len, char **val, int *val_len) {
    int line_len = strlen(line);
    
    // 1. 动态分配临时 Token 缓冲区，绝对不会越界
    char *t_cmd = (char *)malloc(line_len + 1);
    char *t_key = (char *)malloc(line_len + 1);
    char *t_val = (char *)malloc(line_len + 1);
    if (!t_cmd || !t_key || !t_val) {
        free(t_cmd); free(t_key); free(t_val);
        return -1;
    }
    memset(t_cmd, 0, line_len + 1);
    memset(t_key, 0, line_len + 1);
    memset(t_val, 0, line_len + 1);

    const char *p = line;
    p = extract_token(p, t_cmd);
    if (!p) { free(t_cmd); free(t_key); free(t_val); return -1; }
    p = extract_token(p, t_key);
    if (!p) { free(t_cmd); free(t_key); free(t_val); return -1; }
    extract_token(p, t_val);

    // 2. 为外层传递进来的二级指针分配精准内存
    *cmd = (char *)malloc(line_len + 1);
    *key = (char *)malloc(line_len + 1);
    *val = (char *)malloc(line_len + 1);
    if (!*cmd || !*key || !*val) {
        if (*cmd) free(*cmd);
        if (*key) free(*key);
        if (*val) free(*val);
        free(t_cmd); free(t_key); free(t_val);
        return -1;
    }

    *cmd_len = unescape_string(t_cmd, *cmd);
    *key_len = unescape_string(t_key, *key);
    *val_len = unescape_string(t_val, *val);

    free(t_cmd);
    free(t_key);
    free(t_val);

    printf("[PARSE] CMD: %s (%d) | KEY: %s (%d) | VAL: %s (%d)\n", *cmd, *cmd_len, *key, *key_len, *val, *val_len);
    return 0;
}


char* read_all_from_stdin(int *out_len) {
    int capacity = 4096;
    int total = 0;
    char *buf = (char *)malloc(capacity);
    if (!buf) return NULL;

    // 获取当前终端属性
    struct termios oldt, newt;
    int is_tty = isatty(STDIN_FILENO);
    if (is_tty) {
        tcgetattr(STDIN_FILENO, &oldt);
        newt = oldt;
        // 🌟 核心：关闭规范模式（ICANON），强制终端有多少收多少，打破4096限制
        newt.c_lflag &= ~(ICANON); 
        tcsetattr(STDIN_FILENO, TCSANOW, &newt);
    }

    printf("Enter commands (Double Enter / Empty line to send):\n> ");
    fflush(stdout);

    while (1) {
        if (total >= capacity - 4) {
            capacity *= 2;
            char *new_buf = (char *)realloc(buf, capacity);
            if (!new_buf) {
                free(buf);
                if (is_tty) tcsetattr(STDIN_FILENO, TCSANOW, &oldt);
                return NULL;
            }
            buf = new_buf;
        }

        int n = read(STDIN_FILENO, buf + total, capacity - 1 - total);
        if (n < 0) {
            free(buf);
            if (is_tty) tcsetattr(STDIN_FILENO, TCSANOW, &oldt);
            return NULL;
        }
        if (n == 0) break;

        total += n;
        buf[total] = '\0';

        // 检测连续换行（兼容\n\n, \r\n\r\n等）
        if (total >= 2 && buf[total - 1] == '\n' && buf[total - 2] == '\n') break;
        if (total >= 3 && buf[total - 1] == '\n' && buf[total - 2] == '\r' && buf[total - 3] == '\n') break;
        if (total >= 4 && buf[total - 1] == '\n' && buf[total - 2] == '\r' && buf[total - 3] == '\n' && buf[total - 4] == '\r') break;
    }

    // 恢复原有终端属性
    if (is_tty) {
        tcsetattr(STDIN_FILENO, TCSANOW, &oldt);
    }

    *out_len = total;
    return buf;
}

/*
 * 函数功能：将指针数组中的所有命令打包为网络字节序二进制流。
 */
int pack_all_commands(unsigned char *send_buf, int cmd_count, char *cmds[], int cmd_lens[], char *keys[], int key_lens[], char *vals[], int val_lens[]) {
    int offset = 0;
    uint32_t net_count = htonl(cmd_count);
    memcpy(send_buf + offset, &net_count, 4);
    offset += 4;
    
    for (int i = 0; i < cmd_count; i++) {
        uint32_t n_clen = htonl(cmd_lens[i]);
        memcpy(send_buf + offset, &n_clen, 4);
        offset += 4;
        memcpy(send_buf + offset, cmds[i], cmd_lens[i]);
        offset += cmd_lens[i];
        
        uint32_t n_klen = htonl(key_lens[i]);
        memcpy(send_buf + offset, &n_klen, 4);
        offset += 4;
        memcpy(send_buf + offset, keys[i], key_lens[i]);
        offset += key_lens[i];
        
        uint32_t n_vlen = htonl(val_lens[i]);
        memcpy(send_buf + offset, &n_vlen, 4);
        offset += 4;
        memcpy(send_buf + offset, vals[i], val_lens[i]);
        offset += val_lens[i];
    }
    return offset;
}

int connect_to_server(int port) {
    int sock = socket(AF_INET, SOCK_STREAM, 0);
    if (sock < 0) return -1;
    struct sockaddr_in addr;
    memset(&addr, 0, sizeof(addr));
    addr.sin_family = AF_INET;
    addr.sin_port = htons(port);
    addr.sin_addr.s_addr = inet_addr("127.0.0.1");
    if (connect(sock, (struct sockaddr*)&addr, sizeof(addr)) < 0) {
        close(sock);
        return -1;
    }
    return sock;
}

/*
 * 函数功能：[动态扩容版]
 * 阻塞式接收服务端响应字节流。一旦缓冲不够，自动翻倍扩容，解决大报文截断问题。
 */
void recv_and_parse_replies(int sock) {
    int capacity = 4096; // 初始分配 4KB
    unsigned char *buf = (unsigned char *)malloc(capacity);
    if (!buf) return;

    int total_recved = 0;
    uint32_t expected_replies = 0;
    int header_parsed = 0;
    int check_offset = 0;

    while (1) {
        // 如果接收缓冲区满了，立即翻倍扩容
        if (total_recved >= capacity) {
            capacity *= 2;
            unsigned char *new_buf = (unsigned char *)realloc(buf, capacity);
            if (!new_buf) {
                printf("[RECV] Out of memory.\n");
                free(buf);
                return;
            }
            buf = new_buf;
        }

        int n = recv(sock, buf + total_recved, capacity - total_recved, 0);
        if (n <= 0) {
            printf("[RECV] Connection closed or error.\n");
            break;
        }
        total_recved += n;

        // 1. 解析报文头部（总回复条数）
        if (!header_parsed && total_recved >= 4) {
            memcpy(&expected_replies, buf, 4);
            expected_replies = ntohl(expected_replies);
            header_parsed = 1;
            if (expected_replies == 0) break;
        }

        // 2. 判定整个批量包是否接收完整
        if (header_parsed) {
            check_offset = 4; // 跳过 cmd_count
            int is_complete = 1;

            for (uint32_t i = 0; i < expected_replies; i++) {
                if (check_offset + 8 > total_recved) {
                    is_complete = 0;
                    break;
                }

                uint32_t net_len;
                memcpy(&net_len, buf + check_offset + 4, 4);
                uint32_t r_len = ntohl(net_len);

                check_offset += 4 + 4 + r_len;

                if (check_offset > total_recved) {
                    is_complete = 0;
                    break;
                }
            }

            // 3. 数据完整，开始安全解析并打印
            if (is_complete) {
                int print_offset = 4; // 跳过 cmd_count
                printf("[RECV] Total Replies Count: %u\n", expected_replies);

                for (uint32_t i = 0; i < expected_replies; i++) {
                    uint32_t net_status;
                    memcpy(&net_status, buf + print_offset, 4);
                    int status = (int)ntohl(net_status);
                    print_offset += 4;

                    uint32_t net_len;
                    memcpy(&net_len, buf + print_offset, 4);
                    uint32_t r_len = ntohl(net_len);
                    print_offset += 4;

                    if (r_len > 0) {
                        printf("-> Reply [%u]: Status=[%s], Body=%.*s\n", 
                               i, kvs_status_str(status), (int)r_len, buf + print_offset);
                        print_offset += r_len;
                    } else {
                        printf("-> Reply [%u]: Status=[%s] (No Body)\n", 
                               i, kvs_status_str(status));
                    }
                }
                break; // 完美解析完毕，退出流接收循环
            }
        }
    }
    
    free(buf); // 释放动态接收缓冲区
}

int main(int argc, char *argv[]) {
    int port = 2000;
    if (argc > 1) {
        port = atoi(argv[1]);
    }
    
    int total_read = 0;
    char *input_buf = read_all_from_stdin(&total_read);
    if (!input_buf || total_read <= 0) {
        printf("[ERROR] No input read.\n");
        if (input_buf) free(input_buf);
        return 0;
    }
    printf("[DEBUG] Standard Input actually read: %d bytes\n", total_read);

    //char input_buf[BUFFER_SIZE];
    //int total_read = read_multi_lines(input_buf, sizeof(input_buf));
    //if (total_read <= 0) {
        //return 0;
    //}

    // 瘦身：改为指针数组，避免爆栈
    char *cmds[MAX_COMMANDS] = {0};
    int cmd_lens[MAX_COMMANDS] = {0};
    char *keys[MAX_COMMANDS] = {0};
    int key_lens[MAX_COMMANDS] = {0};
    char *vals[MAX_COMMANDS] = {0};
    int val_lens[MAX_COMMANDS] = {0};

    int cmd_count = 0;
    char *saveptr;
    char *line = strtok_r(input_buf, "\n\r", &saveptr);
    while (line != NULL && cmd_count < MAX_COMMANDS) {
        if (strlen(line) > 0) {
            // 传递指针的地址，让解析函数动态分配内存
            if (parse_single_command(line, &cmds[cmd_count], &cmd_lens[cmd_count], 
                                     &keys[cmd_count], &key_lens[cmd_count], 
                                     &vals[cmd_count], &val_lens[cmd_count]) == 0) {
                cmd_count++;
            }
        }
        line = strtok_r(NULL, "\n\r", &saveptr);
    }
    
    free(input_buf);

    if (cmd_count == 0) {
        return 0;
    }

    // 动态计算精准的打包缓存大小
    int total_send_size = 4; // cmd_count 占 4 字节
    for (int i = 0; i < cmd_count; i++) {
        total_send_size += 4 + cmd_lens[i];
        total_send_size += 4 + key_lens[i];
        total_send_size += 4 + val_lens[i];
    }

    unsigned char *send_buf = (unsigned char *)malloc(total_send_size);
    if (!send_buf) {
        for (int i = 0; i < cmd_count; i++) { free(cmds[i]); free(keys[i]); free(vals[i]); }
        return -1;
    }

    int send_size = pack_all_commands(send_buf, cmd_count, cmds, cmd_lens, keys, key_lens, vals, val_lens);
    
    int sock = connect_to_server(port);
    if (sock < 0) {
        printf("Connect failed\n");
        free(send_buf);
        for (int i = 0; i < cmd_count; i++) { free(cmds[i]); free(keys[i]); free(vals[i]); }
        return -1;
    }
    
    send(sock, send_buf, send_size, 0);
    recv_and_parse_replies(sock);
    
    // 资源清理
    close(sock);
    free(send_buf);
    for (int i = 0; i < cmd_count; i++) {
        if (cmds[i]) free(cmds[i]);
        if (keys[i]) free(keys[i]);
        if (vals[i]) free(vals[i]);
    }
    
    return 0;
}