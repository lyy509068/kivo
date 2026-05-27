#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <unistd.h>
#include <arpa/inet.h>
#include <sys/socket.h>
#include <netinet/in.h>

#define MAX_BATCH_CMDS 32

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
        char input_lines[MAX_BATCH_CMDS][512] = {0};
        int cmd_count = 0;

        printf("Input commands (type END to send, 0 to exit):\n");

        while (cmd_count < MAX_BATCH_CMDS) {
            char line_buf[512] = {0};
            if (!fgets(line_buf, sizeof(line_buf), stdin)) break;

            char check_buf[512];
            strcpy(check_buf, line_buf);
            int clen = strlen(check_buf);
            while(clen > 0 && (check_buf[clen-1] == '\n' || check_buf[clen-1] == '\r' || check_buf[clen-1] == ' ')) check_buf[--clen] = '\0';

            if (strcmp(check_buf, "0") == 0 && cmd_count == 0) return close(sock), 0;
            if (strcmp(check_buf, "END") == 0 || strcmp(check_buf, "end") == 0) break; 
            if (clen == 0) continue; 

            char *ptr = line_buf;
            char *next_cmd = NULL;
            
            while (ptr && *ptr != '\0' && cmd_count < MAX_BATCH_CMDS) {
                if (strncmp(ptr, "CMD=", 4) == 0) next_cmd = strstr(ptr + 4, "CMD=");
                else next_cmd = strstr(ptr, "CMD=");

                if (next_cmd) {
                    int sub_len = next_cmd - ptr;
                    if (sub_len > 511) sub_len = 511;
                    memcpy(input_lines[cmd_count], ptr, sub_len);
                    input_lines[cmd_count][sub_len] = '\0';
                    cmd_count++; 
                    ptr = next_cmd;
                } else {
                    strncpy(input_lines[cmd_count], ptr, 511);
                    cmd_count++;
                    break;
                }
            }
        } 
        if (cmd_count == 0) continue;

        char cmds[MAX_BATCH_CMDS][64] = {0}; 
        char file_paths[MAX_BATCH_CMDS][256] = {0};
        char *upload_contents[MAX_BATCH_CMDS] = {0};
        int file_lens[MAX_BATCH_CMDS] = {0};
        int is_file_download[MAX_BATCH_CMDS] = {0};
        
        int buffer_overflow = 0;
        int max_send_size = 4096 * cmd_count;

        printf("\nParsing commands...\n");
        for (int i = 0; i < cmd_count; i++) {
            char *key, *value;
            int key_len, value_len;
            
            parse_line(input_lines[i], cmds[i], &key, &key_len, &value, &value_len, file_paths[i]);

            if (strlen(file_paths[i]) > 0) {
                if (strstr(cmds[i], "SET") || strstr(cmds[i], "MOD")) {
                    upload_contents[i] = read_file_content(file_paths[i], &file_lens[i]);
                    if (!upload_contents[i]) {
                        buffer_overflow = 1; break;
                    }
                    value = upload_contents[i];
                    value_len = file_lens[i];
                    max_send_size += value_len;
                    printf("Upload file: %s (%d bytes)\n", file_paths[i], value_len);
                } 
                else if (strstr(cmds[i], "GET")) {
                    is_file_download[i] = 1;
                    printf("Download file: %s\n", file_paths[i]);
                }
            }

            printf("Cmd %d: %s key=%.*s\n", i + 1, cmds[i], key_len, key);
        }
        if (buffer_overflow) {
            for(int i=0; i<cmd_count; i++) if (upload_contents[i]) free(upload_contents[i]);
            continue;
        }

        char *send_buf = (char*)calloc(1, max_send_size + 1024);
        int total_send_len = 0;
        *(int*)(send_buf) = cmd_count; total_send_len += 4;

        for (int i = 0; i < cmd_count; i++) {
            char *key, *value;
            int key_len, value_len;
            parse_line(input_lines[i], cmds[i], &key, &key_len, &value, &value_len, file_paths[i]);

            if (upload_contents[i]) { value = upload_contents[i]; value_len = file_lens[i]; }

            int one_cmd_len = 0;
            char *temp_buf = (char*)malloc(4096 + value_len);
            build_request(temp_buf, &one_cmd_len, cmds[i], key, key_len, value, value_len);

            memcpy(send_buf + total_send_len, temp_buf, one_cmd_len);
            total_send_len += one_cmd_len;
            free(temp_buf);
        }

        send(sock, send_buf, total_send_len, 0);
        printf("\nSent %d bytes\n", total_send_len);

        free(send_buf);
        for(int i=0; i<cmd_count; i++) if (upload_contents[i]) free(upload_contents[i]);

        int max_recv = 10 * 1024 * 1024;
        char *recv_buf = (char*)malloc(max_recv);
        int recv_len = recv(sock, recv_buf, max_recv - 1, 0);

        if (recv_len <= 0) {
            printf("Server disconnected\n"); free(recv_buf); break;
        }

        printf("\nServer Response:\n");
        printf("Received %d bytes\n", recv_len);
        
        int has_download = 0;
        for (int i = 0; i < cmd_count; i++) {
            if (is_file_download[i]) {
                FILE *out = fopen(file_paths[i], "wb");
                if (out) {
                    fwrite(recv_buf, 1, recv_len, out);
                    fclose(out);
                    printf("File saved to %s\n", file_paths[i]);
                } else {
                    printf("Cannot save file %s\n", file_paths[i]);
                }
                has_download = 1;
                break;
            }
        }

        if (!has_download) {
            fwrite(recv_buf, 1, recv_len, stdout);
            printf("\n");
        }
        printf("\n");
        free(recv_buf);
    }
    close(sock);
    return 0;
}