#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <sys/syscall.h>
#include <unistd.h>
#include "resp.h"
#include "kvstore.h"      
#include "network.h"
#include "repl.h"
#include "rdma.h"

#ifndef SYS_write
#define SYS_write 1
#endif

/* 
 * 内部辅助函数：在指定长度内安全寻找 \r\n 
 */
static const char *find_crlf(const char *buf, int len) {
    for (int i = 0; i < len - 1; i++) {
        if (buf[i] == '\r' && buf[i+1] == '\n') {
            return &buf[i];
        }
    }
    return NULL;
}

/*
 * 探测函数：检查接收缓冲区内是否有一条完整的 RESP 请求
 */
static int has_complete_resp_command(const char *buf, int buf_len, int *out_cmd_len) {
    if (buf_len < 4 || buf[0] != '*') return 0;
    
    const char *p = buf;
    const char *crlf = find_crlf(p, buf_len);
    if (!crlf) return 0; // 没找到第一个 \r\n，说明半包
    
    int argc = atoi(p + 1);
    p = crlf + 2;
    
    for (int i = 0; i < argc; i++) {
        if (p - buf >= buf_len || *p != '$') return 0;
        
        crlf = find_crlf(p, buf_len - (p - buf));
        if (!crlf) return 0;
        
        int arg_len = atoi(p + 1);
        p = crlf + 2;
        
        if (p - buf + arg_len + 2 > buf_len) return 0; 
        if (p[arg_len] != '\r' || p[arg_len+1] != '\n') return 0;
        
        p += arg_len + 2; // 跳到下一个参数开头
    }
    
    *out_cmd_len = p - buf; // 获得当前命令总长度
    return 1;
}

/*
 * 【核心修改 1】：零拷贝解包函数
 * 直接指向 req_buf，并在 \r\n 处替换 \r 为 \0，避免任何字符串拷贝和内存分配
 */
static void resp_unpack(char *req_buf, resp_request_t *req) {
    char *p = req_buf;
    const char *crlf = find_crlf(p, 100); 
    
    req->argc = atoi(p + 1);
    
    // 1. 设置 argv / argv_len 数组指针（小命令完全不需要 malloc）
    if (req->argc <= RESP_STATIC_ARGC) {
        req->argv = req->buf_argv;
        req->argv_len = req->buf_argv_len;
    } else {
        // 超过 16 个参数的大命令，一次性连续分配两个数组
        req->argv = (char **)kvs_malloc(sizeof(char *) * req->argc);
        req->argv_len = (int *)kvs_malloc(sizeof(int) * req->argc);
    }
    
    p = (char *)(crlf + 2);
    
    // 2. 零拷贝提取参数：指针指向原缓冲区并原地写 '\0'
    for (int i = 0; i < req->argc; i++) {
        crlf = find_crlf(p, 128);
        int arg_len = atoi(p + 1);
        p = (char *)(crlf + 2);
        
        req->argv_len[i] = arg_len;
        req->argv[i] = p;          // 零拷贝：直接指向输入缓冲区！
        p[arg_len] = '\0';         // 原地将 '\r' 替换为 '\0'，形成标准 C 字符串
        
        p += arg_len + 2;          // 跳到下一个参数开头
    }
}

/*
 * 【核心修改 2】：极简释放函数
 * argv[i] 指向原缓冲区，无需 free！
 */
static void free_resp_request(resp_request_t *req) {
    // 只有超大命令（argc > 16）才释放外置分配的数组头
    if (req->argc > RESP_STATIC_ARGC) {
        if (req->argv) kvs_free(req->argv);
        if (req->argv_len) kvs_free(req->argv_len);
    }
}

/*
 * 打包函数：把业务层的回复格式化为 RESP 流
 */
static void resp_pack(char *send_buf, int *send_len, resp_reply_t *reply) {
    if (reply->status == KVS_RESP_OK) {
        *send_len += sprintf(send_buf + *send_len, "+OK\r\n");
    } else if (reply->status == KVS_RESP_PONG) {
        *send_len += sprintf(send_buf + *send_len, "+PONG\r\n");
    } else if (reply->status == KVS_RESP_SUCCESS) {
        *send_len += sprintf(send_buf + *send_len, "+OK\r\n");
    } else if (reply->status == KVS_RESP_ERR) {
        *send_len += sprintf(send_buf + *send_len, "-ERR save snapshot failed\r\n");
    } else if (reply->status == KVS_RESP_GET_OK && reply->body) {
        *send_len += sprintf(send_buf + *send_len, "$%d\r\n", reply->body_len);
        memcpy(send_buf + *send_len, reply->body, reply->body_len);
        *send_len += reply->body_len;
        memcpy(send_buf + *send_len, "\r\n", 2);
        *send_len += 2;
        kvs_free(reply->body); 
        reply->body = NULL;
    } else if (reply->status == KVS_RESP_NO_EXISTS) {
        *send_len += sprintf(send_buf + *send_len, "$-1\r\n"); 
    } else if (reply->status == KVS_RESP_EXISTS) {
        *send_len += sprintf(send_buf + *send_len, "-ERR key already exists\r\n");
    } else if (reply->status == KVS_RESP_UNKNOWN) {
        *send_len += sprintf(send_buf + *send_len, "-ERR unknown command\r\n");
    } else if (reply->status == KVS_RESP_PARSE_ERROR) {
        *send_len += sprintf(send_buf + *send_len, "-ERR syntax error\r\n");
    }
}

static cmd_handler_t g_command_handler = NULL;
void protocol_set_command_handler(cmd_handler_t handler) {
    g_command_handler = handler; 
}

/*
 * 打包并自动扩容函数
 */
void resp_pack_with_realloc(char **wbuf, int *wcap, int *wlen, resp_reply_t *reply) {
    int needed = *wlen + 1024 + (reply->body_len > 0 ? reply->body_len : 0);
    
    if (needed > *wcap) {
        int new_cap = *wcap * 2;
        if (new_cap < needed) new_cap = needed;
        
        char *new_buf = (char *)kvs_realloc(*wbuf, new_cap);
        if (!new_buf) {
            perror("kvs_realloc wbuf failed in protocol tier");
            return;
        }
        *wbuf = new_buf;
        *wcap = new_cap;
    }
    
    resp_pack(*wbuf, wlen, reply);
}

/*
 * 【核心修改 3】：协议层流处理入口
 * in_buf 从 const char* 改为 char*，去掉了 saved_resp_cmd 频繁 malloc/free
 */
/*
 * 协议层流处理入口（零拷贝版）
 * 注意：in_buf 为接收缓冲区指针，允许原地替换 '\r' -> '\0'
 */
int protocol_process_stream(char *in_buf, int in_len, int *parsed, char **wbuf, int *wcap, int *wlen, int fd) {
    if (in_len <= 0) {
        *parsed = 0;
        return 0;
    }

    char first_byte = in_buf[0];
    if (first_byte == '*') {
        int processed = 0;

        while (processed < in_len) {
            int single_cmd_len = 0;
            
            // 1. 探测是否有完整 RESP 请求（纯读取，不修改缓冲区）
            if (!has_complete_resp_command(in_buf + processed, in_len - processed, &single_cmd_len)) {
                break; // 半包，跳出循环等待后续数据
            }

            char *cmd_raw_ptr = in_buf + processed; // 当前完整命令的起始指针

            // 2. WAL 原则：在原地修改字符前，直接将原始 RESP 报文写入磁盘
            if (g_enable_persistence) {
                kvs_persistence_write(cmd_raw_ptr, single_cmd_len);
            }

            // 3. 主从复制 Backlog：在原地修改前，直接复制原始 RESP 报文入队
            extern int g_sync_file_done, g_slave_fd;
            if (g_slave_fd > 0 && g_enable_repl_master && 
                ((g_use_tcp_sync && g_sync_file_done) || g_repl_backlog_enabled)) {
                if (g_repl_backlog_count < REPL_BACKLOG_MAX) {
                    int index = g_repl_backlog_tail; 
                    g_repl_backlog[index].data = kvs_malloc(single_cmd_len);
                    memcpy(g_repl_backlog[index].data, cmd_raw_ptr, single_cmd_len); // 纯净的原始 RESP 数据
                    g_repl_backlog[index].len = single_cmd_len;
                    g_repl_backlog_tail = (g_repl_backlog_tail + 1) % REPL_BACKLOG_MAX;
                    g_repl_backlog_count++;
                }
            }

            // 4. 零拷贝解包：此时才将 '\r' 替换为 '\0'，req.argv[i] 直接指向 in_buf 内部
            resp_request_t req;
            resp_unpack(cmd_raw_ptr, &req);

            // 5. 握手与特例处理 (RDMA / SYNC / PING)
            if (g_enable_repl_master) {
                extern struct conn ntyco_conn_list[]; 
                struct conn *c = &ntyco_conn_list[fd];

                if (req.argc > 0) {
                    if (strcmp(req.argv[0], "RDMA_CONNECT") == 0) {
                        c->role = CONN_SLAVE;
                        handle_slave_rdma_connect(&req, wbuf, wcap, wlen, fd);
                        free_resp_request(&req);
                        processed += single_cmd_len;
                        continue; 
                    } else if (g_use_tcp_sync && strcmp(req.argv[0], "SYNC") == 0) {
                        g_slave_fd = fd; 
                    }
                }
            }

            if (req.argc > 0 && strcasecmp(req.argv[0], "PING") == 0) {
                resp_reply_t reply = {KVS_RESP_ERROR, NULL, 0};
                if (req.argc == 1) {
                    reply.status = KVS_RESP_PONG;
                } else if (req.argc == 2) {
                    reply.status = KVS_RESP_GET_OK;
                    reply.body = (char *)kvs_malloc(req.argv_len[1]);
                    memcpy(reply.body, req.argv[1], req.argv_len[1]);
                    reply.body_len = req.argv_len[1];
                } else {
                    reply.status = KVS_RESP_PARSE_ERROR;
                }

                if (wbuf && wcap && wlen) {
                    resp_pack_with_realloc(wbuf, wcap, wlen, &reply);
                }

                free_resp_request(&req);
                if (reply.body) kvs_free(reply.body);
                processed += single_cmd_len;
                continue; 
            }

            // 6. 进入业务层处理普通命令
            resp_reply_t reply = {KVS_RESP_ERROR, NULL, 0}; 
            if (g_command_handler) {
                g_command_handler(&req, &reply); // 注意：如果 handler 要存 key/value 到哈希表，存储引擎内部需自行 strdup/kvs_malloc
            }

            // 7. 打包回复
            if (wbuf && wcap && wlen) {
                resp_pack_with_realloc(wbuf, wcap, wlen, &reply);
            }
            
            // 8. 释放 request 结构（如果 argc <= 16，内部零开销；> 16 则仅 free 数组头）
            free_resp_request(&req); 
            if (reply.body) {
                kvs_free(reply.body);
                reply.body = NULL;
            }

            processed += single_cmd_len; 
        }

        *parsed = processed; 
        return 0; 
    } else {
        return -1; 
    }
}

/*
 * 【核心修改 4】：AOF 恢复处理入口
 */
int protocol_process_recover(char *in_buf, int in_len) {
    if (in_buf == NULL || in_len <= 0) {
        return 0;
    }

    int processed = 0;
    int recovered_count = 0;
    int skipped_count = 0;

    while (processed < in_len) {
        int single_cmd_len = 0;
        
        if (!has_complete_resp_command(in_buf + processed, in_len - processed, &single_cmd_len)) {
            printf("[Recover] Incomplete command at offset %d, stop.\n", processed);
            break; 
        }
        
        resp_request_t req;
        memset(&req, 0, sizeof(resp_request_t));
        resp_unpack(in_buf + processed, &req);

        resp_reply_t reply = {KVS_RESP_ERROR, NULL, 0}; 
        
        if (g_command_handler) {
            g_command_handler(&req, &reply); 
        }
        
        if (reply.status == KVS_RESP_OK) {
            recovered_count++;
        } else {
            skipped_count++;
        }
        
        if (reply.body) {
            kvs_free(reply.body);
            reply.body = NULL;
        }
        
        free_resp_request(&req); 
        processed += single_cmd_len; 
    }
    
    return processed; 
}