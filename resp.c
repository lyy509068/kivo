#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include "resp.h"
#include "kvstore.h"      
#include "network.h"
#include "repl.h"
#include "rdma.h"

/* * 内部辅助函数：在指定长度内安全寻找 \r\n 
 * 避免因为客户端发来恶意数据导致非法内存访问
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
 * 期待格式如：*3\r\n$3\r\nSET\r\n$4\r\nname\r\n$4\r\nalex\r\n
 * 识别超时时间：*4\r\n$3\r\nSET\r\n$4\r\nname\r\n$4\r\nalex\r\n$13\r\n1234567890123\r\n
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
        
        // 检查数据实体 + 末尾 \r\n 的长度是否足够
        if (p - buf + arg_len + 2 > buf_len) return 0; 
        if (p[arg_len] != '\r' || p[arg_len+1] != '\n') return 0;
        
        p += arg_len + 2; // 跳到下一个参数开头
    }
    
    *out_cmd_len = p - buf; // 获得当前命令总长度
    return 1;
}

/*
 * 解包函数：将缓冲区文本切分成干净的 argv 数组
 */
static void resp_unpack(const char *buf, const char *req_buf, resp_request_t *req) {
    const char *p = req_buf;
    const char *crlf = find_crlf(p, 100); 
    
    req->argc = atoi(p + 1);
    req->argv = (char **)kvs_malloc(sizeof(char *) * req->argc);
    req->argv_len = (int *)kvs_malloc(sizeof(int) * req->argc);
    
    p = crlf + 2;
    
    for (int i = 0; i < req->argc; i++) {
        crlf = find_crlf(p, 128);
        int arg_len = atoi(p + 1);
        p = crlf + 2;
        
        req->argv_len[i] = arg_len;
        req->argv[i] = (char *)kvs_malloc(arg_len + 1);
        memcpy(req->argv[i], p, arg_len);
        req->argv[i][arg_len] = '\0'; // 补上 \0 方便业务层用 strcmp，业务层核心应使用 argv_len
        
        p += arg_len + 2;
    }
}


/*
 * 释放解包时分配的内存
 */
static void free_resp_request(resp_request_t *req) {
    if (req->argv) {
        for (int i = 0; i < req->argc; i++) kvs_free(req->argv[i]);
        kvs_free(req->argv);
    }
    if (req->argv_len) kvs_free(req->argv_len);
}

/*
 * 打包函数：把业务层的回复格式化为 RESP 流
 */
static void resp_pack(char *send_buf, int *send_len, resp_reply_t *reply) {
    if (reply->status == KVS_RESP_OK) {
        *send_len += sprintf(send_buf + *send_len, "+OK\r\n");
    } else if (reply->status == KVS_RESP_PONG) {
        *send_len += sprintf(send_buf + *send_len, "+PONG\r\n");
    }else if (reply->status == KVS_RESP_SUCCESS) {
        *send_len += sprintf(send_buf + *send_len, "+OK\r\n");
    }else if (reply->status == KVS_RESP_ERR) {
        *send_len += sprintf(send_buf + *send_len, "-ERR save snapshot failed\r\n");
    }else if (reply->status == KVS_RESP_GET_OK && reply->body) {
        *send_len += sprintf(send_buf + *send_len, "$%d\r\n", reply->body_len);
        memcpy(send_buf + *send_len, reply->body, reply->body_len);
        *send_len += reply->body_len;
        memcpy(send_buf + *send_len, "\r\n", 2);
        *send_len += 2;
        kvs_free(reply->body); // 清理业务层 malloc 出来的数据副本
        reply->body = NULL;
    } else if (reply->status == KVS_RESP_NO_EXISTS) {
        *send_len += sprintf(send_buf + *send_len, "$-1\r\n"); 
    } else if (reply->status == KVS_RESP_EXISTS) {
        *send_len += sprintf(send_buf + *send_len, "-ERR key already exists\r\n");
    } else if (reply->status == KVS_RESP_UNKNOWN) {
        *send_len += sprintf(send_buf + *send_len, "-ERR unknown command\r\n");
    }else if (reply->status == KVS_RESP_PARSE_ERROR) {
        *send_len += sprintf(send_buf + *send_len, "-ERR syntax error\r\n");
    }else {
        *send_len += sprintf(send_buf + *send_len, "-ERR syntax or execution error\r\n");
    }
}

static cmd_handler_t g_command_handler = NULL;
void protocol_set_command_handler(cmd_handler_t handler) {
    g_command_handler = handler; 
}

/*
 * 打包并自动扩容函数：把业务层的回复格式化并追加到网络层的写缓冲区中
 */
void resp_pack_with_realloc(char **wbuf, int *wcap, int *wlen, resp_reply_t *reply) {
    // 估算这次打包大概需要多少安全空间：当前已用长度 + RESP基础协议头尾(1K足够) + body长度
    int needed = *wlen + 1024 + (reply->body_len > 0 ? reply->body_len : 0);
    
    // 动态扩容：如果空间不够，进行翻倍扩容
    if (needed > *wcap) {
        int new_cap = *wcap * 2;
        if (new_cap < needed) new_cap = needed;
        
        char *new_buf = (char *)kvs_realloc(*wbuf, new_cap);
        if (!new_buf) {
            perror("kvs_realloc wbuf failed in protocol tier");
            return; // 扩容失败及时拦截，防止后续越界
        }
        *wbuf = new_buf;
        *wcap = new_cap;
    }
    
    resp_pack(*wbuf, wlen, reply);
}

/*
 * 协议层入口，支持：1.处理客户端命令  2.处理主端同步回复
 */
int protocol_process_stream(const char *in_buf, int in_len, int *parsed, char **wbuf, int *wcap, int *wlen, int fd) {
    if (in_len <= 0) {
        *parsed = 0;
        return 0;
    }

    // 探测首字节
    char first_byte = in_buf[0];

    //来自本地客户端和主端的命令或者是从端获取日志的命令，都是resp协议
    if (first_byte == '*') {
        int processed = 0;

        while (processed < in_len) {
            int single_cmd_len = 0;
            
            // 探测是否有完整 RESP 请求
            if (!has_complete_resp_command(in_buf + processed, in_len - processed, &single_cmd_len)) {
                break; // 半包，跳出循环等下一次
            }
            
            // 保存完整的 RESP 命令数据
            char *saved_resp_cmd = (char *)kvs_malloc(single_cmd_len);
            memcpy(saved_resp_cmd, in_buf + processed, single_cmd_len);
            
            resp_request_t req;
            resp_unpack(in_buf, in_buf + processed, &req);

            if (g_enable_repl_master){
                extern struct conn conn_list[]; 
                struct conn *c = &conn_list[fd];
                if (req.argc > 0) {
                    if (strcmp(req.argv[0], "RDMA_CONNECT") == 0) {
                        c->role = CONN_SLAVE;
                        handle_slave_rdma_connect(&req, wbuf, wcap, wlen, fd);//收到 RDMA_CONNECT 握手命令，不经过网络层，直接回复一个RDMA_CONNECT_ACK
                        free_resp_request(&req);
                        kvs_free(saved_resp_cmd); // 释放保存的命令
                        processed += single_cmd_len;
                        continue; 
                    }
                }
            }
    
            // 进入业务层处理普通命令
            resp_reply_t reply = {KVS_RESP_ERROR, NULL, 0}; 
            if (g_command_handler) {
                g_command_handler(&req, &reply); 
            }

            // 如果这条命令的回复码是OK，写日志并打包回复给客户端
            // 如果检测到来时缓存 追加日志的同时 把增量命令写进缓冲区！！！
            if (wbuf && wcap && wlen) {
                if (reply.status == KVS_RESP_OK) {
                    if (g_enable_persistence || g_enable_repl_master || g_enable_repl_slave) {
                        kvs_persistence_write(saved_resp_cmd, single_cmd_len);
                    }
                    // 是主端，开始缓存标志，缓冲区没有越界
                    if (g_enable_repl_master && g_repl_backlog_enabled && g_repl_backlog_count < REPL_BACKLOG_MAX) {
                        g_repl_backlog[g_repl_backlog_count].data = kvs_malloc(single_cmd_len);
                        memcpy(g_repl_backlog[g_repl_backlog_count].data, saved_resp_cmd, single_cmd_len);
                        g_repl_backlog[g_repl_backlog_count].len = single_cmd_len;
                        g_repl_backlog_count++;
                    }
                }
                resp_pack_with_realloc(wbuf, wcap, wlen, &reply);
            }
            
            // 释放保存的完整命令
            kvs_free(saved_resp_cmd);
            
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
 * 协议层恢复函数：从 AOF 文件中恢复命令
 * 与正常请求处理流程一致，但不返回回复给客户端
 */
int protocol_process_recover(const char *in_buf, int in_len) {
    if (in_buf == NULL || in_len <= 0) {
        return 0;
    }

    int processed = 0;
    int recovered_count = 0;
    int skipped_count = 0;

    while (processed < in_len) {
        int single_cmd_len = 0;
        
        // 探测是否有完整 RESP 请求
        if (!has_complete_resp_command(in_buf + processed, in_len - processed, &single_cmd_len)) {
            printf("[Recover] Incomplete command at offset %d, stop.\n", processed);
            break; // 半包或格式错误，停止恢复
        }
        
        resp_request_t req;
        memset(&req, 0, sizeof(resp_request_t));
        resp_unpack(in_buf, in_buf + processed, &req);

        // 进入业务层处理命令
        resp_reply_t reply = {KVS_RESP_ERROR, NULL, 0}; 
        
        if (g_command_handler) {
            g_command_handler(&req, &reply); 
        }
        
        // 统计恢复结果 
        if (reply.status == KVS_RESP_OK) {
            recovered_count++;
        }else{
            skipped_count++;
        }
        
        // 清理回复中的 body
        if (reply.body) {
            kvs_free(reply.body);
            reply.body = NULL;
        }
        
        free_resp_request(&req); 
        processed += single_cmd_len; 
    }

    // printf("[Recover] Recovery stats: %d commands replayed.\n", recovered_count);
    
    return processed; // 返回处理的字节数
}