#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include "resp.h"
#include "kvstore.h"      
#include "network.h"
#include "repl.h"

/* * 🛠️ 内部辅助函数：在指定长度内安全寻找 \r\n 
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
 * 🔍 探测函数：检查接收缓冲区内是否有一条完整的 RESP 请求
 * 期待格式如：*3\r\n$3\r\nSET\r\n$4\r\nname\r\n$4\r\nalex\r\n
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
    
    *out_cmd_len = p - buf; // 记录这条完整命令占据的总字节数
    return 1;
}

/*
 * ✂️ 解包函数：将缓冲区文本切分成干净的 argv 数组
 */
static void resp_unpack(const char *buf, const char *req_buf, resp_request_t *req, uint32_t base_seq) {
    const char *p = req_buf;
    const char *crlf = find_crlf(p, 100); 
    
    req->argc = atoi(p + 1);
    req->argv = (char **)kvs_malloc(sizeof(char *) * req->argc);
    req->argv_len = (int *)kvs_malloc(sizeof(int) * req->argc);

    // 主端计算当前命令在 TCP 流中的绝对序列号
    #if ENABLE_REPLICATION_MASTER
    if (base_seq != 0) {
        // 当前命令在整个接收缓冲区中的字节偏移量
        size_t offset = (size_t)(req_buf - buf);
        // 当前命令的真实 TCP Seq = 缓冲区基准 Seq + 偏移量
        req->socket_tcp_seq = base_seq + (uint32_t)offset;
    } else {
        req->socket_tcp_seq = 0;
    }
    #else
    req->socket_tcp_seq = 0; 
    #endif
    
    p = crlf + 2;
    
    for (int i = 0; i < req->argc; i++) {
        crlf = find_crlf(p, 128);
        int arg_len = atoi(p + 1);
        p = crlf + 2;
        
        req->argv_len[i] = arg_len;
        req->argv[i] = (char *)kvs_malloc(arg_len + 1);
        memcpy(req->argv[i], p, arg_len);
        req->argv[i][arg_len] = '\0'; // 补上 \0 方便业务层用 strcmp，但业务层核心应使用 argv_len
        
        p += arg_len + 2;
    }
}


/*
 * 🗑️ 释放解包时分配的内存
 */
static void free_resp_request(resp_request_t *req) {
    if (req->argv) {
        for (int i = 0; i < req->argc; i++) kvs_free(req->argv[i]);
        kvs_free(req->argv);
    }
    if (req->argv_len) kvs_free(req->argv_len);
}

/*
 * 📦 打包函数：把业务层的回复格式化为 RESP 流
 */
static void resp_pack(char *send_buf, int *send_len, resp_reply_t *reply) {
    if (reply->status == KVS_RESP_OK) {
        *send_len += sprintf(send_buf + *send_len, "+OK\r\n");
    } 
    else if (reply->status == KVS_RESP_PONG) {
        *send_len += sprintf(send_buf + *send_len, "+PONG\r\n");
    }
    else if (reply->status == KVS_RESP_SAVE_ERR) {
        *send_len += sprintf(send_buf + *send_len, "-ERR save snapshot failed\r\n");
    }
    else if (reply->status == KVS_RESP_GET_OK && reply->body) {
        *send_len += sprintf(send_buf + *send_len, "$%d\r\n", reply->body_len);
        memcpy(send_buf + *send_len, reply->body, reply->body_len);
        *send_len += reply->body_len;
        memcpy(send_buf + *send_len, "\r\n", 2);
        *send_len += 2;
        kvs_free(reply->body); // 协议层负责清理业务层 malloc 出来的数据副本
        reply->body = NULL;
    } 
    else if (reply->status == KVS_RESP_NO_EXISTS) {
        *send_len += sprintf(send_buf + *send_len, "$-1\r\n"); // Redis 标准：找不到返回 null bulk string
    } 
    else if (reply->status == KVS_RESP_EXISTS) {
        *send_len += sprintf(send_buf + *send_len, "-ERR key already exists\r\n");
    } 
    else if (reply->status == KVS_RESP_UNKNOWN) {
        // 针对不认识的命令，返回标准 Redis 格式错误
        *send_len += sprintf(send_buf + *send_len, "-ERR unknown command\r\n");
    }
    else if (reply->status == KVS_RESP_PARSE_ERROR) {
        // 针对参数缺失、参数过多等情况，返回标准 Redis 语法错误
        *send_len += sprintf(send_buf + *send_len, "-ERR syntax error\r\n");
    }
    else {
        // 一般执行错误
        *send_len += sprintf(send_buf + *send_len, "-ERR syntax or execution error\r\n");
    }
}

static cmd_handler_t g_command_handler = NULL;
void protocol_set_command_handler(cmd_handler_t handler) {
    g_command_handler = handler; 
}

/*
 * 📦 打包并自动扩容函数：把业务层的回复格式化并追加到网络层的写缓冲区中
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
 * 🔄 统一自适应协议层核心入口，同时支持：1.处理客户端命令  2.处理主端同步回复
 * 网络层从这里进入
 */
int protocol_process_stream(const char *in_buf, int in_len, int *parsed, char **wbuf, int *wcap, int *wlen, long long *out_val, uint32_t tcp_seq) {
    if (in_len <= 0) {
        *parsed = 0;
        return 0;
    }

    // 探测首字节，判定数据流来源
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
            
            resp_request_t req;
            resp_unpack(in_buf, in_buf + processed, &req, tcp_seq);
    
            // 业务层处理
            resp_reply_t reply = {KVS_RESP_ERROR, NULL, 0}; 
            if (g_command_handler) {
                g_command_handler(&req, &reply); 
            }

            // 打包回复给客户端
            if (wbuf && wcap && wlen) {
                resp_pack_with_realloc(wbuf, wcap, wlen, &reply);
            }
            
            free_resp_request(&req); 
            if (reply.body) {
                kvs_free(reply.body);
                reply.body = NULL;
            }

            processed += single_cmd_len; 
        }

        *parsed = processed; 
        return 0; // 返回 0 代表客户端命令处理正常
    } else {
        return -1; 
    }
}
