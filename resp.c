#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <sys/syscall.h>
#include <unistd.h>
#include <sys/time.h>
#include "resp.h"
#include "kvstore.h"      
#include "network.h"
#include "repl.h"
#include "rdma.h"

#ifndef SYS_write
#define SYS_write 1
#endif

// 判断是否是需要持久化的写命令
static int is_write_command(const char *cmd, int len) {
    if (len == 3 && strncasecmp(cmd, "SET", 3) == 0) return 1;
    if (len == 3 && strncasecmp(cmd, "DEL", 3) == 0) return 1;
    if (len == 3 && strncasecmp(cmd, "MOD", 3) == 0) return 1;
    if (len == 4 && strncasecmp(cmd, "RSET", 4) == 0) return 1;
    if (len == 4 && strncasecmp(cmd, "RDEL", 4) == 0) return 1;
    if (len == 4 && strncasecmp(cmd, "RMOD", 4) == 0) return 1;
    if (len == 4 && strncasecmp(cmd, "HSET", 4) == 0) return 1;
    if (len == 4 && strncasecmp(cmd, "HDEL", 4) == 0) return 1;
    if (len == 4 && strncasecmp(cmd, "HMOD", 4) == 0) return 1;
    if (len == 4 && strncasecmp(cmd, "SSET", 4) == 0) return 1;
    if (len == 4 && strncasecmp(cmd, "SDEL", 4) == 0) return 1;
    if (len == 4 && strncasecmp(cmd, "SMOD", 4) == 0) return 1;
    return 0;
}

// 查表函数
extern command_t *lookup_command(const char *name, int len);

// 全局批量处理函数指针
static cmd_handler_t g_command_handler = NULL;

void protocol_set_command_handler(cmd_handler_t handler) {
    g_command_handler = handler; 
}

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

static void resp_unpack_no_modify(char *req_buf, int buf_len, resp_request_t *req) {
    char *p = req_buf;
    // 使用剩余长度查找第一个 \r\n
    const char *crlf = find_crlf(p, buf_len);
    
    if (!crlf) {
        req->argc = 0;
        req->is_dynamic = 0;
        return;
    }
    
    req->argc = atoi(p + 1);
    
    if (req->argc <= RESP_STATIC_ARGC) {
        req->argv = req->buf_argv;
        req->argv_len = req->buf_argv_len;
        req->is_dynamic = 0;
    } else {
        req->argv = (char **)kvs_malloc(sizeof(char *) * req->argc);
        req->argv_len = (int *)kvs_malloc(sizeof(int) * req->argc);
        req->is_dynamic = 1;
    }
    
    p = (char *)(crlf + 2);
    
    for (int i = 0; i < req->argc; i++) {
        // 计算当前剩余长度
        int remaining = buf_len - (p - req_buf);
        crlf = find_crlf(p, remaining);
        if (!crlf) {
            req->argc = i;
            return;
        }
        int arg_len = atoi(p + 1);
        p = (char *)(crlf + 2);
        
        req->argv_len[i] = arg_len;
        req->argv[i] = p;          // 指向 in_buf，零拷贝
        
        p += arg_len + 2;
    }
}

static void free_resp_request(resp_request_t *req) {
    if (!req) return;
    if (req->is_dynamic) {
        if (req->argv) {
            kvs_free(req->argv);
            req->argv = NULL;
        }
        if (req->argv_len) {
            kvs_free(req->argv_len);
            req->argv_len = NULL;
        }
        req->is_dynamic = 0;
    }
    req->argc = 0;
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

int protocol_process_stream(char *in_buf, int in_len, int *parsed, char **wbuf, int *wcap, int *wlen, int fd) {
    if (in_len <= 0) {
        *parsed = 0;
        return 0;
    }

    if (in_buf[0] != '*') {
        return -1;
    }

    // 预分配池
    #define MAX_ARGC 16
    char *cmd_argv_pool[PIPELINE_MAX][MAX_ARGC];
    int cmd_argv_len_pool[PIPELINE_MAX][MAX_ARGC];

    int processed = 0;
    int cmd_num = 0;
    
    parsed_cmd_t cmds[PIPELINE_MAX];
    resp_reply_t replies[PIPELINE_MAX];

    while (processed < in_len && cmd_num < PIPELINE_MAX) {
        int single_cmd_len = 0;
        
        if (!has_complete_resp_command(in_buf + processed, in_len - processed, &single_cmd_len)) {
            break;
        }

        char *cmd_raw_ptr = in_buf + processed; 

        resp_request_t temp_req;
        memset(&temp_req, 0, sizeof(resp_request_t));
        resp_unpack_no_modify(cmd_raw_ptr, single_cmd_len, &temp_req);

        // 特殊命令处理（RDMA / SYNC / PING）
        int is_special_network_cmd = 0;
        int is_rdma_cmd = (temp_req.argc > 0 && temp_req.argv_len[0] == 12 && 
                          strncasecmp(temp_req.argv[0], "RDMA_CONNECT", 12) == 0);
        int is_sync_cmd = (temp_req.argc > 0 && temp_req.argv_len[0] == 4 && 
                          strncasecmp(temp_req.argv[0], "SYNC", 4) == 0);

        if (g_enable_repl_master && temp_req.argc > 0 && (is_rdma_cmd || is_sync_cmd)) {
            extern struct conn ntyco_conn_list[]; 
            struct conn *c = &ntyco_conn_list[fd];

            if (is_rdma_cmd) {
                c->role = CONN_SLAVE;
                handle_slave_rdma_connect(&temp_req, wbuf, wcap, wlen, fd);
                is_special_network_cmd = 1;
            } else if (g_use_tcp_sync && is_sync_cmd) {
                extern volatile int g_slave_fd;
                g_slave_fd = fd; 
            }
        }

        if (temp_req.argc > 0 && temp_req.argv_len[0] == 4 && 
            strncasecmp(temp_req.argv[0], "PING", 4) == 0) {
            resp_reply_t ping_reply = {KVS_RESP_ERROR, NULL, 0};
            if (temp_req.argc == 1) ping_reply.status = KVS_RESP_PONG;
            else if (temp_req.argc == 2) {
                ping_reply.status = KVS_RESP_GET_OK;
                ping_reply.body = (char *)kvs_malloc(temp_req.argv_len[1]);
                memcpy(ping_reply.body, temp_req.argv[1], temp_req.argv_len[1]);
                ping_reply.body_len = temp_req.argv_len[1];
            } else ping_reply.status = KVS_RESP_PARSE_ERROR;

            if (wbuf && wcap && wlen) {
                resp_pack_with_realloc(wbuf, wcap, wlen, &ping_reply);
            }
            if (ping_reply.body) kvs_free(ping_reply.body);
            is_special_network_cmd = 1;
        }

        if (is_special_network_cmd) {
            free_resp_request(&temp_req);
            processed += single_cmd_len;
            continue; 
        }
        
        // 存储命令信息（使用池，零拷贝）
        int argc = temp_req.argc;
        if (argc > MAX_ARGC) {
            cmds[cmd_num].req.argc = argc;
            cmds[cmd_num].req.is_dynamic = 1;
            cmds[cmd_num].req.argv = (char **)kvs_malloc(sizeof(char *) * argc);
            cmds[cmd_num].req.argv_len = (int *)kvs_malloc(sizeof(int) * argc);
            memcpy(cmds[cmd_num].req.argv, temp_req.argv, sizeof(char *) * argc);
            memcpy(cmds[cmd_num].req.argv_len, temp_req.argv_len, sizeof(int) * argc);
        } else {
            cmds[cmd_num].req.argc = argc;
            cmds[cmd_num].req.is_dynamic = 0;
            cmds[cmd_num].req.argv = cmd_argv_pool[cmd_num];
            cmds[cmd_num].req.argv_len = cmd_argv_len_pool[cmd_num];
            memcpy(cmd_argv_pool[cmd_num], temp_req.argv, sizeof(char *) * argc);
            memcpy(cmd_argv_len_pool[cmd_num], temp_req.argv_len, sizeof(int) * argc);
        }
        
        cmds[cmd_num].cmd = (argc > 0) ? lookup_command(temp_req.argv[0], temp_req.argv_len[0]) : NULL;
        cmds[cmd_num].cmd_raw_ptr = cmd_raw_ptr;
        cmds[cmd_num].single_cmd_len = single_cmd_len;
        
        replies[cmd_num].status = KVS_RESP_ERROR;
        replies[cmd_num].body = NULL;
        replies[cmd_num].body_len = 0;

        // 写入 WAL
        int is_write = (argc > 0 && is_write_command(temp_req.argv[0], temp_req.argv_len[0]));
        if (is_write) {
            if (g_enable_persistence || g_enable_repl_master || g_enable_repl_slave) {
                kvs_persistence_write(cmd_raw_ptr, single_cmd_len);
            }

            extern int g_sync_file_done;
            extern volatile int g_slave_fd;
            if (g_slave_fd > 0 && g_enable_repl_master && ((g_use_tcp_sync && g_sync_file_done) || g_repl_backlog_enabled)) {
                if (g_repl_backlog_count < REPL_BACKLOG_MAX) {
                    int index = g_repl_backlog_tail; 
                    g_repl_backlog[index].data = kvs_malloc(single_cmd_len);
                    memcpy(g_repl_backlog[index].data, cmd_raw_ptr, single_cmd_len);
                    g_repl_backlog[index].len = single_cmd_len;
                    g_repl_backlog_tail = (g_repl_backlog_tail + 1) % REPL_BACKLOG_MAX;
                    g_repl_backlog_count++;
                }
            }
        }

        free_resp_request(&temp_req);
        processed += single_cmd_len;
        cmd_num++;
    }

    *parsed = processed;


    // 阶段 3：批量执行业务层
    if (cmd_num > 0) {
        if (g_command_handler) {
            g_command_handler(cmds, replies, cmd_num);
        } else {
            for (int i = 0; i < cmd_num; i++) {
                replies[i].status = KVS_RESP_UNKNOWN;
            }
        }
    }

    // 阶段 5：打包回复 & 释放
    for (int i = 0; i < cmd_num; i++) {
        if (wbuf && wcap && wlen) {
            resp_pack_with_realloc(wbuf, wcap, wlen, &replies[i]);
        }
        free_resp_request(&cmds[i].req);
        if (replies[i].body) {           
            kvs_free(replies[i].body);
            replies[i].body = NULL;
        }
    }

    return 0;
}

/**
 * 内部删除命令专用处理：生成 RESP 报文，写入 AOF 和主从复制 backlog
 */
void protocol_handle_internal_del(const char *cmd, const void *key, int key_len) {
    int cmd_len = (int)strlen(cmd);
    int head_len = snprintf(NULL, 0, "*2\r\n$%d\r\n%s\r\n$%d\r\n", cmd_len, cmd, key_len);
    int total_len = head_len + key_len + 2; 

    char *resp_buf = (char *)kvs_malloc(total_len + 1);
    if (!resp_buf) return;

    char *p = resp_buf;
    p += sprintf(p, "*2\r\n$%d\r\n%s\r\n$%d\r\n", cmd_len, cmd, key_len);
    memcpy(p, key, key_len);       p += key_len;
    memcpy(p, "\r\n", 2);          p += 2;

    if (g_enable_persistence || g_enable_repl_master || g_enable_repl_slave) kvs_persistence_write(resp_buf, total_len);

    extern int g_sync_file_done;
    extern volatile int g_slave_fd;
    if (g_slave_fd > 0 && g_enable_repl_master &&
        ((g_use_tcp_sync && g_sync_file_done) || g_repl_backlog_enabled)) {
        if (g_repl_backlog_count < REPL_BACKLOG_MAX) {
            int index = g_repl_backlog_tail;
            g_repl_backlog[index].data = kvs_malloc(total_len);
            memcpy(g_repl_backlog[index].data, resp_buf, total_len);
            g_repl_backlog[index].len = total_len;
            g_repl_backlog_tail = (g_repl_backlog_tail + 1) % REPL_BACKLOG_MAX;
            g_repl_backlog_count++;
        }
    }
    kvs_free(resp_buf);
}


int protocol_process_recover(char *in_buf, int in_len) {
    if (in_buf == NULL || in_len <= 0) {
        return 0;
    }

    int processed = 0;

    while (processed < in_len) {
        int single_cmd_len = 0;
        
        if (!has_complete_resp_command(in_buf + processed, in_len - processed, &single_cmd_len)) {
            break;
        }
        
        resp_request_t temp_req;
        memset(&temp_req, 0, sizeof(resp_request_t));
        resp_unpack_no_modify(in_buf + processed, single_cmd_len, &temp_req);

        // 构造单条命令
        parsed_cmd_t cmds[1];
        resp_reply_t replies[1];
        
        cmds[0].req.argc = temp_req.argc;
        cmds[0].req.is_dynamic = 1;
        cmds[0].req.argv = (char **)kvs_malloc(sizeof(char *) * temp_req.argc);
        cmds[0].req.argv_len = (int *)kvs_malloc(sizeof(int) * temp_req.argc);
        memcpy(cmds[0].req.argv, temp_req.argv, sizeof(char *) * temp_req.argc);
        memcpy(cmds[0].req.argv_len, temp_req.argv_len, sizeof(int) * temp_req.argc);
        
        cmds[0].cmd = (temp_req.argc > 0) ? lookup_command(temp_req.argv[0], temp_req.argv_len[0]) : NULL;
        cmds[0].cmd_raw_ptr = NULL;
        cmds[0].single_cmd_len = 0;
        
        replies[0].status = KVS_RESP_ERROR;
        replies[0].body = NULL;
        replies[0].body_len = 0;

        if (g_command_handler) {
            g_command_handler(cmds, replies, 1);
        }

        free_resp_request(&temp_req);
        
        if (replies[0].body) {
            kvs_free(replies[0].body);
        }
        free_resp_request(&cmds[0].req);
        
        processed += single_cmd_len;
    }
    
    return processed;
}

