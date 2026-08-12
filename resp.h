#ifndef KVS_PROTOCOL_H
#define KVS_PROTOCOL_H

#include <stdbool.h>
#include <stdlib.h>
#include <stdint.h>

// 批量处理与命令查表支持
#ifndef PIPELINE_MAX
#define PIPELINE_MAX 200
#endif

struct conn;

// 状态码枚举
typedef enum {
    KVS_RESP_OK,           // +OK\r\n
    KVS_RESP_GET_OK,       // $len\r\n<body_data>\r\n
    KVS_RESP_PONG,
    KVS_RESP_NO_EXISTS,    // $-1\r\n (Redis 的 Key 不存在返回 nil)
    KVS_RESP_EXISTS,       // :1\r\n  (Key 已存在)
    KVS_RESP_SUCCESS,      // :1\r\n  (逻辑成功，如 SAVE/SYNC)
    KVS_RESP_ERROR,        // -ERR execution error\r\n
    KVS_RESP_PARSE_ERROR,  // -ERR wrong number of arguments\r\n
    KVS_RESP_UNKNOWN,      // -ERR unknown command\r\n
    KVS_RESP_ERR           // -ERR system error\r\n
} kvs_status_t;

// 请求结构体
#define RESP_STATIC_ARGC 16
typedef struct resp_request {
    int argc;           // 命令的参数总数
    char **argv;        // 参数字符串数组
    int *argv_len;      // 参数长度数组
    char *buf_argv[RESP_STATIC_ARGC];
    int buf_argv_len[RESP_STATIC_ARGC];
    int is_dynamic;     // 标记 argv/argv_len 是否动态分配
} resp_request_t;

// 响应结构体
typedef struct {
    kvs_status_t status; 
    void *body;          
    int body_len;        
} resp_reply_t;

// 命令结构体 (由业务层定义具体实现)
typedef struct command_s {
    char *name;
    int minargc;
    void (*proc)(resp_request_t *req, resp_reply_t *reply);
} command_t;

// 经过协议层解析和查表后生成的 命令对象
typedef struct {
    resp_request_t req;
    command_t *cmd;
    char *cmd_raw_ptr;      // 新增：指向原始 RESP 报文
    int single_cmd_len;     // 新增：当前命令长度
} parsed_cmd_t;

// 【核心修改】：协议层用来调用业务层的回调函数签名
typedef int (*cmd_handler_t)(parsed_cmd_t *cmds, resp_reply_t *replies, int cmd_num);

// 网络层用来调用协议层的回调函数签名
typedef int (*stream_handler_t)(char *in_buf, int in_len, int *parsed, char **wbuf, int *wcap, int *wlen, int fd);

// 网络传输函数
extern int reactor_start(unsigned short port, stream_handler_t handler);
extern int proactor_start(unsigned short port, stream_handler_t handler);
extern int ntyco_start(unsigned short port, stream_handler_t handler);

// 协议层接口
void protocol_set_command_handler(cmd_handler_t handler);
int protocol_process_stream(char *in_buf, int in_len, int *parsed, char **wbuf, int *wcap, int *wlen, int fd);
int protocol_process_recover(char *in_buf, int in_len);

// 业务层入口声明（供注册使用）
extern int kvs_execute_batch(parsed_cmd_t *cmds, resp_reply_t *replies, int cmd_num);

#endif // KVS_PROTOCOL_H