#ifndef KVS_PROTOCOL_H
#define KVS_PROTOCOL_H

#include <stdbool.h>
#include <stdlib.h>
#include <stdint.h>

struct conn;

// 状态码枚举
typedef enum {
    KVS_RESP_OK = 0,          // 0: 执行成功 (返回 +OK)
    KVS_RESP_ERROR = 1,       // 1: 一般执行错误 (返回 -ERR)
    KVS_RESP_PARSE_ERROR = 2, // 2: 解析错误
    KVS_RESP_GET_OK = 3,      // 3: GET 成功 (返回 $len\r\nbody\r\n)
    KVS_RESP_UNKNOWN = 4,     // 4: 未知命令
    
    KVS_RESP_EXISTS = 5,       // 6: KEY 已存在 (对应 REXISTS 成功返回 1)
    KVS_RESP_NO_EXISTS = 6,    // 7: KEY 不存在 (返回 $-1\r\n)
    
    KVS_RESP_PONG = 7,         // 8: PONG 回应
    KVS_RESP_SUCCESS = 8, // 11: SAVE 快照保存成功 (返回 +OK)
    KVS_RESP_ERR = 9,     // 9: SAVE 快照落盘失败 (返回 -ERR save failed)
    KVS_RESP_SYNC_LOG = 10     // 10：请求同步日志状态码
} kvs_status_t;

// 请求结构体 (协议层解析后，传给业务层)
typedef struct resp_request{
    int argc;           // 命令的参数总数 (例如: SET key val，argc = 3)
    char **argv;        // 参数字符串数组 (argv[0]="SET", argv[1]="key")
    int *argv_len;      // 参数长度数组 (为了保证二进制安全，防止value里包含\0)
    uint32_t socket_tcp_seq;
} resp_request_t;

// 响应结构体 (业务层执行完，传回给协议层)
typedef struct {
    kvs_status_t status; // 业务层执行状态码
    void *body;          // 查询到的值 (仅 GET 命令有效，需业务层 malloc，协议层负责 free)
    int body_len;        // 查询到的值的长度
} resp_reply_t;


typedef int (*cmd_handler_t)(const resp_request_t *req, resp_reply_t *reply);//协议层用来调用业务层
typedef int (*stream_handler_t)(const char *in_buf, int in_len, int *parsed, char **wbuf, int *wcap, int *wlen, long long *out_val, int fd);//网络层用来调用协议层
//网络传输函数
extern int reactor_start(unsigned short port, stream_handler_t handler);
extern int proactor_start(unsigned short port, stream_handler_t handler);
extern int ntyco_start(unsigned short port, stream_handler_t handler);


void protocol_set_command_handler(cmd_handler_t handler);
int protocol_process_stream(const char *in_buf, int in_len, int *parsed, char **wbuf, int *wcap, int *wlen, long long *out_val, int fd);
int protocol_process_recover(const char *in_buf, int in_len);
int protocol_process_udp_silent(const char *in_buf, int in_len);
int kvs_execute_command(const resp_request_t *req, resp_reply_t *reply);


#endif 