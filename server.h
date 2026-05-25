#ifndef SERVER_BINARY_H
#define SERVER_BINARY_H

#include "kvstore.h" // 必须包含，因为 struct conn 里用到了 session_ctx_t (间接依赖)

#define INIT_BUFFER_SIZE 4096

typedef void (*RCALLBACK)(int fd);

// 这里保留 binary_msg_handler 的定义，因为它直接决定了 reactor 如何调用协议层
typedef int (*binary_msg_handler)(void *msg, int length, session_ctx_t *ctx);

// 连接结构体：这是网络层的核心
struct conn {
    int fd;
    
    char *rbuffer;   // 动态接收缓冲区
    int rcapacity;   // 总容量
    int rlength;     // 已用长度
    
    char *wbuffer;   // 动态发送缓冲区
    int wcapacity;   // 总容量
    int wlength;
    
    RCALLBACK send_callback;
    RCALLBACK read_callback;
    RCALLBACK accept_callback;
};

// 启动函数声明
int reactor_start(unsigned short port, binary_msg_handler handler);
int proactor_start(unsigned short port, binary_msg_handler handler);
int ntyco_start(unsigned short port, binary_msg_handler handler);

#endif