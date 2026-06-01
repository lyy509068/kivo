#ifndef SERVER_BINARY_H
#define SERVER_BINARY_H

#include "kvstore.h" 

#define INIT_BUFFER_SIZE 4096

typedef void (*RCALLBACK)(int fd);


typedef int (*stream_handler_t)(const char *in_buf, int in_len, int *parsed, char **wbuf, int *wcap, int *wlen);

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
int reactor_start(unsigned short port, stream_handler_t handler);
int proactor_start(unsigned short port, stream_handler_t handler);
int ntyco_start(unsigned short port, stream_handler_t handler);

#endif