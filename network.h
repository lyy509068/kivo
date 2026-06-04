#ifndef SERVER_BINARY_H
#define SERVER_BINARY_H

#include "kvstore.h" 

#define INIT_BUFFER_SIZE 4096

typedef void (*RCALLBACK)(int fd);


typedef int (*stream_handler_t)(const char *in_buf, int in_len, int *parsed, char **wbuf, int *wcap, int *wlen, long long *out_val);

struct conn {
    int fd;
    
    char *rbuffer;   // 动态接收缓冲区
    int rcapacity;   // 总容量
    int rlength;     // 已用长度
    
    char *wbuffer;   // 动态发送缓冲区
    int wcapacity;   // 总容量
    int wlength;
    
    int file_fd;         // 当前正在发送的文件描述符
    long long file_size; // 文件总大小
    long long file_ptr;  // 已经发送了多少字节

    int local_file_fd;          // 从端落盘用的本地文件描述符
    long long expect_file_size; // 从端期望接收的文件总大小
    long long already_recv_size;// 从端当前已经接收了多少字节
    int is_receiving_file;      // 状态机标志位：1表示正在接收文件，0表示正常命令模式

    RCALLBACK send_callback;
    RCALLBACK read_callback;
    RCALLBACK accept_callback;
};

// 启动函数声明
int reactor_start(unsigned short port, stream_handler_t handler);
int proactor_start(unsigned short port, stream_handler_t handler);
int ntyco_start(unsigned short port, stream_handler_t handler);
int reactor_host_slave_connection(int fd, char *wbuf, int wcap, int wlen);

#endif