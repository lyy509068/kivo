#ifndef SERVER_BINARY_H
#define SERVER_BINARY_H

#include "kvstore.h" 

#define INIT_BUFFER_SIZE 4096

typedef void (*RCALLBACK)(int fd);


typedef int (*stream_handler_t)(const char *in_buf, int in_len, int *parsed, char **wbuf, int *wcap, int *wlen, long long *out_val);

typedef enum {
    CONN_CLIENT = 0, // 普通客户端
    CONN_MASTER = 1  // 主端
} conn_role_t;

struct conn {
    int fd;
    
    char *rbuffer;   // 动态接收缓冲区
    int rcapacity;   // 总容量
    int rlength;     // 已用长度
    
    char *wbuffer;   // 动态发送缓冲区
    int wcapacity;   
    int wlength;
    
    int file_fd;         // 主端正在发送的文件描述符
    long long file_size; // 文件总大小
    long long file_ptr;  // 已经发送了多少字节

    int local_file_fd;          // 从端落盘用的文件描述符
    long long expect_file_size; // 文件总大小
    long long already_recv_size;// 当前已经接收了多少字节
    int is_receiving_file;      // 1表示正在接收文件，0表示正常命令模式

    conn_role_t role; // 当前连接的身份

    RCALLBACK send_callback;
    RCALLBACK read_callback;
    RCALLBACK accept_callback;
};




extern struct repl_conn g_repl;
extern int repl_flush();

// 启动函数声明
int reactor_start(unsigned short port, stream_handler_t handler);
int proactor_start(unsigned short port, stream_handler_t handler);
int ntyco_start(unsigned short port, stream_handler_t handler);
int reactor_host_slave_connection(int fd, char *wbuf, int wcap, int wlen);
int set_event(int fd, int event, int flag);

#endif