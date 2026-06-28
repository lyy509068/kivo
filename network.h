#ifndef SERVER_BINARY_H
#define SERVER_BINARY_H

#include "kvstore.h" 
#include <netinet/in.h> 

//网络设置
#define NETWORK_REACTOR      0
#define NETWORK_PROACTOR     1
#define NETWORK_NTYCO        2
#define NETWORK_SELECT      NETWORK_REACTOR

#define INIT_BUFFER_SIZE 4096
#define MAX_PACKET_SIZE 10 * 1024 * 1024
#define CONNECTION_SIZE 1024
#define QUEUE_DEPTH 1024

extern volatile int server_should_exit;// 退出服务器标志

typedef void (*RCALLBACK)(int fd);// 回调函数recv accept send

typedef int (*stream_handler_t)(const char *in_buf, int in_len, int *parsed, char **wbuf, int *wcap, int *wlen, long long *out_val, uint32_t tcp_seq);// 操作协议层的句柄
// 连接身份标志
typedef enum {
    CONN_CLIENT = 0, // 普通客户端
    CONN_MASTER = 1  // 主端
} conn_role_t;

// io_uring 异步操作上下文定义
typedef enum {
    OP_ACCEPT,
    OP_RECV,
    OP_SEND,
    OP_SENDFILE
} op_type_t;
typedef struct {
    int fd;
    op_type_t type;
} io_ctx_t;


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

    struct rdma_ring_ctx *rdma_ctx;

    //Reactor 核心回调 (Proactor 模式下闲置)
    RCALLBACK send_callback;
    RCALLBACK read_callback;
    RCALLBACK accept_callback;

    
    // 异步 Accept 必须使用持久化的内存空间来接收对端地址，不能使用栈内存！
    struct sockaddr_in clientaddr;
    socklen_t clientlen;

    // 区分不同异步 IO 操作的 user_data 上下文
    io_ctx_t accept_ctx;
    io_ctx_t recv_ctx;
    io_ctx_t send_ctx;
};



// 同步层
extern struct repl_conn g_repl;
extern int repl_flush();

// 函数声明
#if (NETWORK_SELECT == NETWORK_REACTOR)
int reactor_start(unsigned short port, stream_handler_t handler);
int reactor_set_event(int fd, int event, int flag);
struct conn* reactor_host_slave_connection(int fd, char *wbuf, int wcap, int wlen);
#elif (NETWORK_SELECT == NETWORK_PROACTOR)
int proactor_start(unsigned short port, stream_handler_t handler);
void proactor_notify_tx_ready(int fd);
int proactor_host_slave_connection(int fd, char *wbuf, int wcap, int wlen);
#elif (NETWORK_SELECT == NETWORK_NTYCO)
int ntyco_start(unsigned short port, stream_handler_t handler);
int ntyco_host_slave_connection(int fd, char *wbuf, int wcap, int wlen);
#endif



#if (NETWORK_SELECT == NETWORK_REACTOR)

    #define net_host_slave_connection  reactor_host_slave_connection
    #define net_set_event(fd, ev, is_add) reactor_set_event(fd, ev, is_add)

#elif (NETWORK_SELECT == NETWORK_PROACTOR)

    #define net_host_slave_connection  proactor_host_slave_connection
    #define net_set_event(fd, ev, is_add) proactor_notify_tx_ready(fd)

#elif (NETWORK_SELECT == NETWORK_NTYCO) 

    #define net_host_slave_connection     ntyco_host_slave_connection
    #define net_set_event(fd, ev, is_add) ((void)0)

#endif

#endif