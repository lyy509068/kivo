#ifndef SERVER_BINARY_H
#define SERVER_BINARY_H

#include "kvstore.h" 
#include <netinet/in.h> 

//网络设置
#define NETWORK_REACTOR      0
#define NETWORK_PROACTOR     1
#define NETWORK_NTYCO        2
#define NETWORK_SELECT       2

#define INIT_BUFFER_SIZE 4096
#define MAX_PACKET_SIZE 10 * 1024 * 1024
#define CONNECTION_SIZE 1024
#define QUEUE_DEPTH 1024


typedef void (*RCALLBACK)(int fd);// 回调函数recv accept send

typedef int (*stream_handler_t)(const char *in_buf, int in_len, int *parsed, char **wbuf, int *wcap, int *wlen, int fd);// 操作协议层的句柄
// 连接身份标志
typedef enum {
    CONN_CLIENT = 0, // 普通客户端
    CONN_MASTER = 1, // 主端
    CONN_SLAVE  = 2  //从端
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
    
    conn_role_t role; // 当前连接的身份，默认为CONN_CLIENT

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

#if (NETWORK_SELECT == NETWORK_REACTOR)
extern struct conn reactor_conn_list[];
#elif (NETWORK_SELECT == NETWORK_PROACTOR)
extern struct conn proactor_conn_list[];
#elif (NETWORK_SELECT == NETWORK_NTYCO)
extern struct conn ntyco_conn_list[];
#endif

// 函数声明
#if (NETWORK_SELECT == NETWORK_REACTOR)
int reactor_start(unsigned short port, stream_handler_t handler);
int reactor_set_event(int fd, int event, int flag);
struct conn* reactor_host_slave_connection(int fd, char *wbuf, int wcap, int wlen);
void recv_cb(int fd); 
void reactor_close_and_free_connection(int fd);
#elif (NETWORK_SELECT == NETWORK_PROACTOR)
int proactor_start(unsigned short port, stream_handler_t handler);
struct conn* proactor_host_slave_connection(int fd, char *wbuf, int wcap, int wlen);
void submit_recv(int fd);
void proactor_close_and_free_connection(int fd);
#elif (NETWORK_SELECT == NETWORK_NTYCO)
int ntyco_start(unsigned short port, stream_handler_t handler);
struct conn* ntyco_host_slave_connection(int fd, char *wbuf, int wcap, int wlen);
void ntyco_close_and_free_connection(int fd);
#endif



#if (NETWORK_SELECT == NETWORK_REACTOR)

    #define conn_list reactor_conn_list

    #define net_host_slave_connection  reactor_host_slave_connection
    #define net_close_and_free_connection reactor_close_and_free_connection

#elif (NETWORK_SELECT == NETWORK_PROACTOR)

    #define conn_list proactor_conn_list

    #define net_host_slave_connection  proactor_host_slave_connection
    #define net_close_and_free_connection proactor_close_and_free_connection

#elif (NETWORK_SELECT == NETWORK_NTYCO) 

    #define conn_list ntyco_conn_list

    #define net_host_slave_connection     ntyco_host_slave_connection
    #define net_close_and_free_connection ntyco_close_and_free_connection

#endif

#endif 