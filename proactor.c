#include <stdio.h>
#include <liburing.h>
#include <netinet/in.h>
#include <string.h>
#include <unistd.h>
#include <stdlib.h>
#include "server.h"
#include "kvstore.h"

#define EVENT_ACCEPT    0
#define EVENT_READ      1
#define EVENT_WRITE     2

// io_uring 的 user_data 标识
struct conn_info {
    int fd;
    int event;
};

extern int kvs_protocol(char *msg, int length, char *response);

int p_init_server(unsigned short port) {    

    int sockfd = socket(AF_INET, SOCK_STREAM, 0);   
    struct sockaddr_in serveraddr;  
    memset(&serveraddr, 0, sizeof(struct sockaddr_in)); 
    serveraddr.sin_family = AF_INET;    
    serveraddr.sin_addr.s_addr = htonl(INADDR_ANY); 
    serveraddr.sin_port = htons(port);  

    int opt = 1;
    // 第一个参数填入刚才创建的 sockfd
    if (setsockopt(sockfd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt)) < 0) {
        perror("setsockopt");
        close(sockfd);
        return -1;
    }
    
    if (-1 == bind(sockfd, (struct sockaddr*)&serveraddr, sizeof(struct sockaddr))) {       
        perror("bind");     
        return -1;  
    }   

    listen(sockfd, 10);
    
    return sockfd;
}

#define ENTRIES_LENGTH      1024
#define CONNECTION_SIZE     1024


static struct conn conn_list[CONNECTION_SIZE]; 
static binary_msg_handler g_binary_handler = NULL;

// 辅助函数：清理断开的连接并释放内存
static void close_and_free_connection(int fd) {
    close(fd);
    if (conn_list[fd].rbuffer) {
        kvs_free(conn_list[fd].rbuffer);
    }
    if (conn_list[fd].wbuffer) {
        kvs_free(conn_list[fd].wbuffer);
    }
    memset(&conn_list[fd], 0, sizeof(struct conn));
}

int set_event_recv(struct io_uring *ring, int sockfd, int flags) {
    struct io_uring_sqe *sqe = io_uring_get_sqe(ring);

    struct conn_info accept_info = {
        .fd = sockfd,
        .event = EVENT_READ,
    };
    
    // 提交接收任务前，确保 rbuffer 有足够的剩余空间 (逻辑同步自 Reactor)
    if (conn_list[sockfd].rcapacity - conn_list[sockfd].rlength < 4096) {
        int new_capacity = conn_list[sockfd].rcapacity * 2;
        if (new_capacity < 4096) new_capacity = 4096;
        char *new_buf = (char *)kvs_realloc(conn_list[sockfd].rbuffer, new_capacity);
        if (!new_buf) {
            close_and_free_connection(sockfd);
            return -1;
        }
        conn_list[sockfd].rbuffer = new_buf;
        conn_list[sockfd].rcapacity = new_capacity;
    }

    int remaining_space = conn_list[sockfd].rcapacity - conn_list[sockfd].rlength;
    
    // 使用动态指针和计算出的剩余长度
    io_uring_prep_recv(sqe, sockfd, conn_list[sockfd].rbuffer + conn_list[sockfd].rlength, remaining_space, flags);
    memcpy(&sqe->user_data, &accept_info, sizeof(struct conn_info));
    return 0;
}

int set_event_send(struct io_uring *ring, int sockfd, int flags) {
    struct io_uring_sqe *sqe = io_uring_get_sqe(ring);

    struct conn_info accept_info = {
        .fd = sockfd,
        .event = EVENT_WRITE,
    };
    
    // 将整个 wbuffer 积攒的数据投递给内核
    io_uring_prep_send(sqe, sockfd, conn_list[sockfd].wbuffer, conn_list[sockfd].wlength, flags);
    memcpy(&sqe->user_data, &accept_info, sizeof(struct conn_info));
    return 0;
}

int set_event_accept(struct io_uring *ring, int sockfd, struct sockaddr *addr,
                    socklen_t *addrlen, int flags) {

    struct io_uring_sqe *sqe = io_uring_get_sqe(ring);

    struct conn_info accept_info = {
        .fd = sockfd,
        .event = EVENT_ACCEPT,
    };
    
    io_uring_prep_accept(sqe, sockfd, (struct sockaddr*)addr, addrlen, flags);
    memcpy(&sqe->user_data, &accept_info, sizeof(struct conn_info));
    return 0;
}


int proactor_start(unsigned short port, binary_msg_handler handler) {

    int sockfd = p_init_server(port);
    g_binary_handler = handler;

    struct io_uring_params params;
    memset(&params, 0, sizeof(params));

    struct io_uring ring;
    io_uring_queue_init_params(ENTRIES_LENGTH, &ring, &params);

    struct sockaddr_in clientaddr;  
    socklen_t len = sizeof(clientaddr);
    set_event_accept(&ring, sockfd, (struct sockaddr*)&clientaddr, &len, 0);

    while (1) {

        io_uring_submit(&ring);

        struct io_uring_cqe *cqes[128];
        // 此处修正 io_uring_wait_cqe 和 io_uring_peek_batch_cqe 配合逻辑
        struct io_uring_cqe *cqe_wait;
        io_uring_wait_cqe(&ring, &cqe_wait); 
        int nready = io_uring_peek_batch_cqe(&ring, cqes, 128);  // 类似 epoll_wait

        int i = 0;
        for (i = 0; i < nready; i++) {

            struct io_uring_cqe *entries = cqes[i];
            struct conn_info result;
            memcpy(&result, &entries->user_data, sizeof(struct conn_info));
            int fd = result.fd;

            if (result.event == EVENT_ACCEPT) {

                set_event_accept(&ring, sockfd, (struct sockaddr*)&clientaddr, &len, 0);

                int connfd = entries->res;
                if (connfd < 0) continue;

                if (connfd >= CONNECTION_SIZE) {
                    close(connfd);
                    continue;
                }

                // 像 Reactor 那样为新连接分配初始 4KB 内存
                conn_list[connfd].fd = connfd;
                conn_list[connfd].rcapacity = INIT_BUFFER_SIZE;
                conn_list[connfd].rbuffer = (char*)kvs_malloc(INIT_BUFFER_SIZE);
                conn_list[connfd].wcapacity = INIT_BUFFER_SIZE;
                conn_list[connfd].wbuffer = (char*)kvs_malloc(INIT_BUFFER_SIZE);
                conn_list[connfd].rlength = 0;
                conn_list[connfd].wlength = 0;

                if (!conn_list[connfd].rbuffer || !conn_list[connfd].wbuffer) {
                    close_and_free_connection(connfd);
                    continue;
                }

                set_event_recv(&ring, connfd, 0);

            } else if (result.event == EVENT_READ) {  

                int ret = entries->res;

                if (ret <= 0) { // 对方关闭或发生错误
                    close_and_free_connection(fd);
                    continue;
                } 
                
                // 收到数据，推进 rlength
                conn_list[fd].rlength += ret;

                // ========= 同步 Reactor 的状态机与粘包半包解析逻辑 =========
                
                while (conn_list[fd].rlength >= 4) { 
                    char *p = conn_list[fd].rbuffer;
                    int cmd_count = *(int*)p; // 命令总数

                    if (cmd_count <= 0 || cmd_count > 100) { 
                        conn_list[fd].rlength = 0; // 防御性重置
                        break; 
                    }

                    int total_batch_bytes = 4; // 统计包的字节
                    int is_all_received = 1;

                    for (int k = 0; k < cmd_count; k++) {
                        if (conn_list[fd].rlength < total_batch_bytes + 4) { is_all_received = 0; break; }
                        int cmd_len = *(int*)(p + total_batch_bytes);
                        
                        if (conn_list[fd].rlength < total_batch_bytes + 4 + cmd_len + 4) { is_all_received = 0; break; }
                        int key_len = *(int*)(p + total_batch_bytes + 4 + cmd_len);
                        
                        if (conn_list[fd].rlength < total_batch_bytes + 4 + cmd_len + 4 + key_len + 4) { is_all_received = 0; break; }
                        int value_len = *(int*)(p + total_batch_bytes + 4 + cmd_len + 4 + key_len);

                        total_batch_bytes += (4 + cmd_len + 4 + key_len + 4 + value_len);
                        if (conn_list[fd].rlength < total_batch_bytes) { is_all_received = 0; break; }
                    }

                    if (!is_all_received) {
                        break; // 等待下一次 recv 数据补齐
                    }

                    int p_offset = 4;
                    session_ctx_t ctx;
                    ctx.wbuffer = &conn_list[fd].wbuffer;
                    ctx.wcapacity = &conn_list[fd].wcapacity;
                    ctx.wlength = &conn_list[fd].wlength;

                    for (int k = 0; k < cmd_count; k++) {
                        int cmd_len   = *(int*)(p + p_offset);
                        int key_len   = *(int*)(p + p_offset + 4 + cmd_len);
                        int value_len = *(int*)(p + p_offset + 4 + cmd_len + 4 + key_len);
                        
                        int single_cmd_total_len = 4 + cmd_len + 4 + key_len + 4 + value_len;

                        if (g_binary_handler) {             
                            g_binary_handler(p + p_offset, single_cmd_total_len, &ctx);
                        }
                        p_offset += single_cmd_total_len;
                    }

                    int remaining_data = conn_list[fd].rlength - total_batch_bytes;
                    if (remaining_data > 0) {
                        memmove(conn_list[fd].rbuffer, conn_list[fd].rbuffer + total_batch_bytes, remaining_data);
                    }
                    conn_list[fd].rlength = remaining_data;
                    break;
                }

                // ========= 决定后续状态 (读 还是 写) =========
                if (conn_list[fd].wlength > 0) {
                    // 有需要回复给客户端的包，切换到 SEND
                    set_event_send(&ring, fd, 0);
                } else {
                    // 没有需要回写的包（例如拆包没收齐），继续挂起 RECV
                    set_event_recv(&ring, fd, 0);
                }

            }  else if (result.event == EVENT_WRITE) {

                int ret = entries->res;

                if (ret <= 0) {
                    close_and_free_connection(fd);
                    continue;
                }

                if (ret < conn_list[fd].wlength) {
                    // 没发完：将剩余数据前移，并再次挂载 SEND
                    int remaining = conn_list[fd].wlength - ret;
                    memmove(conn_list[fd].wbuffer, conn_list[fd].wbuffer + ret, remaining);
                    conn_list[fd].wlength = remaining;
                    set_event_send(&ring, fd, 0);
                } else {
                    // 全部发送完毕：重置长度，并按需缩容内存
                    conn_list[fd].wlength = 0;
                    
                    if (conn_list[fd].wcapacity > INIT_BUFFER_SIZE * 4) {
                        char *shrunk_buf = (char *)kvs_realloc(conn_list[fd].wbuffer, INIT_BUFFER_SIZE);
                        if (shrunk_buf) {
                            conn_list[fd].wbuffer = shrunk_buf;
                            conn_list[fd].wcapacity = INIT_BUFFER_SIZE;
                        }
                    }
                    
                    // 写完之后，自动切回 RECV，等待客户端下一次请求
                    set_event_recv(&ring, fd, 0);
                }
            }
        }

        io_uring_cq_advance(&ring, nready);
    }
    return 0;
}