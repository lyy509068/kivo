#include <stdio.h>
#include <liburing.h>
#include <netinet/in.h>
#include <string.h>
#include <unistd.h>
#include <stdlib.h>
#include <errno.h>
#include "server.h"
#include "kvstore.h"

#define EVENT_ACCEPT    0
#define EVENT_READ      1
#define EVENT_WRITE     2

// 利用位移将 fd 和 event 打包进 一个 64 位的 user_data，避开不安全的栈内存指针
// 高 32 位存 event，低 32 位存 fd
static inline uint64_t encode_user_data(int fd, int event) {
    return ((uint64_t)event << 32) | (uint32_t)fd;
}

static inline void decode_user_data(uint64_t user_data, int *fd, int *event) {
    *fd = (int)(user_data & 0xFFFFFFFF);
    *event = (int)(user_data >> 32);
}

extern int kvs_protocol(char *msg, int length, char *response);

int p_init_server(unsigned short port) {    
    int sockfd = socket(AF_INET, SOCK_STREAM, 0);   
    struct sockaddr_in serveraddr;  
    memset(&serveraddr, 0, sizeof(struct sockaddr_in)); 
    serveraddr.sin_family = AF_INET;    
    serveraddr.sin_addr.s_addr = htonl(INADDR_ANY); 
    serveraddr.sin_port = htons(port);  

    int opt = 1;
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
    if (!sqe) return -1;
    
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
    
    io_uring_prep_recv(sqe, sockfd, conn_list[sockfd].rbuffer + conn_list[sockfd].rlength, remaining_space, flags);
    sqe->user_data = encode_user_data(sockfd, EVENT_READ);
    return 0;
}

int set_event_send(struct io_uring *ring, int sockfd, int flags) {
    struct io_uring_sqe *sqe = io_uring_get_sqe(ring);
    if (!sqe) return -1;
    
    io_uring_prep_send(sqe, sockfd, conn_list[sockfd].wbuffer, conn_list[sockfd].wlength, flags);
    sqe->user_data = encode_user_data(sockfd, EVENT_WRITE);
    return 0;
}

int set_event_accept(struct io_uring *ring, int sockfd, struct sockaddr *addr,
                    socklen_t *addrlen, int flags) {
    struct io_uring_sqe *sqe = io_uring_get_sqe(ring);
    if (!sqe) return -1;
    
    io_uring_prep_accept(sqe, sockfd, (struct sockaddr*)addr, addrlen, flags);
    sqe->user_data = encode_user_data(sockfd, EVENT_ACCEPT);
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
        // 提交环形队列中新挂载的 SQE
        io_uring_submit(&ring);

        struct io_uring_cqe *cqe;
        // 1. 修正：阻塞等待至少一个 CQE 就绪
        int ret = io_uring_wait_cqe(&ring, &cqe);
        if (ret < 0) {
            if (ret == -EINTR) continue;
            break;
        }

        // 2. 修正：利用安全批量获取，一次性收割所有就绪事件
        struct io_uring_cqe *cqes[128];
        int nready = io_uring_peek_batch_cqe(&ring, cqes, 128);

        for (int i = 0; i < nready; i++) {
            struct io_uring_cqe *entries = cqes[i];
            int fd, event;
            // 完美解析出当前触发的目标文件描述符和事件类型
            decode_user_data(entries->user_data, &fd, &event);

            if (event == EVENT_ACCEPT) {
                // 重新挂起 Accept
                set_event_accept(&ring, sockfd, (struct sockaddr*)&clientaddr, &len, 0);

                int connfd = entries->res;
                if (connfd < 0) continue;
                if (connfd >= CONNECTION_SIZE) { close(connfd); continue; }

                conn_list[connfd].fd = connfd;
                conn_list[connfd].rcapacity = INIT_BUFFER_SIZE;
                conn_list[connfd].rbuffer = (char*)kvs_malloc(INIT_BUFFER_SIZE);
                conn_list[connfd].wcapacity = INIT_BUFFER_SIZE;
                conn_list[connfd].wbuffer = (char*)kvs_malloc(INIT_BUFFER_SIZE);
                conn_list[connfd].rlength = 0;
                conn_list[connfd].wlength = 0;

                if (!conn_list[connfd].rbuffer || !conn_list[connfd].wbuffer) {
                    close_and_free_connection(connfd); continue;
                }
                set_event_recv(&ring, connfd, 0);

            } else if (event == EVENT_READ) {  
                int ret_bytes = entries->res;
                if (ret_bytes <= 0) { 
                    close_and_free_connection(fd); continue;
                } 
                
                conn_list[fd].rlength += ret_bytes;

                // ========= 核心状态机循环：连续解析多个独立包 =========
                while (conn_list[fd].rlength >= 4) { 
                    char *p = conn_list[fd].rbuffer;
                    int cmd_count = *(int*)p; 

                    if (cmd_count <= 0 || cmd_count > 100) { 
                        conn_list[fd].rlength = 0; 
                        break; 
                    }

                    int total_batch_bytes = 4; 
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
                        break; // 字节不够，退出当前包的解析，跳出以继续调用 set_event_recv 补充数据
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
                    
                    // 【关键修复】：这里不能盲目使用 break 终止循环！
                    // 如果本大包字节数消费为 0（异常防御），才退出，否则应当继续 while 转下圈去处理余下的粘包数据
                    if (total_batch_bytes == 0) {
                        break;
                    }
                }

                // 根据响应缓冲区数据，投递下一个 io_uring 异步动作
                if (conn_list[fd].wlength > 0) {
                    set_event_send(&ring, fd, 0);
                } else {
                    set_event_recv(&ring, fd, 0);
                }

            } else if (event == EVENT_WRITE) {
                int ret_send = entries->res;
                if (ret_send <= 0) {
                    close_and_free_connection(fd); continue;
                }

                if (ret_send < conn_list[fd].wlength) {
                    int remaining = conn_list[fd].wlength - ret_send;
                    memmove(conn_list[fd].wbuffer, conn_list[fd].wbuffer + ret_send, remaining);
                    conn_list[fd].wlength = remaining;
                    set_event_send(&ring, fd, 0);
                } else {
                    conn_list[fd].wlength = 0;
                    if (conn_list[fd].wcapacity > INIT_BUFFER_SIZE * 4) {
                        char *shrunk_buf = (char *)kvs_realloc(conn_list[fd].wbuffer, INIT_BUFFER_SIZE);
                        if (shrunk_buf) {
                            conn_list[fd].wbuffer = shrunk_buf;
                            conn_list[fd].wcapacity = INIT_BUFFER_SIZE;
                        }
                    }
                    // 写完了回复，异步 Proactor 切回读事件监听
                    set_event_recv(&ring, fd, 0);
                }
            }
        }
        // 处理完这批 CQE 后，统一批量推进完成队列的头部
        io_uring_cq_advance(&ring, nready);
    }
    return 0;
}