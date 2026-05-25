#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <string.h>
#include <unistd.h>
#include <sys/epoll.h>
#include <sys/time.h>
#include <arpa/inet.h>
#include "server.h"
#include "kvstore.h"

#define CONNECTION_SIZE 1024
#define TIME_SUB_MS(tv1, tv2) ((tv1.tv_sec - tv2.tv_sec) * 1000 + (tv1.tv_usec - tv2.tv_usec) / 1000)

static binary_msg_handler g_binary_handler = NULL;
static int epfd = 0;
static struct conn conn_list[CONNECTION_SIZE] = {0};
static struct timeval begin;

int set_event(int fd, int event, int flag) {
    struct epoll_event ev;
    ev.events = event;
    ev.data.fd = fd;
    if (flag) {
        epoll_ctl(epfd, EPOLL_CTL_ADD, fd, &ev);
    } else {
        epoll_ctl(epfd, EPOLL_CTL_MOD, fd, &ev);
    }
    return 0;
}

// 清理连接并释放动态内存的辅助函数
static void close_and_free_connection(int fd) {
    close(fd);
    epoll_ctl(epfd, EPOLL_CTL_DEL, fd, NULL);
    
    if (conn_list[fd].rbuffer) {
        kvs_free(conn_list[fd].rbuffer);
    }
    if (conn_list[fd].wbuffer) {
        kvs_free(conn_list[fd].wbuffer);
    }
    
    memset(&conn_list[fd], 0, sizeof(struct conn));
}

void recv_cb(int fd) {
    printf("\n[Reactor-Recv] ======= Enter recv_cb for fd: %d =======\n", fd);
    
    // 1. 抽干内核缓冲区（非阻塞读取直到 EAGAIN）
    while (1) {
        if (conn_list[fd].rcapacity - conn_list[fd].rlength < 4096) {
            int new_capacity = conn_list[fd].rcapacity * 2;
            if (new_capacity < 4096) new_capacity = 4096;
            char *new_buf = (char *)kvs_realloc(conn_list[fd].rbuffer, new_capacity);
            if (!new_buf) {
                perror("kvs_realloc rbuffer failed");
                close_and_free_connection(fd);
                return;
            }
            conn_list[fd].rbuffer = new_buf;
            conn_list[fd].rcapacity = new_capacity;
        }

        int remaining_space = conn_list[fd].rcapacity - conn_list[fd].rlength;
        int count = recv(fd, conn_list[fd].rbuffer + conn_list[fd].rlength, remaining_space, MSG_DONTWAIT);   
        
        if (count < 0) {
            if (errno == EAGAIN || errno == EWOULDBLOCK) break;
            perror("recv error");
            close_and_free_connection(fd);
            return;
        }    
        if (count == 0) {
            close_and_free_connection(fd);
            return;
        }    
        conn_list[fd].rlength += count;
    }

    // 2. 状态机解析循环：[ 4字节 Count ] + [ 命令1 ] + [ 命令2 ] + ...
    // 核心修改：移除末尾无条件 break，允许 while 循环连续解析 rbuffer 中残留的多个独立大包
    while (conn_list[fd].rlength >= 4) { 
        char *p = conn_list[fd].rbuffer;
        int cmd_count = *(int*)p; // 当前网络包声明的命令总数
        
        printf("[Reactor-Recv] Protocol Header Checked -> cmd_count parsed: %d\n", cmd_count);

        if (cmd_count <= 0 || cmd_count > 100) { 
            printf("[Reactor-Recv] ALERT! Invalid cmd_count (%d). Dropping buffer and aborting packet.\n", cmd_count);
            conn_list[fd].rlength = 0; // 防御性重置
            break; 
        }

        int total_batch_bytes = 4; // 统计当前完整大包的预期总字节数
        int is_all_received = 1;

        // 验证当前这个包里的所有命令是否在 rbuffer 里全部收齐了
        for (int i = 0; i < cmd_count; i++) {
            // 如果连单条命令的 cmd_len (4字节) 都没收齐，说明当前大包没接收全
            if (conn_list[fd].rlength < total_batch_bytes + 4) { 
                printf("[Reactor-Recv] Loop-%d: Not enough bytes for cmd_len\n", i);
                is_all_received = 0; break; 
            }
            int cmd_len = *(int*)(p + total_batch_bytes);
            
            // 检查固定报头偏移是否收齐 [cmd_len(4)] + [cmd] + [key_len(4)]
            if (conn_list[fd].rlength < total_batch_bytes + 4 + cmd_len + 4) { 
                printf("[Reactor-Recv] Loop-%d: Not enough bytes for fixed header (cmd_len:%d)\n", i, cmd_len);
                is_all_received = 0; break; 
            }
            int key_len = *(int*)(p + total_batch_bytes + 4 + cmd_len);
            
            // 检查 value_len(4) 是否收齐
            if (conn_list[fd].rlength < total_batch_bytes + 4 + cmd_len + 4 + key_len + 4) { 
                printf("[Reactor-Recv] Loop-%d: Not enough bytes for value_len\n", i);
                is_all_received = 0; break; 
            }
            int value_len = *(int*)(p + total_batch_bytes + 4 + cmd_len + 4 + key_len);

            // 累加单条命令的总字节数
            total_batch_bytes += (4 + cmd_len + 4 + key_len + 4 + value_len);
            
            // 如果超出了当前接收到的总长度，说明后面还有命令数据没收全
            if (conn_list[fd].rlength < total_batch_bytes) { 
                printf("[Reactor-Recv] Packet incomplete: rlength (%d) < total_batch_bytes (%d)\n", conn_list[fd].rlength, total_batch_bytes);
                is_all_received = 0; break; 
            }
        }

        // 如果整批命令没有百分之百收全，跳出 while 循环，静静等待下一次 EPOLLIN 事件触发再读
        if (!is_all_received) {
            printf("[Reactor-Recv] Batch packet NOT fully received yet. Waiting for next EPOLLIN.\n");
            break; 
        }

        // 走到这里，说明当前 cmd_count 所代表的整批命令已经全部完整收齐，开始逐条消费
        int p_offset = 4; // 跳过 4 字节的 cmd_count 头

        // 准备业务上下文
        session_ctx_t ctx;
        ctx.wbuffer = &conn_list[fd].wbuffer;
        ctx.wcapacity = &conn_list[fd].wcapacity;
        ctx.wlength = &conn_list[fd].wlength;

        for (int i = 0; i < cmd_count; i++) {
            int cmd_len   = *(int*)(p + p_offset);
            int key_len   = *(int*)(p + p_offset + 4 + cmd_len);
            int value_len = *(int*)(p + p_offset + 4 + cmd_len + 4 + key_len);
            
            int single_cmd_total_len = 4 + cmd_len + 4 + key_len + 4 + value_len;
            
            if (g_binary_handler) {            
                // 传入当前命令的起始指针和其绝对安全的长度
                g_binary_handler(p + p_offset, single_cmd_total_len, &ctx);
            }
            
            // 精准推进偏移量，准备提取下一条命令
            p_offset += single_cmd_total_len;
        }

        // 统一挂载写事件，回复客户端
        set_event(fd, EPOLLOUT, 0);

        // 移除已经处理完的整批大包数据
        int remaining_data = conn_list[fd].rlength - total_batch_bytes;
        if (remaining_data > 0) {
            memmove(conn_list[fd].rbuffer, conn_list[fd].rbuffer + total_batch_bytes, remaining_data);
        }
        conn_list[fd].rlength = remaining_data;
        printf("[Reactor-Recv] Buffer advanced. Remaining unparsed raw data bytes: %d\n", conn_list[fd].rlength);
            
        // 【重要修改】移除了无条件的 break; 
        // 增加防御性判断：如果本轮循环没有消耗任何数据（防止异常数据导致的死循环），才退出
        if (total_batch_bytes == 0) {
            break;
        }
    }
    
    printf("[Reactor-Recv] ======= Exit recv_cb for fd: %d =======\n", fd);
}

void send_cb(int fd) {
    //printf("\n[Reactor-Send] ======= Enter send_cb for fd: %d, wlength to send: %d =======\n", fd, conn_list[fd].wlength);
    // 如果没有数据需要发送，直接切回读事件
    if (conn_list[fd].wlength == 0) {
        //printf("[Reactor-Send] wlength is 0. Direct switching back to EPOLLIN.\n");
        set_event(fd, EPOLLIN, 0); // 这里的0代表触发内核修改事件
        //fflush(stdout);
        return;
    }

    int count = send(fd, conn_list[fd].wbuffer, conn_list[fd].wlength, MSG_DONTWAIT);
    //printf("[Reactor-Send] Kernel send() returned: %d bytes\n", count);
    
    if (count < 0) {
        // 如果 TCP 窗口满了，直接返回，等待下一次内核可写通知（保持 EPOLLOUT）
        if (errno == EAGAIN || errno == EWOULDBLOCK) {
            //printf("[Reactor-Send] EAGAIN/EWOULDBLOCK hit. TCP window full. Staying in EPOLLOUT.\n");
            //fflush(stdout);
            return;
        }
        // 真正的网络错误，关闭连接?
        perror("send error");
        close_and_free_connection(fd);
        return;
    }
    
    // 处理数据未完全发完的情况（分包发送）
    if (count < conn_list[fd].wlength) {
        int remaining = conn_list[fd].wlength - count;
        memmove(conn_list[fd].wbuffer, conn_list[fd].wbuffer + count, remaining);
        conn_list[fd].wlength = remaining;
        //printf("[Reactor-Send] Partial send occurred. Remainder: %d bytes. Staying in EPOLLOUT.\n", remaining);
        //fflush(stdout);
        // 保持 EPOLLOUT 事件，退出等待下一次回调
        return; 
    }
    
    // 确定全部发完后，才能进行缓冲区清空与安全缩容
    conn_list[fd].wlength = 0;
    
    // 如果刚才因为批处理下载大文件导致写缓冲区扩得很大，发送完毕后安全释放内存
    if (conn_list[fd].wcapacity > INIT_BUFFER_SIZE * 4) {
        char *shrunk_buf = (char *)kvs_realloc(conn_list[fd].wbuffer, INIT_BUFFER_SIZE);
        if (shrunk_buf) {
            conn_list[fd].wbuffer = shrunk_buf;
            conn_list[fd].wcapacity = INIT_BUFFER_SIZE;
            //printf("[Reactor-Send] Buffer memory dynamically shrunk.\n");
        }
    }

    //printf("[Reactor-Send] Batch delivery completed. Switching back to EPOLLIN to wait for next client pipeline.\n");
    // 这一批任务彻底交卷，转为监听读事件，等待客户端的下一批 Pipeline
    set_event(fd, EPOLLIN, 0); 
    //printf("[Reactor-Send] ======= Exit send_cb for fd: %d =======\n", fd);
    //fflush(stdout);
}

void accept_cb(int fd) {
    struct sockaddr_in clientaddr;
    socklen_t len = sizeof(clientaddr);
    int clientfd = accept(fd, (struct sockaddr*)&clientaddr, &len);
    
    if (clientfd < 0) return;
    
    if (clientfd >= CONNECTION_SIZE) {// 限制在连接池容量内
        close(clientfd);
        return;
    }
    
    conn_list[clientfd].fd = clientfd;
    conn_list[clientfd].send_callback = send_cb;
    conn_list[clientfd].read_callback = recv_cb;
    conn_list[clientfd].accept_callback = NULL;
    
    // 核心初始化：为每个新连接独立分配 4KB 的初始内存
    conn_list[clientfd].rcapacity = INIT_BUFFER_SIZE;
    conn_list[clientfd].rbuffer = (char*)kvs_malloc(INIT_BUFFER_SIZE);
    
    conn_list[clientfd].wcapacity = INIT_BUFFER_SIZE;
    conn_list[clientfd].wbuffer = (char*)kvs_malloc(INIT_BUFFER_SIZE);
    
    conn_list[clientfd].rlength = 0;
    conn_list[clientfd].wlength = 0;
    
    if (!conn_list[clientfd].rbuffer || !conn_list[clientfd].wbuffer) {
        close_and_free_connection(clientfd);
        return;
    }
    
    set_event(clientfd, EPOLLIN, 1);
    
    if ((clientfd % 1000) == 0) {
        struct timeval current;
        gettimeofday(&current, NULL);
        memcpy(&begin, &current, sizeof(struct timeval));
    }
}

int init_listen_socket(unsigned short port) {
    int sockfd = socket(AF_INET, SOCK_STREAM, 0);
    if (sockfd < 0) return -1;
    
    int opt = 1;
    setsockopt(sockfd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));
    
    struct sockaddr_in servaddr;
    memset(&servaddr, 0, sizeof(servaddr));
    servaddr.sin_family = AF_INET;
    servaddr.sin_addr.s_addr = htonl(INADDR_ANY);
    servaddr.sin_port = htons(port);
    
    if (-1 == bind(sockfd, (struct sockaddr*)&servaddr, sizeof(servaddr))) {
        close(sockfd);
        return -1;
    }
    
    if (-1 == listen(sockfd, 10)) {
        close(sockfd);
        return -1;
    }
    
    return sockfd;
}

int reactor_start(unsigned short port, binary_msg_handler handler) {
    g_binary_handler = handler;
    
    epfd = epoll_create(1);
    if (epfd < 0) return -1;
    
    int listen_fd = init_listen_socket(port);
    if (listen_fd < 0) return -1;
   
    conn_list[listen_fd].fd = listen_fd;
    conn_list[listen_fd].accept_callback = accept_cb;
    conn_list[listen_fd].read_callback = NULL;  
    conn_list[listen_fd].send_callback = NULL;
    
    // 监听套接字不需要分配读写 rbuffer/wbuffer
    conn_list[listen_fd].rbuffer = NULL;
    conn_list[listen_fd].wbuffer = NULL;
    
    set_event(listen_fd, EPOLLIN, 1);
    
    gettimeofday(&begin, NULL);
    
    while (1) {
        struct epoll_event events[1024] = {0};
        int nready = epoll_wait(epfd, events, 1024, -1);
        
        for (int i = 0; i < nready; i++) {
            int connfd = events[i].data.fd;
            
            if (events[i].events & EPOLLIN) {
                if (conn_list[connfd].read_callback) {
                    conn_list[connfd].read_callback(connfd);
                } else if (conn_list[connfd].accept_callback) {
                    conn_list[connfd].accept_callback(connfd);
                }
            }
            
            if (events[i].events & EPOLLOUT) {
                if (conn_list[connfd].send_callback) {
                    conn_list[connfd].send_callback(connfd);
                }
            }
        }
    }
    
    return 0;
}