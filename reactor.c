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

#define MAX_PACKET_SIZE 10 * 1024 * 1024
#define CONNECTION_SIZE 1024

static binary_msg_handler g_binary_handler = NULL;
static int epfd = 0;
static struct conn conn_list[CONNECTION_SIZE] = {0};
static struct timeval begin;



//1表示添加事件 0表示修改事件
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

// 清理连接并释放动态内存
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
    printf("\n======= Enter recv_cb for fd: %d =======\n", fd);
    
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

    while (conn_list[fd].rlength >= 4) { 
        char *p = conn_list[fd].rbuffer;
        int cmd_count_net;
        memcpy(&cmd_count_net, p, 4);
        int cmd_count = ntohl(cmd_count_net); 
        
        printf("[DEBUG-BUF] rlength=%d, rcapacity=%d\n", conn_list[fd].rlength, conn_list[fd].rcapacity);
        printf("Protocol Header Checked -> cmd_count parsed: %d\n", cmd_count);

        if (cmd_count <= 0 || cmd_count > MAX_PACKET_SIZE) { 
            printf("[DEBUG-ERR] Invalid cmd_count=%d\n", cmd_count);
            conn_list[fd].rlength = 0; 
            break; 
        }

        int total_batch_bytes = 4; 
        int is_all_received = 1;

        for (int i = 0; i < cmd_count; i++) {
            printf("[DEBUG-VERIFY] Loop i=%d, current total_batch_bytes=%d\n", i, total_batch_bytes);
            if (conn_list[fd].rlength < total_batch_bytes + 4) {
                printf("[DEBUG-VERIFY] Incomplete: rlength < total_batch_bytes + 4\n");
                is_all_received = 0; break; 
            }
            int cmd_len_net;
            memcpy(&cmd_len_net, p + total_batch_bytes, 4);
            int cmd_len = ntohl(cmd_len_net);
            printf("[DEBUG-VERIFY] Loop i=%d, parsed cmd_len=%d\n", i, cmd_len);
            
            if (conn_list[fd].rlength < total_batch_bytes + 4 + cmd_len + 4) { 
                printf("[DEBUG-VERIFY] Incomplete: rlength < fixed header\n");
                is_all_received = 0; break; 
            }
            int key_len_net;
            memcpy(&key_len_net, p + total_batch_bytes + 4 + cmd_len, 4);
            int key_len = ntohl(key_len_net);
            printf("[DEBUG-VERIFY] Loop i=%d, parsed key_len=%d\n", i, key_len);
            
            if (conn_list[fd].rlength < total_batch_bytes + 4 + cmd_len + 4 + key_len + 4) { 
                printf("[DEBUG-VERIFY] Incomplete: rlength < value_len_header\n");
                is_all_received = 0; break; 
            }
            int value_len_net;
            memcpy(&value_len_net, p + total_batch_bytes + 4 + cmd_len + 4 + key_len, 4);
            int value_len = ntohl(value_len_net);
            printf("[DEBUG-VERIFY] Loop i=%d, parsed value_len=%d\n", i, value_len);
            
            total_batch_bytes += (4 + cmd_len + 4 + key_len + 4 + value_len);
            printf("[DEBUG-VERIFY] Loop i=%d, new total_batch_bytes=%d\n", i, total_batch_bytes);
            
            if (conn_list[fd].rlength < total_batch_bytes) { 
                printf("[DEBUG-VERIFY] Incomplete: rlength < total_batch_bytes\n");
                is_all_received = 0; break; 
            }
        }

        if (!is_all_received) {
            printf("Batch packet NOT fully received yet. Waiting for next EPOLLIN.\n");
            break; 
        }

        int p_offset = 4; // 跳过前4字节的 cmd_count 头部

        // 1. 在栈上或堆上开辟一个响应收集箱（批量命令，动态分配最安全）
        kvs_resp_t *resps = (kvs_resp_t *)kvs_malloc(sizeof(kvs_resp_t) * cmd_count);
        if (resps == NULL) {
            return;// 严重的内存分配失败处理
        }

        // 2. 循环解析网络层收到的多条命令
        for (int i = 0; i < cmd_count; i++) {
            printf("[DEBUG-CONSUME] Loop i=%d, current p_offset=%d\n", i, p_offset);
            int cmd_len_net, key_len_net, value_len_net;
    
            // 仅提取网络序长度
            memcpy(&cmd_len_net, p + p_offset, 4);
            int cmd_len = ntohl(cmd_len_net);
    
            memcpy(&key_len_net, p + p_offset + 4 + cmd_len, 4);
            int key_len = ntohl(key_len_net);
    
            memcpy(&value_len_net, p + p_offset + 4 + cmd_len + 4 + key_len, 4);
            int value_len = ntohl(value_len_net);
    
            int single_cmd_total_len = 4 + cmd_len + 4 + key_len + 4 + value_len;
            printf("[DEBUG-CONSUME] Loop i=%d, cmd_len=%d, key_len=%d, value_len=%d, single_total_len=%d\n", 
           i, cmd_len, key_len, value_len, single_cmd_total_len);
    
            // 假设业务层接口调整为：传入当前命令的首地址、总长度，返回标准响应结构体
            if (g_binary_handler) {            
                printf("[DEBUG-CONSUME] Before calling g_binary_handler for loop i=%d\n", i);
        
                // 业务层内部去解析数据并执行业务，返回结果填入收集箱
                g_binary_handler(p + p_offset, single_cmd_total_len, &resps[i]);
        
                printf("[DEBUG-CONSUME] After calling g_binary_handler for loop i=%d\n", i);
            } else {
            resps[i].status = KVS_RESP_UNKNOWN;
            resps[i].body = NULL;
            resps[i].body_len = 0;
            }
    
            p_offset += single_cmd_total_len;
        }

        // 3. 循环结束，将所有命令的回复交给打包层，统一进行“一次性扩容”与“合并打包”
        // 传入网络层缓冲区的指针，让打包层内部去控扩容
        packet_build_batch(resps, cmd_count, &conn_list[fd].wbuffer, &conn_list[fd].wcapacity, &conn_list[fd].wlength);

        // 4. 善后工作：释放业务层因为 GET_OK 在堆上申请的 body 副本，以及临时收集箱
        for (int i = 0; i < cmd_count; i++) {
            if (resps[i].status == KVS_RESP_GET_OK && resps[i].body != NULL) {
                kvs_free(resps[i].body); // 对应业务层 get 成功时 malloc 的副本
            }
        }
        kvs_free(resps); // 释放响应箱本身

        set_event(fd, EPOLLOUT, 0); 

        int remaining_data = conn_list[fd].rlength - total_batch_bytes;
        printf("[DEBUG-END] remaining_data=%d, total_batch_bytes=%d\n", remaining_data, total_batch_bytes);
        if (remaining_data > 0) {
            memmove(conn_list[fd].rbuffer, conn_list[fd].rbuffer + total_batch_bytes, remaining_data);
        }
        conn_list[fd].rlength = remaining_data;

        printf("Buffer advanced. Remaining unparsed raw data bytes: %d\n", conn_list[fd].rlength);
    }
    
    printf("======= Exit recv_cb for fd: %d =======\n", fd);
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