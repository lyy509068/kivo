#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif

#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <sys/epoll.h>
#include <sys/time.h>
#include <arpa/inet.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <sys/ioctl.h>
#include <linux/sockios.h>

#include <infiniband/verbs.h> 
#include <sys/socket.h>
#include <netinet/in.h>
#include <netinet/tcp.h>

#include "network.h"
#include "kvstore.h"
#include "resp.h"
#include "repl.h" 
#include "rdma.h"
#include "ebpf.h"

static stream_handler_t g_stream_handler = NULL;
static int epfd = 0;
struct conn conn_list[CONNECTION_SIZE] = {0};
static struct timeval begin;

int reactor_set_event(int fd, int event, int flag) {
    struct epoll_event ev;
    if (flag) {
        ev.events = event;
        ev.data.fd = fd;
        epoll_ctl(epfd, EPOLL_CTL_ADD, fd, &ev);
    } else {
        if (conn_list[fd].role == CONN_MASTER || (event & EPOLLOUT)) {
            ev.events = event | EPOLLIN; 
        } else {
            ev.events = event;
        }
        ev.data.fd = fd;
        epoll_ctl(epfd, EPOLL_CTL_MOD, fd, &ev);
    }
    return 0;
}

void close_and_free_connection(int fd) {
    close(fd);
    epoll_ctl(epfd, EPOLL_CTL_DEL, fd, NULL);
    if (conn_list[fd].rbuffer) kvs_free(conn_list[fd].rbuffer);
    if (conn_list[fd].wbuffer) kvs_free(conn_list[fd].wbuffer);
    memset(&conn_list[fd], 0, sizeof(struct conn));
}


void recv_cb(int fd) {
    struct conn *c = &conn_list[fd];
    
    // 等待 RDMA 接收日志完成后，加载日志，回复"SYCN_DOWN"
    #if ENABLE_REPLICATION_SLAVE
    if (c->is_receiving_file) {
        if (rdma_check_transfer_complete(c) == 0) { 
            c->is_receiving_file = 0; // 退出全量模式                       
            kvs_persistence_recover();
            printf("[Reactor] Slave memory database successfully reloaded via RDMA bypass channel.\n");
            
            const char *sync_done = "*2\r\n$9\r\nSYNC_DONE\r\n$1\r\n1\r\n";
            int len = strlen(sync_done);
            int sent = send(fd, sync_done, len, 0);
            if (sent == len) {
                printf("[Reactor] SYNC_DONE sent to master via TCP.\n");
            } else {
                printf("[Reactor Error] Failed to send SYNC_DONE!\n");
            }

            // 挂载监听，接收主端 eBPF 转发过来的增量写命令或者客户端命令
            reactor_set_event(fd, EPOLLIN, 0);
        }
        return; 
    }
    #endif

    // 收到 RESP 命令
    int total_new_bytes = 0; 
    while (1) {
        if (c->rcapacity - c->rlength < 4096) {
            int new_capacity = c->rcapacity * 2;
            if (new_capacity < 4096) new_capacity = 4096;
            char *new_buf = (char *)kvs_realloc(c->rbuffer, new_capacity);
            if (!new_buf) { close_and_free_connection(fd); return; }
            c->rbuffer = new_buf;
            c->rcapacity = new_capacity;
        }
        
        int remaining_space = c->rcapacity - c->rlength;
        int count = recv(fd, c->rbuffer + c->rlength, remaining_space, MSG_DONTWAIT);   
        
        if (count < 0) {
            if (errno == EAGAIN || errno == EWOULDBLOCK) break;
            close_and_free_connection(fd); return;
        }    
        if (count == 0) { close_and_free_connection(fd); return; }    
        c->rlength += count;
        total_new_bytes += count; 
    }

    if (total_new_bytes == 0 && c->rlength == 0) return;
    if (!g_stream_handler) return;


    int total_parsed_bytes = 0; 
    while (c->rlength > total_parsed_bytes) { 
        int parsed_bytes = 0;
        long long expect_file_size = 0; 
        int is_master = (c->role == CONN_MASTER); 
        
        int status = g_stream_handler(
            c->rbuffer + total_parsed_bytes, 
            c->rlength - total_parsed_bytes, 
            &parsed_bytes,                                                                                                                                                                             
            is_master ? NULL : &c->wbuffer,   
            is_master ? NULL : &c->wcapacity,
            is_master ? NULL : &c->wlength, 
            &expect_file_size,
            fd 
        );

        if (status == 1) {
            break; // 半包等待
        } 
        else if (status < 0) {
            if (c->role == CONN_MASTER) c->wlength = 0;
            close_and_free_connection(fd); return;
        }
        
        #if ENABLE_REPLICATION_SLAVE
        // 从端收到同步回应，激活从端 RDMA 硬件接收锁
        else if (status == 20) {
            total_parsed_bytes += parsed_bytes;
            c->expect_file_size = expect_file_size;
            c->is_receiving_file = 1; // 下次进入加载日志模式
            
            c->rlength = 0; 
            total_parsed_bytes = 0; 
            break; 
        }
        #endif 

        total_parsed_bytes += parsed_bytes;
    } 

    if (!c->is_receiving_file && total_parsed_bytes > 0) {
        int remaining_data = c->rlength - total_parsed_bytes;
        if (remaining_data > 0) {
            memmove(c->rbuffer, c->rbuffer + total_parsed_bytes, remaining_data);
        }
        c->rlength = remaining_data;
        
        if (c->role == CONN_CLIENT) {
            if (c->wlength > 0) { reactor_set_event(fd, EPOLLOUT, 0); }
        }
        #if ENABLE_REPLICATION_SLAVE 
        else if (c->role == CONN_MASTER) {
            c->wlength = 0; // 对于主端发来的增量命令，强制截断其写事件
        }
        #endif
    }
}

void send_cb(int fd) { 
    if (conn_list[fd].wlength > 0) {
        int count = send(fd, conn_list[fd].wbuffer, conn_list[fd].wlength, MSG_DONTWAIT);
        if (count < 0) {
            if (errno == EAGAIN || errno == EWOULDBLOCK) return;
            close_and_free_connection(fd); return;
        }
        if (count < conn_list[fd].wlength) {
            int remaining = conn_list[fd].wlength - count;
            memmove(conn_list[fd].wbuffer, conn_list[fd].wbuffer + count, remaining);
            conn_list[fd].wlength = remaining;
            return; 
        }
        conn_list[fd].wlength = 0;
    }
    reactor_set_event(fd, EPOLLIN, 0); 
}

void accept_cb(int fd) {
    struct sockaddr_in clientaddr;
    socklen_t len = sizeof(clientaddr);
    int clientfd = accept(fd, (struct sockaddr*)&clientaddr, &len);
    
    if (clientfd < 0) return;
    if (clientfd >= CONNECTION_SIZE) { close(clientfd); return; }

    conn_list[clientfd].fd = clientfd;
    conn_list[clientfd].send_callback = send_cb;
    conn_list[clientfd].read_callback = recv_cb;
    conn_list[clientfd].accept_callback = NULL;
    conn_list[clientfd].role = CONN_CLIENT; 
        
    conn_list[clientfd].rcapacity = INIT_BUFFER_SIZE;
    conn_list[clientfd].rbuffer = (char*)kvs_malloc(INIT_BUFFER_SIZE);
    conn_list[clientfd].wcapacity = INIT_BUFFER_SIZE;
    conn_list[clientfd].wbuffer = (char*)kvs_malloc(INIT_BUFFER_SIZE);
    conn_list[clientfd].rlength = 0;
    conn_list[clientfd].wlength = 0;
    conn_list[clientfd].is_receiving_file = 0;
    
    if (!conn_list[clientfd].rbuffer || !conn_list[clientfd].wbuffer) {
        close_and_free_connection(clientfd); return;
    }
    
    reactor_set_event(clientfd, EPOLLIN, 1);
}

static int init_listen_socket(unsigned short port) {
    int sockfd = socket(AF_INET, SOCK_STREAM, 0);
    if (sockfd < 0) return -1;
    int opt = 1;
    setsockopt(sockfd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));
    
    struct sockaddr_in servaddr;
    memset(&servaddr, 0, sizeof(servaddr));
    servaddr.sin_family = AF_INET;
    servaddr.sin_addr.s_addr = htonl(INADDR_ANY);
    servaddr.sin_port = htons(port);

    if (-1 == bind(sockfd, (struct sockaddr*)&servaddr, sizeof(servaddr))) { close(sockfd); return -1; }
    if (-1 == listen(sockfd, 10)) { close(sockfd); return -1; }
    return sockfd;
}

int reactor_start(unsigned short port, stream_handler_t handler) {
    g_stream_handler = handler;
    epfd = epoll_create(1);
    if (epfd < 0) return -1;
    
    int listen_fd = init_listen_socket(port);
    if (listen_fd < 0) return -1;
   
    conn_list[listen_fd].fd = listen_fd;
    conn_list[listen_fd].accept_callback = accept_cb;
    reactor_set_event(listen_fd, EPOLLIN, 1);
    
    gettimeofday(&begin, NULL);

    #if ENABLE_REPLICATION_SLAVE
        const char *master_ip = "192.168.92.128";
        unsigned short master_port = 2000;
        //连接主端
        int master_fd = repl_connect_to_master(master_ip, master_port); 
        
        if (master_fd < 0) {
            fprintf(stderr, "[Reactor Error] Slave failed to establish replication link.\n");
        }

    #endif
    
    while (1) {
        struct epoll_event events[1024] = {0};
        int nready = epoll_wait(epfd, events, 1024, 10);
        
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

// 托管长连接对象，确保从端的正常运行
struct conn* reactor_host_slave_connection(int fd, char *wbuf, int wcap, int wlen) {
    if (fd < 0 || fd >= CONNECTION_SIZE) return NULL; 

    struct conn *c = &conn_list[fd];
    c->fd = fd;
    c->read_callback = recv_cb;   
    c->send_callback = send_cb;   
    c->accept_callback = NULL;

    c->role = CONN_MASTER; 

    c->rbuffer = (char *)kvs_malloc(4096); 
    if (!c->rbuffer) return NULL;
    c->rcapacity = 4096;
    c->rlength = 0;
    
    c->wbuffer = wbuf;     
    c->wcapacity = wcap;
    c->wlength = wlen;
    
    c->is_receiving_file = 0;        
    c->local_file_fd = -1;

    reactor_set_event(fd, EPOLLIN, 1); 
    return c;
}