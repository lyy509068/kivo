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
#include <sys/types.h>
#include <sys/stat.h>
#include <sys/sendfile.h>
#include <fcntl.h>
#include "network.h"
#include "kvstore.h"
#include "resp.h"
#include "replication.h"

volatile int server_should_exit;

static stream_handler_t g_stream_handler = NULL;


static int epfd = 0;
static struct conn conn_list[CONNECTION_SIZE] = {0};
static struct timeval begin;


int reactor_set_event(int fd, int event, int flag) {
    struct epoll_event ev;
    
    if (flag) {
        ev.events = event;
        ev.data.fd = fd;
        epoll_ctl(epfd, EPOLL_CTL_ADD, fd, &ev);
    } else {
        // 如果是主端连接，或者为了稳妥起见，挂载写事件时绝不能抹杀读事件
        if (conn_list[fd].role == CONN_MASTER || (event & EPOLLOUT)) {
            ev.events = event | EPOLLIN; // 读写共存
        } else {
            ev.events = event;
        }
        ev.data.fd = fd;
        epoll_ctl(epfd, EPOLL_CTL_MOD, fd, &ev);
    }
    return 0;
}

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

int reactor_send_file(int client_fd, const char *filepath) {
    if (client_fd < 0 || client_fd >= CONNECTION_SIZE || !filepath) return -1;
    
    int f_fd = open(filepath, O_RDONLY);
    if (f_fd < 0) {
        perror("Network: Open transfer file failed");
        return -1;
    }
    
    struct stat st;
    if (fstat(f_fd, &st) < 0) {
        close(f_fd);
        return -1;
    }
    
    conn_list[client_fd].file_fd = f_fd;
    conn_list[client_fd].file_size = st.st_size;
    conn_list[client_fd].file_ptr = 0;

    
    char size_header[128];
    int header_len = sprintf(size_header, "$%lld\r\n", (long long)st.st_size);

    if (conn_list[client_fd].wcapacity - conn_list[client_fd].wlength < header_len) {
        conn_list[client_fd].wbuffer = kvs_realloc(conn_list[client_fd].wbuffer, conn_list[client_fd].wcapacity + 4096);
        conn_list[client_fd].wcapacity += 4096;
    }

    memcpy(conn_list[client_fd].wbuffer + conn_list[client_fd].wlength, size_header, header_len);
    conn_list[client_fd].wlength += header_len;

    reactor_set_event(client_fd, EPOLLOUT, 0);
    return 0;
}

void recv_cb(int fd) {
    struct conn *c = &conn_list[fd];

    // 模式 A：从端专用，二进制文件落盘
    if (c->is_receiving_file) {
        char net_buf[8192];
        int count = recv(fd, net_buf, sizeof(net_buf), MSG_DONTWAIT);
        
        if (count < 0) {
            if (errno == EAGAIN || errno == EWOULDBLOCK) return;
            perror("recv error during file streaming");
            close_and_free_connection(fd);
            return;
        }
        if (count == 0) {
            close_and_free_connection(fd);
            return;
        }

        write(c->local_file_fd, net_buf, count);
        c->already_recv_size += count;

        // 如果文件完整收齐
        if (c->already_recv_size >= c->expect_file_size) {
            close(c->local_file_fd);
            c->local_file_fd = -1;
            c->is_receiving_file = 0; // 解除文件接收模式            
            
            kvs_persistence_recover();// 磁盘数据已完全落盘，现在安全恢复到内存
            printf("[Network] Slave memory database successfully reloaded from new AOF.\n");
        }
        return; // 文件模式下，不走协议层
    }

    // 模式 B：普通RESP命令，服务器接收客户端命令，主端接收从端日志命令，从端接收主端同步命令

    int total_new_bytes = 0; 
    while (1) {
        if (c->rcapacity - c->rlength < 4096) {
            int new_capacity = c->rcapacity * 2;
            if (new_capacity < 4096) new_capacity = 4096;
            char *new_buf = (char *)kvs_realloc(c->rbuffer, new_capacity);
            if (!new_buf) {
                close_and_free_connection(fd);
                return;
            }
            c->rbuffer = new_buf;
            c->rcapacity = new_capacity;
        }
        
        int remaining_space = c->rcapacity - c->rlength;
        int count = recv(fd, c->rbuffer + c->rlength, remaining_space, MSG_DONTWAIT);   
        
        if (count < 0) {
            if (errno == EAGAIN || errno == EWOULDBLOCK) break;
            close_and_free_connection(fd);
            return;
        }    
        if (count == 0) {
            close_and_free_connection(fd);
            return;
        }    
        c->rlength += count;
        total_new_bytes += count; 
    }

    if (total_new_bytes == 0 && c->rlength == 0) {
        return;
    }

    if (!g_stream_handler) return;

    int total_parsed_bytes = 0; 
    
    while (c->rlength > total_parsed_bytes) { 
        int parsed_bytes = 0;
        long long expect_file_size = 0; 
        int is_master = (c->role == CONN_MASTER);// 检查当前连接的身份
        int status = g_stream_handler(
            c->rbuffer + total_parsed_bytes, 
            c->rlength - total_parsed_bytes, 
            &parsed_bytes,                                                             
            is_master ? NULL : &c->wbuffer,   // 如果对象是主端，直接传 NULL！
            is_master ? NULL : &c->wcapacity,
            is_master ? NULL : &c->wlength, 
            &expect_file_size 
        );

        if (status == 1) {
            break; // 半包等待
        } 
        else if (status < 0) {
            //printf("[DEBUG-ERR] Protocol error on fd: %d\n", fd);
            if (c->role == CONN_MASTER) {
                c->wlength = 0; // 从端不给主端回复
            }
            close_and_free_connection(fd);
            return;
        }
        
        // 状态 10：主端收到 SYNC，准备发送文件
        else if (status == 10) {
            total_parsed_bytes += parsed_bytes;
            reactor_send_file(fd, PERSISTENCE_FILE);
            break; 
        }
        
        // 状态 20：从端收到握手头，准备接收文件
        else if (status == 20) {
            total_parsed_bytes += parsed_bytes;

            // 打开持久化日志
            c->local_file_fd = open(PERSISTENCE_FILE, O_WRONLY | O_CREAT | O_TRUNC, 0644);
            if (c->local_file_fd < 0) {
                perror("Slave open persistence file failed");
                close_and_free_connection(fd);
                return;
            }
            c->expect_file_size = expect_file_size;
            c->already_recv_size = 0;
            c->is_receiving_file = 1; 
            
            // 粘包处理：把缓冲区里剩下的二进制直接灌入磁盘
            int leftover = c->rlength - total_parsed_bytes;
            if (leftover > 0) {
                write(c->local_file_fd, c->rbuffer + total_parsed_bytes, leftover);
                c->already_recv_size += leftover; 
            }
            
            // 检查：是不是小文件刚好一步到位收全了
            if (c->already_recv_size >= c->expect_file_size) {
                close(c->local_file_fd);
                c->local_file_fd = -1;
                c->is_receiving_file = 0;                 
                // 收全后安全恢复
                kvs_persistence_recover(); 
            }            
            // 数据已经全部被消费或转移到磁盘，将计数归零，防止走到最后的常规平移引发错乱
            c->rlength = 0; 
            total_parsed_bytes = 0; 
            break; 
        }

        total_parsed_bytes += parsed_bytes;
    } 

    // 平移剩下不够一条命令的常规半包（只有非文件接收状态需要执行）
    if (!c->is_receiving_file && total_parsed_bytes > 0) {
        int remaining_data = c->rlength - total_parsed_bytes;
        if (remaining_data > 0) {
            memmove(c->rbuffer, c->rbuffer + total_parsed_bytes, remaining_data);
        }
        c->rlength = remaining_data;
        
        // 核心分流逻辑：
        if (c->role == CONN_CLIENT) {
            // 普通客户端：检查是否有数据需要回复，如果有，才挂载可写事件
            if (c->wlength > 0) {
                reactor_set_event(fd, EPOLLOUT, 0); 
            }
        } 
        else if (c->role == CONN_MASTER) {
            // 从端接收主端发来的同步命令，执行完毕后，不用回复，所以清空写缓冲区，并且坚决不挂载 EPOLLOUT
            c->wlength = 0; 
        }
    }
}

void send_cb(int fd) { 
    // 无论是普通客户端的回复，还是从端的协议报头最先从这里发出
    if (conn_list[fd].wlength > 0) {
        int count = send(fd, conn_list[fd].wbuffer, conn_list[fd].wlength, MSG_DONTWAIT);
        
        if (count < 0) {
            // TCP 窗口满，直接保留 EPOLLOUT 退出，等待下一次内核可写通知
            if (errno == EAGAIN || errno == EWOULDBLOCK) {
                return; 
            }
            perror("send error");
            if (g_repl.fd == fd) g_repl.fd = -1;
            close_and_free_connection(fd);
            return;
        }
        
        // 处理数据未完全发完的情况（分包发送）
        if (count < conn_list[fd].wlength) {
            int remaining = conn_list[fd].wlength - count;
            memmove(conn_list[fd].wbuffer, conn_list[fd].wbuffer + count, remaining);
            conn_list[fd].wlength = remaining;
            return; // 保持 EPOLLOUT，退出等待下次可写
        }
        
        // 本地缓冲区数据确定全部发完，清空计数
        conn_list[fd].wlength = 0;

        // 普通客户端长连接写缓冲区安全缩容
        if (conn_list[fd].wcapacity > INIT_BUFFER_SIZE * 4) {
            char *shrunk_buf = (char *)kvs_realloc(conn_list[fd].wbuffer, INIT_BUFFER_SIZE);
            if (shrunk_buf) {
                conn_list[fd].wbuffer = shrunk_buf;
                conn_list[fd].wcapacity = INIT_BUFFER_SIZE;
            }
        }
    }

    // 大文件同步流（仅在本地缓冲区彻底清空、且有文件待发时触发）
    if (conn_list[fd].file_fd > 0) {
        struct conn *c = &conn_list[fd];       
        
        // 循环使用 sendfile 发送，直到 TCP 窗口满（返回 EAGAIN）
        while (c->file_ptr < c->file_size) {
            off_t offset = c->file_ptr;
            size_t limit = c->file_size - c->file_ptr;
            if (limit > 65536) limit = 65536;

            ssize_t sent = sendfile(fd, c->file_fd, &offset, limit);
            
            if (sent < 0) {
                if (errno == EAGAIN || errno == EWOULDBLOCK) {
                    return; 
                }
                perror("sendfile error during sync");
                close(c->file_fd);
                c->file_fd = -1;
                if (g_repl.fd == fd) g_repl.fd = -1;
                close_and_free_connection(fd);
                return;
            }
            if (sent == 0) break;
            c->file_ptr += sent; // 推进发送进度
        }
        
        // 检查文件是否完全传输完毕
        if (c->file_ptr >= c->file_size) {
            close(c->file_fd);  // 关闭本地文件
            c->file_fd = -1;   // 状态复位
            c->file_size = 0;
            c->file_ptr = 0;
            
            // 文件发完后，这个fd升级为长连接增量同步通道!!!!!!
            g_repl.fd = fd; 
            
            // 检查发文件期间，有没有新写入的增量命令积压在全局g_repl.wbuffer里，会怎么处理？？？
            if (g_repl.wlength > 0) {
                repl_flush(); 
            } else {
                // 如果没有积压，切回监听从端的输入
                reactor_set_event(fd, EPOLLIN, 0);
            }
        }
        return;
    }

    // 日常增量同步流（当长连接进入平稳期，专门负责推送全局 g_repl 的增量数据）
    if (g_repl.fd > 0 && fd == g_repl.fd) {
        if (g_repl.wlength > 0) {
            int count = send(fd, g_repl.wbuffer, g_repl.wlength, MSG_DONTWAIT);
            if (count < 0) {
                if (errno == EAGAIN || errno == EWOULDBLOCK) return;
                perror("g_repl incremental send error");
                g_repl.fd = -1;
                close_and_free_connection(fd);
                return;
            }
            if (count < g_repl.wlength) {
                int remaining = g_repl.wlength - count;
                memmove(g_repl.wbuffer, g_repl.wbuffer + count, remaining);
                g_repl.wlength = remaining;

                return; // 保持 EPOLLOUT
            }
            g_repl.wlength = 0;
        }
        return;
    }

    // 普通客户端如果顺利走到这里，说明它的 wlength 已经清零，切回读事件
    reactor_set_event(fd, EPOLLIN, 0); 
}


void accept_cb(int fd) {
    struct sockaddr_in clientaddr;
    socklen_t len = sizeof(clientaddr);
    int clientfd = accept(fd, (struct sockaddr*)&clientaddr, &len);
    
    if (clientfd < 0) return;
    
    if (clientfd >= CONNECTION_SIZE) {
        close(clientfd);
        return;
    }

    // 初始化
    conn_list[clientfd].fd = clientfd;
    conn_list[clientfd].send_callback = send_cb;
    conn_list[clientfd].read_callback = recv_cb;
    conn_list[clientfd].accept_callback = NULL;

    conn_list[clientfd].file_fd = -1;  
    conn_list[clientfd].file_size = 0;
    conn_list[clientfd].file_ptr = 0;

    conn_list[clientfd].local_file_fd = -1;
    conn_list[clientfd].expect_file_size = 0;
    conn_list[clientfd].already_recv_size = 0;
    conn_list[clientfd].is_receiving_file = 0;
        
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
    
    reactor_set_event(clientfd, EPOLLIN, 1);
    
    if ((clientfd % 1000) == 0) {
        struct timeval current;
        gettimeofday(&current, NULL);
        memcpy(&begin, &current, sizeof(struct timeval));
    }
}

static int init_listen_socket(unsigned short port) {
    int sockfd = socket(AF_INET, SOCK_STREAM, 0);
    if (sockfd < 0) return -1;
    int opt = 1;
    if (setsockopt(sockfd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt)) < 0) {
        perror("setsockopt SO_REUSEADDR failed");
        close(sockfd);
        return -1;
    }
    
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

int reactor_start(unsigned short port, stream_handler_t handler) {
    g_stream_handler = handler;
    
    epfd = epoll_create(1);
    if (epfd < 0) return -1;
    
    int listen_fd = init_listen_socket(port);
    if (listen_fd < 0) return -1;
   
    conn_list[listen_fd].fd = listen_fd;
    conn_list[listen_fd].accept_callback = accept_cb;
    conn_list[listen_fd].read_callback = NULL;  
    conn_list[listen_fd].send_callback = NULL;
    
    conn_list[listen_fd].rbuffer = NULL;
    conn_list[listen_fd].wbuffer = NULL;
    
    reactor_set_event(listen_fd, EPOLLIN, 1);
    
    gettimeofday(&begin, NULL);

    #if ENABLE_REPLICATION_SLAVE
        const char *slave_ip = "192.168.92.129";
        unsigned short slave_port = 2000;
        repl_connect_to_master(slave_ip, slave_port); 
    #endif
    
    server_should_exit=0;// 从网络层退出
    int shutdown_stage=0;
    while (1) {
        struct epoll_event events[1024] = {0};

        int timeout = server_should_exit ? 50 : -1;
        int nready = epoll_wait(epfd, events, 1024, timeout);
        
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
        if (server_should_exit) {
            if (shutdown_stage == 0) {
                if (listen_fd > 0) {
                    epoll_ctl(epfd, EPOLL_CTL_DEL, listen_fd, NULL);
                    close(listen_fd);
                    listen_fd = -1; 
                }
                
                shutdown_stage = 1;
                continue; 
            } 
            else if (shutdown_stage == 1) {
                
                usleep(20000); 

                for (int fd = 0; fd < CONNECTION_SIZE; fd++) {
                    if (conn_list[fd].fd > 0) {
                        close_and_free_connection(fd); 
                    }
                }
                close(epfd);
                
                break; 
            }
        }
    }
    return 0;
}

// 让外部模块能够把一个已连接的从端 fd 托管给 Reactor
int reactor_host_slave_connection(int fd, char *wbuf, int wcap, int wlen) {
    if (fd < 0 || fd >= CONNECTION_SIZE) return -1; 
    struct conn *c = &conn_list[fd];
    c->fd = fd;// 回调函数绑定
    c->read_callback = recv_cb;   
    c->send_callback = send_cb;   
    c->accept_callback = NULL;

    c->role = CONN_MASTER;

    c->rbuffer = (char *)kvs_malloc(4096); 
    if (!c->rbuffer) return -1;
    
    c->rcapacity = 4096;
    c->rlength = 0;
    
    c->wbuffer = wbuf;     
    c->wcapacity = wcap;
    c->wlength = wlen;
    
    c->is_receiving_file = 0;        
    c->local_file_fd = -1;

    int target_events = EPOLLIN;
    if (wlen > 0) {
        target_events |= EPOLLOUT; 
    }

    reactor_set_event(fd, EPOLLIN, 1); 
    return 0;
}

