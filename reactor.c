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

volatile int server_should_exit;

#define MAX_PACKET_SIZE 10 * 1024 * 1024
#define CONNECTION_SIZE 1024

static stream_handler_t g_stream_handler = NULL;


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

    set_event(client_fd, EPOLLOUT, 0);
    printf("[Network] Master starting FILE_STREAM mode. File size: %lld bytes.\n", (long long)st.st_size);
    
    return 0;
}

#if 0
void recv_cb(int fd) {
    //printf("\n======= Enter recv_cb for fd: %d =======\n", fd);
    
    // 非阻塞网络读取
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

    // 如果没有注入处理器，直接清空缓冲区并返回，防止死循环
    if (!g_stream_handler) {
        printf("[DEBUG-WARN] No stream handler registered! Discarding %d bytes.\n", conn_list[fd].rlength);
        conn_list[fd].rlength = 0;
        return;
    }

    // 2. 将数据全权委托给协议层处理
    int total_parsed_bytes = 0; 
    int commands_executed = 0;

    while (conn_list[fd].rlength > total_parsed_bytes) { 
        int parsed_bytes = 0;

        int status = g_stream_handler(
            conn_list[fd].rbuffer + total_parsed_bytes, 
            conn_list[fd].rlength - total_parsed_bytes, 
            &parsed_bytes,                              
            &conn_list[fd].wbuffer,                     
            &conn_list[fd].wcapacity,                   
            &conn_list[fd].wlength                      
        );

        if (status == 1) {
            break; // 半包，等待下次 EPOLLIN
        } else if (status < 0) {
            printf("[DEBUG-ERR] Protocol error or connection termination on fd: %d\n", fd);
            close_and_free_connection(fd);
            return;
        }

        total_parsed_bytes += parsed_bytes;
        commands_executed++;
    } 

    // 3. 统一平移剩下的未解析数据
    if (total_parsed_bytes > 0) {
        int remaining_data = conn_list[fd].rlength - total_parsed_bytes;
        if (remaining_data > 0) {
            memmove(conn_list[fd].rbuffer, conn_list[fd].rbuffer + total_parsed_bytes, remaining_data);
        }
        conn_list[fd].rlength = remaining_data;
        
        //printf("[DEBUG-END] Executed %d commands. Remaining unparsed raw data: %d bytes\n", commands_executed, conn_list[fd].rlength);
               
        set_event(fd, EPOLLOUT, 0); 
    }
    
    //printf("======= Exit recv_cb for fd: %d =======\n", fd);
}
#else
void recv_cb(int fd) {
    struct conn *c = &conn_list[fd];

    // 模式 A：纯二进制裸流落盘模式（从端专用）
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

        // 直接落盘并记账
        write(c->local_file_fd, net_buf, count);
        c->already_recv_size += count;

        // 检查文件是否完整收齐
        if (c->already_recv_size >= c->expect_file_size) {
            printf("[Network] Slave received full AOF file (%lld bytes). Switching to Command Mode.\n", c->already_recv_size);
            close(c->local_file_fd);
            c->local_file_fd = -1;
            c->is_receiving_file = 0; // 解除文件接收模式
            
            // 磁盘数据已完全落盘，现在安全恢复到内存
            kvs_persistence_recover();
            printf("[Network] Slave memory database successfully reloaded from new AOF.\n");
        }
        return; // 文件模式下，不走后面的协议层
    }

    // 模式 B：正常的 RESP 命令缓冲区模式
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
    }

    if (c->rlength > 0) {
        printf("[Net Recv-Data] Fd %d extracted %d bytes buffer from network. Raw context: %s\n", 
               fd, c->rlength, c->rbuffer);
        fflush(stdout);
    }

    if (!g_stream_handler) return;

    int total_parsed_bytes = 0; 
    
    while (c->rlength > total_parsed_bytes) { 
        int parsed_bytes = 0;
        long long expect_file_size = 0; 

        int status = g_stream_handler(
            c->rbuffer + total_parsed_bytes, 
            c->rlength - total_parsed_bytes, 
            &parsed_bytes,                                                             
            &c->wbuffer,                    
            &c->wcapacity,                  
            &c->wlength,
            &expect_file_size 
        );

        printf("[Net Handler Debug] Fd %d parsed_bytes: %d, handler returned status: %d\n", 
               fd, parsed_bytes, status);
        fflush(stdout);

        if (status == 1) {
            break; // 半包等待
        } 
        else if (status < 0) {
            printf("[DEBUG-ERR] Protocol error on fd: %d\n", fd);
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
            
            // 1：在抹平磁盘文件前，必须调用你的引擎接口清空老内存！
            // ⚠️ 请将下行替换为你 kvstore 真实的清空内存函数，比如 kvs_clear_db()
            //  kvs_clear_all_memory_data(); 

            // 2. 打开最终的持久化日志（清空磁盘旧日志）
            c->local_file_fd = open(PERSISTENCE_FILE, O_WRONLY | O_CREAT | O_TRUNC, 0644);
            if (c->local_file_fd < 0) {
                perror("Slave open persistence file failed");
                close_and_free_connection(fd);
                return;
            }
            c->expect_file_size = expect_file_size;
            c->already_recv_size = 0;
            c->is_receiving_file = 1; 
            
            // 3. 粘包处理：把缓冲区里剩下的二进制直接灌入磁盘
            int leftover = c->rlength - total_parsed_bytes;
            if (leftover > 0) {
                write(c->local_file_fd, c->rbuffer + total_parsed_bytes, leftover);
                c->already_recv_size += leftover; 
            }
            
            // 4. 检查：是不是小文件刚好一步到位收全了
            if (c->already_recv_size >= c->expect_file_size) {
                printf("[Network] Slave downloaded file instantly via sticky packet!\n");
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
        set_event(fd, EPOLLOUT, 0); 
    }
}
#endif

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
    
    //文件发送分支
    if (conn_list[fd].file_fd > 0) {
        struct conn *c = &conn_list[fd];       
        // 循环使用 sendfile 发送，直到 TCP 窗口满（返回 EAGAIN）
        while (c->file_ptr < c->file_size) {
            off_t offset = c->file_ptr;
            // 每次最多尝试发送 64KB，保持单次控制权，防止极大的文件长时间独占内核网络缓冲区
            size_t limit = c->file_size - c->file_ptr;
            if (limit > 65536) limit = 65536;

            ssize_t sent = sendfile(fd, c->file_fd, &offset, limit);
            
            if (sent < 0) {
                // 如果内核的 TCP 发送窗口满了，立刻退出，保留 EPOLLOUT 事件，下回再发
                if (errno == EAGAIN || errno == EWOULDBLOCK) {
                    return; 
                }
                perror("sendfile error during sync");
                close(c->file_fd);
                close_and_free_connection(fd);
                return;
            }
            if (sent == 0) { // 文件意外截断或读到末尾
                break;
            }
            c->file_ptr += sent; // 推进发送进度
        }
        // 检查文件是否完全传输完毕
        if (c->file_ptr >= c->file_size) {
            printf("[Network] File transfer successfully completed for fd:%d! Total sent: %lld bytes.\n", fd, c->file_size);
            close(c->file_fd);  // 关闭文件描述符
            c->file_fd = -1;   // 状态复位
            c->file_size = 0;
            c->file_ptr = 0;
            
            // 文件安全发送完后，恢复常态：切回监听客户端的输入（EPOLLIN）
            set_event(fd, EPOLLIN, 0);
        }
        return;
    }


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

    // 发送文件的回调吗？？？？
    conn_list[clientfd].file_fd = -1;  
    conn_list[clientfd].file_size = 0;
    conn_list[clientfd].file_ptr = 0;

    conn_list[clientfd].local_file_fd = -1;
    conn_list[clientfd].expect_file_size = 0;
    conn_list[clientfd].already_recv_size = 0;
    conn_list[clientfd].is_receiving_file = 0;
    
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
    
    // 监听套接字不需要分配读写 rbuffer/wbuffer
    conn_list[listen_fd].rbuffer = NULL;
    conn_list[listen_fd].wbuffer = NULL;
    
    set_event(listen_fd, EPOLLIN, 1);
    
    gettimeofday(&begin, NULL);
    
    server_should_exit=0;
    int shutdown_stage=0;
    while (1) {
        struct epoll_event events[1024] = {0};

        int timeout = server_should_exit ? 50 : -1;
        int nready = epoll_wait(epfd, events, 1024, timeout);
        
        for (int i = 0; i < nready; i++) {
            int connfd = events[i].data.fd;
            
            if (events[i].events & EPOLLIN) {
                // 🚀 增加这条暴力打印：看看到底是谁把内核唤醒了
                printf("[Net Epoll-In] Active Event triggered on fd: %d, has read_cb: %s\n", 
                       connfd, conn_list[connfd].read_callback ? "YES" : "NO (NULL!)");
                fflush(stdout);
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
                //printf("[Reactor] SHUTDOWN sign captured. Closing listener first to flush remaining data...\n");
                // 1. 先把大门关了，不再接受任何新连接，防止插队
                if (listen_fd > 0) {
                    epoll_ctl(epfd, EPOLL_CTL_DEL, listen_fd, NULL);
                    close(listen_fd);
                    listen_fd = -1; 
                }
                // 2. 状态递进到阶段 1，下一轮循环再来看
                shutdown_stage = 1;
                continue; 
            } 
            else if (shutdown_stage == 1) {
                // 此时，经历了上一轮的循环和可能的事件分发，刚才 recv_cb 挂载的 EPOLLOUT 已经被 send_cb 消费掉了！
                //printf("[Reactor] Flush phase completed. Cleaning up all connections and buffers.\n");
                
                // 给系统内核协议栈一点微小的冲刷剩余时间
                usleep(20000); 

                // 安全释放每个活跃客户端连接的资源和内存
                for (int fd = 0; fd < CONNECTION_SIZE; fd++) {
                    if (conn_list[fd].fd > 0) {
                        close_and_free_connection(fd); 
                    }
                }
                //printf("[Reactor] All client connections cleaned up safely.\n");
                close(epfd);
                
                break; // 完美跳出 while(1) 循环，回到 main 函数
            }
        }
    }
    return 0;
}

// 🚀 追加到 reactor.c 或 network.c 的末尾
// 让外部模块能够把一个已连接的从端 fd 托管给 Reactor
int reactor_host_slave_connection(int fd, char *wbuf, int wcap, int wlen) {
    if (fd < 0 || fd >= CONNECTION_SIZE) return -1; 

    struct conn *c = &conn_list[fd];
    
    // 🚀 核心修复：必须把回调函数牢牢绑死，否则 epoll 醒了找不到人执行
    c->fd = fd;
    c->read_callback = recv_cb;   // 👈 指向接收状态机
    c->send_callback = send_cb;   // 👈 指向发送状态机
    c->accept_callback = NULL;

    c->rbuffer = (char *)kvs_malloc(4096); 
    if (!c->rbuffer) return -1;
    
    c->rcapacity = 4096;
    c->rlength = 0;
    
    c->wbuffer = wbuf;     
    c->wcapacity = wcap;
    c->wlength = wlen;
    
    c->is_receiving_file = 0;        
    c->local_file_fd = -1;

    // 注册 Epoll 读事件
    set_event(fd, EPOLLIN, 0); 

    return 0;
}
