#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <string.h>
#include <unistd.h>
#include <sys/time.h>
#include <arpa/inet.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <sys/sendfile.h>
#include <fcntl.h>
#include <liburing.h> 
#include <poll.h>

#include "network.h"
#include "kvstore.h"
#include "resp.h"
#include "replication.h"

extern volatile int server_should_exit;

static stream_handler_t g_stream_handler = NULL;

static struct io_uring ring; 
static struct conn conn_list[CONNECTION_SIZE] = {0};
static struct timeval begin;


// 预声明提交函数
void submit_accept(int listen_fd);
void submit_recv(int fd);
void submit_send(int fd);


static void close_and_free_connection(int fd) {
    close(fd);
    
    // Proactor 中，套接字关闭后，内核会自动取消相关的未完成请求
    if (conn_list[fd].rbuffer) {
        kvs_free(conn_list[fd].rbuffer);
    }
    if (conn_list[fd].wbuffer) {
        kvs_free(conn_list[fd].wbuffer);
    }
    
    memset(&conn_list[fd], 0, sizeof(struct conn));
}

// 异步提交层

void submit_accept(int listen_fd) {
    struct io_uring_sqe *sqe = io_uring_get_sqe(&ring);
    
    struct conn *c = &conn_list[listen_fd];
    c->accept_ctx.fd = listen_fd;
    c->accept_ctx.type = OP_ACCEPT;
    
    io_uring_prep_accept(sqe, listen_fd, (struct sockaddr*)&c->clientaddr, &c->clientlen, 0);
    io_uring_sqe_set_data(sqe, &c->accept_ctx);
}

void submit_recv(int fd) {
    struct conn *c = &conn_list[fd];
    
    // 确保缓冲区有足够空间，Proactor 必须在提交前分配好空间
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

    struct io_uring_sqe *sqe = io_uring_get_sqe(&ring);
    c->recv_ctx.fd = fd;
    c->recv_ctx.type = OP_RECV;
    
    int remaining_space = c->rcapacity - c->rlength;
    // 提交接收请求，系统会在后台将数据写入缓冲区
    io_uring_prep_recv(sqe, fd, c->rbuffer + c->rlength, remaining_space, 0);
    io_uring_sqe_set_data(sqe, &c->recv_ctx);
}

void submit_send(int fd) {
    struct conn *c = &conn_list[fd];
    if (c->wlength <= 0) return;

    struct io_uring_sqe *sqe = io_uring_get_sqe(&ring);
    c->send_ctx.fd = fd;
    c->send_ctx.type = OP_SEND;
    
    io_uring_prep_send(sqe, fd, c->wbuffer, c->wlength, 0);
    io_uring_sqe_set_data(sqe, &c->send_ctx);
}


// ---------------- 业务处理层 (处理"完成"的数据) ----------------

int proactor_send_file(int client_fd, const char *filepath) {
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

    // 【修复点 1】删除了 conn_list[client_fd].wlength = 0; 从而完美保留了 g_stream_handler 写入的 "+OK\r\n"
    
    char size_header[128];
    int header_len = sprintf(size_header, "$%lld\r\n", (long long)st.st_size);

    // 扩容判断：此时应基于现有的 wlength 叠加计算
    if (conn_list[client_fd].wcapacity - conn_list[client_fd].wlength < header_len) {
        conn_list[client_fd].wbuffer = kvs_realloc(conn_list[client_fd].wbuffer, conn_list[client_fd].wcapacity + 4096);
        conn_list[client_fd].wcapacity += 4096;
    }

    // 【修复点 2】将文件长度头追加到已存在的 "+OK\r\n" 后面，组合成完整的 RESP 格式
    memcpy(conn_list[client_fd].wbuffer + conn_list[client_fd].wlength, size_header, header_len);
    conn_list[client_fd].wlength += header_len;

    // 提交整个 Header 头部包发送
    submit_send(client_fd);
    return 0;
}


// 数据接收【完成】后的处理逻辑
void on_recv_completed(int fd, int res) {
    struct conn *c = &conn_list[fd];

    if (res < 0) {
        close_and_free_connection(fd);
        return;
    }
    if (res == 0) {
        // EOF
        close_and_free_connection(fd);
        return;
    }

    // res 是内核已经帮我们读到的字节数
    int count = res;
    #if ENABLE_REPLICATION_SLAVE
    // 模式 A：从端专用，二进制文件落盘
    if (c->is_receiving_file) {
        // 直接将已接收到缓冲区的数据落盘
        write(c->local_file_fd, c->rbuffer + c->rlength, count);
        c->already_recv_size += count;

        // 如果文件完整收齐
        if (c->already_recv_size >= c->expect_file_size) {
            close(c->local_file_fd);
            c->local_file_fd = -1;
            c->is_receiving_file = 0; // 解除文件接收模式            
            
            kvs_persistence_recover();
            printf("[Network] Slave memory database successfully reloaded from new AOF.\n");
        }
        
        // 文件没收完，或者收完进入常规状态，都需要再次提交 recv 等待下一次数据
        submit_recv(fd);
        return; 
    }
    #endif 
    // 模式 B：普通 RESP 命令
    c->rlength += count;

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
            &expect_file_size 
        );

        if (status == 1) {
            break; // 半包等待
        } 
        else if (status < 0) {
            if (c->role == CONN_MASTER) c->wlength = 0;
            close_and_free_connection(fd);
            return;
        }
        #if ENABLE_REPLICATION_MASTER
        else if (status == 10) {
            // 【修复点 3】这里匹配上全量同步指令后，移出数据残包并提前 return 退出，彻底切断下游的二次发送风险
            total_parsed_bytes += parsed_bytes;
            proactor_send_file(fd, PERSISTENCE_FILE);

            int leftover = c->rlength - total_parsed_bytes;
            if (leftover > 0) {
                memmove(c->rbuffer, c->rbuffer + total_parsed_bytes, leftover);
            }
            c->rlength = leftover;
            return; 
        }
        #endif 
        #if ENABLE_REPLICATION_SLAVE
        else if (status == 20) {
            total_parsed_bytes += parsed_bytes;

            c->local_file_fd = open(PERSISTENCE_FILE, O_WRONLY | O_CREAT | O_TRUNC, 0644);
            if (c->local_file_fd < 0) {
                close_and_free_connection(fd);
                return;
            }
            c->expect_file_size = expect_file_size;
            c->already_recv_size = 0;
            c->is_receiving_file = 1; 
            
            // 粘包处理
            int leftover = c->rlength - total_parsed_bytes;
            if (leftover > 0) {
                write(c->local_file_fd, c->rbuffer + total_parsed_bytes, leftover);
                c->already_recv_size += leftover; 
            }
            
            if (c->already_recv_size >= c->expect_file_size) {
                close(c->local_file_fd);
                c->local_file_fd = -1;
                c->is_receiving_file = 0;                
                kvs_persistence_recover(); 
            }            
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
        
        if (c->role == CONN_MASTER) {
            c->wlength = 0; 
        }
    }

    // 状态分流：如果有需要发出的数据，提交 Send 操作；否则提交 Recv 循环等待新指令
    if (c->role == CONN_CLIENT && c->wlength > 0) {
        submit_send(fd);
    } else {
        submit_recv(fd);
    }
}


// 数据发送【完成】后的处理逻辑
void on_send_completed(int fd, int res) {
    struct conn *c = &conn_list[fd];
    
    if (res < 0) {
        if (g_repl.fd == fd) g_repl.fd = -1;
        close_and_free_connection(fd);
        return;
    }

    // res 是内核实际发出的字节数
    int count = res;

    // 清理发送缓冲区
    if (c->wlength > 0) {
        if (count < c->wlength) {
            int remaining = c->wlength - count;
            memmove(c->wbuffer, c->wbuffer + count, remaining);
            c->wlength = remaining;
            
            // 数据没发完，继续提交发送
            submit_send(fd);
            return;
        }
        c->wlength = 0;
    }

    // 当整体的头部（+OK\r\n$2650000\r\n）异步发送彻底完成后，这里顺理成章地承接底层文件的物理发送
    if (c->file_fd > 0) {
        off_t offset = c->file_ptr;
        size_t limit = c->file_size - c->file_ptr;
        if (limit > 65536) limit = 65536;

        ssize_t sent = sendfile(fd, c->file_fd, &offset, limit);
        if (sent > 0) {
            c->file_ptr += sent;
        } else if (sent < 0 && (errno != EAGAIN && errno != EWOULDBLOCK)) {
            close(c->file_fd);
            c->file_fd = -1;
            close_and_free_connection(fd);
            return;
        }
        
        if (c->file_ptr >= c->file_size) {
            close(c->file_fd);
            c->file_fd = -1;
            c->file_size = 0;
            c->file_ptr = 0;
            g_repl.fd = fd; // 升级通道
            
            if (g_repl.wlength > 0) {
                repl_flush(); 
            } else {
                submit_recv(fd); // 切回读取态
            }
        } else {
            // 文件还没发完，用特殊的 POLLOUT 等待写就绪，简化起见提交一个空 SEND 再次触发该流
            struct io_uring_sqe *sqe = io_uring_get_sqe(&ring);
            c->send_ctx.fd = fd;
            c->send_ctx.type = OP_SENDFILE;
            io_uring_prep_poll_add(sqe, fd, POLLOUT);
            io_uring_sqe_set_data(sqe, &c->send_ctx);
        }
        return;
    }

    // 处理全局增量流逻辑
    if (g_repl.fd > 0 && fd == g_repl.fd) {
        if (g_repl.wlength > 0) {
            // 把 g_repl 的数据搬过来发送
            c->wbuffer = g_repl.wbuffer; 
            c->wlength = g_repl.wlength;
            submit_send(fd);
            return;
        }
    }

    // 数据全发完了，提交 Recv 请求重新监听该连接
    submit_recv(fd);
}


void on_accept_completed(int listen_fd, int clientfd) {
    if (clientfd < 0) return;
    
    // 继续挂载 Accept，保持对监听套接字的监听
    submit_accept(listen_fd);
    
    if (clientfd >= CONNECTION_SIZE) {
        close(clientfd);
        return;
    }

    // 初始化新的连接
    conn_list[clientfd].fd = clientfd;
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
    
    // Proactor: 对于新连接，我们发起第一个异步读取操作
    submit_recv(clientfd);
}

static int init_listen_socket(unsigned short port) {
    int sockfd = socket(AF_INET, SOCK_STREAM, 0);
    if (sockfd < 0) return -1;
    int opt = 1;
    if (setsockopt(sockfd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt)) < 0) {
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

// ---------------- 核心循环层 ----------------

int proactor_start(unsigned short port, stream_handler_t handler) {
    g_stream_handler = handler;
    
    // 初始化 io_uring (取代 epoll_create)
    if (io_uring_queue_init(QUEUE_DEPTH, &ring, 0) < 0) return -1;
    
    int listen_fd = init_listen_socket(port);
    if (listen_fd < 0) return -1;
   
    conn_list[listen_fd].fd = listen_fd;
    conn_list[listen_fd].clientlen = sizeof(struct sockaddr_in);
    
    // Proactor：提交第一个接受连接的请求
    submit_accept(listen_fd);
    
    gettimeofday(&begin, NULL);

    #if ENABLE_REPLICATION_SLAVE
        const char *slave_ip = "192.168.92.129";
        unsigned short slave_port = 2000;
        repl_connect_to_master(slave_ip, slave_port); 
    #endif
    
    server_should_exit = 0;
    int shutdown_stage = 0;
    
    struct io_uring_cqe *cqe;

    while (1) {
        // 提交所有尚未提交的 SQE（请求），并等待至少一个完成事件（CQE）
        io_uring_submit_and_wait(&ring, 1);
        
        unsigned head;
        int count = 0;
        
        // 遍历所有已完成的事件
        io_uring_for_each_cqe(&ring, head, cqe) {
            count++;
            io_ctx_t *ctx = (io_ctx_t *)io_uring_cqe_get_data(cqe);
            if (!ctx) continue;

            int res = cqe->res;
            int fd = ctx->fd;

            switch (ctx->type) {
                case OP_ACCEPT:
                    on_accept_completed(fd, res);
                    break;
                case OP_RECV:
                    on_recv_completed(fd, res);
                    break;
                case OP_SEND:
                    on_send_completed(fd, res);
                    break;
                case OP_SENDFILE: // 对接轮询 sendfile 继续
                    on_send_completed(fd, 0); 
                    break;
            }
        }
        
        // 通知内核这批已完成的事件我们处理过了
        io_uring_cq_advance(&ring, count);
        
        // 退出的逻辑处理
        if (server_should_exit) {
            if (shutdown_stage == 0) {
                if (listen_fd > 0) {
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
                io_uring_queue_exit(&ring);
                break; 
            }
        }
    }
    return 0;
}

// ---------------- 外部模块对接层 ----------------

int proactor_host_slave_connection(int fd, char *wbuf, int wcap, int wlen) {
    if (fd < 0 || fd >= CONNECTION_SIZE) return -1; 
    struct conn *c = &conn_list[fd];
    c->fd = fd;
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

    // Proactor：连接托管后，系统直接根据是否有数据待发送，发起对应的首个异步任务
    if (wlen > 0) {
        submit_send(fd); 
    } else {
        submit_recv(fd);
    }
    
    return 0;
}

void proactor_notify_tx_ready(int fd) {
    if (fd < 0 || fd >= CONNECTION_SIZE) return;
    
    struct conn *c = &conn_list[fd];

    if (g_repl.fd == fd) {
        c->wlength = g_repl.wlength;
        c->wbuffer = g_repl.wbuffer; 
    }

    if (c->wlength > 0) {
        submit_send(fd);
    } 
    else {
        submit_recv(fd);
    }
}