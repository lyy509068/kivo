#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <string.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <arpa/inet.h>
#include <sys/syscall.h>

#include "nty_coroutine.h" 

#include "network.h"
#include "kvstore.h"
#include "resp.h"
#include "replication.h"

extern volatile int server_should_exit;
static stream_handler_t g_stream_handler = NULL;
static struct conn conn_list[CONNECTION_SIZE] = {0};

// 预声明协程函数
void ntyco_client_co(void *arg);
void ntyco_server_co(void *arg);

// 清理连接资源
static void close_and_free_connection(int fd) {
    close(fd); // NtyCo 会在此处自动清理该 fd 关联的协程调度事件
    if (conn_list[fd].rbuffer) kvs_free(conn_list[fd].rbuffer);
    if (conn_list[fd].wbuffer) kvs_free(conn_list[fd].wbuffer);
    memset(&conn_list[fd], 0, sizeof(struct conn));
}


// 依托协程，发送文件不再需要 sendfile 的状态机，直接用标准循环发送，内核会自动 yield
int ntyco_send_file(int client_fd, const char *filepath) {
    int f_fd = open(filepath, O_RDONLY);

    if (f_fd < 0) {
        perror("[DEBUG-ERROR] Open file failed");
        return -1;
    }

    struct stat st;
    if (fstat(f_fd, &st) < 0) { 
        perror("[DEBUG-ERROR] fstat failed");
        close(f_fd); 
        return -1; 
    }
    
    // 发送 RESP 头部（必须严格遵循从端要求的 "+OK\r\n$文件大小\r\n" 格式）
    char size_header[128];
    int header_len = sprintf(size_header, "+OK\r\n$%lld\r\n", (long long)st.st_size);
    
    ssize_t header_sent = send(client_fd, size_header, header_len, 0);
    
    if (header_sent <= 0) {
        perror("[DEBUG-ERROR] Send header failed or connection closed");
        close(f_fd); 
        return -1;
    }
    
    char file_buf[4096];
    ssize_t bytes_read;
    long long total_file_read = 0;
    long long total_file_sent = 0;
    int loop_count = 0;

    // read 可能会因为磁盘 I/O 阻塞
    while ((bytes_read = syscall(SYS_read, f_fd, file_buf, sizeof(file_buf))) > 0) {
        loop_count++;
        total_file_read += bytes_read;

        ssize_t total_sent_this_block = 0;
        int sub_loop_count = 0;
        
        while (total_sent_this_block < bytes_read) {
            sub_loop_count++;
            ssize_t sent = send(client_fd, file_buf + total_sent_this_block, bytes_read - total_sent_this_block, 0);
            
            if (sent <= 0) { 
                perror("[DEBUG-ERROR] Send data block failed");               
                close(f_fd); 
                return -1; 
            }
            total_sent_this_block += sent;
            total_file_sent += sent;
        }
    }

    if (bytes_read < 0) {
        perror("[DEBUG-ERROR] Disk read error occurred");
        close(f_fd);
        return -1;
    }

    close(f_fd);
    return 0;
}

// 从端接收文件
#if ENABLE_REPLICATION_SLAVE
void ntyco_recv_file(int fd, const char *filepath, long long expect_size, char *leftover, int leftover_len) {
    int f_fd = open(filepath, O_WRONLY | O_CREAT | O_TRUNC, 0644);
    if (f_fd < 0) return;

    long long total_recv = 0;

    // 先把解析协议时粘包残留在缓冲区的数据灌入磁盘
    if (leftover_len > 0) {
        syscall(SYS_write, f_fd, leftover, leftover_len);
        total_recv += leftover_len;
    }

    // 循环读取网络直到收满，期间网络无数据时协程自动挂起
    char net_buf[4096];
    while (total_recv < expect_size) {
        long long to_read = expect_size - total_recv;
        if (to_read > sizeof(net_buf)) to_read = sizeof(net_buf);

        int count = recv(fd, net_buf, to_read, 0); // 阻塞式写法
        if (count <= 0) break;

        syscall(SYS_write, f_fd, net_buf, count);
        total_recv += count;
    }

    close(f_fd);
    kvs_persistence_recover(); // 安全落盘后内存恢复
    printf("[Network] Slave memory database successfully reloaded from new AOF.\n");
}
#endif

//代替recv和send
void ntyco_client_co(void *arg) {
    int fd = (int)(long)arg;
    struct conn *c = &conn_list[fd];

    // 初始化本地应用层缓冲区
    c->rcapacity = INIT_BUFFER_SIZE;
    c->rbuffer = (char*)kvs_malloc(INIT_BUFFER_SIZE);
    c->wcapacity = INIT_BUFFER_SIZE;
    c->wbuffer = (char*)kvs_malloc(INIT_BUFFER_SIZE);
    c->rlength = 0;
    c->wlength = 0;

    if (!c->rbuffer || !c->wbuffer) { close_and_free_connection(fd); return; }

    while (!server_should_exit) {
        // 动态扩容判断
        if (c->rcapacity - c->rlength < 4096) {
            int new_capacity = c->rcapacity * 2;
            c->rbuffer = (char *)kvs_realloc(c->rbuffer, new_capacity);
            c->rcapacity = new_capacity;
        }

        // 直接调用标准 recv。NtyCo 会在底层将其加入 epollIN，随后 yield 让出 CPU
        int count = recv(fd, c->rbuffer + c->rlength, c->rcapacity - c->rlength, 0);
        if (count <= 0) {
            break; // 客户端断开或出错
        }
        c->rlength += count;

        // 协议解析与状态分流循环
        int total_parsed_bytes = 0;
        int exit_loop = 0;

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
                break; // 半包，跳出继续等待 recv
            } else if (status < 0) {
                exit_loop = 1; break; // 协议错误
            } 
            #if ENABLE_REPLICATION_MASTER
            else if (status == 10) {
                
                int ans=ntyco_send_file(fd, PERSISTENCE_FILE);
                g_repl.fd = fd; // 全量同步完成，跃升为长连接增量通道
                c->role = CONN_MASTER;

                total_parsed_bytes += parsed_bytes;

                if (ans < 0) {
                    g_repl.fd = -1;
                    c->role = CONN_CLIENT;
                }

                // 处理全量发送期间，客户端可能带来的网络流污染（清空当前读缓存）
                c->rlength = 0;
                total_parsed_bytes = 0;
                break; 
            }
            #endif 
            #if ENABLE_REPLICATION_SLAVE
            else if (status == 20) {
                total_parsed_bytes += parsed_bytes;
                int leftover = c->rlength - total_parsed_bytes;
                
                // 接管剩余流，进入纯文件模式
                ntyco_recv_file(fd, PERSISTENCE_FILE, expect_file_size, c->rbuffer + total_parsed_bytes, leftover);
                
                c->rlength = 0; total_parsed_bytes = 0;
                break;
            }
            #endif
            total_parsed_bytes += parsed_bytes;
        }

        if (exit_loop) break;

        // 平移常规未解析完的半包数据
        if (total_parsed_bytes > 0) {
            int remaining = c->rlength - total_parsed_bytes;
            if (remaining > 0) {
                memmove(c->rbuffer, c->rbuffer + total_parsed_bytes, remaining);
            }
            c->rlength = remaining;
        }

        // 回应客户端（增量模式或从端不给主端回执）
        if (c->role == CONN_CLIENT && c->wlength > 0) {
            send(fd, c->wbuffer, c->wlength, 0); // 直接同步发，满了底层自动 yield
            c->wlength = 0;
        } else if (c->role == CONN_MASTER) {
            c->wlength = 0;
        }
    }

    close_and_free_connection(fd);
}

//初始化
void ntyco_server_co(void *arg) {
    int listen_fd = (int)(long)arg;

    while (!server_should_exit) {
        struct sockaddr_in clientaddr;
        socklen_t len = sizeof(clientaddr);
        
        // 当没有新连接时，NtyCo 将当前 server 协程挂起
        int clientfd = accept(listen_fd, (struct sockaddr*)&clientaddr, &len);
        if (clientfd < 0) continue;

        if (clientfd >= CONNECTION_SIZE) { close(clientfd); continue; }

        // 初始化基础连接数据
        conn_list[clientfd].fd = clientfd;
        conn_list[clientfd].role = CONN_CLIENT; // 默认客户端

        // 为每一个新进来的连接，单独开辟一个独立的协程去伺候它！
        nty_coroutine *client_co = NULL;
        nty_coroutine_create(&client_co, ntyco_client_co, (void*)(long)clientfd);
    }
    close(listen_fd);
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

    if (-1 == bind(sockfd, (struct sockaddr*)&servaddr, sizeof(servaddr))) return -1;
    if (-1 == listen(sockfd, 10)) return -1;
    
    return sockfd;
}

#if ENABLE_REPLICATION_SLAVE
void ntyco_slave_init_co(void *arg) {
    const char *slave_ip = "192.168.92.128"; 
    unsigned short slave_port = 2000;
    
    printf("[NtyCo] Slave initialization coroutine started, connecting to master...\n");
    repl_connect_to_master(slave_ip, slave_port);
}
#endif

int ntyco_start(unsigned short port, stream_handler_t handler) {
    g_stream_handler = handler;

    int listen_fd = init_listen_socket(port);
    if (listen_fd < 0) return -1;

    conn_list[listen_fd].fd = listen_fd;

    // 创建主服务 Master 监听协程
    nty_coroutine *server_co = NULL;
    nty_coroutine_create(&server_co, ntyco_server_co, (void*)(long)listen_fd);

    #if ENABLE_REPLICATION_SLAVE
        nty_coroutine *slave_init_co = NULL;
        nty_coroutine_create(&slave_init_co, ntyco_slave_init_co, NULL);
    #endif

   // 启动 NtyCo 的协程调度器，此函数将独占线程并开始循环调度
    nty_schedule_run();  

    return 0;
}

int ntyco_host_slave_connection(int fd, char *wbuf, int wcap, int wlen) {
    if (fd < 0 || fd >= CONNECTION_SIZE) return -1;
    
    struct conn *c = &conn_list[fd];
    c->fd = fd;
    c->role = CONN_MASTER; // 标记为主端连接（当前节点是从端）

    // 继承外部已有的缓冲区状态
    c->wbuffer = wbuf;     
    c->wcapacity = wcap;
    c->wlength = wlen;

    c->rbuffer = (char *)kvs_malloc(INIT_BUFFER_SIZE);
    c->rcapacity = INIT_BUFFER_SIZE;
    c->rlength = 0;

    // 核心：直接抛进协程调度器，让独立协程去线性处理这个 fd 的读写
    nty_coroutine *client_co = NULL;
    nty_coroutine_create(&client_co, ntyco_client_co, (void*)(long)fd);

    return 0;
}