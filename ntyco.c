#include "nty_coroutine.h"
#include <arpa/inet.h>
#include <string.h>
#include <unistd.h>
#include <stdlib.h>
#include <stdint.h>
#include "server.h" 
#include "kvstore.h"

#define CONNECTION_SIZE     1024

// 全局连接池与处理函数
static struct conn conn_list[CONNECTION_SIZE];
static binary_msg_handler g_binary_handler = NULL;

// 辅助函数：断开连接清理内存
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

// 协程客户端处理主流程：相当于同步的 Read -> Parse -> Write
void server_reader(void *arg) {
    // 💡 修复原代码中的是指针逃逸 Bug：直接将 void* 强转回 int 提取 fd
    int fd = (int)(intptr_t)arg; 
    
    while (1) {
        // 1. 确保 rbuffer 有足够的剩余空间 (跟 Reactor/Proactor 逻辑一致)
        if (conn_list[fd].rcapacity - conn_list[fd].rlength < 4096) {
            int new_capacity = conn_list[fd].rcapacity * 2;
            if (new_capacity < 4096) new_capacity = 4096;
            char *new_buf = (char *)kvs_realloc(conn_list[fd].rbuffer, new_capacity);
            if (!new_buf) goto exit_coroutine;
            conn_list[fd].rbuffer = new_buf;
            conn_list[fd].rcapacity = new_capacity;
        }

        int remaining_space = conn_list[fd].rcapacity - conn_list[fd].rlength;
        
        // 协程底层的 recv 会在没有数据时自动 yield 让出 CPU
        int ret = recv(fd, conn_list[fd].rbuffer + conn_list[fd].rlength, remaining_space, 0);
        
        if (ret <= 0) { // 对方关闭或发生错误
            goto exit_coroutine;
        }
        
        conn_list[fd].rlength += ret;

        // 2. ========= 粘包半包解析逻辑 =========
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
                break; // 等待下一次 recv 补齐数据
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
        }

        // 3. ========= 发送响应逻辑 =========
        if (conn_list[fd].wlength > 0) {
            int total_sent = 0;
            // 协程的 send 可能会写一半，加上循环确保写完
            while (total_sent < conn_list[fd].wlength) {
                int s_ret = send(fd, conn_list[fd].wbuffer + total_sent, conn_list[fd].wlength - total_sent, 0);
                if (s_ret <= 0) {
                    goto exit_coroutine;
                }
                total_sent += s_ret;
            }

            // 发送完毕，重置 wlength 并按需缩容内存
            conn_list[fd].wlength = 0;
            
            if (conn_list[fd].wcapacity > INIT_BUFFER_SIZE * 4) {
                char *shrunk_buf = (char *)kvs_realloc(conn_list[fd].wbuffer, INIT_BUFFER_SIZE);
                if (shrunk_buf) {
                    conn_list[fd].wbuffer = shrunk_buf;
                    conn_list[fd].wcapacity = INIT_BUFFER_SIZE;
                }
            }
        }
    }

exit_coroutine:
    close_and_free_connection(fd);
}


// 监听协程
void server(void *arg) {
    unsigned short port = *(unsigned short *)arg;

    int fd = socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) return ;

    // 解决端口被占用(Time-Wait)的问题
    int opt = 1;
    setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));

    struct sockaddr_in local, remote;
    local.sin_family = AF_INET;
    local.sin_port = htons(port);
    local.sin_addr.s_addr = INADDR_ANY;
    bind(fd, (struct sockaddr*)&local, sizeof(struct sockaddr_in));

    listen(fd, 20);
    printf("NtyCo Binary Server listening on port : %d\n", port);

    while (1) {
        socklen_t len = sizeof(struct sockaddr_in);
        // 协程底层的 accept 也会挂起，有新连接再继续
        int cli_fd = accept(fd, (struct sockaddr*)&remote, &len);
        
        if (cli_fd < 0) continue;
        if (cli_fd >= CONNECTION_SIZE) {
            close(cli_fd);
            continue;
        }

        // 为新连接分配初始 4KB 内存
        conn_list[cli_fd].fd = cli_fd;
        conn_list[cli_fd].rcapacity = INIT_BUFFER_SIZE;
        conn_list[cli_fd].rbuffer = (char*)kvs_malloc(INIT_BUFFER_SIZE);
        conn_list[cli_fd].wcapacity = INIT_BUFFER_SIZE;
        conn_list[cli_fd].wbuffer = (char*)kvs_malloc(INIT_BUFFER_SIZE);
        conn_list[cli_fd].rlength = 0;
        conn_list[cli_fd].wlength = 0;

        if (!conn_list[cli_fd].rbuffer || !conn_list[cli_fd].wbuffer) {
            close_and_free_connection(cli_fd);
            continue;
        }

        // 创建专属的 Reader 协程去服务这个客户端
        nty_coroutine *read_co;
        // 💡 修复：将 cli_fd 的值直接转成指针存入 arg，避免指针指向同一块栈内存导致脏数据
        nty_coroutine_create(&read_co, server_reader, (void*)(intptr_t)cli_fd);
    }
}


// 暴露给外部统一规范的启动入口
int ntyco_start(unsigned short port, binary_msg_handler handler) {
    g_binary_handler = handler;

    nty_coroutine *co = NULL;
    nty_coroutine_create(&co, server, &port);

    nty_schedule_run(); // 启动协程调度器，内部会死循环跑 epoll

    return 0;
}