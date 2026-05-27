#include "nty_coroutine.h"
#include <arpa/inet.h>
#include <string.h>
#include <unistd.h>
#include <stdlib.h>
#include <stdint.h>
#include <errno.h>
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

// 协程客户端处理主流程：支持多包连续动态解析与即时分发
void server_reader(void *arg) {
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
        
        // 协程底层的 recv 会在没有数据时自动 yield 让出 CPU 并挂起
        int ret = recv(fd, conn_list[fd].rbuffer + conn_list[fd].rlength, remaining_space, 0);
        
        if (ret <= 0) { 
            if (errno == EINTR) continue; // 被信号中断则重试
            goto exit_coroutine;
        }
        
        conn_list[fd].rlength += ret;

        // 2. ========= 核心状态机循环：多包连续解析并立即回写 =========
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
                // 当前大包的数据还没收全，退出状态机，回到外层循环继续协程 recv
                break; 
            }

            // 当前大包的业务数据已完全集齐，开始消费
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

            // 裁剪已经处理完的当前包数据
            int remaining_data = conn_list[fd].rlength - total_batch_bytes;
            if (remaining_data > 0) {
                memmove(conn_list[fd].rbuffer, conn_list[fd].rbuffer + total_batch_bytes, remaining_data);
            }
            conn_list[fd].rlength = remaining_data;

            // 3. 【核心修改】解完一包，立即发送一包的响应！
            // 这样如果是多包粘包，可以在本 While 循环里不停地解包->发送，不会错误地回到外层被 recv 挂起
            if (conn_list[fd].wlength > 0) {
                int total_sent = 0;
                while (total_sent < conn_list[fd].wlength) {
                    // 协程的 send 在无法完全写入时也会自动 yield 让出 CPU，写就绪后自动唤醒恢复
                    int s_ret = send(fd, conn_list[fd].wbuffer + total_sent, conn_list[fd].wlength - total_sent, 0);
                    if (s_ret <= 0) {
                        if (errno == EINTR) continue;
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

            // 防御性异常保护：防止产生死循环
            if (total_batch_bytes == 0) {
                break;
            }
        }
    }

exit_coroutine:
    close_and_free_connection(fd);
}


// 监听协程
void server(void *arg) {
    // 💡 修复：通过强转纯数值提取端口号，彻底规避外部栈帧销毁带来的崩溃隐患
    unsigned short port = (unsigned short)(uintptr_t)arg;

    int fd = socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) return ;

    int opt = 1;
    setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));

    struct sockaddr_in local, remote;
    local.sin_family = AF_INET;
    local.sin_port = htons(port);
    local.sin_addr.s_addr = INADDR_ANY;
    
    if (bind(fd, (struct sockaddr*)&local, sizeof(struct sockaddr_in)) < 0) {
        perror("bind");
        close(fd);
        return;
    }

    listen(fd, 20);
    printf("NtyCo Binary Server listening on port : %d\n", port);

    while (1) {
        socklen_t len = sizeof(struct sockaddr_in);
        int cli_fd = accept(fd, (struct sockaddr*)&remote, &len);
        
        if (cli_fd < 0) {
            if (errno == EINTR) continue;
            continue;
        }
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

        nty_coroutine *read_co;
        nty_coroutine_create(&read_co, server_reader, (void*)(intptr_t)cli_fd);
    }
}


// 暴露给外部统一规范的启动入口
int ntyco_start(unsigned short port, binary_msg_handler handler) {
    g_binary_handler = handler;

    nty_coroutine *co = NULL;
    // 💡 修复：将端口号直接转为 void* 传递，杜绝传局部变量指针引发的野指针风险
    nty_coroutine_create(&co, server, (void*)(uintptr_t)port);

    nty_schedule_run(); 

    return 0;
}