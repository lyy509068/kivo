#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif

#include "repl.h"
#include "resp.h"
#include "rdma.h"
#include "ebpf.h"
#include "network.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
#include <fcntl.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <sys/epoll.h>

#define REPL_INIT_BUFFER_SIZE 4096
#define DEFAULT_RDMA_DEVICE "rxe0"

volatile int g_running = 1; 
static pthread_t repl_slave_tid; 
static int g_slave_fd = -1;
static bool g_repl_ctx_ready = false;
static struct repl_context g_repl_ctx = { .fd = -1, .wbuffer = NULL, .wcapacity = 0, .wlength = 0 };

struct rdma_ring_ctx *g_rdma_ctx = NULL; 

extern void recv_cb(int fd); 
extern struct conn conn_list[];

/**
 * @brief  功能：全系统复制引擎的初始化，负责拉起本地 RDMA 硬件上下文以及加载 eBPF 模块。
 * @note   调用层级：系统启动层。在系统 main 函数启动、解析完配置文件后被统一调用。
 */
int repl_init(const char *rdma_dev, const char *ebpf_obj_path, int slave_fd) {
    g_slave_fd = slave_fd;

    const char *final_dev = rdma_dev ? rdma_dev : DEFAULT_RDMA_DEVICE;
    printf("[Repl] Initializing RDMA engine on device: %s\n", final_dev);

    g_rdma_ctx = rdma_ring_init(final_dev);
    if (!g_rdma_ctx) {
        printf("[Repl Error] Failed to init RDMA hardware on device: %s\n", final_dev);
        g_repl_ctx_ready = false; 
        return -1;
    }

    #if ENABLE_REPLICATION_MASTER
    if (ebpf_obj_path) {
        if (ebpf_init_loader(ebpf_obj_path) < 0) {
            printf("[Repl Error] Failed to load eBPF byte code\n");
            g_repl_ctx_ready = false;
            return -1;
        }
    }
    #endif

    g_repl_ctx_ready = true;
    return 0;
}

/**
 * @brief  功能：动态控制 eBPF 内核快车道转发开关。
 * @note   调用层级：控制面业务层。通常在全量 AOF 同步结束后，由主端根据状态机动态调用开启。
 */
int repl_set_ebpf_switch(bool enable) {
    if (!g_repl_ctx_ready) return -1;
    
    int status = ebpf_set_forward_switch(enable ? 1 : 0);
    if (status == 0) {
        printf("[Repl] Successfully %s eBPF kernel fast-forwarding.\n", enable ? "ENABLED" : "DISABLED");
    } else {
        perror("[Repl] Failed to set eBPF switch");
    }
    return status;
}

/**
 * @brief  功能：安全销毁复制引擎。终止后台接收线程，释放 TCP 缓冲区并关闭底层的 RDMA 硬件资源。
 * @note   调用层级：系统退出层。在整个进程准备优雅退出、释放全局资源时调用。
 */
void repl_destroy(void) {
    #if ENABLE_REPLICATION_SLAVE
    if (g_running) {
        g_running = 0;
        
        if (g_rdma_ctx) {
            rdma_ring_destroy(g_rdma_ctx);
            g_rdma_ctx = NULL;
        }
        
        if (repl_slave_tid) {
            pthread_join(repl_slave_tid, NULL);
            repl_slave_tid = 0;
        }
        printf("[Repl] Slave replication thread destroyed cleanly.\n");
    }
    #else
    if (g_rdma_ctx) {
        rdma_ring_destroy(g_rdma_ctx);
        g_rdma_ctx = NULL;
    }
    #endif

    if (g_repl_ctx.wbuffer) {
        free(g_repl_ctx.wbuffer);
        g_repl_ctx.wbuffer = NULL;
    }
    if (g_repl_ctx.fd >= 0) {
        close(g_repl_ctx.fd);
        g_repl_ctx.fd = -1;
    }
}

/**
 * @brief  功能：从端主动向主端发起 TCP 连接，打包本地 RDMA 凭证通过控制面发送，并挂载事件拦截器。
 * @note   调用层级：从端启动/重连主控层。当从端通过命令或配置触发“追随主端”动作时调用。
 */
int repl_connect_to_master(const char *master_ip, unsigned short master_port) {
    g_repl_ctx.fd = socket(AF_INET, SOCK_STREAM, 0);
    if (g_repl_ctx.fd < 0) {
        perror("Slave: Create socket failed");
        return -1;
    }

    struct sockaddr_in master_addr;
    memset(&master_addr, 0, sizeof(master_addr));
    master_addr.sin_family = AF_INET;
    master_addr.sin_port = htons(master_port);
    master_addr.sin_addr.s_addr = inet_addr(master_ip);

    if (connect(g_repl_ctx.fd, (struct sockaddr*)&master_addr, sizeof(master_addr)) < 0) {
        perror("Slave: Connect to master failed");
        close(g_repl_ctx.fd);
        g_repl_ctx.fd = -1;
        return -1; 
    }

    if (g_repl_ctx.wbuffer == NULL) {
        g_repl_ctx.wcapacity = REPL_INIT_BUFFER_SIZE;
        g_repl_ctx.wbuffer = (char *)kvs_malloc(g_repl_ctx.wcapacity);
        if (!g_repl_ctx.wbuffer) {
            close(g_repl_ctx.fd);
            g_repl_ctx.fd = -1;
            return -1;
        }
    }
    g_repl_ctx.wlength = 0;
    
    printf("Slave: Successfully connected to Master at %s:%d\n", master_ip, master_port);
    
    if (!g_rdma_ctx || !g_rdma_ctx->mr_buf || !g_rdma_ctx->qp) {
        fprintf(stderr, "[Repl Error] Slave local RDMA context is not ready before Handshake!\n");
        close(g_repl_ctx.fd);
        g_repl_ctx.fd = -1;
        return -1;
    }

    uint32_t my_rkey = g_rdma_ctx->mr_buf->rkey;
    uint64_t my_vaddr = (uint64_t)(uintptr_t)g_rdma_ctx->buffer;
    uint32_t my_qpn = g_rdma_ctx->qp->qp_num;

    union ibv_gid my_gid;
    memset(&my_gid, 0, sizeof(my_gid));
    if (ibv_query_gid(g_rdma_ctx->ctx, 1, 1, &my_gid) != 0) {
        fprintf(stderr, "[Repl Error] Failed to query local GID!\n");
    }

    char rkey_str[32], vaddr_str[64], qpn_str[32];
    char gid_str[64] = {0}; 

    sprintf(rkey_str, "%u", my_rkey);
    sprintf(vaddr_str, "%lu", my_vaddr);
    sprintf(qpn_str, "%u", my_qpn);

    char *p = gid_str;
    for (int i = 0; i < 16; i++) {
        p += sprintf(p, "%02x", my_gid.raw[i]);
    }

    char sync_cmd[512];
    int cmd_len = sprintf(sync_cmd, "*5\r\n$12\r\nRDMA_CONNECT\r\n$%zu\r\n%s\r\n$%zu\r\n%s\r\n$%zu\r\n%s\r\n$%zu\r\n%s\r\n", 
                          strlen(rkey_str), rkey_str, 
                          strlen(vaddr_str), vaddr_str,
                          strlen(qpn_str), qpn_str,
                          strlen(gid_str), gid_str);
    
    if (g_repl_ctx.wlength + cmd_len <= g_repl_ctx.wcapacity) {
        memcpy(g_repl_ctx.wbuffer + g_repl_ctx.wlength, sync_cmd, cmd_len);
        g_repl_ctx.wlength += cmd_len;
    }

    int total_sent = 0;
    while (total_sent < g_repl_ctx.wlength) {
        int sent = send(g_repl_ctx.fd, g_repl_ctx.wbuffer + total_sent, g_repl_ctx.wlength - total_sent, 0);
        if (sent <= 0) {
            perror("Slave: Synchronous send RDMA_CONNECT failed");
            close(g_repl_ctx.fd);
            g_repl_ctx.fd = -1;
            return -1;
        }
        total_sent += sent;
    }
    
    g_repl_ctx.wlength = 0; 

    int flags = fcntl(g_repl_ctx.fd, F_GETFL, 0);
    if (flags >= 0) {
        fcntl(g_repl_ctx.fd, F_SETFL, flags | O_NONBLOCK);
    }

    struct conn *c = reactor_host_slave_connection(g_repl_ctx.fd, g_repl_ctx.wbuffer, g_repl_ctx.wcapacity, g_repl_ctx.wlength);
    if (c == NULL) {
        close(g_repl_ctx.fd);
        g_repl_ctx.fd = -1;
        return -1;
    }

    rdma_init_context(c);
    conn_list[g_repl_ctx.fd].read_callback = slave_rdma_handshake_read_cb;

    printf("[Repl Slave] Pure RDMA Handshake sent. Network layer callback overrode to interceptor. Waiting for Master metadata...\n");
    return g_repl_ctx.fd;
}

/**
 * @brief  功能：网络层控制面读事件拦截器（半连接过渡态专用）。负责拦截并抽取主端传回的 RDMA_CONNECT_ACK。
 * @note   调用层级：Reactor 事件驱动层。当主端通过控制面返回 ACK 凭证时，由 Epoll 异步触发调用该回调。
 */
void slave_rdma_handshake_read_cb(int fd) {
    struct conn *c = &conn_list[fd];
    int total_new_bytes = 0;

    while (1) {
        if (c->rcapacity - c->rlength < 1024) {
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

    if (strstr(c->rbuffer, "RDMA_CONNECT_ACK") != NULL) {
        printf("[Protocol Slave] Intercepted RDMA_CONNECT_ACK successfully!\n");
        resp_request_t req;
        int parsed_bytes = 0;
        struct ring_meta master_meta;
        char *lines[16];
        int line_idx = 0;
        char *token = strtok(c->rbuffer, "\r\n");

        while (token && line_idx < 16) {
            lines[line_idx++] = token;
            token = strtok(NULL, "\r\n");
        }

        if (line_idx >= 11) {
            master_meta.rkey   = (uint32_t)strtoul(lines[4], NULL, 10);
            master_meta.buf_va = (uint64_t)strtoull(lines[6], NULL, 10); 
            master_meta.qpn    = (uint32_t)strtoul(lines[8], NULL, 10);
            
            char *gid_str = lines[10];
            for (int i = 0; i < 16; i++) {
                unsigned int b;
                sscanf(gid_str + i * 2, "%02x", &b);
                master_meta.gid.raw[i] = (uint8_t)b;
            }

            if (rdma_ring_configure(g_rdma_ctx, &master_meta) != 0) {
                fprintf(stderr, "[Protocol Slave Error] Failed to configure Slave QP to RTS!\n");
                close_and_free_connection(fd);
                return;
            }
            printf("[Protocol Slave] RDMA Pipeline is now RTS (Ready to Receive).\n");

            const char *sync_cmd = "*1\r\n$4\r\nSYNC\r\n";
            int sync_len = strlen(sync_cmd);
            if (send(fd, sync_cmd, sync_len, 0) != sync_len) {
                perror("[Protocol Slave Error] Failed to send SYNC command via TCP");
                close_and_free_connection(fd);
                return;
            }
            printf("[Protocol Slave] SYNC request injected into TCP channel. Handshake over.\n");

            repl_start_slave_engine();

            c->rlength = 0;                      
            c->is_receiving_file = 1;            
            c->read_callback = recv_cb;          
            
            reactor_set_event(fd, EPOLLIN, 0);
        } else {
            fprintf(stderr, "[Protocol Slave Error] Malformed RDMA_CONNECT_ACK packet.\n");
            close_and_free_connection(fd);
        }
    } else {
        printf("[Protocol Slave] Buffering handshake data...\n");
    }
}

/**
 * @brief  功能：显式处理主端发回的 RDMA_CONNECT_ACK 的备用传统函数。配置本地硬件状态机，并发送 SYNC 同步指令。
 * @note   调用层级：协议分发层。如果系统中未开启拦截器挂载，则由普通网络层解析出命令后派发至此。
 */
int handle_slave_rdma_connect_ack(resp_request_t *req, int fd) {
    printf("[Protocol Slave] Intercepted RDMA_CONNECT_ACK. Configuring local hardware...\n");
    
    struct ring_meta master_meta;
    master_meta.rkey = (uint32_t)strtoul(req->argv[1], NULL, 10);
    master_meta.buf_va = (uint64_t)strtoull(req->argv[2], NULL, 10);
    master_meta.qpn = (uint32_t)strtoul(req->argv[3], NULL, 10);
    for (int i = 0; i < 16; i++) {
        unsigned int b;
        sscanf(req->argv[4] + i * 2, "%02x", &b);
        master_meta.gid.raw[i] = (uint8_t)b;
    }

    if (rdma_ring_configure(g_rdma_ctx, &master_meta) != 0) {
        fprintf(stderr, "[Protocol Slave Error] Failed to configure Slave QP to RTS!\n");
        return -1;
    }
    printf("[Protocol Slave] RDMA Pipeline is now RTS (Ready to Receive).\n");

    const char *sync_cmd = "*1\r\n$4\r\nSYNC\r\n"; 
    int sync_len = strlen(sync_cmd);
    
    int sent = send(fd, sync_cmd, sync_len, 0);
    if (sent != sync_len) {
        perror("[Protocol Slave Error] Failed to send SYNC command via TCP");
        return -1;
    }
    printf("[Protocol Slave] SYNC request pipeline injected into TCP channel successfully.\n");

    repl_start_slave_engine();
    return 20; 
}

/**
 * @brief  功能：铺设从端本地初始的 RDMA 接收槽（加锁挂载），并正式拉起专门负责数据同步落盘的后台专属线程。
 * @note   调用层级：从端握手收尾层。在解析到主端合法凭证并将 QP 切换至 RTS 状态后被自动调用。
 */
int repl_start_slave_engine(void) {
    if (!g_rdma_ctx) {
        fprintf(stderr, "[Repl Error] RDMA context is NULL, cannot start slave engine\n");
        return -1;
    }

    for (int i = 0; i < 8; i++) {
        rdma_slave_post_recv_envelope(g_rdma_ctx, i); 
    }

    g_running = 1;
    if (pthread_create(&repl_slave_tid, NULL, pure_rdma_repl_slave_thread, NULL) != 0) {
        fprintf(stderr, "[Repl Error] Failed to create pure RDMA slave thread\n");
        return -1;
    }

    printf("[Repl Slave] Handshake complete. Pure RDMA background replication engine IS RUNNING.\n");
    return 0;
}

/**
 * @brief  功能：从端独立的后台高性能数据接收轮询线程。依靠硬件中断/完成队列（CQ）阻塞等待主端投递全量日志，完成落盘与加载重放。
 * @note   调用层级：独立物理线程。由 `repl_start_slave_engine` 衍生，独立于 Reactor 主循环后台运行。
 */
void* pure_rdma_repl_slave_thread(void *arg) {
    printf("[Repl Slave Thread] Pure RDMA replication engine started.\n");
    
    while (g_running) {
        uint32_t incoming_log_size = 0;
        int ret = rdma_slave_block_and_get_imm(g_rdma_ctx, &incoming_log_size);
        if (ret == 1) {
            printf("[Repl Slave] Hardware DMA Sync Done. Size: %u bytes. Dumping...\n", incoming_log_size);

            int local_fd = open(PERSISTENCE_FILE, O_WRONLY | O_CREAT | O_APPEND, 0644);
            if (local_fd >= 0) {
                write(local_fd, g_rdma_ctx->buffer, incoming_log_size);
                fsync(local_fd);
                close(local_fd);
            }

            kvs_persistence_recover();
            printf("[Repl Slave] Reload successfully!\n");
        }
    }
    return NULL;
}


/**
 * @brief  功能：主端解包从端发来的第一手 RDMA_CONNECT 控制面指令，打通本地 QP，并将主端自身的 RDMA 凭证封装成 RESP 荡回给从端。
 * @note   调用层级：主端协议分发层。主端 TCP 网络层收到普通的集群连接请求，识别到动词为 `RDMA_CONNECT` 时路由分发至此。
 */
void handle_master_rdma_connect(resp_request_t *req, char **wbuf, int *wcap, int *wlen) {
    printf("[Protocol Master] Intercepted RDMA_CONNECT. Shaking hands with slave...\n");
    
    struct ring_meta slave_meta;
    slave_meta.rkey = (uint32_t)strtoul(req->argv[1], NULL, 10);
    slave_meta.buf_va = (uint64_t)strtoull(req->argv[2], NULL, 10);
    slave_meta.qpn = (uint32_t)strtoul(req->argv[3], NULL, 10);
    for (int i = 0; i < 16; i++) {
        unsigned int b;
        sscanf(req->argv[4] + i * 2, "%02x", &b);
        slave_meta.gid.raw[i] = (uint8_t)b;
    }

    if (rdma_ring_configure(g_rdma_ctx, &slave_meta) != 0) {
        fprintf(stderr, "[Protocol Master Error] Failed to configure Master QP to RTS!\n");
    } else {
        printf("[Protocol Master] RDMA Pipeline is now RTS (Ready to Send).\n");
    }

    uint32_t my_rkey = g_rdma_ctx->mr_buf->rkey;
    uint64_t my_vaddr = (uint64_t)(uintptr_t)g_rdma_ctx->buffer;
    uint32_t my_qpn = g_rdma_ctx->qp->qp_num;
    union ibv_gid my_gid;
    memset(&my_gid, 0, sizeof(my_gid));
    ibv_query_gid(g_rdma_ctx->ctx, 1, 1, &my_gid);

    char rkey_str[32], vaddr_str[64], qpn_str[32], gid_str[64];
    sprintf(rkey_str, "%u", my_rkey);
    sprintf(vaddr_str, "%lu", my_vaddr);
    sprintf(qpn_str, "%u", my_qpn);
    char *p = gid_str;
    for (int i = 0; i < 16; i++) {
        p += sprintf(p, "%02x", my_gid.raw[i]);
    }

    char ack_cmd[512];
    int ack_len = sprintf(ack_cmd, "*5\r\n$16\r\nRDMA_CONNECT_ACK\r\n$%zu\r\n%s\r\n$%zu\r\n%s\r\n$%zu\r\n%s\r\n$%zu\r\n%s\r\n", 
                          strlen(rkey_str), rkey_str, 
                          strlen(vaddr_str), vaddr_str,
                          strlen(qpn_str), qpn_str,
                          strlen(gid_str), gid_str);

    if (wbuf && wcap && wlen) {
        int needed = *wlen + ack_len + 1;
        if (needed > *wcap) {
            int new_cap = *wcap * 2;
            if (new_cap < needed) new_cap = needed;
            char *new_buf = (char *)kvs_realloc(*wbuf, new_cap);
            if (new_buf) { *wbuf = new_buf; *wcap = new_cap; }
        }
        memcpy(*wbuf + *wlen, ack_cmd, ack_len);
        *wlen += ack_len;
    }
}

/**
 * @brief  功能：主端数据面零拷贝全量同步函数。拉取本地持久化文件，映射至注册过的 MR，通过高速 RDMA WRITE 带 IMM 远端直写从端内存。
 * @note   调用层级：主端协议命令层。主端接收到从端扔过来的 `SYNC` 命令后，立刻引发此函数，启动大吞吐量直连传输。
 */
int repl_sync_log_via_rdma(void) {
    if (!g_repl_ctx_ready || !g_rdma_ctx) {
        printf("[Repl Error] RDMA context not ready for log sync.\n");
        return -1;
    }
    printf("[Repl] Intercepted SYNC command. Starting RDMA Zero-Copy with IMM...\n");

    int fd = open(PERSISTENCE_FILE, O_RDONLY);
    if (fd < 0) {
        perror("[Repl Error] Failed to open persistence file");
        return -1;
    }

    struct stat st;
    if (fstat(fd, &st) < 0) {
        perror("[Repl Error] fstat failed");
        close(fd);
        return -1;
    }
    size_t file_size = st.st_size;
    if (file_size == 0) {
        printf("[Repl] Log is empty. Nothing to sync.\n");
        close(fd);
        return 0; 
    }

    if (file_size > RING_BUFFER_SIZE) {
        fprintf(stderr, "[Repl Error] Log file size %zu exceeds limit\n", file_size);
        close(fd);
        return -1;
    }

    memset(g_rdma_ctx->buffer, 0, RING_BUFFER_SIZE);
    ssize_t read_bytes = read(fd, g_rdma_ctx->buffer, file_size);
    if (read_bytes != (ssize_t)file_size) {
        perror("[Repl Error] Short read on log file");
        close(fd);
        return -1;
    }
    close(fd); 

    int ret = rdma_master_write_log_imm(g_rdma_ctx, (uint32_t)file_size);
    if (ret != 0) {
        fprintf(stderr, "[Repl Error] Engine failed to write log via IMM. Ret: %d\n", ret);
        return -1;
    }

    printf("[Repl] Successfully pushed %zu bytes to slave and triggered slave hardware event.\n", file_size);
    return 0;
}