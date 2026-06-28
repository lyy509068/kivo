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

// 定义一个静态变量，用来存从端线程的 ID
static pthread_t repl_slave_tid; 

static int g_slave_fd = -1;
static bool g_repl_ctx_ready = false;
static struct repl_context g_repl_ctx = { .fd = -1, .wbuffer = NULL, .wcapacity = 0, .wlength = 0 };

struct rdma_ring_ctx *g_rdma_ctx = NULL; 

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

    // 全局 eBPF 引擎加载（主端快车道拦截专用）
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


int repl_start_slave_engine(void) {
    if (!g_rdma_ctx) {
        fprintf(stderr, "[Repl Error] RDMA context is NULL, cannot start slave engine\n");
        return -1;
    }

    // 预先铺设 8 个捕鼠夹（挂载初始接收槽）
    for (int i = 0; i < 8; i++) {
        rdma_slave_post_recv_envelope(g_rdma_ctx, i); 
    }

    // 槽位挂好后，修改运行标志，正式拉起纯 RDMA 接收后台线程
    g_running = 1;
    if (pthread_create(&repl_slave_tid, NULL, pure_rdma_repl_slave_thread, NULL) != 0) {
        fprintf(stderr, "[Repl Error] Failed to create pure RDMA slave thread\n");
        return -1;
    }

    printf("[Repl Slave] Handshake complete. Pure RDMA background replication engine IS RUNNING.\n");
    return 0;
}

// 前置声明常规网络层接收回调
extern void recv_cb(int fd); 
extern struct conn conn_list[];

// 从端专用：RDMA 握手阶段的 TCP 读事件拦截器
void slave_rdma_handshake_read_cb(int fd) {
    struct conn *c = &conn_list[fd];
    
    // 1. 安全读取主端回传的 TCP 凭证数据
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

    // 2. 协议拦截与轻量化解析 (寻找 RDMA_CONNECT_ACK)
    // 主端发来的是: *5\r\n$16\r\nRDMA_CONNECT_ACK\r\n$<rkey_len>\r\n<rkey>...
    if (strstr(c->rbuffer, "RDMA_CONNECT_ACK") != NULL) {
        printf("[Protocol Slave] Intercepted RDMA_CONNECT_ACK successfully!\n");

        // 借助已有的通用流解析器提取出 resp_request_t 结构
        // 如果你的系统有现成的解析函数，可以直接调用。这里我们用简易的 RESP 参数切割演示：
        resp_request_t req;
        int parsed_bytes = 0;
        
        // 🛠️ 假设你的 g_stream_handler 内部会把 c->rbuffer 转化为 req 结构
        // 这里可以直接复用你现有的 handle_slave_rdma_connect_ack 逻辑：
        
        /* ------------- 开始配置本地 RDMA 硬件 ------------- */
        struct ring_meta master_meta;
        
        // 简易解析示范：根据你的标准 RESP 布局，从业务层解出 4 个核心参数
        // 实际项目中推荐调用你统一的 RESP 了解包函数
        char *lines[16];
        int line_idx = 0;
        char *token = strtok(c->rbuffer, "\r\n");
        while (token && line_idx < 16) {
            lines[line_idx++] = token;
            token = strtok(NULL, "\r\n");
        }

        // 分别对应: 
        // lines[0]=*5, lines[2]=RDMA_CONNECT_ACK, lines[4]=rkey, lines[6]=vaddr, lines[8]=qpn, lines[10]=gid
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

            // 将从端本地的 QP 状态机一步到位配置到 RTS
            if (rdma_ring_configure(g_rdma_ctx, &master_meta) != 0) {
                fprintf(stderr, "[Protocol Slave Error] Failed to configure Slave QP to RTS!\n");
                close_and_free_connection(fd);
                return;
            }
            printf("[Protocol Slave] RDMA Pipeline is now RTS (Ready to Receive).\n");

            // 3. 【阶段三关键动作】向主端定向发射 SYNC 信号
            const char *sync_cmd = "*1\r\n$4\r\nSYNC\r\n";
            int sync_len = strlen(sync_cmd);
            if (send(fd, sync_cmd, sync_len, 0) != sync_len) {
                perror("[Protocol Slave Error] Failed to send SYNC command via TCP");
                close_and_free_connection(fd);
                return;
            }
            printf("[Protocol Slave] SYNC request injected into TCP channel. Handshake over.\n");

            // 4. 拉起你的纯 RDMA 后台异步接收线程 (准备在后台抓取主端 DMA 灌过来的日志)
            repl_start_slave_engine();

            /* ------------- 🏁 核心状态机复位切换 ------------- */
            c->rlength = 0;                      // 清空当前控制面缓冲区
            c->is_receiving_file = 1;            // 🎯 架起拦截高墙，允许网络层挂起等待日志
            c->read_callback = recv_cb;          // 🎯 卸磨杀驴，把接听回调无缝换回常规的 recv_cb！
            
            // 刷新 Epoll 状态
            reactor_set_event(fd, EPOLLIN, 0);
        } else {
            fprintf(stderr, "[Protocol Slave Error] Malformed RDMA_CONNECT_ACK packet.\n");
            close_and_free_connection(fd);
        }
    } else {
        // 如果读到了杂质数据或者半包，继续保留拦截器等待下一次读取
        printf("[Protocol Slave] Buffering handshake data...\n");
    }
}


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

    // 此时是阻塞模式连接
    if (connect(g_repl_ctx.fd, (struct sockaddr*)&master_addr, sizeof(master_addr)) < 0) {
        perror("Slave: Connect to master failed");
        close(g_repl_ctx.fd);
        g_repl_ctx.fd = -1;
        return -1; 
    }

    // 初始化发送缓冲区
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
    
    // RDMA 安全拦截
    if (!g_rdma_ctx || !g_rdma_ctx->mr_buf || !g_rdma_ctx->qp) {
        fprintf(stderr, "[Repl Error] Slave local RDMA context is not ready before Handshake!\n");
        close(g_repl_ctx.fd);
        g_repl_ctx.fd = -1;
        return -1;
    }

    // 动态提取从端本地的 RDMA 凭证
    uint32_t my_rkey = g_rdma_ctx->mr_buf->rkey;
    uint64_t my_vaddr = (uint64_t)(uintptr_t)g_rdma_ctx->buffer;
    uint32_t my_qpn = g_rdma_ctx->qp->qp_num;

    // 查询从端本地的 GID
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

    // 将 16 字节的 GID 转化为 32 位 Hex 字符串
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
    
    // 复制到缓冲区
    if (g_repl_ctx.wlength + cmd_len <= g_repl_ctx.wcapacity) {
        memcpy(g_repl_ctx.wbuffer + g_repl_ctx.wlength, sync_cmd, cmd_len);
        g_repl_ctx.wlength += cmd_len;
    }

    // 阻塞安全发送
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

    // 升级为非阻塞模式
    int flags = fcntl(g_repl_ctx.fd, F_GETFL, 0);
    if (flags >= 0) {
        fcntl(g_repl_ctx.fd, F_SETFL, flags | O_NONBLOCK);
    }

    // 托管连接到 Reactor
    struct conn *c = reactor_host_slave_connection(g_repl_ctx.fd, g_repl_ctx.wbuffer, g_repl_ctx.wcapacity, g_repl_ctx.wlength);
    if (c == NULL) {
        close(g_repl_ctx.fd);
        g_repl_ctx.fd = -1;
        return -1;
    }

    // RDMA 上下文绑定
    rdma_init_context(c);

    conn_list[g_repl_ctx.fd].read_callback = slave_rdma_handshake_read_cb;

    printf("[Repl Slave] Pure RDMA Handshake sent. Network layer callback overrode to interceptor. Waiting for Master metadata...\n");
    return g_repl_ctx.fd;
}

int repl_sync_log_via_rdma(void) {
    if (!g_repl_ctx_ready || !g_rdma_ctx) {
        printf("[Repl Error] RDMA context not ready for log sync.\n");
        return -1;
    }
    printf("[Repl] Intercepted SYNC command. Starting RDMA Zero-Copy with IMM...\n");

    // 1. 打开本地持久化 AOF 日志文件
    int fd = open(PERSISTENCE_FILE, O_RDONLY);
    if (fd < 0) {
        perror("[Repl Error] Failed to open persistence file");
        return -1;
    }

    // 2. 获取文件大小
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

    // 3. 将 AOF 文件内容读到已注册 MR 的本地缓冲区
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

void repl_destroy(void) {
    #if ENABLE_REPLICATION_SLAVE
    // 如果从端线程在运行，先下达终止令
    if (g_running) {
        g_running = 0;
        
        // 销毁底层的 RDMA 上下文，这会关闭完成通道，强行弹开阻塞在硬件事件上的线程
        if (g_rdma_ctx) {
            rdma_ring_destroy(g_rdma_ctx);
            g_rdma_ctx = NULL;
        }
        
        // 安全收尸，回收线程栈资源
        if (repl_slave_tid) {
            pthread_join(repl_slave_tid, NULL);
            repl_slave_tid = 0;
        }
        printf("[Repl] Slave replication thread destroyed cleanly.\n");
    }
    #else
    // 主端直接销毁底层的 RDMA 资源即可
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

void* pure_rdma_repl_slave_thread(void *arg) {
    printf("[Repl Slave Thread] Pure RDMA replication engine started.\n");
    
    while (g_running) {
        uint32_t incoming_log_size = 0;
        
        // 阻塞等待，当主端数据写完且发出 IMM 时，这里瞬间惊醒
        int ret = rdma_slave_block_and_get_imm(g_rdma_ctx, &incoming_log_size);
        if (ret == 1) {
            printf("[Repl Slave] Hardware DMA Sync Done. Size: %u bytes. Dumping...\n", incoming_log_size);

            // 落盘：从挂载的 g_rdma_ctx->buffer 头部抓取最新内容
            int local_fd = open(PERSISTENCE_FILE, O_WRONLY | O_CREAT | O_APPEND, 0644);
            if (local_fd >= 0) {
                write(local_fd, g_rdma_ctx->buffer, incoming_log_size);
                fsync(local_fd);
                close(local_fd);
            }

            // 内存加载重放
            kvs_persistence_recover();
            printf("[Repl Slave] Reload successfully!\n");
        }
    }
    return NULL;
}

// 主端专用：处理从端发来的 RDMA_CONNECT 握手请求
void handle_master_rdma_connect(resp_request_t *req, char **wbuf, int *wcap, int *wlen) {
    printf("[Protocol Master] Intercepted RDMA_CONNECT. Shaking hands with slave...\n");
    
    // 剥离并解析从端的硬件凭证
    struct ring_meta slave_meta;
    slave_meta.rkey = (uint32_t)strtoul(req->argv[1], NULL, 10);
    slave_meta.buf_va = (uint64_t)strtoull(req->argv[2], NULL, 10);
    slave_meta.qpn = (uint32_t)strtoul(req->argv[3], NULL, 10);
    for (int i = 0; i < 16; i++) {
        unsigned int b;
        sscanf(req->argv[4] + i * 2, "%02x", &b);
        slave_meta.gid.raw[i] = (uint8_t)b;
    }

    // 配置主端本地 QP 状态机到 RTS 状态（大路通车！）
    if (rdma_ring_configure(g_rdma_ctx, &slave_meta) != 0) {
        fprintf(stderr, "[Protocol Master Error] Failed to configure Master QP to RTS!\n");
    } else {
        printf("[Protocol Master] RDMA Pipeline is now RTS (Ready to Send).\n");
    }

    // 准备主端本地的 RDMA 凭证并打包回传给从端
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

    // 同样组装成 5 参数的 RESP 数组，动词为 RDMA_CONNECT_ACK
    char ack_cmd[512];
    int ack_len = sprintf(ack_cmd, "*5\r\n$16\r\nRDMA_CONNECT_ACK\r\n$%zu\r\n%s\r\n$%zu\r\n%s\r\n$%zu\r\n%s\r\n$%zu\r\n%s\r\n", 
                          strlen(rkey_str), rkey_str, 
                          strlen(vaddr_str), vaddr_str,
                          strlen(qpn_str), qpn_str,
                          strlen(gid_str), gid_str);

    // 将 ACK 回复挂载到网络层的写缓冲区中
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


// 从端专用：处理主端荡回来的 RDMA_CONNECT_ACK 响应
int handle_slave_rdma_connect_ack(resp_request_t *req, int fd) {
    printf("[Protocol Slave] Intercepted RDMA_CONNECT_ACK. Configuring local hardware...\n");
    
    // 解析主端传过来的硬件凭证
    struct ring_meta master_meta;
    master_meta.rkey = (uint32_t)strtoul(req->argv[1], NULL, 10);
    master_meta.buf_va = (uint64_t)strtoull(req->argv[2], NULL, 10);
    master_meta.qpn = (uint32_t)strtoul(req->argv[3], NULL, 10);
    for (int i = 0; i < 16; i++) {
        unsigned int b;
        sscanf(req->argv[4] + i * 2, "%02x", &b);
        master_meta.gid.raw[i] = (uint8_t)b;
    }

    // 将从端的本地 QP 状态机配置到 RTS 状态
    if (rdma_ring_configure(g_rdma_ctx, &master_meta) != 0) {
        fprintf(stderr, "[Protocol Slave Error] Failed to configure Slave QP to RTS!\n");
        return -1;
    }
    printf("[Protocol Slave] RDMA Pipeline is now RTS (Ready to Receive).\n");

    // 发送日志命令
    const char *sync_cmd = "*1\r\n$4\r\nSYNC\r\n"; 
    int sync_len = strlen(sync_cmd);
    
    int sent = send(fd, sync_cmd, sync_len, 0);
    if (sent != sync_len) {
        perror("[Protocol Slave Error] Failed to send SYNC command via TCP");
        return -1;
    }
    printf("[Protocol Slave] SYNC request pipeline injected into TCP channel successfully.\n");

    // 唤醒纯 RDMA 后台异步接收引擎
    repl_start_slave_engine();

    return 20; 
}




