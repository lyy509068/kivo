#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif

#include "repl.h"
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

#define REPL_INIT_BUFFER_SIZE 4096

static int g_slave_fd = -1;
static bool g_repl_ctx_ready = false;
static struct repl_context g_repl_ctx = { .fd = -1, .wbuffer = NULL, .wcapacity = 0, .wlength = 0 };

struct rdma_ring_ctx *g_rdma_ctx = NULL; // 定义全局全局 RDMA 上下文

int repl_init(const char *rdma_dev, const char *ebpf_obj_path, int slave_fd) {
    g_slave_fd = slave_fd;

    // 全局的 RDMA 资源初始化
    if (rdma_dev) {
        g_rdma_ctx = rdma_ring_init(rdma_dev);
        if (!g_rdma_ctx) {
            printf("[Repl Error] Failed to init RDMA hardware\n");
            return -1;
        }
    }

    // 全局 eBPF 引擎加载
    #if  ENABLE_REPLICATION_SLAVE
    if (ebpf_obj_path) {
        if (ebpf_init_loader(ebpf_obj_path) < 0) {
            printf("[Repl Error] Failed to load eBPF byte code\n");
            return -1;
        }
    }
    #endif

    g_repl_ctx_ready = true;
    return 0;
}

// TCP 握手与 Reactor 托孤
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
    
    int flags = fcntl(g_repl_ctx.fd, F_GETFL, 0);
    if (flags >= 0) {
        fcntl(g_repl_ctx.fd, F_SETFL, flags | O_NONBLOCK);
    }

    printf("Slave: Successfully connected to Master at %s:%d\n", master_ip, master_port);
    
    // 发送 RESP 协议的同步命令
    const char *sync_cmd = "*1\r\n$4\r\nSYNC\r\n"; 
    int cmd_len = strlen(sync_cmd);

    if (g_repl_ctx.wlength + cmd_len <= g_repl_ctx.wcapacity) {
        memcpy(g_repl_ctx.wbuffer + g_repl_ctx.wlength, sync_cmd, cmd_len);
        g_repl_ctx.wlength += cmd_len;
    }

    int sent = send(g_repl_ctx.fd, g_repl_ctx.wbuffer, g_repl_ctx.wlength, 0);
    if (sent > 0) {
        g_repl_ctx.wlength -= sent; 
    }

    // 托孤给网络层 Reactor，由底层响应主端发回的数据
    if (reactor_host_slave_connection(g_repl_ctx.fd, g_repl_ctx.wbuffer, g_repl_ctx.wcapacity, g_repl_ctx.wlength) < 0) {
        close(g_repl_ctx.fd);
        g_repl_ctx.fd = -1;
        return -1;
    }

    return g_repl_ctx.fd;
}

// RDMA发送日志
int repl_sync_log_via_rdma(void) {
    if (!g_repl_ctx_ready || !g_rdma_ctx) {
        printf("[Repl Error] RDMA context not ready for log sync.\n");
        return -1;
    }
    printf("[Repl] Intercepted SYNC command. Starting RDMA Zero-Copy for Logs...\n");

    // 打开本地持久化 AOF 日志文件
    int fd = open(PERSISTENCE_FILE, O_RDONLY);
    if (fd < 0) {
        perror("[Repl Error] Failed to open persistence file");
        return -1;
    }
    // 获取文件大小，防止越界
    struct stat st;
    if (fstat(fd, &st) < 0) {
        perror("[Repl Error] fstat persistence file failed");
        close(fd);
        return -1;
    }
    size_t file_size = st.st_size;
    if (file_size == 0) {
        printf("[Repl] Persistence file is empty. Nothing to sync.\n");
        close(fd);
        return 0; // 空文件不传输
    }

    // 环形缓冲区大小上限检查（16MB）
    if (file_size > RING_BUFFER_SIZE) {
        fprintf(stderr, "[Repl Error] Log file size (%zu) exceeds RDMA buffer limit (%d)\n", file_size, RING_BUFFER_SIZE);
        close(fd);
        return -1;
    }

    // 将文件内容读取到已经注册过 MR 的 RDMA 本地缓冲区
    // 必须用已注册内存，网卡硬件才能做 DMA 搬运   这里是从端注册一块内存吗？帮我检查我的代码中哪个地方注册了内存？
    memset(g_rdma_ctx->buffer, 0, RING_BUFFER_SIZE);
    ssize_t read_bytes = read(fd, g_rdma_ctx->buffer, file_size);
    if (read_bytes != (ssize_t)file_size) {
        perror("[Repl Error] Failed to read full file into RDMA buffer");
        close(fd);
        return -1;
    }
    close(fd); // 读完立刻释放文件句柄

    // 配置 RDMA Write 传输：直接单向刷入从端数据内存的【起始位置(offset 0)】
    struct ibv_sge sge = {
        .addr   = (uint64_t)(uintptr_t)g_rdma_ctx->buffer,
        .length = (uint32_t)file_size,
        .lkey   = g_rdma_ctx->mr_buf->lkey
    };

    struct ibv_send_wr wr = {
        .wr_id      = 999, // 历史全量同步专属 ID
        .next       = NULL,
        .sg_list    = &sge,
        .num_sge    = 1,
        .opcode     = IBV_WR_RDMA_WRITE, // 硬件单向隐形写
        .send_flags = IBV_SEND_SIGNALED,
        .wr = {
            .rdma = {
                // 从零开始覆盖从端的环形缓存区，完成历史数据注入
                .remote_addr = g_rdma_ctx->remote_meta.buf_va, 
                .rkey        = g_rdma_ctx->remote_meta.rkey
            }
        }
    };

    // 投递给 HCA 网卡
    struct ibv_send_wr *bad_wr;
    if (ibv_post_send(g_rdma_ctx->qp, &wr, &bad_wr)) {
        perror("[Repl Error] ibv_post_send failed during historical sync");
        return -1;
    }

    // 阻塞等待本地网卡通过 PCIe 把数据全部送上物理链路
    struct ibv_wc wc;
    while (ibv_poll_cq(g_rdma_ctx->cq, 1, &wc) == 0);
    if (wc.status != IBV_WC_SUCCESS) {
        fprintf(stderr, "[Repl Error] RDMA write failed with status: %d\n", wc.status);
        return -1;
    }

    printf("[Repl] Successfully synced %zu bytes of historical logs via RDMA Zero-Copy.\n", file_size);
    return 0;
}

// 从端专用，打开 ebpf 闸门，准备接收增量命令，我的理解正确吗
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
    if (g_repl_ctx.wbuffer) {
        free(g_repl_ctx.wbuffer);
        g_repl_ctx.wbuffer = NULL;
    }
    if (g_repl_ctx.fd >= 0) {
        close(g_repl_ctx.fd);
        g_repl_ctx.fd = -1;
    }
}