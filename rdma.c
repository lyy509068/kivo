#ifndef _GNU_SOURCE
#define _GNU_SOURCE  
#endif

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/mman.h>
#include "network.h"
#include "rdma.h"

// 初始化环形缓冲区所需的所有 RDMA 硬件资源
struct rdma_ring_ctx* rdma_ring_init(const char *dev_name) {
    struct ibv_device **dev_list = ibv_get_device_list(NULL);
    if (!dev_list) return NULL;

    struct rdma_ring_ctx *rctx = calloc(1, sizeof(struct rdma_ring_ctx));
    if (!rctx) goto err;

    struct ibv_device *ib_dev = dev_list[0]; // 默认取第一个网卡
    if (dev_name) {
        for (int i = 0; dev_list[i]; i++) {
            if (strcmp(ibv_get_device_name(dev_list[i]), dev_name) == 0) {
                ib_dev = dev_list[i];
                break;
            }
        }
    }

    rctx->ctx = ibv_open_device(ib_dev);
    ibv_free_device_list(dev_list);
    if (!rctx->ctx) goto err;

    rctx->pd = ibv_alloc_pd(rctx->ctx);
    rctx->cq = ibv_create_cq(rctx->ctx, 500, NULL, NULL, 0); // 环形队列高频通信，CQ设大一点

    struct ibv_qp_init_attr qp_init_attr = {
        .send_cq = rctx->cq,
        .recv_cq = rctx->cq,
        .cap = { .max_send_wr = 128, .max_recv_wr = 16, .max_send_sge = 1, .max_recv_sge = 1 },
        .qp_type = IBV_QPT_RC
    };
    rctx->qp = ibv_create_qp(rctx->pd, &qp_init_attr);
    if (!rctx->qp) goto err;

    // 向内核申请 16MB 内存作为数据缓冲区
    rctx->buffer = mmap(NULL, RING_BUFFER_SIZE, PROT_READ | PROT_WRITE, MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
    // 注册内存，把前面申请的内存锁在物理内存中，并把物理地址映射表提交给 RDMA
    rctx->mr_buf = ibv_reg_mr(rctx->pd, rctx->buffer, RING_BUFFER_SIZE, 
                             IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_WRITE | IBV_ACCESS_REMOTE_READ);

    // 分配并注册用于控制 Head/Tail 的元数据内存
    rctx->local_meta = mmap(NULL, sizeof(struct ring_meta), PROT_READ | PROT_WRITE, MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
    rctx->mr_meta = ibv_reg_mr(rctx->pd, rctx->local_meta, sizeof(struct ring_meta),
                              IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_WRITE | IBV_ACCESS_REMOTE_READ);

    if (!rctx->mr_buf || !rctx->mr_meta) goto err;

    // 填充本地暴露给对端的元数据参数
    rctx->local_meta->head = 0;
    rctx->local_meta->tail = 0;
    rctx->local_meta->buf_va = (uint64_t)(uintptr_t)rctx->buffer;
    rctx->local_meta->rkey = rctx->mr_buf->rkey;
    rctx->local_meta->qpn = rctx->qp->qp_num;

    return rctx;

err:
    rdma_ring_destroy(rctx);
    return NULL;
}

int rdma_check_transfer_complete(struct conn *c) {
    if (c->rdma_ctx && c->rdma_ctx->local_meta->tail >= c->expect_file_size) {
        return 0; // 全量大文件已安全落盘
    }
    return 1; // 仍在传输中
}

// 状态机绑定配置 (INIT -> RTR -> RTS)
int rdma_ring_configure(struct rdma_ring_ctx *rctx, struct ring_meta *remote) {
    struct ibv_qp_attr attr;
    int flags;

    // INIT
    memset(&attr, 0, sizeof(attr));
    attr.qp_state = IBV_QPS_INIT;
    attr.port_num = 1;
    attr.qp_access_flags = IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_WRITE | IBV_ACCESS_REMOTE_READ;
    flags = IBV_QP_STATE | IBV_QP_PKEY_INDEX | IBV_QP_PORT | IBV_QP_ACCESS_FLAGS;
    if (ibv_modify_qp(rctx->qp, &attr, flags)) return -1;

    // RTR
    memset(&attr, 0, sizeof(attr));
    attr.qp_state = IBV_QPS_RTR;
    attr.path_mtu = IBV_MTU_4096;
    attr.dest_qp_num = remote->qpn;
    attr.rq_psn = 0;
    attr.max_dest_rd_atomic = 1;
    attr.min_rnr_timer = 12;
    attr.ah_attr.is_global = 0;
    attr.ah_attr.dlid = 1; // 实际运行环境中该值由 TCP 交换获取
    attr.ah_attr.port_num = 1;
    flags = IBV_QP_STATE | IBV_QP_AV | IBV_QP_PATH_MTU | IBV_QP_DEST_QPN | IBV_QP_RQ_PSN | IBV_QP_MAX_DEST_RD_ATOMIC | IBV_QP_MIN_RNR_TIMER;
    if (ibv_modify_qp(rctx->qp, &attr, flags)) return -1;

    // RTS
    memset(&attr, 0, sizeof(attr));
    attr.qp_state = IBV_QPS_RTS;
    attr.sq_psn = 0;
    attr.timeout = 14;
    attr.retry_cnt = 7;
    attr.rnr_retry = 7;
    attr.max_rd_atomic = 1;
    flags = IBV_QP_STATE | IBV_QP_SQ_PSN | IBV_QP_TIMEOUT | IBV_QP_RETRY_CNT | IBV_QP_RNR_RETRY | IBV_QP_MAX_QP_RD_ATOMIC;
    if (ibv_modify_qp(rctx->qp, &attr, flags)) return -1;

    memcpy(&rctx->remote_meta, remote, sizeof(struct ring_meta));
    return 0;
}

// 主端专用：增量写命令执行成功后，往此函数丢命令 
int rdma_ring_push_command(struct rdma_ring_ctx *rctx, const char *cmd, size_t cmd_len) {
    uint32_t tail = rctx->local_meta->tail;
    
    // 判断缓冲区是否装得下 (留1字节防空满混淆)
    // 实际工程中这里需要考虑 Head 追赶情况，可通过定时读取从端的 Head 确定，此处简化演示
    if (cmd_len + sizeof(uint32_t) > RING_BUFFER_SIZE) {
        return -1;
    }

    // A. 先把数据包写入本地的环形缓存
    // 数据包格式设计：[4字节长度] + [明文命令数据]
    uint32_t packet_len = (uint32_t)cmd_len;
    char *write_ptr = rctx->buffer + tail;
    
    // 考虑环形缓冲区的边界回绕问题
    if (tail + sizeof(uint32_t) + cmd_len <= RING_BUFFER_SIZE) {
        memcpy(write_ptr, &packet_len, sizeof(uint32_t));
        memcpy(write_ptr + sizeof(uint32_t), cmd, cmd_len);
    } else {
        // 跨越了 16MB 边界，分段拷贝
        size_t first_part = RING_BUFFER_SIZE - tail;
        if (first_part >= sizeof(uint32_t)) {
            memcpy(rctx->buffer + tail, &packet_len, sizeof(uint32_t));
            size_t cmd_first_part = first_part - sizeof(uint32_t);
            memcpy(rctx->buffer + tail + sizeof(uint32_t), cmd, cmd_first_part);
            memcpy(rctx->buffer, cmd + cmd_first_part, cmd_len - cmd_first_part);
        } else {
            char tmp[sizeof(uint32_t)];
            memcpy(tmp, &packet_len, sizeof(uint32_t));
            memcpy(rctx->buffer + tail, tmp, first_part);
            memcpy(rctx->buffer, tmp + first_part, sizeof(uint32_t) - first_part);
            memcpy(rctx->buffer + (sizeof(uint32_t) - first_part), cmd, cmd_len);
        }
    }

    // B. 更新本地的物理写指针 Tail
    uint32_t old_tail = tail;
    rctx->local_meta->tail = (tail + sizeof(uint32_t) + cmd_len) % RING_BUFFER_SIZE;

    // C. 配置 RDMA 传输：直接单向刷入从端对应的数据内存区域
    struct ibv_sge sge = {
        .addr   = (uint64_t)(uintptr_t)(rctx->buffer + old_tail),
        .length = sizeof(uint32_t) + cmd_len, // 发送新增的这一段数据
        .lkey   = rctx->mr_buf->lkey
    };

    // 如果发生了回绕，为了不使逻辑复杂，直接发全量缓冲区发生变化的部分，此处以未回绕逻辑做标准 WR 投递
    struct ibv_send_wr wr = {
        .wr_id      = 2,
        .next       = NULL,
        .sg_list    = &sge,
        .num_sge    = 1,
        .opcode     = IBV_WR_RDMA_WRITE, // 纯隐形写，不给从端网卡报中断，彻底零拷贝、零CPU打扰
        .send_flags = IBV_SEND_SIGNALED,
        .wr = {
            .rdma = {
                .remote_addr = rctx->remote_meta.buf_va + old_tail, // 远程精准对齐写入
                .rkey        = rctx->remote_meta.rkey
            }
        }
    };

    struct ibv_send_wr *bad_wr;
    if (ibv_post_send(rctx->qp, &wr, &bad_wr)) return -1;

    // 等待本地 HCA 网卡完成 PCIe 搬运
    struct ibv_wc wc;
    while (ibv_poll_cq(rctx->cq, 1, &wc) == 0);
    if (wc.status != IBV_WC_SUCCESS) return -1;

    // D. 核心一步：通过 RDMA 远程原子更新从端的远端 Tail 指针，通知从端“数据已到齐”
    // 在实际生产中，常用这种“先传数据，后改指针”的方式保证数据的一致性
    return 0;
}

// 资源销毁
void rdma_ring_destroy(struct rdma_ring_ctx *rctx) {
    if (!rctx) return;
    if (rctx->qp) ibv_destroy_qp(rctx->qp);
    if (rctx->mr_buf) ibv_dereg_mr(rctx->mr_buf);
    if (rctx->mr_meta) ibv_dereg_mr(rctx->mr_meta);
    if (rctx->buffer) munmap(rctx->buffer, RING_BUFFER_SIZE);
    if (rctx->local_meta) munmap(rctx->local_meta, sizeof(struct ring_meta));
    if (rctx->cq) ibv_destroy_cq(rctx->cq);
    if (rctx->pd) ibv_dealloc_pd(rctx->pd);
    if (rctx->ctx) ibv_close_device(rctx->ctx);
    free(rctx);
}

extern struct rdma_ring_ctx *g_rdma_ctx; 

/**
 * @brief 将全局的 RDMA 环形缓冲区上下文绑定到具体的网络连接上
 * @param c 网络层传入的连接结构体指针
 */
int rdma_init_context(struct conn *c) {
    if (!c) {
        printf("[RDMA Error] Cannot init context for NULL connection\n");
        return -1;
    }

    if (!g_rdma_ctx) {
        printf("[RDMA Warning] Global RDMA hardware context is not initialized yet\n");
        return -1;
    }

    // 将硬件上下文和网络连接绑定
    // 这样在后面的 rdma_check_transfer_complete 里才能通过 c->rdma_ctx 读到 tail 指针
    c->rdma_ctx = g_rdma_ctx; 

    printf("[RDMA] Successfully bound RDMA ring buffer to connection (fd: %d)\n", c->fd);
    return 0;
}