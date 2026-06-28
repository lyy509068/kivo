#ifndef _GNU_SOURCE
#define _GNU_SOURCE  
#endif

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/mman.h>
#include "network.h"
#include "rdma.h"


struct rdma_ring_ctx* rdma_ring_init(const char *dev_name) {
    struct ibv_device **dev_list = ibv_get_device_list(NULL);
    if (!dev_list) return NULL;

    struct rdma_ring_ctx *rctx = calloc(1, sizeof(struct rdma_ring_ctx));
    if (!rctx) goto err;

    rctx->channel = ibv_create_comp_channel(rctx->ctx);
    if (!rctx->channel) goto err;

    rctx->pd = ibv_alloc_pd(rctx->ctx);

    rctx->cq = ibv_create_cq(rctx->ctx, 500, NULL, rctx->channel, 0); 
    if (!rctx->cq) goto err;

    ibv_req_notify_cq(rctx->cq, 0);

    struct ibv_device *ib_dev = dev_list[0]; 
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

int rdma_ring_configure(struct rdma_ring_ctx *rctx, struct ring_meta *remote) {
    struct ibv_qp_attr attr;
    int flags;
    int ret; // 接收内核错误码

    // ------------------ 1. INIT 阶段 ------------------
    memset(&attr, 0, sizeof(attr));
    attr.qp_state = IBV_QPS_INIT;
    attr.port_num = 1;
    attr.qp_access_flags = IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_WRITE | IBV_ACCESS_REMOTE_READ;
    flags = IBV_QP_STATE | IBV_QP_PKEY_INDEX | IBV_QP_PORT | IBV_QP_ACCESS_FLAGS;
    
    ret = ibv_modify_qp(rctx->qp, &attr, flags);
    if (ret != 0) {
        fprintf(stderr, "[RDMA Kernel Error] QP transition to INIT failed: %s (error_code: %d)\n", 
                strerror(ret), ret);
        return -1;
    }

    // ------------------ 2. RTR (Ready to Receive) 阶段 ------------------
    memset(&attr, 0, sizeof(attr));
    attr.qp_state = IBV_QPS_RTR;
    attr.path_mtu = IBV_MTU_1024;
    attr.dest_qp_num = remote->qpn;
    attr.rq_psn = 0;
    attr.max_dest_rd_atomic = 1;
    attr.min_rnr_timer = 12;

    attr.ah_attr.is_global = 1;           // 开启全局路由历史 (RoCE 必备)
    attr.ah_attr.dlid = 0;                // RoCE 环境下 LID 必须强制为 0
    attr.ah_attr.sl = 0;
    attr.ah_attr.src_path_bits = 0;
    attr.ah_attr.port_num = 1;
    
    attr.ah_attr.grh.dgid = remote->gid;  // 绑定刚刚跨网络传过来的远端从机 GID
    attr.ah_attr.grh.flow_label = 0;
    attr.ah_attr.grh.hop_limit = 64;
    attr.ah_attr.grh.sgid_index = 1;      // 本地 GID 索引。注：如果测试依然报 22，可以尝试改为 1 或 2 (对应 RoCE v2)

    flags = IBV_QP_STATE | IBV_QP_AV | IBV_QP_PATH_MTU | IBV_QP_DEST_QPN | IBV_QP_RQ_PSN | IBV_QP_MAX_DEST_RD_ATOMIC | IBV_QP_MIN_RNR_TIMER;
    
    ret = ibv_modify_qp(rctx->qp, &attr, flags);
    if (ret != 0) {
        fprintf(stderr, "[RDMA Kernel Error] QP transition to RTR failed: %s (error_code: %d)\n", 
                strerror(ret), ret);
        fprintf(stderr, "[RDMA Hint] If error is 'Invalid argument'(22), check if your environment is RoCE (Ethernet). RoCE requires dlid=0 and is_global=1.\n");
        return -2; // 返回 -2 代表死在 RTR
    }

    // ------------------ 3. RTS (Ready to Send) 阶段 ------------------
    memset(&attr, 0, sizeof(attr));
    attr.qp_state = IBV_QPS_RTS;
    attr.sq_psn = 0;
    attr.timeout = 14;
    attr.retry_cnt = 7;
    attr.rnr_retry = 7;
    attr.max_rd_atomic = 1;
    flags = IBV_QP_STATE | IBV_QP_SQ_PSN | IBV_QP_TIMEOUT | IBV_QP_RETRY_CNT | IBV_QP_RNR_RETRY | IBV_QP_MAX_QP_RD_ATOMIC;
    
    ret = ibv_modify_qp(rctx->qp, &attr, flags);
    if (ret != 0) {
        fprintf(stderr, "[RDMA Kernel Error] QP transition to RTS failed: %s (error_code: %d)\n", 
                strerror(ret), ret);
        return -3; // 返回 -3 代表死在 RTS
    }

    memcpy(&rctx->remote_meta, remote, sizeof(struct ring_meta));
    return 0;
}

void rdma_ring_destroy(struct rdma_ring_ctx *rctx) {
    if (!rctx) return;
    if (rctx->qp) ibv_destroy_qp(rctx->qp);
    if (rctx->mr_buf) ibv_dereg_mr(rctx->mr_buf);
    if (rctx->mr_meta) ibv_dereg_mr(rctx->mr_meta);
    if (rctx->buffer) munmap(rctx->buffer, RING_BUFFER_SIZE);
    if (rctx->local_meta) munmap(rctx->local_meta, sizeof(struct ring_meta));
    if (rctx->cq) ibv_destroy_cq(rctx->cq);
    if (rctx->channel) ibv_comp_channel_destroy(rctx->channel);
    if (rctx->pd) ibv_dealloc_pd(rctx->pd);
    if (rctx->ctx) ibv_close_device(rctx->ctx);
    free(rctx);
}

extern struct rdma_ring_ctx *g_rdma_ctx; 

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

int rdma_master_write_log_imm(struct rdma_ring_ctx *rctx, uint32_t log_size) {
    if (!rctx || log_size == 0) return -1;

    // 1. 配置本地数据源地址（SGE）
    struct ibv_sge sge = {
        .addr   = (uint64_t)(uintptr_t)rctx->buffer, // 本地要发送的日志缓存
        .length = log_size,
        .lkey   = rctx->mr_buf->lkey
    };

    // 2. 核心配置：构建带立即数的单边写工作请求 (WR)
    struct ibv_send_wr wr = {
        .wr_id      = 99, // 自定义标识符，用于 CQ 校验
        .next       = NULL,
        .sg_list    = &sge,
        .num_sge    = 1,
        .opcode     = IBV_WR_RDMA_WRITE_WITH_IMM, // ✨ 核心：带立即数的单边写
        .send_flags = IBV_SEND_SIGNALED,          // 本地发送完也产生完成事件
        .imm_data   = htonl(log_size),            // ✨ 核心：将长度作为立即数注入包头（转网络字节序）
        .wr.rdma = {
            .remote_addr = rctx->remote_meta.buf_va, // 远端物理内存虚拟首地址
            .rkey        = rctx->remote_meta.rkey    // 远端访问密钥
        }
    };

    struct ibv_send_wr *bad_wr = NULL;
    // 3. 交给网卡硬件硬件发射
    if (ibv_post_send(rctx->qp, &wr, &bad_wr) != 0) {
        perror("[RDMA Master] ibv_post_send failed");
        return -2;
    }

    // 4. 同步等待本地网卡把数据彻底推出去（高吞吐场景下，主端发出即可，此处等待确保安全）
    struct ibv_wc wc;
    while (ibv_poll_cq(rctx->cq, 1, &wc) == 0);

    if (wc.status != IBV_WC_SUCCESS) {
        fprintf(stderr, "[RDMA Master] Send CQ expansion error: %s (status: %d)\n", 
                ibv_wc_status_str(wc.status), wc.status);
        return -3;
    }

    return 0; // 发送成功
}

int rdma_slave_post_recv_envelope(struct rdma_ring_ctx *rctx, uint64_t wr_id) {
    struct ibv_recv_wr wr = {
        .wr_id   = wr_id, // 槽位 ID
        .next    = NULL,
        .sg_list = NULL,  // 不需要指定接收内存，因为单边写数据直接由主端指定目的地直插内存
        .num_sge = 0
    };
    struct ibv_recv_wr *bad_wr = NULL;
    
    int ret = ibv_post_recv(rctx->qp, &wr, &bad_wr);
    if (ret != 0) {
        fprintf(stderr, "[RDMA Slave] Failed to post recv envelope: %s\n", strerror(ret));
    }
    return ret;
}

int rdma_slave_block_and_get_imm(struct rdma_ring_ctx *rctx, uint32_t *out_log_size) {
    struct ibv_cq *cq;
    void *cq_context;

    // 1. 【Kernel Bypass 的精髓】阻塞等待完成通道里的硬中断事件（不占用 CPU）
    if (ibv_get_cq_event(rctx->channel, &cq, &cq_context) != 0) {
        return -1;
    }

    // 2. 确认收到事件确认（RDMA 规范要求）
    ibv_ack_cq_events(cq, 1);

    // 3. 【类似于 Epoll 的每次苏醒重置】重新向网卡申请下一次传输完成通知
    ibv_req_notify_cq(cq, 0);

    // 4. 从完成队列 (CQ) 中把完成包拉出来
    struct ibv_wc wc;
    int num_completions = ibv_poll_cq(cq, 1, &wc);
    if (num_completions <= 0) {
        return 0; // 空事件或抖动
    }

    if (wc.status != IBV_WC_SUCCESS) {
        fprintf(stderr, "[RDMA Slave] CQE status error: %s\n", ibv_wc_status_str(wc.status));
        return -2;
    }

    // 5. 判定是否为对端推过来的双边立即数通知
    if (wc.opcode & IBV_WC_RECV) {
        // ✨ 从传输层 Header 中直接剥离主端写进来的立即数，转换为主机字节序
        *out_log_size = ntohl(wc.imm_data); 
        
        // 6. 拿走一个立即数，意味着消耗了一个接收槽，必须当场再给网卡补齐一个！
        rdma_slave_post_recv_envelope(rctx, wc.wr_id);
        
        return 1; // 成功收到日志信号
    }

    return 0;
}

