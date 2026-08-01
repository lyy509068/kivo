#include "rdma.h" // 根据你的实际文件名修改
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <arpa/inet.h>

#define MAX_RECV_WR 16

struct rdma_ring_ctx* rdma_ring_init(const char *dev_name) {
    struct rdma_ring_ctx *rctx = calloc(1, sizeof(*rctx));
    if (!rctx) return NULL;

    int num_devices;
    struct ibv_device **dev_list = ibv_get_device_list(&num_devices);
    if (!dev_list) goto err;

    // 选择设备
    struct ibv_device *ib_dev = NULL;
    for (int i = 0; i < num_devices; i++) {
        if (!dev_name || strcmp(ibv_get_device_name(dev_list[i]), dev_name) == 0) {
            ib_dev = dev_list[i];
            break;
        }
    }
    if (!ib_dev) goto err;

    rctx->ctx = ibv_open_device(ib_dev);
    ibv_free_device_list(dev_list);
    if (!rctx->ctx) goto err;

    rctx->pd = ibv_alloc_pd(rctx->ctx);
    if (!rctx->pd) goto err;

    // 只有 Recv 才需要使用 channel 进行事件阻塞唤醒
    rctx->channel = ibv_create_comp_channel(rctx->ctx);
    if (!rctx->channel) goto err;

    /* 【核心修复1】分离 Send CQ 和 Recv CQ */
    // send_cq: 不绑定 channel，直接无阻塞轮询，避免与事件机制冲突
    rctx->send_cq = ibv_create_cq(rctx->ctx, 128, NULL, NULL, 0);
    // recv_cq: 绑定 channel，用于 block 等待对方的 imm
    rctx->recv_cq = ibv_create_cq(rctx->ctx, MAX_RECV_WR, NULL, rctx->channel, 0);
    if (!rctx->send_cq || !rctx->recv_cq) goto err;

    // 预先向 recv_cq 申请事件通知 (仅关注 Recv)
    if (ibv_req_notify_cq(rctx->recv_cq, 0) != 0) goto err;

    struct ibv_qp_init_attr qp_attr = {
        .send_cq = rctx->send_cq, // 绑定自己的发送队列
        .recv_cq = rctx->recv_cq, // 绑定自己的接收队列
        .cap = {
            .max_send_wr = 128,
            .max_recv_wr = MAX_RECV_WR,
            .max_send_sge = 1,
            .max_recv_sge = 1,
        },
        .qp_type = IBV_QPT_RC,
    };

    rctx->qp = ibv_create_qp(rctx->pd, &qp_attr);
    if (!rctx->qp) goto err;

    // 分配并注册大块 Ring Buffer
    posix_memalign((void**)&rctx->buffer, 4096, RING_BUFFER_SIZE);
    rctx->mr_buf = ibv_reg_mr(rctx->pd, rctx->buffer, RING_BUFFER_SIZE,
                              IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_WRITE | IBV_ACCESS_REMOTE_READ);
    
    // 分配并注册 Meta 控制元数据
    posix_memalign((void**)&rctx->local_meta, 4096, sizeof(struct ring_meta));
    memset(rctx->local_meta, 0, sizeof(struct ring_meta));
    rctx->mr_meta = ibv_reg_mr(rctx->pd, rctx->local_meta, sizeof(struct ring_meta),
                               IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_WRITE | IBV_ACCESS_REMOTE_READ);

    // 填充本地 meta 的信息以供交换
    rctx->local_meta->buf_va = (uint64_t)(uintptr_t)rctx->buffer;
    rctx->local_meta->rkey   = rctx->mr_buf->rkey;
    rctx->local_meta->qpn    = rctx->qp->qp_num;

    // 我们可以直接获取 GID 给握手使用 (假设 GID index 为 1，取决于 RoCEv2 环境)
    ibv_query_gid(rctx->ctx, 1, 1, &rctx->local_meta->gid);

    return rctx;

err:
    rdma_ring_destroy(rctx);
    return NULL;
}

int rdma_ring_configure(struct rdma_ring_ctx *rctx, struct ring_meta *remote) {
    // 拷贝远端信息
    memcpy(&rctx->remote_meta, remote, sizeof(struct ring_meta));

    // INIT
    struct ibv_qp_attr attr = {
        .qp_state = IBV_QPS_INIT,
        .pkey_index = 0,
        .port_num = 1,
        .qp_access_flags = IBV_ACCESS_REMOTE_WRITE | IBV_ACCESS_REMOTE_READ
    };
    if (ibv_modify_qp(rctx->qp, &attr, IBV_QP_STATE | IBV_QP_PKEY_INDEX | IBV_QP_PORT | IBV_QP_ACCESS_FLAGS))
        return -1;

    // RTR (Ready To Receive)
    attr.qp_state = IBV_QPS_RTR;
    attr.path_mtu = IBV_MTU_1024;
    attr.dest_qp_num = remote->qpn;
    attr.rq_psn = 0;
    attr.max_dest_rd_atomic = 1;
    attr.min_rnr_timer = 12;
    attr.ah_attr.is_global = 1;
    attr.ah_attr.dlid = 0;
    attr.ah_attr.sl = 0;
    attr.ah_attr.src_path_bits = 0;
    attr.ah_attr.port_num = 1;
    memcpy(&attr.ah_attr.grh.dgid, &remote->gid, 16);
    attr.ah_attr.grh.flow_label = 0;
    attr.ah_attr.grh.hop_limit = 255;
    attr.ah_attr.grh.sgid_index = 1;

    if (ibv_modify_qp(rctx->qp, &attr, IBV_QP_STATE | IBV_QP_AV | IBV_QP_PATH_MTU | 
                      IBV_QP_DEST_QPN | IBV_QP_RQ_PSN | IBV_QP_MAX_DEST_RD_ATOMIC | IBV_QP_MIN_RNR_TIMER))
        return -2;

    /* 【核心修复2】在这里挂满最初的 Receive 队列 (布满捕鼠夹) */
    for (int i = 0; i < MAX_RECV_WR; i++) {
        rdma_slave_post_recv_envelope(rctx, i);
    }

    // RTS (Ready To Send)
    attr.qp_state = IBV_QPS_RTS;
    attr.timeout = 14;
    attr.retry_cnt = 7;
    attr.rnr_retry = 7;
    attr.sq_psn = 0;
    attr.max_rd_atomic = 1;

    if (ibv_modify_qp(rctx->qp, &attr, IBV_QP_STATE | IBV_QP_TIMEOUT | 
                      IBV_QP_RETRY_CNT | IBV_QP_RNR_RETRY | IBV_QP_SQ_PSN | IBV_QP_MAX_QP_RD_ATOMIC))
        return -3;

    return 0;
}

int rdma_slave_post_recv_envelope(struct rdma_ring_ctx *rctx, uint64_t wr_id) {
    struct ibv_recv_wr wr;
    struct ibv_recv_wr *bad_wr = NULL;

    memset(&wr, 0, sizeof(wr));
    wr.wr_id   = wr_id;
    wr.sg_list = NULL; // 只是为了接收 IMM 通知，不需要挂载具体的内存接收数据
    wr.num_sge = 0;

    if (ibv_post_recv(rctx->qp, &wr, &bad_wr) != 0) {
        // fprintf(stderr, "Failed to post receive envelope\n");
        return -1;
    }
    return 0;
}

int rdma_master_write_log_imm(struct rdma_ring_ctx *rctx, uint32_t payload_len, uint32_t imm_val) {
    if (!rctx) return -1;

    struct ibv_sge sge;
    memset(&sge, 0, sizeof(sge));

    struct ibv_send_wr wr;
    memset(&wr, 0, sizeof(wr));

    /* 【核心修复3】解决网卡驱动由于 0 字节造成的 IBV_WC_LOC_QP_OP_ERR */
    // 无论 payload_len 是多少(就算是 0)，都必须给定一个合法的内存地址！
    sge.addr   = (uint64_t)(uintptr_t)rctx->buffer; 
    sge.length = payload_len; // 发送 0 字节也是合法的，只要 SGE 存在
    sge.lkey   = rctx->mr_buf->lkey;
    
    wr.sg_list = &sge;
    wr.num_sge = 1; 

    wr.wr_id      = 99; // 标记本次 Send 的标识符
    wr.next       = NULL;
    wr.opcode     = IBV_WR_RDMA_WRITE_WITH_IMM;
    wr.send_flags = IBV_SEND_SIGNALED;
    wr.imm_data   = htonl(imm_val); 

    // 这个逻辑可以加上 Ring Buffer 指针计算，此处按简化的基地址书写
    wr.wr.rdma.remote_addr = rctx->remote_meta.buf_va;
    wr.wr.rdma.rkey        = rctx->remote_meta.rkey;

    struct ibv_send_wr *bad_wr = NULL;
    if (ibv_post_send(rctx->qp, &wr, &bad_wr) != 0) {
        fprintf(stderr, "[RDMA] ibv_post_send failed\n");
        return -2;
    }

    struct ibv_wc wc;
    while (1) {
        /* 【核心修复1】只 poll send_cq，绝对不会误吞并丢弃 Recv 的事件 */
        int poll_result = ibv_poll_cq(rctx->send_cq, 1, &wc);
        if (poll_result > 0) {
            if (wc.wr_id == 99) break; // 成功探测到自己的发送完成
        } else if (poll_result < 0) {
            return -3;
        }
    }

    if (wc.status != IBV_WC_SUCCESS) {
        fprintf(stderr, "[RDMA] Send Failed. status: %s(%d)\n", 
                ibv_wc_status_str(wc.status), wc.status);
        return -4;
    }
    return 0;
}

int rdma_slave_block_and_get_imm(struct rdma_ring_ctx *rctx, uint32_t *out_log_size) {
    struct ibv_cq *ev_cq;
    void *ev_ctx;

    // 1. 阻塞等待 Event Channel 通知
    if (ibv_get_cq_event(rctx->channel, &ev_cq, &ev_ctx) != 0) {
        return -1;
    }

    // 2. 确认收到事件，必须调用防止事件通道积压
    ibv_ack_cq_events(ev_cq, 1);

    // 3. 必须再次请求通知，为下一次 block 做准备
    if (ibv_req_notify_cq(ev_cq, 0) != 0) {
        return -2;
    }

    // 4. 从 recv_cq 中取出工作完成报告 (WC)
    struct ibv_wc wc;
    int polled = 0;
    while ((polled = ibv_poll_cq(ev_cq, 1, &wc)) == 0); // 取出直到成功

    if (polled < 0 || wc.status != IBV_WC_SUCCESS) {
        fprintf(stderr, "[RDMA] Recv Failed. status: %s(%d)\n", 
                ibv_wc_status_str(wc.status), wc.status);
        return -3;
    }

    // 5. 校验操作码是否为我们期望的
    if (wc.opcode & IBV_WC_RECV_RDMA_WITH_IMM) {
        if (out_log_size) {
            *out_log_size = ntohl(wc.imm_data); 
        }
    }

    /* 【关键机制】消费了一个捕鼠夹，必须立刻原样填补回去 */
    rdma_slave_post_recv_envelope(rctx, wc.wr_id);

    return 0;
}

void rdma_ring_destroy(struct rdma_ring_ctx *rctx) {
    if (!rctx) return;
    
    if (rctx->qp) ibv_destroy_qp(rctx->qp);
    
    if (rctx->send_cq) ibv_destroy_cq(rctx->send_cq);
    if (rctx->recv_cq) ibv_destroy_cq(rctx->recv_cq);
    
    if (rctx->channel) ibv_destroy_comp_channel(rctx->channel);
    
    if (rctx->mr_buf) ibv_dereg_mr(rctx->mr_buf);
    if (rctx->mr_meta) ibv_dereg_mr(rctx->mr_meta);
    
    if (rctx->buffer) free(rctx->buffer);
    if (rctx->local_meta) free(rctx->local_meta);
    
    if (rctx->pd) ibv_dealloc_pd(rctx->pd);
    if (rctx->ctx) ibv_close_device(rctx->ctx);
    
    free(rctx);
}