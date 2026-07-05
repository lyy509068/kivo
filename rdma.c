#ifndef _GNU_SOURCE
#define _GNU_SOURCE  
#endif

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/mman.h>
#include <arpa/inet.h>   
#include <infiniband/verbs.h>
#include "network.h"
#include "rdma.h"

// 内部辅助函数：动态获取指定网卡上的 RoCEv2 IPv4 GID 索引，彻底消灭 Hardcode 22 错误
static int rdma_get_local_rocev2_gid_index(struct ibv_context *ctx, int port_num) {
    union ibv_gid tg;
    // 优先寻找符合 RoCEv2 IPv4 特征的 GID 槽位 (::ffff:x.x.x.x)
    for (int i = 0; i < 16; i++) {
        if (ibv_query_gid(ctx, port_num, i, &tg) != 0) break;
        if (tg.global.interface_id == 0 && tg.global.subnet_prefix == 0) continue;

        if (tg.raw[10] == 0xff && tg.raw[11] == 0xff) {
            return i; // 精准匹配到 RoCEv2 IPv4 索引
        }
    }
    // 次级备选：寻找任何一个非空的有效的本地 GID
    for (int i = 0; i < 16; i++) {
        if (ibv_query_gid(ctx, port_num, i, &tg) == 0) {
            if (tg.global.interface_id != 0 || tg.global.subnet_prefix != 0) {
                return i;
            }
        }
    }
    return 0; // 保底
}

struct rdma_ring_ctx* rdma_ring_init(const char *dev_name) {
    struct ibv_device **dev_list = ibv_get_device_list(NULL);
    if (!dev_list) {
        fprintf(stderr, "[RDMA Init Error] No RDMA devices found on this system.\n");
        return NULL;
    }

    struct rdma_ring_ctx *rctx = calloc(1, sizeof(struct rdma_ring_ctx));
    if (!rctx) {
        ibv_free_device_list(dev_list);
        return NULL;
    }

    // 精准匹配指定的网卡名，若指定了却找不到则直接熔断
    struct ibv_device *ib_dev = NULL; 
    if (dev_name) {
        for (int i = 0; dev_list[i]; i++) {
            if (strcmp(ibv_get_device_name(dev_list[i]), dev_name) == 0) {
                ib_dev = dev_list[i];
                break;
            }
        }
        if (!ib_dev) {
            fprintf(stderr, "[RDMA Init Error] Specified device '%s' not found!\n", dev_name);
            ibv_free_device_list(dev_list);
            free(rctx);
            return NULL;
        }
    } else {
        ib_dev = dev_list[0]; 
    }

    printf("[RDMA] Successfully attached to hardware device: %s\n", ibv_get_device_name(ib_dev));

    rctx->ctx = ibv_open_device(ib_dev);
    ibv_free_device_list(dev_list); 
    if (!rctx->ctx) goto err;

    rctx->channel = ibv_create_comp_channel(rctx->ctx);
    if (!rctx->channel) goto err;

    rctx->pd = ibv_alloc_pd(rctx->ctx);
    if (!rctx->pd) goto err;

    rctx->cq = ibv_create_cq(rctx->ctx, 500, NULL, rctx->channel, 0); 
    if (!rctx->cq) goto err;

    if (ibv_req_notify_cq(rctx->cq, 0) != 0) goto err;

    struct ibv_qp_init_attr qp_init_attr = {
        .send_cq = rctx->cq,
        .recv_cq = rctx->cq,
        .cap = { .max_send_wr = 128, .max_recv_wr = 16, .max_send_sge = 1, .max_recv_sge = 1 },
        .qp_type = IBV_QPT_RC
    };
    rctx->qp = ibv_create_qp(rctx->pd, &qp_init_attr);
    if (!rctx->qp) goto err;

    // 使用 MAP_SHARED 规避 fork 场景下的内存解绑隐患
    rctx->buffer = mmap(NULL, RING_BUFFER_SIZE, PROT_READ | PROT_WRITE, MAP_SHARED | MAP_ANONYMOUS, -1, 0);
    if (rctx->buffer == MAP_FAILED) goto err;
    
    rctx->mr_buf = ibv_reg_mr(rctx->pd, rctx->buffer, RING_BUFFER_SIZE, 
                             IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_WRITE | IBV_ACCESS_REMOTE_READ);

    rctx->local_meta = mmap(NULL, sizeof(struct ring_meta), PROT_READ | PROT_WRITE, MAP_SHARED | MAP_ANONYMOUS, -1, 0);
    if (rctx->local_meta == MAP_FAILED) goto err;
    
    rctx->mr_meta = ibv_reg_mr(rctx->pd, rctx->local_meta, sizeof(struct ring_meta),
                              IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_WRITE | IBV_ACCESS_REMOTE_READ);

    if (!rctx->mr_buf || !rctx->mr_meta) goto err;

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
        return 0; 
    }
    return 1;
}

int rdma_ring_configure(struct rdma_ring_ctx *rctx, struct ring_meta *remote) {
    struct ibv_qp_attr attr;
    int flags;
    int ret;

    // 动态识别当前硬件的传输层协议类型
    enum ibv_transport_type transport = rctx->ctx->device->transport_type;

    // ------------------ 1. INIT 阶段 ------------------
    memset(&attr, 0, sizeof(attr));
    attr.qp_state = IBV_QPS_INIT;
    attr.port_num = 1;
    attr.qp_access_flags = IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_WRITE | IBV_ACCESS_REMOTE_READ;
    flags = IBV_QP_STATE | IBV_QP_PKEY_INDEX | IBV_QP_PORT | IBV_QP_ACCESS_FLAGS;
    
    ret = ibv_modify_qp(rctx->qp, &attr, flags);
    if (ret != 0) {
        fprintf(stderr, "[RDMA Kernel Error] QP to INIT failed: %s (code: %d)\n", strerror(ret), ret);
        return -1;
    }

    // ------------------ 2. RTR (Ready to Receive) 阶段 ------------------
    memset(&attr, 0, sizeof(attr));
    attr.qp_state = IBV_QPS_RTR;

    if (transport == IBV_TRANSPORT_IWARP) {
        // iWARP 协议极其轻量，RTR 状态变更只需要声明状态本身
        flags = IBV_QP_STATE;
    } else {
        // RoCE / InfiniBand 专有链路配置
        int target_local_gid_index = rdma_get_local_rocev2_gid_index(rctx->ctx, 1);
        
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
        
        attr.ah_attr.grh.dgid = remote->gid;  
        attr.ah_attr.grh.flow_label = 0;
        attr.ah_attr.grh.hop_limit = 64;
        attr.ah_attr.grh.sgid_index = target_local_gid_index; 

        flags = IBV_QP_STATE | IBV_QP_AV | IBV_QP_PATH_MTU | IBV_QP_DEST_QPN | 
                IBV_QP_RQ_PSN | IBV_QP_MAX_DEST_RD_ATOMIC | IBV_QP_MIN_RNR_TIMER;
    }
    
    ret = ibv_modify_qp(rctx->qp, &attr, flags);
    if (ret != 0) {
        fprintf(stderr, "[RDMA Kernel Error] QP to RTR failed: %s (code: %d)\n", strerror(ret), ret);
        return -2;
    }

    // ------------------ 3. RTS (Ready to Send) 阶段 ------------------
    memset(&attr, 0, sizeof(attr));
    attr.qp_state = IBV_QPS_RTS;

    if (transport == IBV_TRANSPORT_IWARP) {
        // iWARP RTS 剪除硬重试和硬件超时掩码，避免内核抛出 22 错误
        attr.max_rd_atomic = 1;
        flags = IBV_QP_STATE | IBV_QP_MAX_QP_RD_ATOMIC;
    } else {
        // RoCE / InfiniBand 专有硬件流控与重传配置
        attr.sq_psn = 0;
        attr.timeout = 14;
        attr.retry_cnt = 7;
        attr.rnr_retry = 7;
        attr.max_rd_atomic = 1;
        flags = IBV_QP_STATE | IBV_QP_SQ_PSN | IBV_QP_TIMEOUT | 
                IBV_QP_RETRY_CNT | IBV_QP_RNR_RETRY | IBV_QP_MAX_QP_RD_ATOMIC;
    }
    
    ret = ibv_modify_qp(rctx->qp, &attr, flags);
    if (ret != 0) {
        fprintf(stderr, "[RDMA Kernel Error] QP to RTS failed: %s (code: %d)\n", strerror(ret), ret);
        return -3;
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
    if (rctx->channel) ibv_destroy_comp_channel(rctx->channel);
    if (rctx->pd) ibv_dealloc_pd(rctx->pd);
    if (rctx->ctx) ibv_close_device(rctx->ctx);
    free(rctx);
}

extern struct rdma_ring_ctx *g_rdma_ctx; 

int rdma_init_context(struct conn *c) {
    if (!c || !g_rdma_ctx) return -1;
    c->rdma_ctx = g_rdma_ctx; 
    return 0;
}

int rdma_master_write_log_imm(struct rdma_ring_ctx *rctx, uint32_t log_size) {
    if (!rctx || log_size == 0) return -1;

    struct ibv_sge sge = {
        .addr   = (uint64_t)(uintptr_t)rctx->buffer,
        .length = log_size,
        .lkey   = rctx->mr_buf->lkey
    };

    struct ibv_send_wr wr = {
        .wr_id      = 99, 
        .next       = NULL,
        .sg_list    = &sge,
        .num_sge    = 1,
        .opcode     = IBV_WR_RDMA_WRITE_WITH_IMM,
        .send_flags = IBV_SEND_SIGNALED,
        .imm_data   = htonl(log_size), 
        .wr.rdma = {
            .remote_addr = rctx->remote_meta.buf_va,
            .rkey        = rctx->remote_meta.rkey
        }
    };

    struct ibv_send_wr *bad_wr = NULL;
    if (ibv_post_send(rctx->qp, &wr, &bad_wr) != 0) {
        perror("[RDMA Master] ibv_post_send failed");
        return -2;
    }

    struct ibv_wc wc;
    int poll_result;
    while (1) {
        poll_result = ibv_poll_cq(rctx->cq, 1, &wc);
        if (poll_result > 0) {
            if (wc.wr_id == 99) break;
        } else if (poll_result < 0) {
            return -3;
        }
    }

    if (wc.status != IBV_WC_SUCCESS) {
        fprintf(stderr, "[RDMA Master Error] Send CQE unsuccessful: %s\n", ibv_wc_status_str(wc.status));
        return -4;
    }

    return 0; 
}

int rdma_slave_post_recv_envelope(struct rdma_ring_ctx *rctx, uint64_t wr_id) {
    struct ibv_recv_wr wr = {
        .wr_id   = wr_id,
        .next    = NULL,
        .sg_list = NULL,  
        .num_sge = 0
    };
    struct ibv_recv_wr *bad_wr = NULL;
    return ibv_post_recv(rctx->qp, &wr, &bad_wr);
}

int rdma_slave_block_and_get_imm(struct rdma_ring_ctx *rctx, uint32_t *out_log_size) {
    struct ibv_cq *cq;
    void *cq_context;

    if (ibv_get_cq_event(rctx->channel, &cq, &cq_context) != 0) {
        return -1;
    }

    ibv_ack_cq_events(cq, 1);
    ibv_req_notify_cq(cq, 0); 

    struct ibv_wc wc;
    int num_completions = ibv_poll_cq(cq, 1, &wc);
    if (num_completions <= 0) {
        return 0; 
    }

    if (wc.status != IBV_WC_SUCCESS) {
        fprintf(stderr, "[RDMA Slave] CQE status error: %s\n", ibv_wc_status_str(wc.status));
        return -2;
    }

    // 采用精准的 verbs 枚举匹配
    if (wc.opcode == IBV_WC_RECV_RDMA_WITH_IMM) {
        *out_log_size = ntohl(wc.imm_data); 
        rdma_slave_post_recv_envelope(rctx, wc.wr_id);
        return 1; 
    }

    return 0;
}