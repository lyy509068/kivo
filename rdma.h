#ifndef RDMA_RING_BUFFER_H
#define RDMA_RING_BUFFER_H

#include <stdint.h>
#include <stddef.h>
#include <infiniband/verbs.h>
#define RING_BUFFER_SIZE (1024 * 1024 * 16) // 16MB 环形缓冲区，足够支撑高频增量命令

struct conn;

// 环形缓冲区的核心控制元数据
struct ring_meta {
    uint32_t head;       // 读指针（从端维护）
    uint32_t tail;       // 写指针（主端维护）
    uint64_t buf_va;     // 数据缓冲区的虚拟地址
    uint32_t rkey;       // 远端内存密钥
    uint32_t qpn;        // QP 编号
    union ibv_gid gid;   // 储存远端 GID 路由信息
};

// RDMA Ring Buffer 管理上下文
struct rdma_ring_ctx {
    struct ibv_context      *ctx;
    struct ibv_comp_channel *channel;
    struct ibv_pd           *pd;
    struct ibv_cq           *cq;
    struct ibv_qp           *qp;
    struct ibv_mr           *mr_buf; // 注册给数据缓冲区的 MR
    struct ibv_mr           *mr_meta;// 注册给元数据的 MR（用于主从同步指针）
    
    char                    *buffer;    // 16MB 真实的环形数据内存块
    struct ring_meta        *local_meta; // 本地指针控制结构
    struct ring_meta        remote_meta;// 远端对等体的内存控制结构
};

// 初始化环形缓冲区所需的所有 RDMA 硬件资源
struct rdma_ring_ctx* rdma_ring_init(const char *dev_name);

// 状态机绑定配置 (INIT -> RTR -> RTS)
int rdma_ring_configure(struct rdma_ring_ctx *rctx, struct ring_meta *remote);

// 资源销毁
void rdma_ring_destroy(struct rdma_ring_ctx *rctx);

// 通过 RDMA 单边写将大块日志轰入从端，并附带立即数（日志大小）
int rdma_master_write_log_imm(struct rdma_ring_ctx *rctx, uint32_t log_size);

// 向网卡接收队列 (RQ) 投递一个空的接收请求（布设捕鼠夹）这里只拦截立即数
int rdma_slave_post_recv_envelope(struct rdma_ring_ctx *rctx, uint64_t wr_id);

// 阻塞等待网卡硬件层的传输完成事件，并精准剥离出主端发来的日志大小
int rdma_slave_block_and_get_imm(struct rdma_ring_ctx *rctx, uint32_t *out_log_size);

#endif 