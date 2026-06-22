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
};

// RDMA Ring Buffer 管理上下文
struct rdma_ring_ctx {
    struct ibv_context   *ctx;
    struct ibv_pd        *pd;
    struct ibv_cq        *cq;
    struct ibv_qp        *qp;
    struct ibv_mr        *mr_buf; // 注册给数据缓冲区的 MR
    struct ibv_mr        *mr_meta;// 注册给元数据的 MR（用于主从同步指针）
    
    char                 *buffer;    // 16MB 真实的环形数据内存块
    struct ring_meta     *local_meta; // 本地指针控制结构
    struct ring_meta     remote_meta;// 远端对等体的内存控制结构
};

// 初始化增量环形缓冲区资源（主从通用）
struct rdma_ring_ctx* rdma_ring_init(const char *dev_name);

// 状态机配置与对端握手绑定
int rdma_ring_configure(struct rdma_ring_ctx *rctx, struct ring_meta *remote);

// 主端业务层专用：业务成功后，将增量命令压入 Ring Buffer 并通过硬件刷向从端
int rdma_ring_push_command(struct rdma_ring_ctx *rctx, const char *cmd, size_t cmd_len);

int rdma_check_transfer_complete(struct conn *c);

// 销毁释放
void rdma_ring_destroy(struct rdma_ring_ctx *rctx);

int rdma_init_context(struct conn *c);

#endif 