#ifndef REPLICATION_H
#define REPLICATION_H

#include <stdbool.h>

#define ENABLE_REPLICATION_MASTER 0
#define ENABLE_REPLICATION_SLAVE  1

// 客户端连接主端专用的全局状态上下文（等价于你的 g_repl）
struct repl_context {
    int fd;
    char *wbuffer;
    int wcapacity;
    int wlength;
};

// 初始化全量与增量同步环境
int repl_init(const char *rdma_dev, const char *ebpf_obj_path, int slave_fd);

// 向主端发起 TCP 握手并托管连接
int repl_connect_to_master(const char *master_ip, unsigned short master_port);

// 【全量同步接口】使用 RDMA 发送日志
int repl_sync_log_via_rdma(void);

// 【增量同步接口】控制 eBPF 的内核套接字缓冲闸门
int repl_set_ebpf_switch(bool enable);

// 清理释放
void repl_destroy(void);

#endif 