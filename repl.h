#ifndef REPLICATION_H
#define REPLICATION_H

#include <stdbool.h>

#define ENABLE_REPLICATION_MASTER 1
#define ENABLE_REPLICATION_SLAVE  0

struct repl_context {
    int fd;
    char *wbuffer;
    int wcapacity;
    int wlength;
};

// 前置结构体声明，防止头文件相互包含带来的编译冲突
struct resp_request;
typedef struct resp_request resp_request_t;


// 第一部分：通用生命周期与控制管理                      

int repl_init(const char *rdma_dev, const char *ebpf_obj_path, int slave_fd);

int repl_set_ebpf_switch(bool enable);

void repl_destroy(void);


// 第二部分：从端（Slave）核心函数         

int repl_connect_to_master(const char *master_ip, unsigned short master_port);

void slave_rdma_handshake_read_cb(int fd);

int handle_slave_rdma_connect_ack(resp_request_t *req, int fd);

int repl_start_slave_engine(void);

void* pure_rdma_repl_slave_thread(void *arg);


// 第三部分：主端（Master）核心函数                   

void handle_master_rdma_connect(resp_request_t *req, char **wbuf, int *wcap, int *wlen);

int repl_sync_log_via_rdma(void);

#endif 