#ifndef REPLICATION_H
#define REPLICATION_H

#include <stdbool.h>
#include "resp.h"

extern int g_enable_persistence;
extern int g_enable_snapshot;
extern int g_enable_ttl;
extern int g_enable_mempool;
extern int g_enable_repl_master;
extern int g_enable_repl_slave;
extern int g_use_tcp_sync;
extern int g_use_rdma_sync;

struct repl_context {
    int fd;
    char *wbuffer;
    int wcapacity;
    int wlength;
};

int repl_init(const char *rdma_dev);
void repl_destroy(void);

int repl_connect_to_master(const char *master_ip, unsigned short master_port);
int repl_start_slave_engine(void);
void* pure_rdma_repl_slave_thread(void *arg);
void* tcp_sendfile_recv_thread(void *arg);

void handle_slave_rdma_connect(resp_request_t *req, char **wbuf, int *wcap, int *wlen, int fd);
int repl_sync_log_via_rdma(void);
int repl_flush_backlog_via_rdma(void);
int repl_sync_log_via_tcp(void);
int repl_flush_backlog_via_tcp(void);
void repl_push_cmd(const char *cmd, void *key, int key_len, void *value, int value_len);

#endif