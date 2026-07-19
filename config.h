// config.h
#ifndef CONFIG_H
#define CONFIG_H

typedef struct {
    int port;
    int persistence;   // 0=NONE, 1=AOF
    int snapshot;      // 0=OFF, 1=ON
    int expire;
    int mempool;
    int replication;   // 0=OFF, 1=MASTER, 2=SLAVE
    int transport;     // 0=RDMA, 1=TCP
} server_config_t;

int load_config(const char *filename, server_config_t *cfg);

#endif