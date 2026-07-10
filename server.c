#include <stdio.h>
#include <stdlib.h>
#include <pthread.h>
#include "kvstore.h"   
#include "resp.h"    
#include "network.h" 
#include "repl.h"  
#include "rdma.h"
#include "ebpf.h"
#include "config.h"

// 全局配置变量
int g_enable_persistence = 0;
int g_enable_snapshot    = 0;
int g_enable_ttl         = 0;
int g_enable_mempool     = 0;
int g_enable_repl_master = 0;
int g_enable_repl_slave  = 0;

#ifndef RDMA_DEV_NAME
#define RDMA_DEV_NAME "rxe0"
#endif

#ifndef EBPF_OBJ_PATH
#define EBPF_OBJ_PATH "./sync_filter.bpf.o"
#endif

pthread_t repl_slave_tid;
extern volatile int g_running;
int BEGIN_IN = 0;

#if ENABLE_ARRAY
extern kvs_array_t global_array;
#endif
#if ENABLE_RBTREE
extern kvs_rbtree_t global_rbtree;
#endif
#if ENABLE_HASH
extern kvs_hash_t global_hash;
#endif
#if ENABLE_SKIPLIST
extern kvs_skip_t global_skip;
#endif

int init_kvengine(void) {
    if (g_enable_ttl) {
        if (kvs_init_locks() != 0) { printf("Failed to init locks\n"); return -1; }
    }

    if (g_enable_mempool) {
        #if ENABLE_ARRAY
        array_item_pool = mem_pool_create(sizeof(kvs_array_item_t));
        #endif
        #if ENABLE_RBTREE
        rbtree_node_pool = mem_pool_create(sizeof(rbtree_node_binary_t));
        #endif
        #if ENABLE_HASH
        hash_node_pool = mem_pool_create(sizeof(hashnode_t));
        #endif
        #if ENABLE_SKIPLIST
        skip_node_pool = mem_pool_create(sizeof(skipnode_binary_t));
        #endif
    }

    #if ENABLE_ARRAY
    memset(&global_array, 0, sizeof(kvs_array_t));
    kvs_array_create(&global_array);
    #endif
    #if ENABLE_RBTREE
    memset(&global_rbtree, 0, sizeof(kvs_rbtree_t));
    kvs_rbtree_create(&global_rbtree);
    #endif
    #if ENABLE_HASH
    memset(&global_hash, 0, sizeof(kvs_hash_t));
    kvs_hash_create(&global_hash);
    #endif
    #if ENABLE_SKIPLIST
    memset(&global_skip, 0, sizeof(kvs_skip_t));
    kvs_skip_create(&global_skip);
    #endif

    if (g_enable_snapshot) kvs_snapshot_load();

    if (g_enable_persistence || g_enable_repl_master || g_enable_repl_slave) {
        kvs_persistence_init();
        kvs_persistence_recover();
    }

    if (g_enable_ttl) {
        if (expire_thread_init() != 0) return -1;
    }

    if (g_enable_repl_master || g_enable_repl_slave) {
        const char *rdma_dev = RDMA_DEV_NAME;
        const char *ebpf_path = g_enable_repl_master ? EBPF_OBJ_PATH : NULL;
        if (repl_init(rdma_dev, ebpf_path) != 0) return -1;
        if (g_enable_repl_slave) {
            g_running = 1;
            pthread_create(&repl_slave_tid, NULL, pure_rdma_repl_slave_thread, NULL);
        }
    }
    return 0;
}

void dest_kvengine(void) {
    if (g_enable_repl_master || g_enable_repl_slave) {
        if (g_enable_repl_slave) {
            g_running = 0;
            repl_destroy();
            pthread_join(repl_slave_tid, NULL);
        } else {
            repl_destroy();
        }
    }

    if (g_enable_ttl) expire_thread_destroy();

    if (g_enable_persistence || g_enable_repl_master || g_enable_repl_slave) kvs_persistence_close();

    if (g_enable_snapshot) kvs_snapshot_save();

    #if ENABLE_ARRAY
    kvs_array_destroy(&global_array);
    #endif
    #if ENABLE_RBTREE
    kvs_rbtree_destroy(&global_rbtree);
    #endif
    #if ENABLE_HASH
    kvs_hash_destroy(&global_hash);
    #endif
    #if ENABLE_SKIPLIST
    kvs_skip_destroy(&global_skip);
    #endif

    if (g_enable_mempool) {
        #if ENABLE_ARRAY
        if (array_item_pool) mem_pool_destroy(array_item_pool);
        #endif
        #if ENABLE_RBTREE
        if (rbtree_node_pool) mem_pool_destroy(rbtree_node_pool);
        #endif
        #if ENABLE_HASH
        if (hash_node_pool) mem_pool_destroy(hash_node_pool);
        #endif
        #if ENABLE_SKIPLIST
        if (skip_node_pool) mem_pool_destroy(skip_node_pool);
        #endif
    }
    if (g_enable_ttl) kvs_destroy_locks();
}

int main(int argc, char *argv[]) {
    server_config_t cfg;
    const char *config_file = (argc >= 2) ? argv[1] : "config.conf";
    load_config(config_file, &cfg);

    g_enable_persistence = (cfg.persistence == 1);          
    g_enable_snapshot    = (cfg.snapshot == 1);             
    g_enable_ttl         = cfg.expire;
    g_enable_mempool     = cfg.mempool;
    g_enable_repl_master = (cfg.replication == 1);
    g_enable_repl_slave  = (cfg.replication == 2);

    protocol_set_command_handler(kvs_execute_command);
    init_kvengine();

    #if (NETWORK_SELECT == NETWORK_REACTOR)
    reactor_start(cfg.port, protocol_process_stream);
    #elif (NETWORK_SELECT == NETWORK_PROACTOR)
    proactor_start(cfg.port, protocol_process_stream);
    #elif (NETWORK_SELECT == NETWORK_NTYCO)
    ntyco_start(cfg.port, protocol_process_stream);
    #endif

    dest_kvengine();
    return 0;
}