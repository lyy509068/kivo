#include <stdio.h>
#include <stdlib.h>
#include <pthread.h>
#include "kvstore.h"   
#include "resp.h"    
#include "network.h" 
#include "repl.h"  
#include "rdma.h"
#include "ebpf.h"

#ifndef RDMA_DEV_NAME
#define RDMA_DEV_NAME "mlx5_0"
#endif

#ifndef EBPF_OBJ_PATH
#define EBPF_OBJ_PATH "./sync_filter.bpf.o"
#endif

pthread_t repl_slave_tid;

extern volatile int g_running;

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
    #if ENABLE_TTL
    if (kvs_init_locks() != 0) {
        printf("Failed to init segment locks\n");
        return -1;
    }
    #endif
    
    #if ENABLE_MEM_POOL

    #if ENABLE_ARRAY
        array_item_pool = mem_pool_create(sizeof(kvs_array_item_t));
        if (!array_item_pool) {
            printf("Failed to create array item memory pool\n");
            return -1;
        }
    #endif
    #if ENABLE_RBTREE
        rbtree_node_pool = mem_pool_create(sizeof(rbtree_node_binary_t));
        if (!rbtree_node_pool) {
            printf("Failed to create rbtree node memory pool\n");
            return -1;
        }
    #endif
    #if ENABLE_HASH
        hash_node_pool = mem_pool_create(sizeof(hashnode_t));
        if (!hash_node_pool) {
            printf("Failed to create hash node memory pool\n");
            return -1;
        }
    #endif
    #if ENABLE_SKIPLIST
        skip_node_pool = mem_pool_create(sizeof(skipnode_binary_t));
        if (!skip_node_pool) {
            printf("Failed to create skiplist node memory pool\n");
            return -1;
        }
    #endif

    #endif

    // 引擎结构
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
    
    // 恢复数据
    #if ENABLE_SNAPSHOT
        kvs_snapshot_load();        
    #endif
    #if (ENABLE_PERSISTENCE || ENABLE_REPLICATION_MASTER || ENABLE_REPLICATION_SLAVE)
        kvs_persistence_init();
        kvs_persistence_recover(); 
    #endif

    #if ENABLE_TTL
        if (expire_thread_init() != 0) {
            return -1;
        }
    #endif

    #if (ENABLE_REPLICATION_MASTER || ENABLE_REPLICATION_SLAVE)
        const char *rdma_dev = RDMA_DEV_NAME;
        const char *ebpf_path = NULL;

        #if ENABLE_REPLICATION_MASTER
            ebpf_path = EBPF_OBJ_PATH; 
        #endif

        if (repl_init(rdma_dev, ebpf_path) != 0) {
            printf("[Engine Error] Failed to initialize global replication environment\n");
            return -1;
        }

        #if ENABLE_REPLICATION_SLAVE
            g_running = 1; 
            if (pthread_create(&repl_slave_tid, NULL, pure_rdma_repl_slave_thread, NULL) != 0) {
                printf("[Engine Error] Failed to create pure RDMA slave replication thread\n");
                return -1;
            }
            printf("[Engine] Pure RDMA replication background thread spawned successfully.\n");
        #endif
    #endif

    return 0;
}

void dest_kvengine(void) {

    #if (ENABLE_REPLICATION_MASTER || ENABLE_REPLICATION_SLAVE)
        printf("[Engine] Destroying replication environment and kernel resources...\n");
        #if ENABLE_REPLICATION_SLAVE
            g_running = 0; 
            repl_destroy(); 
            pthread_join(repl_slave_tid, NULL);
            printf("[Engine] RDMA slave replication thread exited cleanly.\n");
        #else
            repl_destroy(); 
        #endif
    #endif

    #if ENABLE_TTL
        expire_thread_destroy(); 
    #endif

    #if (ENABLE_PERSISTENCE || ENABLE_REPLICATION_MASTER || ENABLE_REPLICATION_SLAVE)
        kvs_persistence_close();        // 关闭并刷盘 AOF 日志文件流
    #endif

    // 释放本地内存引擎
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

    // 释放内存池
    #if ENABLE_MEM_POOL
        if (array_item_pool) mem_pool_stats(array_item_pool);
        if (rbtree_node_pool) mem_pool_stats(rbtree_node_pool);
        if (hash_node_pool) mem_pool_stats(hash_node_pool);
        if (skip_node_pool) mem_pool_stats(skip_node_pool);
    
        mem_pool_destroy(array_item_pool);
        mem_pool_destroy(rbtree_node_pool);
        mem_pool_destroy(hash_node_pool);
        mem_pool_destroy(skip_node_pool);
    #endif
    #if ENABLE_TTL
        kvs_destroy_locks(); // 释放锁
    #endif 
}


int main(int argc, char *argv[]) {
    if (argc != 2) {
        printf("Usage: %s <port>\n", argv[0]);
        return -1;
    }
    int port = atoi(argv[1]);

    init_kvengine();
    

    // 告诉协议层调用 kvs_execute_command 执行命令
    protocol_set_command_handler(kvs_execute_command);

    // 告诉网络层调用 protocol_process_stream 去处理包
    #if (NETWORK_SELECT == NETWORK_REACTOR)
        reactor_start(port, protocol_process_stream);  
    #elif (NETWORK_SELECT == NETWORK_PROACTOR)
        proactor_start(port, protocol_process_stream);
    #elif (NETWORK_SELECT == NETWORK_NTYCO)
        ntyco_start(port, protocol_process_stream);
    #endif
    dest_kvengine();
    return 0;
}