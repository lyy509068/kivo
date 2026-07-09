#ifndef MEMPOOL_H
#define MEMPOOL_H

#include <stddef.h>
#include <pthread.h>

#define POOL_BLOCK_SIZE (64 * 1024)  // 64KB per block

typedef struct mem_header {
    void *owner;
    size_t size;
} mem_header_t;

typedef struct pool_block {
    void *data;
    int capacity;
    struct pool_block *next;
} pool_block_t;

typedef struct mem_pool {
    size_t user_size;
    size_t chunk_size;
    int block_capacity;
    void **free_list;
    pool_block_t *blocks;
    size_t total_allocated;
    size_t total_freed;
    pthread_mutex_t lock;  // 独立锁
} mem_pool_t;

extern mem_pool_t *array_item_pool;
extern mem_pool_t *rbtree_node_pool;
extern mem_pool_t *hash_node_pool;
extern mem_pool_t *skip_node_pool;

mem_pool_t *mem_pool_create(size_t user_size);
void *mem_pool_alloc(mem_pool_t *pool);
void mem_pool_free(mem_pool_t *pool, void *ptr);
void mem_pool_destroy(mem_pool_t *pool);
void mem_pool_stats(mem_pool_t *pool);

#endif