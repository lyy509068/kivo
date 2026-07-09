#include "mempool.h"
#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <pthread.h>

mem_pool_t *array_item_pool = NULL;
mem_pool_t *rbtree_node_pool = NULL;
mem_pool_t *hash_node_pool = NULL;
mem_pool_t *skip_node_pool = NULL;

mem_pool_t *mem_pool_create(size_t user_size) {
    if (user_size == 0) return NULL;
    
    mem_pool_t *pool = (mem_pool_t *)malloc(sizeof(mem_pool_t));
    if (!pool) return NULL;
    
    pool->user_size = (user_size + 7) & ~7;
    pool->chunk_size = ((sizeof(mem_header_t) + pool->user_size) + 7) & ~7;
    pool->block_capacity = POOL_BLOCK_SIZE / pool->chunk_size;
    if (pool->block_capacity < 1) pool->block_capacity = 1;
    
    pool->free_list = NULL;
    pool->blocks = NULL;
    pool->total_allocated = 0;
    pool->total_freed = 0;
    
    pthread_mutex_init(&pool->lock, NULL);  
    
    return pool;
}

static int add_new_block(mem_pool_t *pool) {
    int block_count = 0;
    for (pool_block_t *b = pool->blocks; b; b = b->next) block_count++;
    
    int new_capacity = pool->block_capacity;
    if (block_count > 0) {
        new_capacity = pool->block_capacity;
        for (int i = 0; i < block_count && new_capacity < 4096; i++) {
            new_capacity *= 2;
        }
        if (new_capacity > 4096) new_capacity = 4096;
    }
    
    size_t block_mem_size = new_capacity * pool->chunk_size;
    size_t total_size = sizeof(pool_block_t) + block_mem_size;
    pool_block_t *block = (pool_block_t *)malloc(total_size);
    if (!block) return -1;
    
    block->data = (void *)((char *)block + sizeof(pool_block_t));
    block->capacity = new_capacity;
    block->next = pool->blocks;
    pool->blocks = block;
    
    for (int i = 0; i < new_capacity; i++) {
        void *chunk = (char *)block->data + i * pool->chunk_size;
        *(void **)chunk = pool->free_list;
        pool->free_list = (void **)chunk;
    }
    return 0;
}

void *mem_pool_alloc(mem_pool_t *pool) {
    if (!pool) return NULL;
    
    pthread_mutex_lock(&pool->lock);  
    
    if (!pool->free_list) {
        if (add_new_block(pool) != 0) {
            pthread_mutex_unlock(&pool->lock);
            return NULL;
        }
    }
    
    void *chunk = (void *)pool->free_list;
    pool->free_list = *(void ***)chunk;
    
    mem_header_t *header = (mem_header_t *)chunk;
    header->owner = pool;
    header->size = pool->user_size;
    
    pool->total_allocated++;
    
    pthread_mutex_unlock(&pool->lock);
    
    return (void *)((char *)chunk + sizeof(mem_header_t));
}

void mem_pool_free(mem_pool_t *pool, void *user_ptr) {
    if (!pool || !user_ptr) return;
    
    void *chunk = (char *)user_ptr - sizeof(mem_header_t);
    mem_header_t *header = (mem_header_t *)chunk;
    
    // 校验 owner
    if (header->owner != pool) {
        fprintf(stderr, "[Pool Error] Wrong pool for free! Expected %p, got %p\n",
                (void*)pool, (void*)header->owner);
        return;
    }
    
    pthread_mutex_lock(&pool->lock);  
    
    *(void **)chunk = (void *)pool->free_list;
    pool->free_list = (void **)chunk;
    pool->total_freed++;
    
    pthread_mutex_unlock(&pool->lock);
}

void mem_pool_destroy(mem_pool_t *pool) {
    if (!pool) return;
    
    pool_block_t *block = pool->blocks;
    while (block) {
        pool_block_t *next = block->next;
        free(block);
        block = next;
    }
    pthread_mutex_destroy(&pool->lock);  
    free(pool);
}