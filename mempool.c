// mem_pool.c
#include "mempool.h"
#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <unistd.h>  

mem_pool_t *array_item_pool = NULL;
mem_pool_t *rbtree_node_pool = NULL;
mem_pool_t *hash_node_pool = NULL;
mem_pool_t *skip_node_pool = NULL;


mem_pool_t *mem_pool_create(size_t user_size) {
    if (user_size == 0) return NULL;
    
    mem_pool_t *pool = (mem_pool_t *)malloc(sizeof(mem_pool_t));
    if (!pool) return NULL;
    
    // 1. 将用户请求大小对齐到 8 字节 (防止 CPU 内存不对齐异常)
    pool->user_size = (user_size + 7) & ~7;
    
    // 2. 实际分配单元 = 头部大小 + 对齐后的用户大小
    pool->chunk_size = sizeof(mem_header_t) + pool->user_size;
    
    // 3. 计算一个块能装多少个单元
    pool->block_capacity = POOL_BLOCK_SIZE / pool->chunk_size;
    if (pool->block_capacity < 1) pool->block_capacity = 1;
    
    pool->free_list = NULL;
    pool->blocks = NULL;
    pool->total_allocated = 0;
    pool->total_freed = 0;
    
    return pool;
}

static int add_new_block(mem_pool_t *pool) {
    size_t block_mem_size = pool->block_capacity * pool->chunk_size;
    void *block_mem = malloc(block_mem_size);
    if (!block_mem) return -1;
    
    pool_block_t *block = (pool_block_t *)malloc(sizeof(pool_block_t));
    if (!block) {
        free(block_mem);
        return -1;
    }
    block->data = block_mem;
    block->next = pool->blocks;
    pool->blocks = block;
    
    // 切割内存，加入 free_list
    for (int i = 0; i < pool->block_capacity; i++) {
        void *chunk = (char *)block_mem + i * pool->chunk_size;
        // 在空闲状态下，用 chunk 的前 8 个字节存储下一个空闲块的指针
        *(void **)chunk = pool->free_list;
        pool->free_list = chunk;
    }
    return 0;
}

void *mem_pool_alloc(mem_pool_t *pool) {
    if (!pool) return NULL;
    
    if (!pool->free_list) {
        if (add_new_block(pool) != 0) return NULL;
    }
    
    // 从空闲链表取出一个 chunk
    void *chunk = pool->free_list;
    pool->free_list = *(void **)chunk; 
    
    // **核心**：写入元数据头
    mem_header_t *header = (mem_header_t *)chunk;
    header->owner = pool;
    header->size = pool->user_size;
    
    pool->total_allocated++;
    
    // 返回给用户的是越过 header 之后的可用数据地址
    return (void *)((char *)chunk + sizeof(mem_header_t));
}

void mem_pool_free(mem_pool_t *pool, void *user_ptr) {
    if (!pool || !user_ptr) return;
    
    // 往回倒退，找到真正的 chunk 起始位置
    void *chunk = (char *)user_ptr - sizeof(mem_header_t);
    
    // 将其重新挂回空闲链表
    *(void **)chunk = pool->free_list;
    pool->free_list = chunk;
    
    pool->total_freed++;
}

void mem_pool_destroy(mem_pool_t *pool) {
    if (!pool) return;
    pool_block_t *block = pool->blocks;
    while (block) {
        pool_block_t *next = block->next;
        free(block->data);
        free(block);
        block = next;
    }
    free(pool);
}


