#ifndef MEM_POOL_H
#define MEM_POOL_H

#include <stdlib.h>
#include <string.h>

#define POOL_BLOCK_SIZE 4096  // 每个大块 4KB


// 核心：内存块的“元数据头” (16字节对齐)
typedef struct mem_header {
    void *owner;      // 指向分配它的 mem_pool_t。如果是普通 malloc，这里是 NULL
    size_t size;      // 用户真实请求的大小
} mem_header_t;

// 内存块结构
typedef struct pool_block {
    void *data;                 // 块内存
    struct pool_block *next;    // 下一个块
} pool_block_t;

// 内存池结构
typedef struct mem_pool {
    void **free_list;           // 空闲链表头
    size_t user_size;           // 用户需要的数据大小（已 8 字节对齐）
    size_t chunk_size;          // 实际分配单元大小 (Header + user_size)
    int block_capacity;         // 每个大块能容纳的单元数
    pool_block_t *blocks;       // 所有块的链表
    int total_allocated;
    int total_freed;
} mem_pool_t;

// API
mem_pool_t *mem_pool_create(size_t user_size);
void *mem_pool_alloc(mem_pool_t *pool);
void mem_pool_free(mem_pool_t *pool, void *user_ptr);
void mem_pool_destroy(mem_pool_t *pool);
void mem_pool_stats(mem_pool_t *pool);

#endif