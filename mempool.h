#ifndef MEMPOOL_H
#define MEMPOOL_H

#include <stddef.h>
#include <stdint.h>

#define POOL_BLOCK_SIZE    (64 * 1024)          // 固定块大小 64KB，也是对齐掩码单位
#define MAX_SLAB_SIZE      8192
#define POOL_BLOCK_MAGIC   0xDEADBEEFCAFEBABEULL  // block 魔数，用于识别池分配

typedef struct mem_pool mem_pool_t;

typedef struct pool_block {
    struct pool_block *next;
    mem_pool_t        *pool;      // 所属内存池 (O(1)反推时直接获取)
    int               capacity;   // 该块可容纳的 chunk 数
    int               free_count; // 当前空闲 chunk 数
    uint64_t          magic;      // 魔数标识
    unsigned char    *data;       // 数据区起始地址
} pool_block_t;

struct mem_pool {
    size_t        chunk_size;     // 每个 chunk 的大小（8 字节对齐）
    void         *free_list;      // 空闲 chunk 链表头（LIFO）
    pool_block_t *blocks;         // 所有 block 链表头（头插法）
    pool_block_t *current_block;  // 当前顺序分配的 block
    void         *next_free;      // 当前 block 中下一个可顺序分配的 chunk
    size_t        remaining;      // 当前 block 剩余可顺序分配的 chunk 数
};

extern mem_pool_t *g_size_map[MAX_SLAB_SIZE + 1];
extern int g_enable_mempool;

mem_pool_t *mem_pool_create(size_t user_size);
void mem_pool_destroy(mem_pool_t *pool);
void *mem_pool_alloc(mem_pool_t *pool);
void mem_pool_trim(mem_pool_t *pool);

void kvs_mempool_init(void);
void *kvs_malloc(size_t size);
void kvs_free(void *ptr);
void *kvs_realloc(void *ptr, size_t size);
void kvs_mempool_trim_all(void);
#endif