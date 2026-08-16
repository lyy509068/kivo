#include "mempool.h"
#include <stdlib.h>
#include <string.h>
#include <malloc.h>
#include <stdint.h>
#include "kvstore.h"

mem_pool_t *g_size_map[MAX_SLAB_SIZE + 1] = {NULL};

//  Slab 配置 
static const size_t slab_classes[] = {32, 64, 128, 256, 512, 1024, 2048, 4096, 8192};
#define SLAB_COUNT (sizeof(slab_classes)/sizeof(slab_classes[0]))
static mem_pool_t *slab_pools[SLAB_COUNT];

//  O(1) 地址掩码反推宏 
// 根据 ptr 直接抹零低 16 位，算出其所属 64KB 块的首地址
static inline pool_block_t *get_block_from_ptr(void *ptr) {
    return (pool_block_t *)((uintptr_t)ptr & ~((uintptr_t)POOL_BLOCK_SIZE - 1));
}

//  内存池核心函数 

mem_pool_t *mem_pool_create(size_t user_size) {
    if (user_size == 0) return NULL;

    mem_pool_t *pool = (mem_pool_t *)malloc(sizeof(mem_pool_t));
    if (!pool) return NULL;

    // chunk 大小：8 字节对齐，且最小为一个指针大小
    pool->chunk_size = (user_size + 7) & ~7;
    if (pool->chunk_size < sizeof(void *)) pool->chunk_size = sizeof(void *);

    pool->free_list = NULL;
    pool->blocks = NULL;
    pool->current_block = NULL;
    pool->next_free = NULL;
    pool->remaining = 0;

    return pool;
}

// 分配一个新的 64KB 对齐块
static int add_new_block(mem_pool_t *pool) {
    void *mem = NULL;
    if (posix_memalign(&mem, POOL_BLOCK_SIZE, POOL_BLOCK_SIZE) != 0)
        return -1;

    pool_block_t *block = (pool_block_t *)mem;

    block->next = NULL;
    block->pool = pool;
    block->magic = POOL_BLOCK_MAGIC;
    block->data = (unsigned char *)block + sizeof(pool_block_t);
    block->capacity = (POOL_BLOCK_SIZE - sizeof(pool_block_t)) / pool->chunk_size;
    block->free_count = 0;

    // 插入 blocks 链表头
    block->next = pool->blocks;
    pool->blocks = block;

    pool->current_block = block;
    pool->next_free = block->data;
    pool->remaining = block->capacity;

    return 0;
}

void *mem_pool_alloc(mem_pool_t *pool) {
    if (!pool) return NULL;

    // 优先从 free_list 分配
    if (pool->free_list) {
        void *chunk = pool->free_list;
        pool->free_list = *(void **)chunk;   // 弹出头部

        // O(1) 地址推导，找出所属 block 并更新空闲计数
        pool_block_t *block = get_block_from_ptr(chunk);
        block->free_count--;
        return chunk;
    }

    // 从当前 block 顺序分配
    if (pool->remaining == 0) {
        if (add_new_block(pool) != 0) return NULL;
    }

    void *chunk = pool->next_free;
    pool->next_free = (char *)pool->next_free + pool->chunk_size;
    pool->remaining--;

    return chunk;
}

// 从 free_list 中移除所有属于指定 block 的节点 (同样使用 O(1) 极速优化)
static void remove_block_from_freelist(mem_pool_t *pool, pool_block_t *block) {
    void **pp = &pool->free_list;
    while (*pp) {
        void *curr = *pp;
        // O(1) 掩码推导当前节点属于哪个 block
        pool_block_t *curr_block = get_block_from_ptr(curr);
        if (curr_block == block) {
            *pp = *(void **)curr;   // 跳过此节点
        } else {
            pp = (void **)curr;     // 移动到下一个
        }
    }
}

// 内部释放函数：已知 pool 和 block，执行归还与可能的回收
static void mem_pool_free_with_block(mem_pool_t *pool, void *ptr, pool_block_t *block) {
    *(void **)ptr = pool->free_list;
    pool->free_list = ptr;

    block->free_count++;

    // 若整个 block 完全空闲，则回收
    if (block->free_count == block->capacity) {
        pool_block_t **pp = &pool->blocks;
        while (*pp) {
            if (*pp == block) {
                *pp = block->next;
                break;
            }
            pp = &(*pp)->next;
        }

        remove_block_from_freelist(pool, block);

        if (pool->current_block == block) {
            pool->current_block = NULL;
            pool->next_free = NULL;
            pool->remaining = 0;
        }

        free(block);
    }
}

void mem_pool_trim(mem_pool_t *pool) {
    if (!pool) return;

    pool_block_t *block = pool->blocks;
    while (block) {
        pool_block_t *next = block->next;
        if (block->free_count == block->capacity) {
            pool_block_t **pp = &pool->blocks;
            while (*pp) {
                if (*pp == block) {
                    *pp = block->next;
                    break;
                }
                pp = &(*pp)->next;
            }

            remove_block_from_freelist(pool, block);

            if (pool->current_block == block) {
                pool->current_block = NULL;
                pool->next_free = NULL;
                pool->remaining = 0;
            }

            free(block);
        }
        block = next;
    }
}

//  Slab 初始化 

static int g_initialized = 0;

void kvs_mempool_init(void) {
    if (g_initialized) return;

    for (int i = 0; i < SLAB_COUNT; i++) {
        slab_pools[i] = mem_pool_create(slab_classes[i]);
    }

    for (size_t size = 1; size <= MAX_SLAB_SIZE; size++) {
        g_size_map[size] = NULL;
        for (int i = 0; i < SLAB_COUNT; i++) {
            if (slab_classes[i] >= size) {
                g_size_map[size] = slab_pools[i];
                break;
            }
        }
    }
    g_initialized = 1;
}

//  通用分配接口 

mem_pool_t *array_item_pool;
mem_pool_t *rbtree_node_pool;
mem_pool_t *hash_node_pool;
mem_pool_t *skip_node_pool;

// 用于 O(1) 识别 Fallback，概率上不可能碰撞
#define FALLBACK_MAGIC 0xFBCAFEBABEDEADULL

// Fallback 头：自带魔数和 size
typedef struct {
    size_t size;
    uint64_t magic;
} fallback_header_t;

void *kvs_malloc(size_t size) {
    if (size == 0) return NULL;
    if (!g_enable_mempool) return malloc(size);

    if (size <= MAX_SLAB_SIZE) {
        if (!g_size_map[size]) kvs_mempool_init();
        mem_pool_t *pool = g_size_map[size];
        if (pool) {
            return mem_pool_alloc(pool);   // 直接返回 chunk，无头部
        }
    }

    // 大对象走 fallback，带上我们自定义的 fallback 魔数
    fallback_header_t *fh = (fallback_header_t *)malloc(sizeof(fallback_header_t) + size);
    if (!fh) return NULL;
    fh->size = size;
    fh->magic = FALLBACK_MAGIC; // 写入魔数
    return fh + 1; // 返回给用户的指针
}

void kvs_free(void *ptr) {
    if (!ptr) return;
    if (!g_enable_mempool) { free(ptr); return; }

    // 1. 优先通过 fallback 头部的魔数来精准判断。
    // 安全性原理：无论 ptr 是 Slab 分配还是 Fallback 分配，(ptr - 1) 永远落在安全的内存段中，
    // 强读 fh->magic 绝对不会引发 Segmentation Fault。
    fallback_header_t *fh = (fallback_header_t *)ptr - 1;
    if (fh->magic == FALLBACK_MAGIC) {
        free(fh);
        return; 
    }

    // 2. 如果不是 Fallback，则一定是 Slab 内存池中的 chunk
    // O(1) 核心魔法：通过地址掩码直接反推 64KB 对齐的 block 首地址
    pool_block_t *block = get_block_from_ptr(ptr);

    // 双重安全校验，确认为内存池
    if (block->magic == POOL_BLOCK_MAGIC) {
        mem_pool_free_with_block(block->pool, ptr, block);
    } else {
        // 异常防御兜底：防止用户传入野指针
        free(ptr);
    }
}

void *kvs_realloc(void *ptr, size_t size) {
    if (!ptr) return kvs_malloc(size);
    if (size == 0) { kvs_free(ptr); return NULL; }
    if (!g_enable_mempool) return realloc(ptr, size);

    // 优先检查是否为 Fallback 内存
    fallback_header_t *fh = (fallback_header_t *)ptr - 1;
    if (fh->magic == FALLBACK_MAGIC) {
        size_t old_size = fh->size;
        if (size <= old_size) {
            return ptr; // 没超出原分配量，原地返回
        }
        void *new_ptr = kvs_malloc(size);
        if (new_ptr) {
            memcpy(new_ptr, ptr, old_size);
            kvs_free(ptr);
        }
        return new_ptr;
    }

    // 通过地址掩码反推 Slab
    pool_block_t *block = get_block_from_ptr(ptr);
    if (block->magic == POOL_BLOCK_MAGIC) {
        size_t old_size = block->pool->chunk_size;
        if (size <= old_size) {
            return ptr; // Slab 块容量足够，原地返回
        }
        // 扩容拷贝
        void *new_ptr = kvs_malloc(size);
        if (new_ptr) {
            memcpy(new_ptr, ptr, old_size);
            kvs_free(ptr);
        }
        return new_ptr;
    }

    return realloc(ptr, size);
}

void kvs_mempool_trim_all(void) {
    for (int i = 0; i < SLAB_COUNT; i++) {
        if (slab_pools[i]) {
            mem_pool_trim(slab_pools[i]);
        }
    }
    // 所有池回收完成后，统一让 glibc 归还物理内存
    malloc_trim(0);
}

