#include "mempool.h"
#include <stdlib.h>
#include <string.h>
#include "kvstore.h" 

mem_pool_t *g_typed_pools[OBJ_MAX] = {NULL};
mem_pool_t *g_size_map[MAX_SLAB_SIZE + 1] = {NULL};

mem_pool_t *mem_pool_create(size_t user_size) {
    if (user_size == 0) return NULL;
    mem_pool_t *pool = (mem_pool_t *)malloc(sizeof(mem_pool_t));
    if (!pool) return NULL;

    pool->chunk_size = (user_size + 7) & ~7;
    if (pool->chunk_size < sizeof(void *)) pool->chunk_size = sizeof(void *);

    pool->block_capacity = POOL_BLOCK_SIZE / pool->chunk_size;
    if (pool->block_capacity < 1) pool->block_capacity = 1;

    pool->free_list = NULL;
    pool->blocks = NULL;
    pool->current_block = NULL;
    pool->next_free = NULL;
    pool->remaining = 0;
    return pool;
}

void mem_pool_destroy(mem_pool_t *pool) {
    if (!pool) return;
    pool_block_t *block = pool->blocks;
    while (block) {
        pool_block_t *next = block->next;
        free(block);
        block = next;
    }
    free(pool);
}

static int add_new_block(mem_pool_t *pool) {
    size_t block_mem_size = (size_t)pool->block_capacity * pool->chunk_size;
    pool_block_t *block = (pool_block_t *)malloc(sizeof(pool_block_t) + block_mem_size);
    if (!block) return -1;

    block->data = (void *)(block + 1);
    block->capacity = pool->block_capacity;
    block->next = pool->blocks;
    pool->blocks = block;

    pool->current_block = block;
    pool->next_free = block->data;
    pool->remaining = pool->block_capacity;
    return 0;
}

void *mem_pool_alloc(mem_pool_t *pool) {
    if (!pool) return NULL;

    if (pool->free_list) {
        void *ptr = pool->free_list;
        pool->free_list = *(void **)ptr; 
        return ptr;
    }
    
    if (pool->remaining == 0) {
        if (add_new_block(pool) != 0) return NULL;
    }
    
    void *ptr = pool->next_free;
    pool->next_free = (char *)pool->next_free + pool->chunk_size;
    pool->remaining--;
    return ptr;
}

void mem_pool_free(mem_pool_t *pool, void *ptr) {
    if (!pool || !ptr) return;
    *(void **)ptr = pool->free_list;
    pool->free_list = ptr;
}

static const size_t slab_classes[] = {32, 64, 128, 256, 512, 1024, 2048, 4096, 8192};
#define SLAB_COUNT (sizeof(slab_classes)/sizeof(slab_classes[0]))
static mem_pool_t *slab_pools[SLAB_COUNT];

static int g_initialized = 0;

void kvs_mempool_init(void) {
    if (g_initialized) return;
    
    // 获取结构体字节数
    g_typed_pools[OBJ_ARRAY]  = mem_pool_create(sizeof(kvs_array_item_t));
    g_typed_pools[OBJ_RBTREE] = mem_pool_create(sizeof(rbtree_node_binary_t));
    g_typed_pools[OBJ_HASH]   = mem_pool_create(sizeof(hashnode_t));
    g_typed_pools[OBJ_SKIP]   = mem_pool_create(sizeof(skipnode_binary_t));

    // 初始化通用 Slab 池
    for (int i = 0; i < SLAB_COUNT; i++) {
        slab_pools[i] = mem_pool_create(slab_classes[i]);
    }

    // 建立 Size -> Pool 映射表
    for (size_t size = 1; size <= MAX_SLAB_SIZE; size++) {
        size_t req_size = size + sizeof(void *); 
        g_size_map[size] = NULL;
        for (int i = 0; i < SLAB_COUNT; i++) {
            if (slab_classes[i] >= req_size) {
                g_size_map[size] = slab_pools[i];
                break;
            }
        }
    }
    
    g_initialized = 1;
}