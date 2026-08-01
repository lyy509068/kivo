#ifndef MEMPOOL_H
#define MEMPOOL_H

#include <stddef.h>
#include <stdint.h>

#define POOL_BLOCK_SIZE (64 * 1024)
#define MAX_SLAB_SIZE 4096


typedef enum {
    OBJ_ARRAY = 0,
    OBJ_RBTREE,
    OBJ_HASH,
    OBJ_SKIP,
    OBJ_MAX
} kvs_obj_type_t;

typedef struct pool_block {
    void *data;
    int capacity;
    struct pool_block *next;
} pool_block_t;

typedef struct mem_pool {
    size_t chunk_size;          
    int block_capacity;         
    
    void *free_list;            
    pool_block_t *blocks;       

    pool_block_t *current_block;
    void *next_free;            
    size_t remaining;           
} mem_pool_t;


extern mem_pool_t *g_typed_pools[OBJ_MAX];
extern mem_pool_t *g_size_map[MAX_SLAB_SIZE + 1];
extern int g_enable_mempool;


mem_pool_t *mem_pool_create(size_t user_size);
void mem_pool_destroy(mem_pool_t *pool);
void *mem_pool_alloc(mem_pool_t *pool);
void mem_pool_free(mem_pool_t *pool, void *ptr);


void kvs_mempool_init(void);    

void *kvs_malloc_type(kvs_obj_type_t type, size_t size);
void kvs_free_type(kvs_obj_type_t type, void *ptr);

void *kvs_malloc(size_t size);
void kvs_free(void *ptr);
void *kvs_calloc(size_t nmemb, size_t size);
void *kvs_realloc(void *ptr, size_t size);

#endif