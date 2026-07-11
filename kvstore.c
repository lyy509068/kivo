

#define _XOPEN_SOURCE 600
#include <pthread.h>
#include "kvstore.h"
#include <string.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include "mempool.h"
#include <arpa/inet.h>
#include <strings.h> 
#include "resp.h" 
#include "repl.h"
#include "ebpf.h"
#include "rdma.h"

extern struct rdma_ring_ctx *g_rdma_ctx;

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

//分段锁
pthread_rwlock_t seg_locks[LOCK_SEGMENTS];

//分配锁
static inline int get_segment_index(const char *key, size_t len) {
    if (!key || len == 0) return 0;
    unsigned long hash = 5381;
    for (size_t i = 0; i < len; i++) {
        hash = ((hash << 5) + hash) + (unsigned char)key[i];
    }
    return hash % LOCK_SEGMENTS;
}

//初始化锁
int kvs_init_locks(void) {
    for (int i = 0; i < LOCK_SEGMENTS; i++) {
        if (pthread_rwlock_init(&seg_locks[i], NULL) != 0) {
            return -1;
        }
    }
    return 0;
}

//销毁锁
void kvs_destroy_locks(void) {
    for (int i = 0; i < LOCK_SEGMENTS; i++) {
        pthread_rwlock_destroy(&seg_locks[i]);
    }
}


//清理线程
static pthread_t global_expire_thread;
static volatile int expire_thread_running = 0;

//初始化删除线程
int expire_thread_init(void) {
    int ret = kvs_expire_thread_start();
    if (ret != 0) {
        fprintf(stderr, "ERROR: Failed to create background expire workers\n");
        return -1;
    }
    return 0;
}
//销毁删除线程
void expire_thread_destroy(void) {
    if (expire_thread_running) {
        expire_thread_running = 0; 
        pthread_join(global_expire_thread, NULL);
    }
}


//内存池
extern mem_pool_t *array_item_pool;
extern mem_pool_t *rbtree_node_pool;
extern mem_pool_t *hash_node_pool;
extern mem_pool_t *skip_node_pool;

void *kvs_malloc(size_t size) {
    if (size == 0) return NULL;

    if (g_enable_mempool) {
        if (size == sizeof(kvs_array_item_t) && array_item_pool) {
            void *ptr = mem_pool_alloc(array_item_pool);
            if (ptr) { mem_header_t *h = (mem_header_t *)((char *)ptr - sizeof(mem_header_t)); h->owner = array_item_pool; h->size = size; }
            return ptr;
        }
        if (size == sizeof(rbtree_node_binary_t) && rbtree_node_pool) {
            void *ptr = mem_pool_alloc(rbtree_node_pool);
            if (ptr) { mem_header_t *h = (mem_header_t *)((char *)ptr - sizeof(mem_header_t)); h->owner = rbtree_node_pool; h->size = size; }
            return ptr;
        }
        if (size == sizeof(hashnode_t) && hash_node_pool) {
            void *ptr = mem_pool_alloc(hash_node_pool);
            if (ptr) { mem_header_t *h = (mem_header_t *)((char *)ptr - sizeof(mem_header_t)); h->owner = hash_node_pool; h->size = size; }
            return ptr;
        }
        if (size == sizeof(skipnode_binary_t) && skip_node_pool) {
            void *ptr = mem_pool_alloc(skip_node_pool);
            if (ptr) { mem_header_t *h = (mem_header_t *)((char *)ptr - sizeof(mem_header_t)); h->owner = skip_node_pool; h->size = size; }
            return ptr;
        }
        size_t chunk_size = sizeof(mem_header_t) + size;
        void *chunk = malloc(chunk_size);
        if (!chunk) return NULL;
        mem_header_t *h = (mem_header_t *)chunk;
        h->owner = NULL; h->size = size;
        return (void *)((char *)chunk + sizeof(mem_header_t));
    }

    return malloc(size);
}

void kvs_free(void *ptr) {
    if (!ptr) return;
    if (g_enable_mempool) {
        mem_header_t *h = (mem_header_t *)((char *)ptr - sizeof(mem_header_t));
        if (h->owner) { mem_pool_free((mem_pool_t *)h->owner, ptr); return; }
        free(h); return;
    }
    free(ptr);
}

void *kvs_calloc(size_t nmemb, size_t size) {
    size_t total = nmemb * size;
    void *ptr = kvs_malloc(total);
    if (ptr) memset(ptr, 0, total);
    return ptr;
}

void *kvs_realloc(void *ptr, size_t size) {
    if (!ptr) return kvs_malloc(size);
    if (size == 0) { kvs_free(ptr); return NULL; }
    if (g_enable_mempool) {
        mem_header_t *h = (mem_header_t *)((char *)ptr - sizeof(mem_header_t));
        if (h->owner) {
            void *new_ptr = kvs_malloc(size);
            if (new_ptr) {
                size_t copy_size = (size < h->size) ? size : h->size;
                memcpy(new_ptr, ptr, copy_size);
                kvs_free(ptr);
            }
            return new_ptr;
        }
        void *new_chunk = realloc(h, sizeof(mem_header_t) + size);
        if (!new_chunk) return NULL;
        ((mem_header_t *)new_chunk)->size = size;
        return (void *)((char *)new_chunk + sizeof(mem_header_t));
    }
    return realloc(ptr, size);
}


typedef struct {
    const char *cmd_name;
    int cmd_len;
    int cmd_enum;//序号
} kvs_cmd_map_t;

enum {
    // Array
    CMD_SET = 0, CMD_GET, CMD_DEL, CMD_MOD, CMD_EXISTS,
    // RBTree
    CMD_RSET=5, CMD_RGET, CMD_RDEL, CMD_RMOD, CMD_REXISTS,
    // Hash 
    CMD_HSET=10, CMD_HGET, CMD_HDEL, CMD_HMOD, CMD_HEXISTS,
    // SkipList
    CMD_SSET=15, CMD_SGET, CMD_SDEL, CMD_SMOD, CMD_SEXISTS,

    CMD_PING=20, CMD_SAVE, CMD_REPL_SYNC,  CMD_REPL_SYNC_DONE, CMD_UNKNOWN
};

const kvs_cmd_map_t kvs_cmd_list[] = {
    {"SET",      3, CMD_SET},      {"GET",      3, CMD_GET},      {"DEL",      3, CMD_DEL},      {"MOD",      3, CMD_MOD},      {"EXISTS",    6, CMD_EXISTS},
    {"RSET",     4, CMD_RSET},     {"RGET",     4, CMD_RGET},     {"RDEL",     4, CMD_RDEL},     {"RMOD",     4, CMD_RMOD},     {"REXISTS",   7, CMD_REXISTS},
    {"HSET",     4, CMD_HSET},     {"HGET",     4, CMD_HGET},     {"HDEL",     4, CMD_HDEL},     {"HMOD",     4, CMD_HMOD},     {"HEXISTS",   7, CMD_HEXISTS},
    {"SSET",     4, CMD_SSET},     {"SGET",     4, CMD_SGET},     {"SDEL",     4, CMD_SDEL},     {"SMOD",     4, CMD_SMOD},     {"SEXISTS",   7, CMD_SEXISTS},
    {"PING",     4, CMD_PING},     {"SAVE",     4, CMD_SAVE},     {"SYNC",     4, CMD_REPL_SYNC}, {"SYNC_DONE",9,CMD_REPL_SYNC_DONE}, {"UNKNOWN",  7, CMD_UNKNOWN}
};


#define KVS_CMD_LIST_SIZE (sizeof(kvs_cmd_list) / sizeof(kvs_cmd_list[0]))//命令总数

int kvs_execute_command(const resp_request_t *req, resp_reply_t *reply) {
    if (!req || req->argc == 0 || !reply) {
        fprintf(stderr, "[EXEC DEBUG] Invalid params: req=%p, argc=%d, reply=%p\n", 
               (void*)req, req ? req->argc : -1, (void*)reply);
        fflush(stderr);
        return -1;
    }
    // 初始化响应结构体
    reply->status = KVS_RESP_ERROR;
    reply->body = NULL;
    reply->body_len = 0;

    // 提取命令字符串和长度
    char *cmd_str = req->argv[0];
    int cmd_len = req->argv_len[0];
    int target_cmd = CMD_UNKNOWN;

    // 匹配字符串命令类型
    for (int i = 0; i < KVS_CMD_LIST_SIZE; i++) {
        if (cmd_len == kvs_cmd_list[i].cmd_len && 
            strncasecmp(cmd_str, kvs_cmd_list[i].cmd_name, cmd_len) == 0) {
            target_cmd = kvs_cmd_list[i].cmd_enum;
            break;
        }
    }

    // 提取 Key 和 Value 
    // RESP 协议层已经帮我们切分好了，直接拿来用
    char *key = (req->argc > 1) ? req->argv[1] : NULL;
    int key_len = (req->argc > 1) ? req->argv_len[1] : 0;
    
    char *value = (req->argc > 2) ? req->argv[2] : NULL;
    int value_len = (req->argc > 2) ? req->argv_len[2] : 0;

    // 构造底层存储引擎需要的 KV 结构体
    kv_data_t kv_key = {key, (size_t)key_len};
    kv_data_t kv_value = {value, (size_t)value_len};

    int ret = 0;
    kv_data_t *result = NULL;

    // 计算分段锁索引与默认过期时间
    int idx = key ? get_segment_index(key, key_len) : 0;
    unsigned long long expire_time = 0;

    // 解析过期时间
    // argv[3] 可能是 "EX" 或 "PX" 关键字，也可能是直接的过期时间戳
    if (g_enable_ttl && req->argc >= 4 && req->argv[3] != NULL) {
        // 方式1：Redis 标准格式 SET key value EX 10 或 SET key value PX 10000
        if (req->argc >= 5 && req->argv[4] != NULL) {
            if (req->argv_len[3] == 2 && strncasecmp(req->argv[3], "EX", 2) == 0) {
                long long seconds = atoll(req->argv[4]);
                if (seconds > 0) {
                    expire_time = get_current_ms() + (seconds * 1000);
                }
            } 
            else if (req->argv_len[3] == 2 && strncasecmp(req->argv[3], "PX", 2) == 0) {
                long long milliseconds = atoll(req->argv[4]);
                if (milliseconds > 0) {
                    expire_time = get_current_ms() + milliseconds;
                }
            }
        }
        // 方式2：AOF 恢复时的格式 SET key value expire_timestamp
        // 直接使用 argv[3] 作为过期时间戳
        else {
            expire_time = atoll(req->argv[3]);
            // 如果解析出来的时间戳已经过期且小于当前时间，说明是绝对时间戳
            // 如果解析出来的是很小的数（比如相对秒数），需要转换
            if (expire_time > 0 && expire_time < 1000000000) {
                // 可能是相对秒数，转换为绝对时间戳
                expire_time = get_current_ms() + (expire_time * 1000);
            }
        }
    }

    switch (target_cmd) {
    #if ENABLE_ARRAY
        case CMD_SET:
            if (req->argc < 3) { reply->status = KVS_RESP_PARSE_ERROR; break; } 
            pthread_rwlock_wrlock(&seg_locks[0]); // 写入操作加写锁
            ret = kvs_array_set(&global_array, &kv_key, &kv_value, expire_time);
            //printf("[KVS_DEBUG] [ARRAY_SET] Engine returned ret = %d\n", ret); 
            if (ret == 0) {
                reply->status = KVS_RESP_OK;
            }
            else if (ret == 1) { reply->status = KVS_RESP_EXISTS; }
            else { reply->status = KVS_RESP_ERROR; }
            pthread_rwlock_unlock(&seg_locks[0]); // 释放锁
            break;

        case CMD_GET:
            if (req->argc < 2) { reply->status = KVS_RESP_PARSE_ERROR; break; }
            pthread_rwlock_rdlock(&seg_locks[0]); // 读取操作加读锁
            result = kvs_array_get(&global_array, &kv_key);
            //printf("[KVS_DEBUG] [ARRAY_GET] Engine returned result ptr = %p\n", (void*)result); 
            if (result && result->data && result->len > 0) {
                //printf("[KVS_DEBUG] [ARRAY_GET] Data found! Len: %zu, Content: %.*s\n", result->len, (int)result->len, (char*)result->data); 
                reply->status = KVS_RESP_GET_OK;
                // 为了彻底解耦且防并发删除，在锁/生命周期内 malloc 拷贝一份副本
                reply->body = kvs_malloc(result->len+1);
                if (reply->body) {
                    memcpy(reply->body, result->data, result->len);
                    reply->body_len = result->len;
                } else { reply->status = KVS_RESP_ERROR; }
            } else { reply->status = KVS_RESP_NO_EXISTS; }
            pthread_rwlock_unlock(&seg_locks[0]); // 释放锁
            break;
        
        case CMD_DEL:
            if (req->argc < 2) { reply->status = KVS_RESP_PARSE_ERROR; break; }
            pthread_rwlock_wrlock(&seg_locks[0]); // 删除操作加写锁
            ret = kvs_array_del(&global_array, &kv_key);
            //printf("[KVS_DEBUG] [ARRAY_DEL] Engine returned ret = %d\n", ret); 
            if (ret == 0) {
                reply->status = KVS_RESP_OK;  
            } else { reply->status = KVS_RESP_NO_EXISTS; }
            pthread_rwlock_unlock(&seg_locks[0]); // 释放锁
            break;

        case CMD_MOD:
            if (req->argc < 3) { reply->status = KVS_RESP_PARSE_ERROR; break; }
            pthread_rwlock_wrlock(&seg_locks[0]); // 修改操作加写锁
            ret = kvs_array_mod(&global_array, &kv_key, &kv_value, expire_time);
            //printf("[KVS_DEBUG] [ARRAY_MOD] Engine returned ret = %d\n", ret); 
            if (ret == 0) {
                reply->status = KVS_RESP_OK;
            }
            else if (ret == 1) { reply->status = KVS_RESP_NO_EXISTS; }
            else { reply->status = KVS_RESP_ERROR; }
            pthread_rwlock_unlock(&seg_locks[0]); // 释放锁
            break;

        case CMD_EXISTS:
            if (req->argc < 2) { reply->status = KVS_RESP_PARSE_ERROR; break; }
            pthread_rwlock_rdlock(&seg_locks[0]); // 存在性查询加读锁
            ret = kvs_array_exist(&global_array, &kv_key);
            //printf("[KVS_DEBUG] [ARRAY_EXIST] Engine returned ret = %d\n", ret); 
            reply->status = (ret == 0) ? KVS_RESP_EXISTS : KVS_RESP_NO_EXISTS;
            pthread_rwlock_unlock(&seg_locks[0]); // 释放锁
            break;
    #endif
    
    #if ENABLE_RBTREE
        case CMD_RSET:
            if (req->argc < 3) { reply->status = KVS_RESP_PARSE_ERROR; break; }
            pthread_rwlock_wrlock(&seg_locks[1]); // 红黑树写操作加写锁
            ret = kvs_rbtree_set(&global_rbtree, &kv_key, &kv_value, expire_time);
            //fprintf(stderr, "[KVS_DEBUG] [RBTREE_SET] Engine returned ret = %d\n", ret);
            //fflush(stderr); 
            if (ret == 0) {
                reply->status = KVS_RESP_OK;
            }
            else if (ret == 1) { reply->status = KVS_RESP_EXISTS; }
            else { reply->status = KVS_RESP_ERROR; }
            pthread_rwlock_unlock(&seg_locks[1]); // 释放锁
            break;

        case CMD_RGET:
            if (req->argc < 2) { reply->status = KVS_RESP_PARSE_ERROR; break; }
            pthread_rwlock_rdlock(&seg_locks[1]); // 红黑树读操作加读锁
            result = kvs_rbtree_get(&global_rbtree, &kv_key);
            //printf("[KVS_DEBUG] [RBTREE_GET] Engine returned result ptr = %p\n", (void*)result); 
            if (result && result->data && result->len > 0) {
                //printf("[KVS_DEBUG] [RBTREE_GET] Data found! Len: %zu, Content: %.*s\n", result->len, (int)result->len, (char*)result->data); 
                reply->status = KVS_RESP_GET_OK;
                reply->body = kvs_malloc(result->len+1);
                if (reply->body) {
                    memcpy(reply->body, result->data, result->len);
                    reply->body_len = result->len;
                } else { reply->status = KVS_RESP_ERROR; }
            } else { reply->status = KVS_RESP_NO_EXISTS; }
            pthread_rwlock_unlock(&seg_locks[1]); // 释放锁
            break;
        
        case CMD_RDEL:
            if (req->argc < 2) { reply->status = KVS_RESP_PARSE_ERROR; break; }
            pthread_rwlock_wrlock(&seg_locks[1]); // 红黑树删除操作加写锁
            ret = kvs_rbtree_del(&global_rbtree, &kv_key);
            //printf("[KVS_DEBUG] [RBTREE_DEL] Engine returned ret = %d\n", ret); 
            if (ret == 0) {
                reply->status = KVS_RESP_OK;
            } else { reply->status = KVS_RESP_NO_EXISTS; }
            pthread_rwlock_unlock(&seg_locks[1]); // 释放锁
            break;

        case CMD_RMOD:
            if (req->argc < 3) { reply->status = KVS_RESP_PARSE_ERROR; break; }
            pthread_rwlock_wrlock(&seg_locks[1]); // 红黑树修改操作加写锁
            ret = kvs_rbtree_mod(&global_rbtree, &kv_key, &kv_value, expire_time);
            //printf("[KVS_DEBUG] [RBTREE_MOD] Engine returned ret = %d\n", ret); 
            if (ret == 0) {
                reply->status = KVS_RESP_OK;
            }
            else if (ret == 1) { reply->status = KVS_RESP_NO_EXISTS; }
            else { reply->status = KVS_RESP_ERROR; }
            pthread_rwlock_unlock(&seg_locks[1]); // 释放锁
            break;

        case CMD_REXISTS:
            if (req->argc < 2) { reply->status = KVS_RESP_PARSE_ERROR; break; }
            pthread_rwlock_rdlock(&seg_locks[1]); // 红黑树查询操作加读锁
            ret = kvs_rbtree_exist(&global_rbtree, &kv_key);
            //printf("[KVS_DEBUG] [RBTREE_EXIST] Engine returned ret = %d\n", ret); 
            reply->status = (ret == 0) ? KVS_RESP_EXISTS : KVS_RESP_NO_EXISTS;
            pthread_rwlock_unlock(&seg_locks[1]); // 释放锁
            break;
    #endif

    #if ENABLE_HASH
        case CMD_HSET:
            if (req->argc < 3) { reply->status = KVS_RESP_PARSE_ERROR; break; }
            {
                // 直接使用系统自带算好的 idx 变量作为分段锁索引
                pthread_rwlock_wrlock(&seg_locks[idx]); 
                
                ret = kvs_hash_set(&global_hash, &kv_key, &kv_value, expire_time);
                if (ret == 0) {
                    reply->status = KVS_RESP_OK;
                }
                else if (ret == 1) { reply->status = KVS_RESP_EXISTS; }
                else { reply->status = KVS_RESP_ERROR; }
                
                pthread_rwlock_unlock(&seg_locks[idx]); // 释放分段锁
            }
            break;

        case CMD_HGET:
            if (req->argc < 2) { reply->status = KVS_RESP_PARSE_ERROR; break; }
            {
                // 直接使用 idx 变量加读锁
                pthread_rwlock_rdlock(&seg_locks[idx]); 
                
                result = kvs_hash_get(&global_hash, &kv_key);
                if (result && result->data && result->len > 0) {
                    reply->status = KVS_RESP_GET_OK;
                    reply->body = kvs_malloc(result->len+1);
                    if (reply->body) {
                        memcpy(reply->body, result->data, result->len);
                        reply->body_len = result->len;
                    } else { reply->status = KVS_RESP_ERROR; }
                } else { reply->status = KVS_RESP_NO_EXISTS; }
                
                pthread_rwlock_unlock(&seg_locks[idx]); 
            }
            break;

        case CMD_HDEL:
            if (req->argc < 2) { reply->status = KVS_RESP_PARSE_ERROR; break; }
            {
                // 直接使用 idx 变量加写锁
                pthread_rwlock_wrlock(&seg_locks[idx]); 
                
                ret = kvs_hash_del(&global_hash, &kv_key);
                if (ret == 0) {
                    reply->status = KVS_RESP_OK;
                } else { reply->status = KVS_RESP_NO_EXISTS; }
                
                pthread_rwlock_unlock(&seg_locks[idx]); 
            }
            break;

        case CMD_HMOD:
            if (req->argc < 3) { reply->status = KVS_RESP_PARSE_ERROR; break; }
            {
                // 直接使用 idx 变量加写锁
                pthread_rwlock_wrlock(&seg_locks[idx]); 
                
                ret = kvs_hash_mod(&global_hash, &kv_key, &kv_value, expire_time);
                if (ret == 0) {
                    reply->status = KVS_RESP_OK;
                }
                else if (ret == 1) { reply->status = KVS_RESP_NO_EXISTS; }
                else { reply->status = KVS_RESP_ERROR; }
                
                pthread_rwlock_unlock(&seg_locks[idx]); 
            }
            break;

        case CMD_HEXISTS:
            if (req->argc < 2) { reply->status = KVS_RESP_PARSE_ERROR; break; }
            {
                // 直接使用 idx 变量加读锁
                pthread_rwlock_rdlock(&seg_locks[idx]); 
                
                ret = kvs_hash_exist(&global_hash, &kv_key);
                reply->status = (ret == 0) ? KVS_RESP_EXISTS : KVS_RESP_NO_EXISTS;
                
                pthread_rwlock_unlock(&seg_locks[idx]); 
            }
            break;
    #endif

    #if ENABLE_SKIPLIST 
        case CMD_SSET:
            if (req->argc < 3) { reply->status = KVS_RESP_PARSE_ERROR; break; }
            pthread_rwlock_wrlock(&seg_locks[2]); // 跳表写操作加写锁
            ret = kvs_skip_set(&global_skip, &kv_key, &kv_value, expire_time);
             
            //printf("[KVS_DEBUG] [SKIP_SET] Engine returned ret = %d\n", ret); 
            
            if (ret == 0) {
                reply->status = KVS_RESP_OK;
            }
            else if (ret == 1) { reply->status = KVS_RESP_EXISTS; }
            else { reply->status = KVS_RESP_ERROR; }
            pthread_rwlock_unlock(&seg_locks[2]); // 释放锁
            break;

        case CMD_SGET:
            if (req->argc < 2) { reply->status = KVS_RESP_PARSE_ERROR; break; }
            pthread_rwlock_rdlock(&seg_locks[2]); // 跳表读操作加读锁
            result = kvs_skip_get(&global_skip, &kv_key);
            
            if (result && result->data && result->len > 0) {
                reply->status = KVS_RESP_GET_OK;
                // 在锁释放之前完成内存拷贝，多线程下绝对安全
                reply->body = kvs_malloc(result->len+1);
                if (reply->body) {
                    memcpy(reply->body, result->data, result->len);
                    reply->body_len = result->len;
                } else { reply->status = KVS_RESP_ERROR; }
                
            } else {
                reply->status = KVS_RESP_NO_EXISTS;
            }
            pthread_rwlock_unlock(&seg_locks[2]); // 释放锁
            break;

        case CMD_SDEL:
            if (req->argc < 2) { reply->status = KVS_RESP_PARSE_ERROR; break; }
            pthread_rwlock_wrlock(&seg_locks[2]); // 跳表删除操作加写锁
            ret = kvs_skip_del(&global_skip, &kv_key);
            
            //printf("[KVS_DEBUG] [SKIP_DEL] Engine returned ret = %d\n", ret); 
            
            if (ret == 0) {
                reply->status = KVS_RESP_OK;
            } else { reply->status = KVS_RESP_NO_EXISTS; }
            pthread_rwlock_unlock(&seg_locks[2]); // 释放锁
            break;

        case CMD_SMOD:
            if (req->argc < 3) { reply->status = KVS_RESP_PARSE_ERROR; break; }
            pthread_rwlock_wrlock(&seg_locks[2]); // 跳表修改操作加写锁
            ret = kvs_skip_mod(&global_skip, &kv_key, &kv_value, expire_time);
            
            //printf("[KVS_DEBUG] [SKIP_MOD] Engine returned ret = %d\n", ret); 
            
            if (ret == 0) {
                reply->status = KVS_RESP_OK;
            }
            else if (ret == 1) { reply->status = KVS_RESP_NO_EXISTS; }
            else { reply->status = KVS_RESP_ERROR; }
            pthread_rwlock_unlock(&seg_locks[2]); // 释放锁
            break;

        case CMD_SEXISTS:
            if (req->argc < 2) { reply->status = KVS_RESP_PARSE_ERROR; break; }
            pthread_rwlock_rdlock(&seg_locks[2]); // 跳表查询操作加读锁
            ret = kvs_skip_exist(&global_skip, &kv_key);
            
            //printf("[KVS_DEBUG] [SKIP_EXIST] Engine returned ret = %d\n", ret); 
            reply->status = (ret == 0) ? KVS_RESP_EXISTS : KVS_RESP_NO_EXISTS;
            pthread_rwlock_unlock(&seg_locks[2]); // 释放锁
            break;
    #endif
        case CMD_PING:{
            if (req->argc == 1) {
                reply->status = KVS_RESP_PONG;
            } 
            else if (req->argc == 2) {
                reply->status = KVS_RESP_GET_OK; 
                reply->body = kvs_malloc(key_len+1);
                if (reply->body) {
                    memcpy(reply->body, key, key_len);
                    reply->body_len = key_len;
                } else {
                    reply->status = KVS_RESP_ERROR;
                }
            } 
            else { 
                reply->status = KVS_RESP_PARSE_ERROR; 
            }
            break;
        }
        case CMD_SAVE: {
            if (req->argc != 1) {
                reply->status = KVS_RESP_PARSE_ERROR; 
            } else {
                extern int kvs_snapshot_save(void); 
                int ret = kvs_snapshot_save();
                
                if (ret == 0) {
                    reply->status = KVS_RESP_SUCCESS;       
                } else {
                    reply->status = KVS_RESP_ERR; 
                }
            }
            break;
        }
        case CMD_REPL_SYNC: { 
            printf("Received 'SYNC' command from slave.\n");
            fflush(stdout);

            if (g_enable_repl_master){
                if(g_enable_ttl) expire_thread_pause();// 暂停超时删除线程
                g_repl_backlog_enabled = 1;  // 开始缓存
                g_repl_backlog_count = 0;
                if (g_rdma_ctx) {            
                    if (repl_sync_log_via_rdma() != 0) {
                        printf("[Repl Error] Zero-Copy log sync via RDMA failed!\n");
                        fflush(stdout);
                    }
                } else {
                    fprintf(stderr, "[Repl Error] RDMA context is not initialized! Cannot sync.\n");
                    fflush(stdout);
                }
            }
            reply->status = KVS_RESP_SUCCESS;
            
            break;
        }
        case CMD_REPL_SYNC_DONE: {
            printf("[Master] Received SYNC_DONE from slave.\n");

            if (repl_flush_backlog_via_rdma() != 0) {
                fprintf(stderr, "[Repl Error] Failed to flush backlog to slave!\n");
                break;
            }
            repl_destroy();// 释放 RDMA 资源

            if (g_enable_repl_master){
                if(g_enable_ttl){
                    extern int BEGIN_IN;
                    BEGIN_IN = 1; //增量持久化开始标志
                    expire_thread_resume();// 恢复超时删除线程
                }
            /*if (ebpf_register_slave() == 0) {
                if (ebpf_set_forward_switch(1) == 0) {
                    printf("[Master] eBPF TC clone switch ENABLED.\n");
                }
            }*/
            }

            break;
        }
        
        case CMD_UNKNOWN:

        default:
            reply->status = KVS_RESP_UNKNOWN;
            break;
    }


    return 0;
}

