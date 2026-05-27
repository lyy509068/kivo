

#define _XOPEN_SOURCE 600
#include "kvstore.h"
#include <pthread.h>
#include <string.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include "mempool.h"
#include "replication.h"

extern const kvs_cmd_map_t kvs_cmd_list[];

#define ENABLE_TTL 0
#define DEFAULT_TTL_MS  20000
int64_t default_expire = 0;

int expire_time=0;

#define LOCK_SEGMENTS 32
pthread_rwlock_t seg_locks[LOCK_SEGMENTS];
// 跳表专属的全局读写锁
//static pthread_rwlock_t skip_global_lock = PTHREAD_RWLOCK_INITIALIZER;

static pthread_t global_expire_thread;
static volatile int expire_thread_running = 0;

static inline int get_segment_index(const char *key, size_t len) {
    if (!key || len == 0) return 0;
    unsigned long hash = 5381;
    for (size_t i = 0; i < len; i++) {
        hash = ((hash << 5) + hash) + (unsigned char)key[i];
    }
    return hash % LOCK_SEGMENTS;
}

int kvs_init_locks(void) {
    for (int i = 0; i < LOCK_SEGMENTS; i++) {
        if (pthread_rwlock_init(&seg_locks[i], NULL) != 0) {
            return -1;
        }
    }
    return 0;
}

void kvs_destroy_locks(void) {
    for (int i = 0; i < LOCK_SEGMENTS; i++) {
        pthread_rwlock_destroy(&seg_locks[i]);
    }
}

int expire_thread_init(void) {
    int ret = kvs_expire_thread_start();
    if (ret != 0) {
        fprintf(stderr, "ERROR: Failed to create background expire workers\n");
        return -1;
    }
    return 0;
}

void expire_thread_destroy(void) {
    if (expire_thread_running) {
        expire_thread_running = 0; // 通知线程退出
        pthread_join(global_expire_thread, NULL);
    }
}


#if ENABLE_MEM_POOL
extern mem_pool_t *array_item_pool;
extern mem_pool_t *rbtree_node_pool;
extern mem_pool_t *hash_node_pool;
extern mem_pool_t *skip_node_pool;

void *kvs_malloc(size_t size) {
    if (size == 0) return NULL;

    // 根据结构体大小路由到内存池
        if (size == sizeof(kvs_array_item_t) && array_item_pool) 
            return mem_pool_alloc(array_item_pool);
    
        if (size == sizeof(rbtree_node_binary_t) && rbtree_node_pool)
            return mem_pool_alloc(rbtree_node_pool);
        
        if (size == sizeof(hashnode_t) && hash_node_pool)
            return mem_pool_alloc(hash_node_pool);
        
        if (size == sizeof(skipnode_binary_t) && skip_node_pool)
            return mem_pool_alloc(skip_node_pool);

    // 如果不在内存池中，走标准 malloc，但“必须”加上 Header！
    size_t chunk_size = sizeof(mem_header_t) + size;
    void *chunk = malloc(chunk_size);
    if (!chunk) return NULL;
    
    mem_header_t *header = (mem_header_t *)chunk;
    header->owner = NULL; // NULL 代表它是系统 malloc 分配的
    header->size = size;
    
    return (void *)((char *)chunk + sizeof(mem_header_t));
}
void kvs_free(void *ptr) {
    if (!ptr) return;
    
    // 往回倒退 16 字节，看它的“身份证”
    mem_header_t *header = (mem_header_t *)((char *)ptr - sizeof(mem_header_t));
    
    if (header->owner != NULL) {
        // 如果 owner 不为空，说明是从池子里分配的，交给池子处理
        mem_pool_free((mem_pool_t *)header->owner, ptr);
    } else {
        // 如果 owner 为空，说明是上面走标准 malloc 分配的
        free(header); 
    }
}
void *kvs_calloc(size_t nmemb, size_t size) {
    size_t total = nmemb * size;
    void *ptr = kvs_malloc(total);
    if (ptr) {
        memset(ptr, 0, total);
    }
    return ptr;
}
void *kvs_realloc(void *ptr, size_t size) {
    if (!ptr) return kvs_malloc(size);
    if (size == 0) { kvs_free(ptr); return NULL; }
    
    mem_header_t *header = (mem_header_t *)((char *)ptr - sizeof(mem_header_t));
    
    // 如果它是内存池的数据
    if (header->owner != NULL) {
        void *new_ptr = kvs_malloc(size);
        if (new_ptr) {
            size_t copy_size = (size < header->size) ? size : header->size;
            memcpy(new_ptr, ptr, copy_size);
            kvs_free(ptr);
        }
        return new_ptr;
    } 
    // 如果它是系统内存，直接利用原生 realloc 的高性能原地扩容特性
    else {
        // realloc 整个大块 (Header + Data)
        void *new_chunk = realloc(header, sizeof(mem_header_t) + size);
        if (!new_chunk) return NULL;
        
        mem_header_t *new_header = (mem_header_t *)new_chunk;
        new_header->size = size; // 更新大小
        // owner 依然是 NULL
        
        return (void *)((char *)new_chunk + sizeof(mem_header_t));
    }
}
void mem_pool_stats(mem_pool_t *pool) {
    if (!pool) return;
    
    int active_items = pool->total_allocated - pool->total_freed;
    
    // 计算总块数
    int total_blocks = 0;
    pool_block_t *curr = pool->blocks;
    while (curr) {
        total_blocks++;
        curr = curr->next;
    }
    
    size_t block_mem_size = pool->block_capacity * pool->chunk_size;
    size_t total_mem = total_blocks * (block_mem_size + sizeof(pool_block_t));
    
    printf("Memory Pool: active=%d, total_mem=%.2f KB\n", 
           active_items, total_mem / 1024.0);
    
    // 打印系统内存（虚拟内存和物理内存）
    pid_t pid = getpid();
    char path[256];
    char line[256];
    FILE *fp;
    
    sprintf(path, "/proc/%d/status", pid);
    fp = fopen(path, "r");
    if (fp) {
        while (fgets(line, sizeof(line), fp)) {
            if (strncmp(line, "VmSize:", 7) == 0) {
                printf("  %s", line);
            } else if (strncmp(line, "VmRSS:", 6) == 0) {
                printf("  %s", line);
            }
        }
        fclose(fp);
    }
}

#else  // 关闭内存池时，使用系统函数
void *kvs_malloc(size_t size) {
    return malloc(size);
}

void kvs_free(void *ptr) {
    free(ptr);
}

void *kvs_calloc(size_t nmemb, size_t size) {
    return calloc(nmemb, size);
}

void *kvs_realloc(void *ptr, size_t size) {
    return realloc(ptr, size);
}

void mem_pool_stats(mem_pool_t *pool) {
    (void)pool;
}

#endif



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

void dest_kvengine(void);

enum {
    // Array
    CMD_SET = 0, CMD_GET, CMD_DEL, CMD_MOD, CMD_EXIST,
    // RBTree
    CMD_RSET, CMD_RGET, CMD_RDEL, CMD_RMOD, CMD_REXIST,
    // Hash 
    CMD_HSET, CMD_HGET, CMD_HDEL, CMD_HMOD, CMD_HEXIST,
    // SkipList
    CMD_SSET, CMD_SGET, CMD_SDEL, CMD_SMOD, CMD_SEXIST,

    CMD_SHUTDOWN,
    CMD_UNKNOWN
};

const kvs_cmd_map_t kvs_cmd_list[] = {
    {"SET",      3, CMD_SET},      {"GET",      3, CMD_GET},      {"DEL",      3, CMD_DEL},      {"MOD",      3, CMD_MOD},      {"EXIST",    5, CMD_EXIST},
    {"RSET",     4, CMD_RSET},     {"RGET",     4, CMD_RGET},     {"RDEL",     4, CMD_RDEL},     {"RMOD",     4, CMD_RMOD},     {"REXIST",   6, CMD_REXIST},
    {"HSET",     4, CMD_HSET},     {"HGET",     4, CMD_HGET},     {"HDEL",     4, CMD_HDEL},     {"HMOD",     4, CMD_HMOD},     {"HEXIST",   6, CMD_HEXIST},
    {"SSET",     4, CMD_SSET},     {"SGET",     4, CMD_SGET},     {"SDEL",     4, CMD_SDEL},     {"SMOD",     4, CMD_SMOD},     {"SEXIST",   6, CMD_SEXIST},
    {"SHUTDOWN", 8, CMD_SHUTDOWN}
};
#define KVS_CMD_LIST_SIZE (sizeof(kvs_cmd_list) / sizeof(kvs_cmd_list[0]))

static int ensure_capacity(session_ctx_t *ctx, int needed) {
    if (!ctx || !ctx->wbuffer || !ctx->wcapacity || !ctx->wlength) return -1;

    // 总容量 = 当前已经攒下的数据长度 + 本次需要追加的长度
    int total_needed = *(ctx->wlength) + needed;

    // 如果当前总容量不够，则执行扩容
    if (*(ctx->wcapacity) < total_needed) {
        // 初始给 4KB，否则按原有容量的 2 倍向上翻倍
        int new_capacity = (*ctx->wcapacity == 0) ? 4096 : (*ctx->wcapacity * 2);
        
        // 如果翻倍后的容量依然装不下 total_needed，则直接扩容到刚好满足 total_needed
        if (new_capacity < total_needed) {
            new_capacity = total_needed;
        }

        // 重新分配内存
        char *new_buf = (char *)kvs_realloc(*(ctx->wbuffer), new_capacity);
        if (!new_buf) {
            perror("kvs_realloc failed in handler");
            return -1;
        }

        *(ctx->wbuffer) = new_buf;
        *(ctx->wcapacity) = new_capacity;
    }
    return 0;
}

#if ENABLE_PERSISTENCE
void log_binary_command(const char *cmd, void *key, int key_len, void *value, int value_len) {

    if (!cmd || !key || key_len <= 0) {
        return; 
    }

    int cmd_len = (int)strlen(cmd);

    int payload_len = 4 + cmd_len + 4 + key_len + 4 + (value ? value_len : 0);
    // 向上进行 8 字节对齐
    int total_len = (payload_len + 7) & ~7; 

    char *buf = (char *)kvs_malloc(total_len);
    if (!buf) {
        fprintf(stderr, "[AOF Error] Memory pool exhausted for log allocation! Size: %d\n", total_len);
        return;
    }
    int pos = 0;
    // 序列化命令
    *(int*)(buf + pos) = cmd_len; pos += 4;
    memcpy(buf + pos, cmd, cmd_len); pos += cmd_len;
    // 序列化 Key
    *(int*)(buf + pos) = key_len; pos += 4;
    memcpy(buf + pos, key, key_len); pos += key_len;
    // 序列化 Value
    if (value && value_len > 0) {
        *(int*)(buf + pos) = value_len; pos += 4;
        memcpy(buf + pos, value, value_len); pos += value_len;
    } else {
        *(int*)(buf + pos) = 0; pos += 4;
    }
    kvs_persistence_write(buf, payload_len);
    kvs_free(buf);
}
#endif 

int kvs_protocol(void *msg, int msg_len, session_ctx_t *ctx) {
    if (!msg || msg_len <= 0 || !ctx || !ctx->wbuffer) return -1;

    // 动态计算指针漂移
    int cmd_len = *(int*)msg;
    char *cmd   = msg + 4;

    int key_len = *(int*)(msg + 4 + cmd_len);
    char *key   = msg + 4 + cmd_len + 4;

    int value_len = *(int*)(msg + 4 + cmd_len + 4 + key_len);
    char *value   = msg + 4 + cmd_len + 4 + key_len + 4;

    // 防止解析出来的长度越界
    if (4 + cmd_len + 4 + key_len + 4 + value_len > msg_len) {
        if (ensure_capacity(ctx, 128) == 0) {
            *(ctx->wlength) += sprintf(*(ctx->wbuffer) + *(ctx->wlength), "PARSE ERROR\r\n");
        }
        return -1;
    }

    // 构造面向底层所有引擎的通用二进制安全传输体 (kv_key, kv_value)
    kv_data_t kv_key = {key, (size_t)key_len};
    kv_data_t kv_value = {value, (size_t)value_len};
    int target_cmd = CMD_UNKNOWN;

    // 匹配字符串命令类型 (将字符串 cmd 转成对应的 target_cmd 枚举)
    for (int i = 0; i < KVS_CMD_LIST_SIZE; i++) {
        if (cmd_len == kvs_cmd_list[i].cmd_len && 
            memcmp(cmd, kvs_cmd_list[i].cmd_name, cmd_len) == 0) {
            target_cmd = kvs_cmd_list[i].cmd_enum;
            break;
        }
    }

    int ret = 0;
    kv_data_t *result = NULL;
    
    // 计算分段锁索引与默认过期时间
    int idx = get_segment_index(key, key_len);
    #if ENABLE_TTL
    default_expire = get_current_ms() + DEFAULT_TTL_MS;
    #endif

    switch (target_cmd) {
    #if ENABLE_ARRAY
        case CMD_SET:
            if (ensure_capacity(ctx, 128) != 0) return -1;
            ret = kvs_array_set(&global_array, &kv_key, &kv_value, default_expire);
            if (ret == 0) {
                char *write_ptr = *(ctx->wbuffer) + *(ctx->wlength);
                int written = sprintf(write_ptr, "OK\r\n");
                *(ctx->wlength) += written;

                #if ENABLE_PERSISTENCE
                log_binary_command("SET", key, key_len, value, value_len);
                #endif
                #if ENABLE_REPLICATION
                repl_push_cmd("SET", key, key_len, value, value_len);
                #endif
            }
            else if (ret == 1) {
                char *write_ptr = *(ctx->wbuffer) + *(ctx->wlength);
                int written = sprintf(write_ptr, "EXIST\r\n");
                *(ctx->wlength) += written;
            }
            else {
                char *write_ptr = *(ctx->wbuffer) + *(ctx->wlength);
                int written = sprintf(write_ptr, "ERROR\r\n");
                *(ctx->wlength) += written;
            }
            break;

        case CMD_GET: {
            int val_len = kvs_array_get_value_len(key, key_len);
            if (ensure_capacity(ctx, (val_len > 0 ? val_len : 0) + 128) != 0) return -1;
            
            result = kvs_array_get(&global_array, &kv_key);
            char *write_ptr = *(ctx->wbuffer) + *(ctx->wlength);

            if (result && result->data && result->len > 0) {
                memcpy(write_ptr, result->data, result->len);
                *(ctx->wlength) += result->len;
                //追加换行符
                char *tail_ptr = *(ctx->wbuffer) + *(ctx->wlength);
                int tail_written = sprintf(tail_ptr, "\r\n");
                *(ctx->wlength) += tail_written;
            } else {
                int written = sprintf(write_ptr, "NO EXIST\r\n");
                *(ctx->wlength) += written;
            }
        }
            break;
        
        case CMD_DEL:
            if (ensure_capacity(ctx, 128) != 0) return -1;
            ret = kvs_array_del(&global_array, &kv_key);
            {
                char *write_ptr = *(ctx->wbuffer) + *(ctx->wlength);
                if (ret == 0) {
                    int written = sprintf(write_ptr, "OK\r\n");
                    *(ctx->wlength) += written;
                    #if ENABLE_PERSISTENCE
                    log_binary_command("DEL", key, key_len, NULL, 0); 
                    #endif
                    #if ENABLE_REPLICATION
                    repl_push_cmd("DEL", key, key_len, NULL, 0);
                    #endif
                } else {
                    int written = sprintf(write_ptr, "NO EXIST\r\n");
                    *(ctx->wlength) += written;
                }
            }
            break;

        case CMD_MOD:
            if (ensure_capacity(ctx, 128) != 0) return -1;
            ret = kvs_array_mod(&global_array, &kv_key, &kv_value, default_expire);
            {
                char *write_ptr = *(ctx->wbuffer) + *(ctx->wlength);
                if (ret == 0) {
                    int written = sprintf(write_ptr, "OK\r\n");
                    *(ctx->wlength) += written;
                    
                    #if ENABLE_PERSISTENCE
                    log_binary_command("MOD", key, key_len, value, value_len);
                    #endif
                    #if ENABLE_REPLICATION
                    repl_push_cmd("MOD", key, key_len, value, value_len);
                    #endif 
                }
                else if (ret == 1) {
                    int written = sprintf(write_ptr, "NO EXIST\r\n");
                    *(ctx->wlength) += written;
                }
                else {
                    int written = sprintf(write_ptr, "ERROR\r\n");
                    *(ctx->wlength) += written;
                }
            }
            break;

        case CMD_EXIST:
            if (ensure_capacity(ctx, 128) != 0) return -1;
            ret = kvs_array_exist(&global_array, &kv_key);
            {
                char *write_ptr = *(ctx->wbuffer) + *(ctx->wlength);
                int written = sprintf(write_ptr, (ret == 0) ? "EXIST\r\n" : "NO EXIST\r\n");
                *(ctx->wlength) += written;
            }
            break;
    #endif
    
    #if ENABLE_RBTREE
        case CMD_RSET: {
            if (ensure_capacity(ctx, 128) != 0) return -1;
            int ret = kvs_rbtree_set(&global_rbtree, &kv_key, &kv_value, default_expire);
            char *write_ptr = *(ctx->wbuffer) + *(ctx->wlength);
            if (ret == 0) {
                *(ctx->wlength) += sprintf(write_ptr, "OK\r\n");
                #if ENABLE_PERSISTENCE
                log_binary_command("RSET", key, key_len, value, value_len);
                #endif
                #if ENABLE_REPLICATION
                repl_push_cmd("RSET", key, key_len, value, value_len);
                #endif
            }
            else if (ret == 1) {
                *(ctx->wlength) += sprintf(write_ptr, "EXIST\r\n");
            }
            else {
                *(ctx->wlength) += sprintf(write_ptr, "ERROR\r\n");
            }
            break;
        }

        case CMD_RGET: {
            int val_len = kvs_rbtree_get_value_len(key, key_len);
            if (ensure_capacity(ctx, (val_len > 0 ? val_len : 0) + 128) != 0) return -1;

            kv_data_t *result = kvs_rbtree_get(&global_rbtree, &kv_key);
            char *write_ptr = *(ctx->wbuffer) + *(ctx->wlength);

            if (result && result->data && result->len > 0) {
                memcpy(write_ptr, result->data, result->len);
                *(ctx->wlength) += result->len;
                
                char *tail_ptr = *(ctx->wbuffer) + *(ctx->wlength);
                *(ctx->wlength) += sprintf(tail_ptr, "\r\n");
            } else {
                *(ctx->wlength) += sprintf(write_ptr, "NO EXIST\r\n");
            }
            break;
        }
        
        case CMD_RDEL: {
            if (ensure_capacity(ctx, 128) != 0) return -1;
            int ret = kvs_rbtree_del(&global_rbtree, &kv_key);
            char *write_ptr = *(ctx->wbuffer) + *(ctx->wlength);
            
            if (ret == 0) {
                *(ctx->wlength) += sprintf(write_ptr, "OK\r\n");
                #if ENABLE_PERSISTENCE
                log_binary_command("RDEL", key, key_len, NULL, 0); 
                #endif
                #if ENABLE_REPLICATION
                repl_push_cmd("RDEL", key, key_len, NULL, 0);
                #endif
            } else {
                *(ctx->wlength) += sprintf(write_ptr, "NO EXIST\r\n");
            }
            break;
        }

        case CMD_RMOD: {
            if (ensure_capacity(ctx, 128) != 0) return -1;
            int ret = kvs_rbtree_mod(&global_rbtree, &kv_key, &kv_value, default_expire);
            char *write_ptr = *(ctx->wbuffer) + *(ctx->wlength);
            
            if (ret == 0) {
                *(ctx->wlength) += sprintf(write_ptr, "OK\r\n");
                #if ENABLE_PERSISTENCE
                log_binary_command("RMOD", key, key_len, value, value_len);
                #endif
                #if ENABLE_REPLICATION
                repl_push_cmd("RMOD", key, key_len, value, value_len);
                #endif 
            }
            else if (ret == 1) {
                *(ctx->wlength) += sprintf(write_ptr, "NO EXIST\r\n");
            }
            else {
                *(ctx->wlength) += sprintf(write_ptr, "ERROR\r\n");
            }
            break;
        }

        case CMD_REXIST: {
            if (ensure_capacity(ctx, 128) != 0) return -1;
            int ret = kvs_rbtree_exist(&global_rbtree, &kv_key);
            char *write_ptr = *(ctx->wbuffer) + *(ctx->wlength);
            
            *(ctx->wlength) += sprintf(write_ptr, (ret == 0) ? "EXIST\r\n" : "NO EXIST\r\n");
            break;
        }
    #endif

    #if ENABLE_HASH
        case CMD_HSET: {
            if (ensure_capacity(ctx, 128) != 0) return -1;
            int ret = kvs_hash_set(&global_hash, &kv_key, &kv_value, default_expire);
            char *write_ptr = *(ctx->wbuffer) + *(ctx->wlength);
            if (ret == 0) {
                *(ctx->wlength) += sprintf(write_ptr, "OK\r\n");
                #if ENABLE_PERSISTENCE
                log_binary_command("HSET", key, key_len, value, value_len);
                #endif
                #if ENABLE_REPLICATION
                repl_push_cmd("HSET", key, key_len, value, value_len);
                #endif
            }
            else if (ret == 1) {
                *(ctx->wlength) += sprintf(write_ptr, "EXIST\r\n");
            }
            else {
                *(ctx->wlength) += sprintf(write_ptr, "ERROR\r\n");
            }
            break;
        }

        case CMD_HGET: {
            int val_len = kvs_hash_get_value_len(&global_hash, &kv_key);
            if (ensure_capacity(ctx, (val_len > 0 ? val_len : 0) + 128) != 0) return -1;

            kv_data_t *result = kvs_hash_get(&global_hash, &kv_key);
            char *write_ptr = *(ctx->wbuffer) + *(ctx->wlength);

            if (result && result->data && result->len > 0) {
                memcpy(write_ptr, result->data, result->len);
                *(ctx->wlength) += result->len;
                
                char *tail_ptr = *(ctx->wbuffer) + *(ctx->wlength);
                *(ctx->wlength) += sprintf(tail_ptr, "\r\n");
            } else {
                *(ctx->wlength) += sprintf(write_ptr, "NO EXIST\r\n");
            }
            break;
        }

        case CMD_HDEL: {
            if (ensure_capacity(ctx, 128) != 0) return -1;
            int ret = kvs_hash_del(&global_hash, &kv_key);
            char *write_ptr = *(ctx->wbuffer) + *(ctx->wlength);
            
            if (ret == 0) {
                *(ctx->wlength) += sprintf(write_ptr, "OK\r\n");
                #if ENABLE_PERSISTENCE
                log_binary_command("HDEL", key, key_len, NULL, 0); 
                #endif
                #if ENABLE_REPLICATION
                repl_push_cmd("HDEL", key, key_len, NULL, 0);
                #endif
            } else {
                *(ctx->wlength) += sprintf(write_ptr, "NO EXIST\r\n");
            }
            break;
        }

        case CMD_HMOD: {
            if (ensure_capacity(ctx, 128) != 0) return -1;
            int ret = kvs_hash_mod(&global_hash, &kv_key, &kv_value, default_expire);
            char *write_ptr = *(ctx->wbuffer) + *(ctx->wlength);
            
            if (ret == 0) {
                *(ctx->wlength) += sprintf(write_ptr, "OK\r\n");
                #if ENABLE_PERSISTENCE
                log_binary_command("HMOD", key, key_len, value, value_len);
                #endif
                #if ENABLE_REPLICATION
                repl_push_cmd("HMOD", key, key_len, value, value_len);
                #endif 
            }
            else if (ret == 1) {
                *(ctx->wlength) += sprintf(write_ptr, "NO EXIST\r\n");
            }
            else {
                *(ctx->wlength) += sprintf(write_ptr, "ERROR\r\n");
            }
            break;
        }

        case CMD_HEXIST: {
            if (ensure_capacity(ctx, 128) != 0) return -1;
            int ret = kvs_hash_exist(&global_hash, &kv_key);
            char *write_ptr = *(ctx->wbuffer) + *(ctx->wlength);
            
            *(ctx->wlength) += sprintf(write_ptr, (ret == 0) ? "EXIST\r\n" : "NO EXIST\r\n");
            break;
        }
    #endif

    #if ENABLE_SKIPLIST 
        case CMD_SSET: {
            if (ensure_capacity(ctx, 128) != 0) return -1;
            
            pthread_rwlock_wrlock(&seg_locks[idx]); // 加写锁
            int ret = kvs_skip_set(&global_skip, &kv_key, &kv_value, default_expire);
            pthread_rwlock_unlock(&seg_locks[idx]); // 解锁
            
            char *write_ptr = *(ctx->wbuffer) + *(ctx->wlength);
            if (ret == 0) {
                *(ctx->wlength) += sprintf(write_ptr, "OK\r\n");
                #if ENABLE_PERSISTENCE
                log_binary_command("SSET", key, key_len, value, value_len);
                #endif
                #if ENABLE_REPLICATION
                repl_push_cmd("SSET", key, key_len, value, value_len);
                #endif
            }
            else if (ret == 1) {
                *(ctx->wlength) += sprintf(write_ptr, "EXIST\r\n");
            }
            else {
                *(ctx->wlength) += sprintf(write_ptr, "ERROR\r\n");
            }
            break;
        }
        case CMD_SGET: {
            pthread_rwlock_wrlock(&seg_locks[idx]); 
            kv_data_t *result = kvs_skip_get(&global_skip, &kv_key);
            
            if (result && result->data && result->len > 0) {
                // 拿到实际长度后，再确保缓冲区容量
                if (ensure_capacity(ctx, result->len + 128) != 0) {
                    pthread_rwlock_unlock(&seg_locks[idx]);
                    return -1;
                }

                char *write_ptr = *(ctx->wbuffer) + *(ctx->wlength);
                memcpy(write_ptr, result->data, result->len);
                *(ctx->wlength) += result->len;
                
                char *tail_ptr = *(ctx->wbuffer) + *(ctx->wlength);
                *(ctx->wlength) += sprintf(tail_ptr, "\r\n");
                
                pthread_rwlock_unlock(&seg_locks[idx]); // 解锁
            } else {
                pthread_rwlock_unlock(&seg_locks[idx]); // 没找到或者过期被删除了，先解锁
                
                // 扩容和写 buffer 可以在无锁状态下进行（ctx 是当前连接独享的）
                if (ensure_capacity(ctx, 128) != 0) return -1;
                char *write_ptr = *(ctx->wbuffer) + *(ctx->wlength);
                *(ctx->wlength) += sprintf(write_ptr, "NO EXIST\r\n");
            }
            break;
        }
        case CMD_SDEL: {
            if (ensure_capacity(ctx, 128) != 0) return -1;
            
            pthread_rwlock_wrlock(&seg_locks[idx]); // 加写锁
            int ret = kvs_skip_del(&global_skip, &kv_key);
            pthread_rwlock_unlock(&seg_locks[idx]); // 解锁
            
            char *write_ptr = *(ctx->wbuffer) + *(ctx->wlength);
            
            if (ret == 0) {
                *(ctx->wlength) += sprintf(write_ptr, "OK\r\n");
                #if ENABLE_PERSISTENCE
                log_binary_command("SDEL", key, key_len, NULL, 0); 
                #endif
                #if ENABLE_REPLICATION
                repl_push_cmd("SDEL", key, key_len, NULL, 0);
                #endif
            } else {
                *(ctx->wlength) += sprintf(write_ptr, "NO EXIST\r\n");
            }
            break;
        }

        case CMD_SMOD: {
            if (ensure_capacity(ctx, 128) != 0) return -1;
            
            pthread_rwlock_wrlock(&seg_locks[idx]); // 加写锁
            int ret = kvs_skip_mod(&global_skip, &kv_key, &kv_value, default_expire);
            pthread_rwlock_unlock(&seg_locks[idx]); // 解锁
            
            char *write_ptr = *(ctx->wbuffer) + *(ctx->wlength);
            
            if (ret == 0) {
                *(ctx->wlength) += sprintf(write_ptr, "OK\r\n");
                #if ENABLE_PERSISTENCE
                log_binary_command("SMOD", key, key_len, value, value_len);
                #endif
                #if ENABLE_REPLICATION
                repl_push_cmd("SMOD", key, key_len, value, value_len);
                #endif
            }
            else if (ret == 1) {
                *(ctx->wlength) += sprintf(write_ptr, "NO EXIST\r\n");
            }
            else {
                *(ctx->wlength) += sprintf(write_ptr, "ERROR\r\n");
            }
            break;
        }
        case CMD_SEXIST: {
            if (ensure_capacity(ctx, 128) != 0) return -1;
            pthread_rwlock_wrlock(&seg_locks[idx]); 
            int ret = kvs_skip_exist(&global_skip, &kv_key);
            pthread_rwlock_unlock(&seg_locks[idx]); // 解锁
            
            char *write_ptr = *(ctx->wbuffer) + *(ctx->wlength);
            *(ctx->wlength) += sprintf(write_ptr, (ret == 0) ? "EXIST\r\n" : "NO EXIST\r\n");
            break;
        }
    #endif
    
    #if 0
        case CMD_SSET: {
            // 1. 先安全扩容
            if (ensure_capacity(ctx, 128) != 0) return -1;
            
            pthread_rwlock_wrlock(&skip_global_lock); 
            int ret = kvs_skip_set(&global_skip, &kv_key, &kv_value, default_expire);
            pthread_rwlock_unlock(&skip_global_lock); 
            
            // 2. 💡 核心安全修改：永远在扩容和锁后，重新解引用获取最新的 wbuffer 基地址
            char *base_ptr = *(ctx->wbuffer);
            int current_len = *(ctx->wlength);
            
            if (ret == 0) {
                // 动态计算绝对安全的写入偏移量
                *(ctx->wlength) += sprintf(base_ptr + current_len, "OK\r\n");
                #if ENABLE_PERSISTENCE
                log_binary_command("SSET", key, key_len, value, value_len);
                #endif
            }
            else if (ret == 1) {
                *(ctx->wlength) += sprintf(base_ptr + current_len, "EXIST\r\n");
            }
            else {
                *(ctx->wlength) += sprintf(base_ptr + current_len, "ERROR\r\n");
            }
            break;
        }

        case CMD_SGET: {
            // 💡 修正：因为底层 get 包含惰性删除，属于隐式写操作，必须加全局写锁
            pthread_rwlock_wrlock(&skip_global_lock); 
            kv_data_t *result = kvs_skip_get(&global_skip, &kv_key);
            
            if (result && result->data && result->len > 0) {
                if (ensure_capacity(ctx, result->len + 128) != 0) {
                    pthread_rwlock_unlock(&skip_global_lock);
                    return -1;
                }

                char *write_ptr = *(ctx->wbuffer) + *(ctx->wlength);
                memcpy(write_ptr, result->data, result->len);
                *(ctx->wlength) += result->len;
                
                char *tail_ptr = *(ctx->wbuffer) + *(ctx->wlength);
                *(ctx->wlength) += sprintf(tail_ptr, "\r\n");
                
                pthread_rwlock_unlock(&skip_global_lock); 
            } else {
                pthread_rwlock_unlock(&skip_global_lock); 
                
                if (ensure_capacity(ctx, 128) != 0) return -1;
                char *write_ptr = *(ctx->wbuffer) + *(ctx->wlength);
                *(ctx->wlength) += sprintf(write_ptr, "NO EXIST\r\n");
            }
            break;
        }

        case CMD_SDEL: {
            if (ensure_capacity(ctx, 128) != 0) return -1;
            
            // 💡 修正：使用跳表全局写锁
            pthread_rwlock_wrlock(&skip_global_lock); 
            int ret = kvs_skip_del(&global_skip, &kv_key);
            pthread_rwlock_unlock(&skip_global_lock); 
            
            char *write_ptr = *(ctx->wbuffer) + *(ctx->wlength);
            
            if (ret == 0) {
                *(ctx->wlength) += sprintf(write_ptr, "OK\r\n");
                #if ENABLE_PERSISTENCE
                log_binary_command("SDEL", key, key_len, NULL, 0); 
                #endif
                #if ENABLE_REPLICATION
                repl_push_cmd("SDEL", key, key_len, NULL, 0);
                #endif
            } else {
                *(ctx->wlength) += sprintf(write_ptr, "NO EXIST\r\n");
            }
            break;
        }

        case CMD_SMOD: {
            if (ensure_capacity(ctx, 128) != 0) return -1;
            
            // 💡 修正：使用跳表全局写锁
            pthread_rwlock_wrlock(&skip_global_lock); 
            int ret = kvs_skip_mod(&global_skip, &kv_key, &kv_value, default_expire);
            pthread_rwlock_unlock(&skip_global_lock); 
            
            char *write_ptr = *(ctx->wbuffer) + *(ctx->wlength);
            
            if (ret == 0) {
                *(ctx->wlength) += sprintf(write_ptr, "OK\r\n");
                #if ENABLE_PERSISTENCE
                log_binary_command("SMOD", key, key_len, value, value_len);
                #endif
                #if ENABLE_REPLICATION
                repl_push_cmd("SMOD", key, key_len, value, value_len);
                #endif
            }
            else if (ret == 1) {
                *(ctx->wlength) += sprintf(write_ptr, "NO EXIST\r\n");
            }
            else {
                *(ctx->wlength) += sprintf(write_ptr, "ERROR\r\n");
            }
            break;
        }

        case CMD_SEXIST: {
            if (ensure_capacity(ctx, 128) != 0) return -1;
            
            // 💡 修正：底层存在性判断调用了 get（含惰性删除），必须加全局写锁
            pthread_rwlock_wrlock(&skip_global_lock); 
            int ret = kvs_skip_exist(&global_skip, &kv_key);
            pthread_rwlock_unlock(&skip_global_lock); 
            
            char *write_ptr = *(ctx->wbuffer) + *(ctx->wlength);
            *(ctx->wlength) += sprintf(write_ptr, (ret == 0) ? "EXIST\r\n" : "NO EXIST\r\n");
            break;
        }
    #endif

        case CMD_SHUTDOWN:
            if (ensure_capacity(ctx, 128) == 0) 
                *(ctx->wlength) += sprintf(*(ctx->wbuffer), "SHUTDOWN\r\n");
            dest_kvengine();
            exit(0);

        default:
            if (ensure_capacity(ctx, 256) == 0)
                *(ctx->wlength) += sprintf(*(ctx->wbuffer), "UNKNOWN COMMAND\r\n");
            break;
        }

    return 0;
}

int init_kvengine(void) {
// 1. 基础组件初始化（全局锁、内存池）
    if (kvs_init_locks() != 0) {
        printf("Failed to init segment locks\n");
        return -1;
    }

#if ENABLE_MEM_POOL
    array_item_pool = mem_pool_create(sizeof(kvs_array_item_t));
    rbtree_node_pool = mem_pool_create(sizeof(rbtree_node_binary_t));
    hash_node_pool = mem_pool_create(sizeof(hashnode_t));
    skip_node_pool = mem_pool_create(sizeof(skipnode_binary_t));
    
    if (!array_item_pool || !rbtree_node_pool || !hash_node_pool || !skip_node_pool) {
        printf("Failed to create memory pools\n");
        return -1;
    }
#endif

// 2. 空引擎结构初始化
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
    
// 3. 从磁盘恢复数据（先快照，再用 AOF 日志追平增量）
#if ENABLE_SNAPSHOT
    kvs_snapshot_load();        
#endif
#if ENABLE_PERSISTENCE
    kvs_persistence_init();
    kvs_persistence_recover(); // 此时内存引擎中数据已完整
#endif

// 4. 开启定时持久化任务
#if ENABLE_SNAPSHOT
    kvs_snapshot_auto_save(1); 
#endif

// 5. 建立主从同步连接（确保本地数据完全恢复后，再开始同步新数据）
#if ENABLE_REPLICATION
    const char *slave_ip = "192.168.92.129";
    unsigned short slave_port = 2000;
    repl_connect_to_slave(slave_ip, slave_port); 
#endif 

    // 6. 最后拉起后端定时超时删除线程（防止恢复期间提前触发删除）
    if (expire_thread_init() != 0) {
        return -1;
    }

    return 0;
}

void dest_kvengine(void) {
// 1. 立刻切断一切外部/后台异步写操作的来源
    expire_thread_destroy(); // 关掉超时清理线程

#if ENABLE_REPLICATION
    repl_close();            // 优先关闭主从同步，防止后续落盘/销毁阶段产生数据抖动
#endif

#if ENABLE_SNAPSHOT
    printf("\n[DEBUG] Entering dest_kvengine...\n");
    fflush(stdout);
    kvs_snapshot_auto_save_stop();  // 停止自动快照定时器
    kvs_snapshot_save();            // 最后做一次强制全量快照落盘
#endif

#if ENABLE_PERSISTENCE
    kvs_persistence_close();        // 关闭并刷盘 AOF 日志文件流
#endif

// 2. 彻底安全的释放本地内存引擎
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

// 3. 释放底层支撑组件（内存池、锁）
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

    kvs_destroy_locks(); // 最后释放锁
}


int main(int argc, char *argv[]) {
    if (argc != 2) return -1;
    int port = atoi(argv[1]);

    init_kvengine();
    
#if (NETWORK_SELECT == NETWORK_REACTOR)
    reactor_start(port, kvs_protocol);  
#elif (NETWORK_SELECT == NETWORK_PROACTOR)
    proactor_start(port, kvs_protocol);
#elif (NETWORK_SELECT == NETWORK_NTYCO)
    ntyco_start(port, kvs_protocol);
#endif

    dest_kvengine();
    return 0;
}