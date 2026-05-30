

#define _XOPEN_SOURCE 600
#include "kvstore.h"
#include <pthread.h>
#include <string.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include "mempool.h"
#include "replication.h"
#include <arpa/inet.h>

extern const kvs_cmd_map_t kvs_cmd_list[];

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



//超时删除开关
#define ENABLE_TTL 0
#define DEFAULT_TTL_MS  20000
int64_t default_expire = 0;
int expire_time=0;
//分段锁，超时删除时保护数据
#define LOCK_SEGMENTS 32
pthread_rwlock_t seg_locks[LOCK_SEGMENTS];
//超时清理线程
static pthread_t global_expire_thread;
static volatile int expire_thread_running = 0;
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

#define KVS_CMD_LIST_SIZE (sizeof(kvs_cmd_list) / sizeof(kvs_cmd_list[0]))//命令总数

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

int kvs_protocol(void *msg, int msg_len, kvs_resp_t *resp) {

    if (!msg || msg_len <= 0 || !resp) return -1;

    // 初始化响应结构体
    resp->status = KVS_RESP_ERROR;
    resp->body = NULL;
    resp->body_len = 0;

    int offset=0;

    if (offset + 4 > msg_len) { resp->status = KVS_RESP_PARSE_ERROR; return -1; }
    int cmd_len_net;
    memcpy(&cmd_len_net, (char *)msg + offset, 4); // 从当前偏移位置安全拷贝4字节
    int cmd_len = ntohl(cmd_len_net);              // 转换为本地小端序
    offset += 4;                                   // 游标向后移动4字节
    if (offset + cmd_len > msg_len) { resp->status = KVS_RESP_PARSE_ERROR; return -1; }// 防止解析出来的长度越界
    char *cmd = (char *)msg + offset;              // 绑定当前位置给 cmd 指针
    offset += cmd_len;                             // 游标跳过 cmd 字符串本身

    if (offset + 4 > msg_len) { resp->status = KVS_RESP_PARSE_ERROR; return -1; }
    int key_len_net;
    memcpy(&key_len_net, (char *)msg + offset, 4); 
    int key_len = ntohl(key_len_net);              
    offset += 4;                                   
    if (offset + key_len > msg_len) { resp->status = KVS_RESP_PARSE_ERROR; return -1; }
    char *key = (char *)msg + offset;              
    offset += key_len;                             
    
    if (offset + 4 > msg_len) { resp->status = KVS_RESP_PARSE_ERROR; return -1; }
    int value_len_net;
    memcpy(&value_len_net, (char *)msg + offset, 4); 
    int value_len = ntohl(value_len_net);            
    offset += 4;                                     
    if (offset + value_len > msg_len) { resp->status = KVS_RESP_PARSE_ERROR; return -1; }
    char *value = (char *)msg + offset;            
    offset += value_len;                           

    // 构造二进制安全传输体
    kv_data_t kv_key = {key, (size_t)key_len};
    kv_data_t kv_value = {value, (size_t)value_len};
    int target_cmd = CMD_UNKNOWN;

    // 匹配字符串命令类型
    for (int i = 0; i < KVS_CMD_LIST_SIZE; i++) {
        if (cmd_len == kvs_cmd_list[i].cmd_len && 
            memcmp(cmd, kvs_cmd_list[i].cmd_name, cmd_len) == 0) {
            target_cmd = kvs_cmd_list[i].cmd_enum;
            break;
        }
    }

    int ret = 0;
    kv_data_t *result = NULL;
    unsigned long long default_expire = 0;
    
    // 计算分段锁索引与默认过期时间
    int idx = get_segment_index(key, key_len);
    #if ENABLE_TTL
    default_expire = get_current_ms() + DEFAULT_TTL_MS;
    #endif

    switch (target_cmd) {
    #if ENABLE_ARRAY
        case CMD_SET:
            ret = kvs_array_set(&global_array, &kv_key, &kv_value, default_expire);
            if (ret == 0) {
                resp->status = KVS_RESP_OK;
                #if ENABLE_PERSISTENCE
                log_binary_command("SET", key, key_len, value, value_len);
                #endif
                #if ENABLE_REPLICATION
                repl_push_cmd("SET", key, key_len, value, value_len);
                #endif
            }
            else if (ret == 1) { resp->status = KVS_RESP_EXIST; }
            else { resp->status = KVS_RESP_ERROR; }
            break;

        case CMD_GET:
            result = kvs_array_get(&global_array, &kv_key);
            if (result && result->data && result->len > 0) {
                resp->status = KVS_RESP_GET_OK;
                // 💡 核心安全机制：为了彻底解耦且防并发删除，在锁/生命周期内 malloc 拷贝一份副本
                resp->body = kvs_malloc(result->len);
                if (resp->body) {
                    memcpy(resp->body, result->data, result->len);
                    resp->body_len = result->len;
                } else { resp->status = KVS_RESP_ERROR; }
            } else { resp->status = KVS_RESP_NO_EXIST; }
            break;
        
        case CMD_DEL:
            ret = kvs_array_del(&global_array, &kv_key);
            if (ret == 0) {
                resp->status = KVS_RESP_OK;
                #if ENABLE_PERSISTENCE
                log_binary_command("DEL", key, key_len, NULL, 0); 
                #endif
                #if ENABLE_REPLICATION
                repl_push_cmd("DEL", key, key_len, NULL, 0);
                #endif
            } else { resp->status = KVS_RESP_NO_EXIST; }
            break;

        case CMD_MOD:
            ret = kvs_array_mod(&global_array, &kv_key, &kv_value, default_expire);
            if (ret == 0) {
                resp->status = KVS_RESP_OK;
                #if ENABLE_PERSISTENCE
                log_binary_command("MOD", key, key_len, value, value_len);
                #endif
                #if ENABLE_REPLICATION
                repl_push_cmd("MOD", key, key_len, value, value_len);
                #endif 
            }
            else if (ret == 1) { resp->status = KVS_RESP_NO_EXIST; }
            else { resp->status = KVS_RESP_ERROR; }
            break;

        case CMD_EXIST:
            ret = kvs_array_exist(&global_array, &kv_key);
            resp->status = (ret == 0) ? KVS_RESP_EXIST : KVS_RESP_NO_EXIST;
            break;
    #endif
    
    #if ENABLE_RBTREE
        case CMD_RSET:
            ret = kvs_rbtree_set(&global_rbtree, &kv_key, &kv_value, default_expire);
            if (ret == 0) {
                resp->status = KVS_RESP_OK;
                #if ENABLE_PERSISTENCE
                log_binary_command("RSET", key, key_len, value, value_len);
                #endif
                #if ENABLE_REPLICATION
                repl_push_cmd("RSET", key, key_len, value, value_len);
                #endif
            }
            else if (ret == 1) { resp->status = KVS_RESP_EXIST; }
            else { resp->status = KVS_RESP_ERROR; }
            break;

        case CMD_RGET:
            result = kvs_rbtree_get(&global_rbtree, &kv_key);
            if (result && result->data && result->len > 0) {
                resp->status = KVS_RESP_GET_OK;
                resp->body = kvs_malloc(result->len);
                if (resp->body) {
                    memcpy(resp->body, result->data, result->len);
                    resp->body_len = result->len;
                } else { resp->status = KVS_RESP_ERROR; }
            } else { resp->status = KVS_RESP_NO_EXIST; }
            break;
        
        case CMD_RDEL:
            ret = kvs_rbtree_del(&global_rbtree, &kv_key);
            if (ret == 0) {
                resp->status = KVS_RESP_OK;
                #if ENABLE_PERSISTENCE
                log_binary_command("RDEL", key, key_len, NULL, 0); 
                #endif
                #if ENABLE_REPLICATION
                repl_push_cmd("RDEL", key, key_len, NULL, 0);
                #endif
            } else { resp->status = KVS_RESP_NO_EXIST; }
            break;

        case CMD_RMOD:
            ret = kvs_rbtree_mod(&global_rbtree, &kv_key, &kv_value, default_expire);
            if (ret == 0) {
                resp->status = KVS_RESP_OK;
                #if ENABLE_PERSISTENCE
                log_binary_command("RMOD", key, key_len, value, value_len);
                #endif
                #if ENABLE_REPLICATION
                repl_push_cmd("RMOD", key, key_len, value, value_len);
                #endif 
            }
            else if (ret == 1) { resp->status = KVS_RESP_NO_EXIST; }
            else { resp->status = KVS_RESP_ERROR; }
            break;

        case CMD_REXIST:
            ret = kvs_rbtree_exist(&global_rbtree, &kv_key);
            resp->status = (ret == 0) ? KVS_RESP_EXIST : KVS_RESP_NO_EXIST;
            break;
    #endif

    #if ENABLE_HASH
        case CMD_HSET:
            ret = kvs_hash_set(&global_hash, &kv_key, &kv_value, default_expire);
            if (ret == 0) {
                resp->status = KVS_RESP_OK;
                #if ENABLE_PERSISTENCE
                log_binary_command("HSET", key, key_len, value, value_len);
                #endif
                #if ENABLE_REPLICATION
                repl_push_cmd("HSET", key, key_len, value, value_len);
                #endif
            }
            else if (ret == 1) { resp->status = KVS_RESP_EXIST; }
            else { resp->status = KVS_RESP_ERROR; }
            break;

        case CMD_HGET:
            result = kvs_hash_get(&global_hash, &kv_key);
            if (result && result->data && result->len > 0) {
                resp->status = KVS_RESP_GET_OK;
                resp->body = kvs_malloc(result->len);
                if (resp->body) {
                    memcpy(resp->body, result->data, result->len);
                    resp->body_len = result->len;
                } else { resp->status = KVS_RESP_ERROR; }
            } else { resp->status = KVS_RESP_NO_EXIST; }
            break;

        case CMD_HDEL:
            ret = kvs_hash_del(&global_hash, &kv_key);
            if (ret == 0) {
                resp->status = KVS_RESP_OK;
                #if ENABLE_PERSISTENCE
                log_binary_command("HDEL", key, key_len, NULL, 0); 
                #endif
                #if ENABLE_REPLICATION
                repl_push_cmd("HDEL", key, key_len, NULL, 0);
                #endif
            } else { resp->status = KVS_RESP_NO_EXIST; }
            break;

        case CMD_HMOD:
            ret = kvs_hash_mod(&global_hash, &kv_key, &kv_value, default_expire);
            if (ret == 0) {
                resp->status = KVS_RESP_OK;
                #if ENABLE_PERSISTENCE
                log_binary_command("HMOD", key, key_len, value, value_len);
                #endif
                #if ENABLE_REPLICATION
                repl_push_cmd("HMOD", key, key_len, value, value_len);
                #endif 
            }
            else if (ret == 1) { resp->status = KVS_RESP_NO_EXIST; }
            else { resp->status = KVS_RESP_ERROR; }
            break;

        case CMD_HEXIST:
            ret = kvs_hash_exist(&global_hash, &kv_key);
            resp->status = (ret == 0) ? KVS_RESP_EXIST : KVS_RESP_NO_EXIST;
            break;
    #endif

    #if ENABLE_SKIPLIST 
        case CMD_SSET:
            pthread_rwlock_wrlock(&seg_locks[idx]); 
            ret = kvs_skip_set(&global_skip, &kv_key, &kv_value, default_expire);
            pthread_rwlock_unlock(&seg_locks[idx]); 
            
            if (ret == 0) {
                resp->status = KVS_RESP_OK;
                #if ENABLE_PERSISTENCE
                log_binary_command("SSET", key, key_len, value, value_len);
                #endif
                #if ENABLE_REPLICATION
                repl_push_cmd("SSET", key, key_len, value, value_len);
                #endif
            }
            else if (ret == 1) { resp->status = KVS_RESP_EXIST; }
            else { resp->status = KVS_RESP_ERROR; }
            break;

        case CMD_SGET:
            pthread_rwlock_wrlock(&seg_locks[idx]); 
            result = kvs_skip_get(&global_skip, &kv_key);
            if (result && result->data && result->len > 0) {
                resp->status = KVS_RESP_GET_OK;
                // 💡 在锁释放之前完成内存拷贝，多线程下绝对安全
                resp->body = kvs_malloc(result->len);
                if (resp->body) {
                    memcpy(resp->body, result->data, result->len);
                    resp->body_len = result->len;
                } else { resp->status = KVS_RESP_ERROR; }
                pthread_rwlock_unlock(&seg_locks[idx]); 
            } else {
                pthread_rwlock_unlock(&seg_locks[idx]); 
                resp->status = KVS_RESP_NO_EXIST;
            }
            break;

        case CMD_SDEL:
            pthread_rwlock_wrlock(&seg_locks[idx]); 
            ret = kvs_skip_del(&global_skip, &kv_key);
            pthread_rwlock_unlock(&seg_locks[idx]); 
            
            if (ret == 0) {
                resp->status = KVS_RESP_OK;
                #if ENABLE_PERSISTENCE
                log_binary_command("SDEL", key, key_len, NULL, 0); 
                #endif
                #if ENABLE_REPLICATION
                repl_push_cmd("SDEL", key, key_len, NULL, 0);
                #endif
            } else { resp->status = KVS_RESP_NO_EXIST; }
            break;

        case CMD_SMOD:
            pthread_rwlock_wrlock(&seg_locks[idx]); 
            ret = kvs_skip_mod(&global_skip, &kv_key, &kv_value, default_expire);
            pthread_rwlock_unlock(&seg_locks[idx]); 
            
            if (ret == 0) {
                resp->status = KVS_RESP_OK;
                #if ENABLE_PERSISTENCE
                log_binary_command("SMOD", key, key_len, value, value_len);
                #endif
                #if ENABLE_REPLICATION
                repl_push_cmd("SMOD", key, key_len, value, value_len);
                #endif
            }
            else if (ret == 1) { resp->status = KVS_RESP_NO_EXIST; }
            else { resp->status = KVS_RESP_ERROR; }
            break;

        case CMD_SEXIST:
            pthread_rwlock_wrlock(&seg_locks[idx]); 
            ret = kvs_skip_exist(&global_skip, &kv_key);
            pthread_rwlock_unlock(&seg_locks[idx]); 
            resp->status = (ret == 0) ? KVS_RESP_EXIST : KVS_RESP_NO_EXIST;
            break;
    #endif

        case CMD_SHUTDOWN:
            // 💡 业务层只标记状态，不再粗暴地直接 exit(0)
            resp->status = KVS_RESP_SHUTDOWN;
            break;

        default:
            resp->status = KVS_RESP_UNKNOWN;
            break;
        }

    return 0;
}


int init_kvengine(void) {
// 全局锁
    if (kvs_init_locks() != 0) {
        printf("Failed to init segment locks\n");
        return -1;
    }
//内存池
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

// 引擎结构
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
    
// 恢复数据（先快照，再用AOF日志追平增量）
#if ENABLE_SNAPSHOT
    kvs_snapshot_load();        
#endif
#if ENABLE_PERSISTENCE
    kvs_persistence_init();
    kvs_persistence_recover(); 
#endif

// 定时持久化
#if ENABLE_SNAPSHOT
    kvs_snapshot_auto_save(1); 
#endif

// 建立主从同步连接
#if ENABLE_REPLICATION
    const char *slave_ip = "192.168.92.129";
    unsigned short slave_port = 2000;
    repl_connect_to_slave(slave_ip, slave_port); 
#endif 

// 定时超时删除线程
    if (expire_thread_init() != 0) {
        return -1;
    }

    return 0;
}

void dest_kvengine(void) {
// 关掉超时清理线程
expire_thread_destroy(); 
// 关闭主从同步
#if ENABLE_REPLICATION
    repl_close();            
#endif

#if ENABLE_SNAPSHOT
    kvs_snapshot_auto_save_stop();  // 停止自动快照定时器
    kvs_snapshot_save();            // 最后做一次强制全量快照落盘
#endif

#if ENABLE_PERSISTENCE
    kvs_persistence_close();        // 关闭并刷盘 AOF 日志文件流
#endif

// 释放本地内存引擎
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

// 释放内存池
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

    kvs_destroy_locks(); // 释放锁
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