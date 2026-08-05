#define _XOPEN_SOURCE 600
#include <pthread.h>
#include "kvstore.h"
#include <string.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <sys/time.h>
#include "mempool.h"
#include <arpa/inet.h>
#include <strings.h> 
#include "resp.h" 
#include "repl.h"
#include "rdma.h"
#include "expire.h"
#include <ctype.h>
#include <sys/types.h>
#include <sys/wait.h>

// 全局保存子进程 PID
static pid_t g_save_pid = -1;

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

//内存池相关函数
mem_pool_t *array_item_pool;
mem_pool_t *rbtree_node_pool;
mem_pool_t *hash_node_pool;
mem_pool_t *skip_node_pool;

typedef struct {
    size_t size;
    void *owner; 
} fallback_header_t;

void *kvs_malloc_type(kvs_obj_type_t type, size_t size) {
    if (g_enable_mempool && type < OBJ_MAX) {
        if (!g_typed_pools[type]) kvs_mempool_init(); 
        return mem_pool_alloc(g_typed_pools[type]);
    }
    return malloc(size); 
}

void kvs_free_type(kvs_obj_type_t type, void *ptr) {
    if (!ptr) return;
    if (g_enable_mempool && type < OBJ_MAX) {
        if (g_typed_pools[type]) {
            mem_pool_free(g_typed_pools[type], ptr);
            return;
        }
    }
    free(ptr);
}

void *kvs_malloc(size_t size) {
    if (size == 0) return NULL;
    if (!g_enable_mempool) return malloc(size);

    if (size <= MAX_SLAB_SIZE) {
        if (!g_size_map[size]) kvs_mempool_init();
        mem_pool_t *pool = g_size_map[size];
        if (pool) {
            void **ptr = (void **)mem_pool_alloc(pool);
            if (ptr) {
                *ptr = pool;        
                return ptr + 1;    
            }
        }
    }

    fallback_header_t *fh = (fallback_header_t *)malloc(sizeof(fallback_header_t) + size);
    if (!fh) return NULL;
    fh->owner = NULL;
    fh->size = size;
    return fh + 1;
}

void kvs_free(void *ptr) {
    if (!ptr) return;
    if (!g_enable_mempool) { free(ptr); return; }

    void **base = (void **)ptr - 1;       
    mem_pool_t *pool = (mem_pool_t *)*base; 

    if (pool != NULL) {
        mem_pool_free(pool, base);
    } else {
        fallback_header_t *fh = (fallback_header_t *)ptr - 1;
        free(fh);
    }
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
    if (!g_enable_mempool) return realloc(ptr, size);

    void **base = (void **)ptr - 1;
    mem_pool_t *pool = (mem_pool_t *)*base;
    size_t old_size;

    if (pool != NULL) {
        old_size = pool->chunk_size - sizeof(void *);
    } else {
        fallback_header_t *fh = (fallback_header_t *)ptr - 1;
        old_size = fh->size;
    }

    if (size <= old_size) return ptr;

    void *new_ptr = kvs_malloc(size);
    if (new_ptr) {
        memcpy(new_ptr, ptr, old_size);
        kvs_free(ptr);
    }
    return new_ptr;
}

// 命令处理函数

static void kvs_set_reply_body(resp_reply_t *reply, kv_data_t *result) {
    if (result && result->data && result->len > 0) {
        reply->status = KVS_RESP_GET_OK;
        reply->body = kvs_malloc(result->len + 1);
        if (reply->body) {
            memcpy(reply->body, result->data, result->len);
            reply->body_len = result->len;
        } else { 
            reply->status = KVS_RESP_ERROR; 
        }
    } else { 
        reply->status = KVS_RESP_NO_EXISTS; 
    }
}

#if ENABLE_ARRAY
static void cmd_array_set(resp_request_t *req, resp_reply_t *reply) {
    if (req->argc < 3) { reply->status = KVS_RESP_PARSE_ERROR; return; }
    kv_data_t k = {req->argv[1], (size_t)req->argv_len[1]};
    kv_data_t v = {req->argv[2], (size_t)req->argv_len[2]};
    int ret = kvs_array_set(&global_array, &k, &v, 0);
    reply->status = (ret == 0) ? KVS_RESP_OK : KVS_RESP_ERROR;
}

static void cmd_array_get(resp_request_t *req, resp_reply_t *reply) {
    if (req->argc < 2) { reply->status = KVS_RESP_PARSE_ERROR; return; }
    kv_data_t k = {req->argv[1], (size_t)req->argv_len[1]};
    kvs_set_reply_body(reply, kvs_array_get(&global_array, &k));
}

static void cmd_array_del(resp_request_t *req, resp_reply_t *reply) {
    if (req->argc < 2) { reply->status = KVS_RESP_PARSE_ERROR; return; }
    kv_data_t k = {req->argv[1], (size_t)req->argv_len[1]};
    reply->status = (kvs_array_del(&global_array, &k) == 0) ? KVS_RESP_OK : KVS_RESP_NO_EXISTS;
}

static void cmd_array_mod(resp_request_t *req, resp_reply_t *reply) {
    if (req->argc < 3) { reply->status = KVS_RESP_PARSE_ERROR; return; }
    kv_data_t k = {req->argv[1], (size_t)req->argv_len[1]};
    kv_data_t v = {req->argv[2], (size_t)req->argv_len[2]};
    int ret = kvs_array_mod(&global_array, &k, &v, 0);
    reply->status = (ret == 0) ? KVS_RESP_OK : KVS_RESP_NO_EXISTS;
}

static void cmd_array_exists(resp_request_t *req, resp_reply_t *reply) {
    if (req->argc < 2) { reply->status = KVS_RESP_PARSE_ERROR; return; }
    kv_data_t k = {req->argv[1], (size_t)req->argv_len[1]};
    reply->status = (kvs_array_exist(&global_array, &k) == 0) ? KVS_RESP_EXISTS : KVS_RESP_NO_EXISTS;
}
#endif

#if ENABLE_RBTREE
static void cmd_rbtree_set(resp_request_t *req, resp_reply_t *reply) {
    if (req->argc < 3) { reply->status = KVS_RESP_PARSE_ERROR; return; }
    kv_data_t k = {req->argv[1], (size_t)req->argv_len[1]};
    kv_data_t v = {req->argv[2], (size_t)req->argv_len[2]};
    int ret = kvs_rbtree_set(&global_rbtree, &k, &v, 0);
    reply->status = (ret == 0) ? KVS_RESP_OK : KVS_RESP_ERROR;
}

static void cmd_rbtree_get(resp_request_t *req, resp_reply_t *reply) {
    if (req->argc < 2) { reply->status = KVS_RESP_PARSE_ERROR; return; }
    kv_data_t k = {req->argv[1], (size_t)req->argv_len[1]};
    kvs_set_reply_body(reply, kvs_rbtree_get(&global_rbtree, &k));
}

static void cmd_rbtree_del(resp_request_t *req, resp_reply_t *reply) {
    if (req->argc < 2) { reply->status = KVS_RESP_PARSE_ERROR; return; }
    kv_data_t k = {req->argv[1], (size_t)req->argv_len[1]};
    reply->status = (kvs_rbtree_del(&global_rbtree, &k) == 0) ? KVS_RESP_OK : KVS_RESP_NO_EXISTS;
}

static void cmd_rbtree_mod(resp_request_t *req, resp_reply_t *reply) {
    if (req->argc < 3) { reply->status = KVS_RESP_PARSE_ERROR; return; }
    kv_data_t k = {req->argv[1], (size_t)req->argv_len[1]};
    kv_data_t v = {req->argv[2], (size_t)req->argv_len[2]};
    int ret = kvs_rbtree_mod(&global_rbtree, &k, &v, 0);
    reply->status = (ret == 0) ? KVS_RESP_OK : KVS_RESP_NO_EXISTS;
}

static void cmd_rbtree_exists(resp_request_t *req, resp_reply_t *reply) {
    if (req->argc < 2) { reply->status = KVS_RESP_PARSE_ERROR; return; }
    kv_data_t k = {req->argv[1], (size_t)req->argv_len[1]};
    reply->status = (kvs_rbtree_exist(&global_rbtree, &k) == 0) ? KVS_RESP_EXISTS : KVS_RESP_NO_EXISTS;
}
#endif

#if ENABLE_HASH
static void cmd_hash_set(resp_request_t *req, resp_reply_t *reply) {
    if (req->argc < 3) { reply->status = KVS_RESP_PARSE_ERROR; return; }
    kv_data_t k = {req->argv[1], (size_t)req->argv_len[1]};
    kv_data_t v = {req->argv[2], (size_t)req->argv_len[2]};
    int ret = kvs_hash_set(&global_hash, &k, &v, 0);
    reply->status = (ret == 0) ? KVS_RESP_OK : KVS_RESP_ERROR;
}

static void cmd_hash_get(resp_request_t *req, resp_reply_t *reply) {
    if (req->argc < 2) { reply->status = KVS_RESP_PARSE_ERROR; return; }
    kv_data_t k = {req->argv[1], (size_t)req->argv_len[1]};
    kvs_set_reply_body(reply, kvs_hash_get(&global_hash, &k));
}

static void cmd_hash_del(resp_request_t *req, resp_reply_t *reply) {
    if (req->argc < 2) { reply->status = KVS_RESP_PARSE_ERROR; return; }
    kv_data_t k = {req->argv[1], (size_t)req->argv_len[1]};
    reply->status = (kvs_hash_del(&global_hash, &k) == 0) ? KVS_RESP_OK : KVS_RESP_NO_EXISTS;
}

static void cmd_hash_mod(resp_request_t *req, resp_reply_t *reply) {
    if (req->argc < 3) { reply->status = KVS_RESP_PARSE_ERROR; return; }
    kv_data_t k = {req->argv[1], (size_t)req->argv_len[1]};
    kv_data_t v = {req->argv[2], (size_t)req->argv_len[2]};
    int ret = kvs_hash_mod(&global_hash, &k, &v, 0);
    reply->status = (ret == 0) ? KVS_RESP_OK : KVS_RESP_NO_EXISTS;
}

static void cmd_hash_exists(resp_request_t *req, resp_reply_t *reply) {
    if (req->argc < 2) { reply->status = KVS_RESP_PARSE_ERROR; return; }
    kv_data_t k = {req->argv[1], (size_t)req->argv_len[1]};
    reply->status = (kvs_hash_exist(&global_hash, &k) == 0) ? KVS_RESP_EXISTS : KVS_RESP_NO_EXISTS;
}
#endif

#if ENABLE_SKIPLIST
static void cmd_skip_set(resp_request_t *req, resp_reply_t *reply) {
    if (req->argc < 3) { reply->status = KVS_RESP_PARSE_ERROR; return; }
    kv_data_t k = {req->argv[1], (size_t)req->argv_len[1]};
    kv_data_t v = {req->argv[2], (size_t)req->argv_len[2]};
    int ret = kvs_skip_set(&global_skip, &k, &v, 0);
    reply->status = (ret == 0) ? KVS_RESP_OK : KVS_RESP_ERROR;
}

static void cmd_skip_get(resp_request_t *req, resp_reply_t *reply) {
    if (req->argc < 2) { reply->status = KVS_RESP_PARSE_ERROR; return; }
    kv_data_t k = {req->argv[1], (size_t)req->argv_len[1]};
    kvs_set_reply_body(reply, kvs_skip_get(&global_skip, &k));
}

static void cmd_skip_del(resp_request_t *req, resp_reply_t *reply) {
    if (req->argc < 2) { reply->status = KVS_RESP_PARSE_ERROR; return; }
    kv_data_t k = {req->argv[1], (size_t)req->argv_len[1]};
    reply->status = (kvs_skip_del(&global_skip, &k) == 0) ? KVS_RESP_OK : KVS_RESP_NO_EXISTS;
}

static void cmd_skip_mod(resp_request_t *req, resp_reply_t *reply) {
    if (req->argc < 3) { reply->status = KVS_RESP_PARSE_ERROR; return; }
    kv_data_t k = {req->argv[1], (size_t)req->argv_len[1]};
    kv_data_t v = {req->argv[2], (size_t)req->argv_len[2]};
    int ret = kvs_skip_mod(&global_skip, &k, &v, 0);
    reply->status = (ret == 0) ? KVS_RESP_OK : KVS_RESP_NO_EXISTS;
}

static void cmd_skip_exists(resp_request_t *req, resp_reply_t *reply) {
    if (req->argc < 2) { reply->status = KVS_RESP_PARSE_ERROR; return; }
    kv_data_t k = {req->argv[1], (size_t)req->argv_len[1]};
    reply->status = (kvs_skip_exist(&global_skip, &k) == 0) ? KVS_RESP_EXISTS : KVS_RESP_NO_EXISTS;
}
#endif

// 系统命令
static void cmd_sys_save(resp_request_t *req, resp_reply_t *reply) {
    if (req->argc != 1) { reply->status = KVS_RESP_PARSE_ERROR; return; }
    if (g_save_pid != -1) {
        if (waitpid(g_save_pid, NULL, WNOHANG) == g_save_pid) g_save_pid = -1;
    }
    if (g_save_pid != -1) { reply->status = KVS_RESP_ERR; return; }
    
    pid_t pid = fork();
    if (pid < 0) reply->status = KVS_RESP_ERR;
    else if (pid == 0) {
        extern int kvs_snapshot_save(void);
        kvs_snapshot_save();
        _exit(0);
    } else {
        g_save_pid = pid;
        reply->status = KVS_RESP_SUCCESS; 
    }
}

static void cmd_sys_sync(resp_request_t *req, resp_reply_t *reply) {
    extern volatile int g_repl_backlog_enabled;
    extern int g_repl_backlog_count;
    g_repl_backlog_enabled = 1;  
    g_repl_backlog_count = 0;    
    if (g_use_tcp_sync) {
        if (repl_sync_log_via_tcp() != 0) printf("[Repl Error] TCP log sync failed!\n");
    } else if (g_rdma_ctx) {            
        if (repl_sync_log_via_rdma() != 0) printf("[Repl Error] RDMA sync failed!\n");
    }
    reply->status = KVS_RESP_SUCCESS;
}

static void cmd_sys_sync_done(resp_request_t *req, resp_reply_t *reply) {
    repl_destroy();
    extern int g_sync_file_done;
    extern volatile int g_repl_backlog_enabled;
    g_sync_file_done = 1;
    g_repl_backlog_enabled = 0;
    reply->status = KVS_RESP_SUCCESS;
}

// command_t 命令表

command_t g_cmd_table[] = {
#if ENABLE_ARRAY
    {"SET",    3, cmd_array_set},
    {"GET",    2, cmd_array_get},
    {"DEL",    2, cmd_array_del},
    {"MOD",    3, cmd_array_mod},
    {"EXISTS", 2, cmd_array_exists},
#endif
#if ENABLE_RBTREE
    {"RSET",    3, cmd_rbtree_set},
    {"RGET",    2, cmd_rbtree_get},
    {"RDEL",    2, cmd_rbtree_del},
    {"RMOD",    3, cmd_rbtree_mod},
    {"REXISTS", 2, cmd_rbtree_exists},
#endif
#if ENABLE_HASH
    {"HSET",    3, cmd_hash_set},
    {"HGET",    2, cmd_hash_get},
    {"HDEL",    2, cmd_hash_del},
    {"HMOD",    3, cmd_hash_mod},
    {"HEXISTS", 2, cmd_hash_exists},
#endif
#if ENABLE_SKIPLIST
    {"SSET",    3, cmd_skip_set},
    {"SGET",    2, cmd_skip_get},
    {"SDEL",    2, cmd_skip_del},
    {"SMOD",    3, cmd_skip_mod},
    {"SEXISTS", 2, cmd_skip_exists},
#endif
    {"SAVE",      1, cmd_sys_save},
    {"SYNC",      1, cmd_sys_sync},
    {"SYNC_DONE", 1, cmd_sys_sync_done},
    {NULL, 0, NULL}  // 哨兵
};

#define CMD_HASH_SIZE 64
static command_t *g_cmd_hash[CMD_HASH_SIZE];
static int g_cmd_hash_inited = 0;

static uint32_t cmd_hash(const char *name) {
    uint32_t hash = 5381;
    while (*name) {
        hash = ((hash << 5) + hash) + (unsigned char)toupper(*name++);
    }
    return hash;
}

void kvs_cmd_init(void) {
    if (g_cmd_hash_inited) return;
    memset(g_cmd_hash, 0, sizeof(g_cmd_hash));

    for (int i = 0; g_cmd_table[i].name != NULL; i++) {
        uint32_t hash = cmd_hash(g_cmd_table[i].name);
        uint32_t idx = hash & (CMD_HASH_SIZE - 1);
        while (g_cmd_hash[idx] != NULL) {
            idx = (idx + 1) & (CMD_HASH_SIZE - 1);
        }
        g_cmd_hash[idx] = &g_cmd_table[i];
    }
    g_cmd_hash_inited = 1;
}

command_t *lookup_command(const char *name) {
    if (!name) return NULL;
    if (!g_cmd_hash_inited) kvs_cmd_init();

    uint32_t hash = cmd_hash(name);
    uint32_t idx = hash & (CMD_HASH_SIZE - 1);

    while (g_cmd_hash[idx] != NULL) {
        if (strcasecmp(g_cmd_hash[idx]->name, name) == 0) {
            return g_cmd_hash[idx];
        }
        idx = (idx + 1) & (CMD_HASH_SIZE - 1);
    }
    return NULL;
}

// 批量执行入口

int kvs_execute_batch(parsed_cmd_t *cmds, resp_reply_t *replies, int cmd_num) {
    for (int i = 0; i < cmd_num; i++) {
        replies[i].status = KVS_RESP_ERROR;
        replies[i].body = NULL;
        replies[i].body_len = 0;

        if (cmds[i].cmd && cmds[i].cmd->proc) {
            cmds[i].cmd->proc(&cmds[i].req, &replies[i]);
        } else {
            replies[i].status = KVS_RESP_UNKNOWN;
        }
    }
    return 0;
}