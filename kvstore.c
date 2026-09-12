#define _XOPEN_SOURCE 600
#include <pthread.h>
#include "kvstore.h"
#include <string.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <sys/time.h>
#include "mempool.h"
#include <malloc.h>
#include <arpa/inet.h>
#include <strings.h> 
#include "resp.h" 
#include "repl.h"
#include "rdma.h"
#include "expire.h"
#include <ctype.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <time.h>
#include "ai_chat.h"

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
extern kvs_hash_t global_hash1;   // KEEP/MATCH   语义缓存
extern kvs_hash_t global_hash2;   // SETCTX/GETCTX  上下文
extern kvs_hash_t global_hash3;   // SETREC/GETREC  全量记录
extern kvs_hash_t global_hash4;   // SETIDX/GETIDX  关键词索引
#endif
#if ENABLE_SKIPLIST
extern kvs_skip_t global_skip;
#endif

/**
 * 解析命令中的过期时间参数
 * 支持格式：
 *   - SET key value EX 10        (秒级相对)
 *   - SET key value PX 10000     (毫秒级相对)
 *   - SET key value 1234567890   (绝对时间戳，AOF恢复用)
 *   - SET key value 10           (小数值当作相对秒数)
 * 返回绝对过期时间戳(ms)，0 表示永不过期
 */
int64_t parse_ttl(resp_request_t *req) {
    if (req->argc < 4) return 0;
    if (!req->argv[3]) return 0;

    // 格式1：SET key value EX 10 / PX 10000 (Redis 标准)
    if (req->argc >= 5 && req->argv[4]) {
        if (req->argv_len[3] == 2 && strncasecmp(req->argv[3], "EX", 2) == 0) {
            long long sec = atoll(req->argv[4]);
            if (sec > 0) return get_current_ms() + sec * 1000;
            return 0;
        }
        if (req->argv_len[3] == 2 && strncasecmp(req->argv[3], "PX", 2) == 0) {
            long long ms = atoll(req->argv[4]);
            if (ms > 0) return get_current_ms() + ms;
            return 0;
        }
    }

    // 格式2：SET key value <timestamp> (AOF 恢复 / 直接传时间戳)
    long long val = atoll(req->argv[3]);
    if (val <= 0) return 0;

    // 如果时间戳很小（小于 1000000000，约 2001年），当作相对秒数
    if (val < 1000000000) {
        return get_current_ms() + val * 1000;
    }

    // 否则当作绝对毫秒时间戳
    return (int64_t)val;
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
    int64_t expire=0;
    if(g_enable_ttl)expire=parse_ttl(req);
    int ret = kvs_array_set(&global_array, &k, &v, expire);
    if(g_enable_ttl && ret == 0 ) expire_push_cmd(EXPIRE_TYPE_ARRAY, &k, expire, (uint64_t)expire);
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
    int64_t expire = 0;
    if (g_enable_ttl) expire = parse_ttl(req);
    int ret = kvs_rbtree_set(&global_rbtree, &k, &v, expire);
    if (g_enable_ttl && ret == 0 && expire > 0) expire_push_cmd(EXPIRE_TYPE_RBTREE, &k, expire, (uint64_t)expire);
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
    int64_t expire = 0;
    if (g_enable_ttl) expire = parse_ttl(req);
    int ret = kvs_hash_set(&global_hash, &k, &v, expire);
    if (g_enable_ttl && ret == 0 && expire > 0) expire_push_cmd(EXPIRE_TYPE_HASH, &k, expire, (uint64_t)expire);
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
     int64_t expire = 0;
    if (g_enable_ttl) expire = parse_ttl(req);
    int ret = kvs_skip_set(&global_skip, &k, &v, expire);
    if (g_enable_ttl && ret == 0 && expire > 0) expire_push_cmd(EXPIRE_TYPE_SKIPLIST, &k, expire, (uint64_t)expire);
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
}

static void cmd_sys_sync_done(resp_request_t *req, resp_reply_t *reply) {
    printf("Received SYNC_DONE.\n");
    repl_destroy();
    extern int g_sync_file_done;
    extern volatile int g_repl_backlog_enabled;
    g_sync_file_done = 1;
    g_repl_backlog_enabled = 0;
}

static void cmd_sys_memtrim(resp_request_t *req, resp_reply_t *reply) {
    if (req->argc != 1) {
        reply->status = KVS_RESP_PARSE_ERROR;
        return;
    }

    if (g_enable_mempool) {
        kvs_mempool_trim_all();
    } else {
        malloc_trim(0);
    }

    reply->status = KVS_RESP_OK;
}

/* KEEP命令处理：存储键值并构建索引 */
static void cmd_keep(resp_request_t *req, resp_reply_t *reply){
    if (req->argc < 3) {
        reply->status = KVS_RESP_PARSE_ERROR;
        return;
    }
    kv_data_t key = { req->argv[1], (size_t)req->argv_len[1] };
    kv_data_t value = { req->argv[2], (size_t)req->argv_len[2] };

    if (kvs_hash_set(&global_hash1, &key, &value, 0) != 0) {
        reply->status = KVS_RESP_ERROR;
        return;
    }

    size_t key_len = (size_t)req->argv_len[1];
    char *index_key = kvs_malloc(key_len + 1);
    if (index_key == NULL) {
        reply->status = KVS_RESP_ERROR;
        return;
    }
    memcpy(index_key, req->argv[1], key_len);
    index_key[key_len] = '\0';

    int ret = keep_build_index(index_key, req->argv[2], (size_t)req->argv_len[2]);
    kvs_free(index_key);

    if (ret != 0) {
        reply->status = KVS_RESP_ERROR;
        return;
    }
    reply->status = KVS_RESP_OK;
}

/* MATCH命令处理：根据问题匹配答案 */
static void cmd_match(resp_request_t *req, resp_reply_t *reply){
    if (req->argc != 2) {
        reply->status = KVS_RESP_PARSE_ERROR;
        return;
    }

    size_t question_len = (size_t)req->argv_len[1];
    char *question = kvs_malloc(question_len + 1);
    if (question == NULL) {
        reply->status = KVS_RESP_ERROR;
        return;
    }
    memcpy(question, req->argv[1], question_len);
    question[question_len] = '\0';

    char *answer = NULL;
    size_t answer_len = 0;
    float score = 0.0f;
    int ret = match_find_answer(question, &answer, &answer_len, &score);
    kvs_free(question);

    if (ret < 0) {
        reply->status = KVS_RESP_ERROR;
        return;
    }
    if (ret == 1 || answer == NULL) {
        reply->status = KVS_RESP_NO_EXISTS;
        reply->body = NULL;
        reply->body_len = 0;
        return;
    }

    char *tmp = kvs_malloc(answer_len + 1);
    if (tmp == NULL) {
        kvs_free(answer);
        reply->status = KVS_RESP_ERROR;
        return;
    }
    memcpy(tmp, answer, answer_len);
    tmp[answer_len] = '\0';
    reply->body = tmp;
    reply->body_len = answer_len;
    reply->status = KVS_RESP_GET_OK;
    kvs_free(answer);
}

/* ============================================================
 * 1. SETCTX ID json ttl    → global_hash2（带 TTL）
 * ============================================================ */
static void cmd_setctx(resp_request_t *req, resp_reply_t *reply) {
    if (req->argc < 4) { reply->status = KVS_RESP_PARSE_ERROR; return; }

    kv_data_t k = { req->argv[1], (size_t)req->argv_len[1] };
    kv_data_t v = { req->argv[2], (size_t)req->argv_len[2] };

    // 解析 TTL（秒 → 绝对毫秒时间戳）
    int64_t ttl_sec = atoll(req->argv[3]);
    int64_t expire = (ttl_sec > 0)
                   ? (int64_t)(time(NULL) * 1000 + ttl_sec * 1000)
                   : 0;

    int ret = kvs_hash_set(&global_hash2, &k, &v, expire);
    if (ret != 0) {
        reply->status = KVS_RESP_ERROR;
        return;
    }

    // 注册过期定时器
    if (g_enable_ttl && expire > 0) {
        expire_push_cmd(EXPIRE_TYPE_HASH2, &k, expire, (uint64_t)expire);
    }

    reply->status = KVS_RESP_OK;
}

/* ============================================================
 * 2. GETCTX ID    → global_hash2 返回整条 JSON
 * ============================================================ */
static void cmd_getctx(resp_request_t *req, resp_reply_t *reply) {
    if (req->argc != 2) { reply->status = KVS_RESP_PARSE_ERROR; return; }

    kv_data_t k = { req->argv[1], (size_t)req->argv_len[1] };
    kvs_set_reply_body(reply, kvs_hash_get(&global_hash2, &k));
}

/* ============================================================
 * 3. SETREC ID json    → global_hash3（无 TTL）
 * ============================================================ */
static void cmd_setrec(resp_request_t *req, resp_reply_t *reply) {
    if (req->argc != 3) { reply->status = KVS_RESP_PARSE_ERROR; return; }

    kv_data_t k = { req->argv[1], (size_t)req->argv_len[1] };
    kv_data_t v = { req->argv[2], (size_t)req->argv_len[2] };

    int ret = kvs_hash_set(&global_hash3, &k, &v, 0);
    reply->status = (ret == 0) ? KVS_RESP_OK : KVS_RESP_ERROR;
}

/* ============================================================
 * 4. GETREC ID    → global_hash3 返回整条 JSON
 * ============================================================ */
static void cmd_getrec(resp_request_t *req, resp_reply_t *reply) {
    if (req->argc != 2) { reply->status = KVS_RESP_PARSE_ERROR; return; }

    kv_data_t k = { req->argv[1], (size_t)req->argv_len[1] };
    kvs_set_reply_body(reply, kvs_hash_get(&global_hash3, &k));
}

/* ============================================================
 * 5. SETIDX 关键词 ID    → global_hash4（追加语义）
 * ============================================================ */
static void cmd_setidx(resp_request_t *req, resp_reply_t *reply) {
    if (req->argc != 3) { reply->status = KVS_RESP_PARSE_ERROR; return; }

    kv_data_t k  = { req->argv[1], (size_t)req->argv_len[1] };
    kv_data_t id = { req->argv[2], (size_t)req->argv_len[2] };

    int ret = kvs_hash_append(&global_hash4, &k, &id);
    reply->status = (ret == 0) ? KVS_RESP_OK : KVS_RESP_ERROR;
}

/* ============================================================
 * 6. GETIDX 关键词    → global_hash4 返回逗号分隔的 ID 列表
 * ============================================================ */
static void cmd_getidx(resp_request_t *req, resp_reply_t *reply) {
    if (req->argc != 2) { reply->status = KVS_RESP_PARSE_ERROR; return; }

    kv_data_t k = { req->argv[1], (size_t)req->argv_len[1] };
    kvs_set_reply_body(reply, kvs_hash_get(&global_hash4, &k));
}

/* ============================================================
 * 7. ZADD 时间戳 ID    → global_skip（复合 key 排序）
 * ============================================================ */
static void cmd_zadd(resp_request_t *req, resp_reply_t *reply) {
    if (req->argc != 3) { reply->status = KVS_RESP_PARSE_ERROR; return; }

    int64_t ts = atoll(req->argv[1]);
    if (ts <= 0) { reply->status = KVS_RESP_PARSE_ERROR; return; }

    kv_data_t id = { req->argv[2], (size_t)req->argv_len[2] };
    int ret = kvs_zset_add(&global_skip, ts, &id);
    reply->status = (ret == 0) ? KVS_RESP_OK : KVS_RESP_ERROR;
}

/* ============================================================
 * 8. ZRANGE start stop    → 取最近 N 条（如 ZRANGE 0 9 取最近 10 条）
 *   start 必须为 0
 *   N = stop - start + 1
 * ============================================================ */
static void cmd_zrange(resp_request_t *req, resp_reply_t *reply) {
    if (req->argc != 3) { reply->status = KVS_RESP_PARSE_ERROR; return; }

    long start = atol(req->argv[1]);
    long stop  = atol(req->argv[2]);

    // 只支持 "从头开始" 的语义，start 必须为 0
    if (start != 0 || stop < 0) {
        reply->status = KVS_RESP_PARSE_ERROR;
        return;
    }

    int n = (int)(stop - start + 1);
    if (n <= 0) {
        reply->status = KVS_RESP_NO_EXISTS;
        return;
    }

    char **ids = NULL;
    int count = 0;
    int ret = kvs_zset_recent(&global_skip, n, &ids, &count);
    if (ret != 0) {
        reply->status = KVS_RESP_ERROR;
        return;
    }
    if (count == 0) {
        reply->status = KVS_RESP_NO_EXISTS;
        return;
    }

    // 拼接成逗号分隔字符串
    size_t total = 0;
    for (int i = 0; i < count; i++) total += strlen(ids[i]) + 1;

    char *buf = (char *)kvs_malloc(total + 1);
    if (!buf) {
        kvs_zset_free_list(ids, count);
        reply->status = KVS_RESP_ERROR;
        return;
    }

    char *p = buf;
    for (int i = 0; i < count; i++) {
        if (i > 0) *p++ = ',';
        size_t l = strlen(ids[i]);
        memcpy(p, ids[i], l);
        p += l;
    }
    *p = '\0';

    kvs_zset_free_list(ids, count);

    reply->body = buf;
    reply->body_len = p - buf;
    reply->status = KVS_RESP_GET_OK;
}

/* ============================================================
 * DELCTX ID    → 从 global_hash2 删除指定上下文
 * 说明：主要供 AOF 恢复使用，客户端一般不会主动发
 *       超时删除由 expire 线程处理后，写入 AOF 的也是 DELCTX
 * ============================================================ */
static void cmd_delctx(resp_request_t *req, resp_reply_t *reply) {
    if (req->argc != 2) {
        reply->status = KVS_RESP_PARSE_ERROR;
        return;
    }

    kv_data_t k = { req->argv[1], (size_t)req->argv_len[1] };

    int ret = kvs_hash_del(&global_hash2, &k);
    reply->status = (ret == 0) ? KVS_RESP_OK : KVS_RESP_NO_EXISTS;
}

/* ============================================================
 * 通用辅助：把 ID 数组对应的记录拼成换行分隔的字符串返回
 * 每条记录一行，客户端按 '\n' split 即可
 * ============================================================ */
static void build_records_newline(resp_reply_t *reply, char **ids, int count) {
    if (!ids || count == 0) {
        reply->status = KVS_RESP_NO_EXISTS;
        return;
    }

    size_t cap = 4096;
    size_t len = 0;
    char *buf = (char *)kvs_malloc(cap);
    if (!buf) { reply->status = KVS_RESP_ERROR; return; }

    int written = 0;
    for (int i = 0; i < count; i++) {
        kv_data_t id_key = { ids[i], strlen(ids[i]), 0 };
        kv_data_t *rec = kvs_hash_get(&global_hash3, &id_key);
        if (!rec || rec->len == 0) continue;

        // 需要空间：换行符 + 记录 + 结束余量
        size_t need = len + rec->len + 2;
        if (need > cap) {
            while (need > cap) cap *= 2;
            char *nb = (char *)kvs_realloc(buf, cap);
            if (!nb) { kvs_free(buf); reply->status = KVS_RESP_ERROR; return; }
            buf = nb;
        }

        if (written > 0) buf[len++] = '\n';

        memcpy(buf + len, rec->data, rec->len);
        len += rec->len;
        written++;
    }

    if (written == 0) {
        kvs_free(buf);
        reply->status = KVS_RESP_NO_EXISTS;
        return;
    }

    reply->body = buf;
    reply->body_len = len;
    reply->status = KVS_RESP_GET_OK;
}

/* ============================================================
 * GETIDXREC 关键词    → 返回该关键词下所有记录（换行分隔）
 * ============================================================ */
static void cmd_getidxrec(resp_request_t *req, resp_reply_t *reply) {
    if (req->argc != 2) { reply->status = KVS_RESP_PARSE_ERROR; return; }

    kv_data_t k = { req->argv[1], (size_t)req->argv_len[1] };

    kv_data_t *list = kvs_hash_get(&global_hash4, &k);
    if (!list || list->len == 0) {
        reply->status = KVS_RESP_NO_EXISTS;
        return;
    }

    char *ldata = (char *)list->data;
    char **ids = (char **)kvs_malloc(sizeof(char *) * 8);
    if (!ids) { reply->status = KVS_RESP_ERROR; return; }
    int count = 0, cap = 8;

    size_t start = 0;
    for (size_t i = 0; i <= list->len; i++) {
        if (i == list->len || ldata[i] == ',') {
            size_t id_len = i - start;
            if (id_len > 0) {
                if (count >= cap) {
                    cap *= 2;
                    char **nb = (char **)kvs_realloc(ids, sizeof(char *) * cap);
                    if (!nb) {
                        for (int j = 0; j < count; j++) kvs_free(ids[j]);
                        kvs_free(ids);
                        reply->status = KVS_RESP_ERROR;
                        return;
                    }
                    ids = nb;
                }
                char *id = (char *)kvs_malloc(id_len + 1);
                if (!id) {
                    for (int j = 0; j < count; j++) kvs_free(ids[j]);
                    kvs_free(ids);
                    reply->status = KVS_RESP_ERROR;
                    return;
                }
                memcpy(id, ldata + start, id_len);
                id[id_len] = '\0';
                ids[count++] = id;
            }
            start = i + 1;
        }
    }

    build_records_newline(reply, ids, count);

    for (int j = 0; j < count; j++) kvs_free(ids[j]);
    kvs_free(ids);
}

/* ============================================================
 * ZRANGEREC 0 N    → 返回最近 N 条记录（换行分隔）
 * ============================================================ */
static void cmd_zrangerec(resp_request_t *req, resp_reply_t *reply) {
    if (req->argc != 3) { reply->status = KVS_RESP_PARSE_ERROR; return; }

    long start = atol(req->argv[1]);
    long stop  = atol(req->argv[2]);

    if (start != 0 || stop < 0) {
        reply->status = KVS_RESP_PARSE_ERROR;
        return;
    }

    int n = (int)(stop - start + 1);
    if (n <= 0) { reply->status = KVS_RESP_NO_EXISTS; return; }

    char **ids = NULL;
    int count = 0;
    int ret = kvs_zset_recent(&global_skip, n, &ids, &count);
    if (ret != 0) { reply->status = KVS_RESP_ERROR; return; }
    if (count == 0) { reply->status = KVS_RESP_NO_EXISTS; return; }

    build_records_newline(reply, ids, count);

    kvs_zset_free_list(ids, count);
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
    /* 原有命令：走 global_hash（老表，保留） */
    {"HSET",    3, cmd_hash_set},
    {"HGET",    2, cmd_hash_get},
    {"HDEL",    2, cmd_hash_del},
    {"HMOD",    3, cmd_hash_mod},
    {"HEXISTS", 2, cmd_hash_exists},

    /* 语义缓存：走 global_hash1 */
    {"KEEP",    3, cmd_keep},
    {"MATCH",   2, cmd_match},

    /* 上下文：走 global_hash2（带 TTL） */
    {"SETCTX",  4, cmd_setctx},
    {"GETCTX",  2, cmd_getctx},
    {"DELCTX",  2, cmd_delctx},

    /* 全量记录：走 global_hash3（无 TTL） */
    {"SETREC",  3, cmd_setrec},
    {"GETREC",  2, cmd_getrec},
    {"GETIDXREC", 2, cmd_getidxrec},

    /* 关键词索引：走 global_hash4（追加语义） */
    {"SETIDX",  3, cmd_setidx},
    {"GETIDX",  2, cmd_getidx},
#endif
#if ENABLE_SKIPLIST
    {"SSET",    3, cmd_skip_set},
    {"SGET",    2, cmd_skip_get},
    {"SDEL",    2, cmd_skip_del},
    {"SMOD",    3, cmd_skip_mod},
    {"SEXISTS", 2, cmd_skip_exists},

    /* 时间索引：走跳表 */
    {"ZADD",    3, cmd_zadd},
    {"ZRANGE",  3, cmd_zrange},
    {"ZRANGEREC", 3, cmd_zrangerec},
#endif
    {"SAVE",      1, cmd_sys_save},
    {"SYNC",      1, cmd_sys_sync},
    {"SYNC_DONE", 1, cmd_sys_sync_done},
    {"MEMTRIM",   1, cmd_sys_memtrim},
    {NULL, 0, NULL}
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

/*
 * 查表函数（支持带长度和不带长度）
 * 如果 len > 0，按长度比较；如果 len == 0，按字符串比较（依赖 \0 结尾）
 */
command_t *lookup_command(const char *name, int len) {
    if (!name) return NULL;
    if (!g_cmd_hash_inited) kvs_cmd_init();

    uint32_t hash = 5381;
    int compare_len = (len > 0) ? len : (int)strlen(name);
    
    // 计算哈希
    for (int i = 0; i < compare_len && name[i]; i++) {
        hash = ((hash << 5) + hash) + (unsigned char)toupper(name[i]);
    }
    uint32_t idx = hash & (CMD_HASH_SIZE - 1);

    while (g_cmd_hash[idx] != NULL) {
        int cmd_len = (int)strlen(g_cmd_hash[idx]->name);
        // 长度匹配且内容匹配（使用 strncasecmp）
        if (cmd_len == compare_len &&
            strncasecmp(g_cmd_hash[idx]->name, name, compare_len) == 0) {
            return g_cmd_hash[idx];
        }
        idx = (idx + 1) & (CMD_HASH_SIZE - 1);
    }
    return NULL;
}

// 批量执行入口

int kvs_execute_batch(parsed_cmd_t *cmds, resp_reply_t *replies, int cmd_num) {

        replies[0].status = KVS_RESP_ERROR;
        replies[0].body = NULL;
        replies[0].body_len = 0;

        if (cmds[0].cmd && cmds[0].cmd->proc) {
            cmds[0].cmd->proc(&cmds[0].req, &replies[0]);
        } else {
            replies[0].status = KVS_RESP_UNKNOWN;
        }

    return 0;
}