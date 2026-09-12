#include "kvstore.h"
#include <string.h>
#include <stdlib.h>
#include <stdint.h>   
#include <sys/time.h> 
#include <unistd.h>
#include <pthread.h>

#if ENABLE_SKIPLIST

kvs_skip_t global_skip = {0};

/* 标准 kv_data 比较 */
static inline int kv_data_compare_safe(const kv_data_t *a, const kv_data_t *b) {
    if (a == b) return 0;
    if (!a || !a->data) return (!b || !b->data) ? 0 : -1;
    if (!b || !b->data) return 1;
    size_t min_len = a->len < b->len ? a->len : b->len;
    int cmp = memcmp(a->data, b->data, min_len);
    if (cmp != 0) return cmp;
    if (a->len < b->len) return -1;
    if (a->len > b->len) return 1;
    return 0;
}

static inline int64_t skip_now_if_ttl(void) {
    if (!g_enable_ttl) return 0;
    struct timeval tv;
    gettimeofday(&tv, NULL);
    return (int64_t)tv.tv_sec * 1000 + tv.tv_usec / 1000;
}

static inline uint32_t xorshift32(uint32_t *state) {
    uint32_t x = *state;
    x ^= x << 13;
    x ^= x >> 17;
    x ^= x << 5;
    return *state = x;
}

static int random_level(void) {
    static __thread uint32_t seed = 0;
    if (seed == 0) seed = (uint32_t)(uintptr_t)pthread_self() ^ (uint32_t)time(NULL);
    
    int level = 0;
    while ((xorshift32(&seed) & 0xFFFF) < (0xFFFF >> 2) && level < MAX_LEVEL) {
        level++;
    }
    return level;
}

/* 节点创建：一次性分配 node + forward 数组 + key 数据 + value 数据 */
static skipnode_binary_t* skipnode_create(int level, kv_data_t *key, kv_data_t *value, int64_t expire_time) {
    size_t key_len = (key && key->data) ? key->len : 0;
    size_t val_len = (value && value->data) ? value->len : 0;
    size_t forward_bytes = (level + 1) * sizeof(skipnode_binary_t *);
    size_t total_size = sizeof(skipnode_binary_t) + forward_bytes + key_len + val_len;

    skipnode_binary_t *node = (skipnode_binary_t *)kvs_malloc(total_size);
    if (!node) return NULL;

    node->level = level;
    node->expire_time = expire_time;

    for (int i = 0; i <= level; i++) {
        node->forward[i] = NULL;
    }

    char *payload = (char *)node + sizeof(skipnode_binary_t) + forward_bytes;

    node->key.data = payload;
    node->key.len = key_len;
    if (key_len > 0 && key->data) {
        memcpy(node->key.data, key->data, key_len);
    }

    node->value.data = payload + key_len;
    node->value.len = val_len;
    if (val_len > 0 && value->data) {
        memcpy(node->value.data, value->data, val_len);
    }

    return node;
}

/* 节点销毁：单次内存释放 */
static inline void skipnode_destroy(skipnode_binary_t *node) {
    if (node) {
        kvs_free(node);
    }
}

/* 初始化 */
int kvs_skip_create(kvs_skip_t *skip) {
    if (!skip) return -1;
    
    skip->level = 0;
    skip->count = 0;
    
    kv_data_t empty_key = {NULL, 0};
    kv_data_t empty_value = {NULL, 0};
    
    skip->header = skipnode_create(MAX_LEVEL, &empty_key, &empty_value, 0);
    if (!skip->header) return -1;
    
    return 0;
}

/* 销毁 */
void kvs_skip_destroy(kvs_skip_t *skip) {
    if (!skip || !skip->header) return;
    
    skipnode_binary_t *current = skip->header->forward[0];
    while (current) {
        skipnode_binary_t *next = current->forward[0];
        skipnode_destroy(current);
        current = next;
    }
    
    skipnode_destroy(skip->header);
    skip->header = NULL;
    skip->level = 0;
    skip->count = 0;
}

/* 查找并记录更新路径 */
static void skip_find_update(kvs_skip_t *skip, kv_data_t *key, 
                             skipnode_binary_t *update[MAX_LEVEL + 1],
                             skipnode_binary_t **found_node) {
    skipnode_binary_t *current = skip->header;
    
    for (int i = skip->level; i >= 0; i--) {
        while (current->forward[i] && 
               kv_data_compare_safe(&current->forward[i]->key, key) < 0) {
            current = current->forward[i];
        }
        update[i] = current;
    }
    
    current = current->forward[0];
    
    if (found_node) {
        if (current && kv_data_compare_safe(&current->key, key) == 0) {
            *found_node = current;
        } else {
            *found_node = NULL;
        }
    }
}

static int handle_expired_node(kvs_skip_t *skip, skipnode_binary_t *node,
                               skipnode_binary_t *update[MAX_LEVEL + 1], int64_t now) {
    if (!node || node->expire_time <= 0) return 0;
    if (now <= node->expire_time) return 0;
    
    for (int i = 0; i <= skip->level; i++) {
        if (update[i]->forward[i] == node) {
            update[i]->forward[i] = node->forward[i];
        }
    }
    
    skipnode_destroy(node);
    skip->count--;
    
    while (skip->level > 0 && skip->header->forward[skip->level] == NULL) {
        skip->level--;
    }
    
    return 1;
}

/* SET */
int kvs_skip_set(kvs_skip_t *skip, kv_data_t *key, kv_data_t *value, int64_t expire_time) {
    if (!skip || !skip->header || !key || !value) return -1;
    
    skipnode_binary_t *update[MAX_LEVEL + 1];
    skipnode_binary_t *current = NULL;
    
    skip_find_update(skip, key, update, &current);
    
    if (current) {
        if (value->len <= current->value.len) {
            memcpy(current->value.data, value->data, value->len);
            current->value.len = value->len;
            current->expire_time = expire_time;
            return 0;
        } else {
            // 原 value 空间不足，重建节点替换
            kvs_skip_del(skip, key);
            skip_find_update(skip, key, update, &current);
        }
    }
    
    int level = random_level();
    if (level > skip->level) {
        for (int i = skip->level + 1; i <= level; i++) {
            update[i] = skip->header;
        }
        skip->level = level;
    }
    
    skipnode_binary_t *new_node = skipnode_create(level, key, value, expire_time);
    if (!new_node) return -2;
    
    for (int i = 0; i <= level; i++) {
        new_node->forward[i] = update[i]->forward[i];
        update[i]->forward[i] = new_node;
    }
    
    skip->count++;
    return 0;
}

/* GET */
kv_data_t* kvs_skip_get(kvs_skip_t *skip, kv_data_t *key) {
    if (!skip || !skip->header || !key) return NULL;
    
    skipnode_binary_t *update[MAX_LEVEL + 1];
    skipnode_binary_t *current = NULL;
    
    skip_find_update(skip, key, update, &current);
    
    if (current && g_enable_ttl && current->expire_time > 0) {
        int64_t now = skip_now_if_ttl();
        if (now > current->expire_time) {
            handle_expired_node(skip, current, update, now);
            return NULL;
        }
    }
    
    return current ? &current->value : NULL;
}

/* DEL */
int kvs_skip_del(kvs_skip_t *skip, kv_data_t *key) {
    if (!skip || !skip->header || !key) return -1;
    
    skipnode_binary_t *update[MAX_LEVEL + 1];
    skipnode_binary_t *current = NULL;
    
    skip_find_update(skip, key, update, &current);
    
    if (!current) return 1;
    
    for (int i = 0; i <= skip->level; i++) {
        if (update[i]->forward[i] == current) {
            update[i]->forward[i] = current->forward[i];
        }
    }
    
    skipnode_destroy(current);
    skip->count--;
    
    while (skip->level > 0 && skip->header->forward[skip->level] == NULL) {
        skip->level--;
    }
    
    return 0;
}

int kvs_skip_del_if_expired(kvs_skip_t *skip, kv_data_t *key, int64_t expected_expire) {
    if (!skip || !skip->header || !key) return 0;
    
    skipnode_binary_t *update[MAX_LEVEL + 1];
    skipnode_binary_t *current = NULL;
    
    skip_find_update(skip, key, update, &current);
    
    if (current && current->expire_time == expected_expire) {
        kvs_skip_del(skip, key);
        return 1;
    }
    return 0;
}

int kvs_skip_mod(kvs_skip_t *skip, kv_data_t *key, kv_data_t *value, int64_t expire_time) {
    return kvs_skip_set(skip, key, value, expire_time);
}

int kvs_skip_exist(kvs_skip_t *skip, kv_data_t *key) {
    if (!skip || !key) return -1;
    kv_data_t *value = kvs_skip_get(skip, key);
    return (value != NULL) ? 0 : 1;
}

void kvs_skip_foreach(kvs_skip_t *skip, void (*callback)(kv_data_t *key, kv_data_t *value, void *arg), void *arg) {
    if (!skip || !skip->header || !callback) return;
    
    int64_t now = 0;
    int check_expire = g_enable_ttl;
    if (check_expire) now = skip_now_if_ttl();
    
    skipnode_binary_t *node = skip->header->forward[0];
    
    while (node) {
        skipnode_binary_t *next = node->forward[0];
        
        if (check_expire && node->expire_time > 0 && now > node->expire_time) {
            node = next;
            continue;
        }
        
        callback(&node->key, &node->value, arg);
        node = next;
    }
}

int kvs_skip_get_value_len(kvs_skip_t *skip, kv_data_t *key) {
    kv_data_t *value = kvs_skip_get(skip, key);
    return value ? (int)value->len : -1;
}

/* ============================================================
 * ZSet 封装：复合 key = "15位零填充时间戳:ID"
 * 复用现有跳表的字典序排序，时间戳升序 == 字典序升序
 * ============================================================ */

/* 拼 key 的内部辅助函数，返回 key 长度，失败返回 -1 */
static int zset_make_key(char *buf, size_t buf_size, int64_t ts,
                         const void *id, size_t id_len) {
    int n = snprintf(buf, buf_size, "%015lld:", (long long)ts);
    if (n < 0 || (size_t)n + id_len >= buf_size) return -1;
    memcpy(buf + n, id, id_len);
    return n + (int)id_len;
}

/* 从复合 key 中提取 ID（冒号后面的部分） */
static char *zset_extract_id(const char *kdata, int klen) {
    if (klen < 16) return NULL;
    // 前 15 位是时间戳，第 16 位是冒号，后面是 ID
    int id_len = klen - 16;
    char *id = (char *)kvs_malloc(id_len + 1);
    if (!id) return NULL;
    memcpy(id, kdata + 16, id_len);
    id[id_len] = '\0';
    return id;
}

/* ------------------------------------------------------------
 * 1. 插入：ZADD 时间戳 ID
 * ------------------------------------------------------------ */
int kvs_zset_add(kvs_skip_t *skip, int64_t timestamp, kv_data_t *id) {
    if (!skip || !skip->header || !id || !id->data || id->len == 0) {
        return -1;
    }

    char keybuf[128];
    int klen = zset_make_key(keybuf, sizeof(keybuf), timestamp,
                             id->data, id->len);
    if (klen < 0) return -1;

    kv_data_t key = { keybuf, (size_t)klen, 0 };
    kv_data_t empty_val = { NULL, 0, 0 };

    return kvs_skip_set(skip, &key, &empty_val, 0);
}

/* ------------------------------------------------------------
 * 2. 取最近 N 条：返回 ID 数组（调用方负责释放）
 * ------------------------------------------------------------ */
int kvs_zset_recent(kvs_skip_t *skip, int n,
                    char ***out_ids, int *out_count) {
    if (!skip || !skip->header || n <= 0 || !out_ids || !out_count) {
        return -1;
    }
    *out_ids = NULL;
    *out_count = 0;

    // 先数总数
    int total = 0;
    skipnode_binary_t *node = skip->header->forward[0];
    while (node) {
        total++;
        node = node->forward[0];
    }
    if (total == 0) return 0;

    int start = (total > n) ? (total - n) : 0;
    int want = total - start;

    char **ids = (char **)kvs_malloc(sizeof(char *) * want);
    if (!ids) return -1;

    // 跳过前 start 个
    node = skip->header->forward[0];
    for (int i = 0; i < start && node; i++) {
        node = node->forward[0];
    }

    int idx = 0;
    while (node && idx < want) {
        char *id = zset_extract_id((const char *)node->key.data,
                                   (int)node->key.len);
        if (id) {
            ids[idx++] = id;
        }
        node = node->forward[0];
    }

    *out_ids = ids;
    *out_count = idx;
    return 0;
}

/* ------------------------------------------------------------
 * 3. 范围查：[min_ts, max_ts] 内的所有 ID
 * ------------------------------------------------------------ */
int kvs_zset_range(kvs_skip_t *skip, int64_t min_ts, int64_t max_ts,
                   char ***out_ids, int *out_count) {
    if (!skip || !skip->header || !out_ids || !out_count) return -1;
    if (min_ts > max_ts) return -1;

    *out_ids = NULL;
    *out_count = 0;

    char min_prefix[20], max_prefix[20];
    snprintf(min_prefix, sizeof(min_prefix), "%015lld", (long long)min_ts);
    snprintf(max_prefix, sizeof(max_prefix), "%015lld", (long long)max_ts);

    // 第一次遍历：统计数量
    int count = 0;
    skipnode_binary_t *node = skip->header->forward[0];
    while (node) {
        if (node->key.len < 16) { node = node->forward[0]; continue; }
        const char *kdata = (const char *)node->key.data;

        int cmp_min = strncmp(kdata, min_prefix, 15);
        int cmp_max = strncmp(kdata, max_prefix, 15);

        if (cmp_min < 0) { node = node->forward[0]; continue; }
        if (cmp_max > 0) break;   // 后面都比 max 大，直接退出
        count++;
        node = node->forward[0];
    }
    if (count == 0) return 0;

    char **ids = (char **)kvs_malloc(sizeof(char *) * count);
    if (!ids) return -1;

    // 第二次遍历：收集
    node = skip->header->forward[0];
    int idx = 0;
    while (node && idx < count) {
        if (node->key.len < 16) { node = node->forward[0]; continue; }
        const char *kdata = (const char *)node->key.data;

        int cmp_min = strncmp(kdata, min_prefix, 15);
        int cmp_max = strncmp(kdata, max_prefix, 15);

        if (cmp_min < 0) { node = node->forward[0]; continue; }
        if (cmp_max > 0) break;

        char *id = zset_extract_id(kdata, (int)node->key.len);
        if (id) ids[idx++] = id;
        node = node->forward[0];
    }

    *out_ids = ids;
    *out_count = idx;
    return 0;
}

/* ------------------------------------------------------------
 * 释放 ID 数组
 * ------------------------------------------------------------ */
void kvs_zset_free_list(char **ids, int count) {
    if (!ids) return;
    for (int i = 0; i < count; i++) {
        if (ids[i]) kvs_free(ids[i]);
    }
    kvs_free(ids);
}

#endif