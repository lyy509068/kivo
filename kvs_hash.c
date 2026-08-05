#include <stdlib.h>
#include <string.h>
#include <stdint.h>   
#include <sys/time.h> 
#include "kvstore.h" 
#if ENABLE_HASH
#define DEFAULT_HASH_SLOTS 262144 

kvs_hash_t global_hash={0};

static inline int64_t hash_now_if_ttl(void) {
    if (!g_enable_ttl) return 0;
    struct timeval tv;
    gettimeofday(&tv, NULL);
    return (int64_t)tv.tv_sec * 1000 + tv.tv_usec / 1000;
}


int kvs_hash_create(kvs_hash_t *hash) {
    if (!hash) return -1;

    hash->max_slots = DEFAULT_HASH_SLOTS;
    hash->count = 0;
    
    // 使用 kvs_calloc 初始化桶
    hash->buckets = (hashnode_t **)kvs_calloc(hash->max_slots, sizeof(hashnode_t *));
    
    if (hash->buckets == NULL) {
        return -1;
    }
    
    return 0;
}


void kvs_hash_destroy(kvs_hash_t *hash) {
    if (!hash || !hash->buckets) return;
    for (int i = 0; i < hash->max_slots; i++) {
        hashnode_t *node = hash->buckets[i];
        while (node) {
            hashnode_t *next = node->next;
            // 使用辅助函数释放内部数据内存
            kv_data_destroy(&node->key);
            kv_data_destroy(&node->value);
            kvs_free_type(OBJ_HASH, node);
            node = next;
        }
    }
    kvs_free(hash->buckets);
    hash->buckets = NULL;
    hash->count = 0;
}

int kvs_hash_set(kvs_hash_t *hash, kv_data_t *key, kv_data_t *value, int64_t expire_time) {
    if (!hash || !key || !value) return -1;
    unsigned long h = kv_data_hash(key, hash->max_slots);
    
    // 检查是否存在
    hashnode_t *node = hash->buckets[h];
    while (node) {
        if (kv_data_compare(&node->key, key) == 0) {
            // 直接更新 value，不检查过期
            kv_data_destroy(&node->value);
            if (kv_data_dup(&node->value, value) != 0) return -2;
            node->expire_time = expire_time;
            return 0;
        }
        node = node->next;
    }

    // 创建新节点
    hashnode_t *new_node = (hashnode_t *)kvs_malloc_type(OBJ_HASH, sizeof(hashnode_t));
    if (!new_node) return -1;
    memset(new_node, 0, sizeof(hashnode_t));

    if (kv_data_dup(&new_node->key, key) != 0 || 
        kv_data_dup(&new_node->value, value) != 0) {
        kv_data_destroy(&new_node->key);
        kvs_free_type(OBJ_HASH, new_node);
        return -1;
    }

    new_node->expire_time = expire_time;
    new_node->next = hash->buckets[h];
    hash->buckets[h] = new_node;
    hash->count++;
    return 0;
}

kv_data_t *kvs_hash_get(kvs_hash_t *hash, kv_data_t *key) {
    if (!hash || !key) return NULL;
    unsigned long h = kv_data_hash(key, hash->max_slots);
    
    hashnode_t *node = hash->buckets[h];
    while (node) {
        if (kv_data_compare(&node->key, key) == 0) {
            if (g_enable_ttl && node->expire_time > 0) {
                int64_t now = hash_now_if_ttl();
                if (now > node->expire_time) {
                    kvs_hash_del(hash, key);
                    return NULL;
                }
            }
            return &node->value;
        }
        node = node->next;
    }
    return NULL;
}

int kvs_hash_mod(kvs_hash_t *hash, kv_data_t *key, kv_data_t *value, int64_t expire_time) {
    // MOD 合并为 SET，不再单独实现
    return kvs_hash_set(hash, key, value, expire_time);
}

int kvs_hash_del(kvs_hash_t *hash, kv_data_t *key) {
    if (!hash || !key) return -1;
    unsigned long h = kv_data_hash(key, hash->max_slots);
    
    hashnode_t *node = hash->buckets[h];
    hashnode_t *prev = NULL;
    
    while (node) {
        if (kv_data_compare(&node->key, key) == 0) {
            if (prev) prev->next = node->next;
            else hash->buckets[h] = node->next;
            
            kv_data_destroy(&node->key);
            kv_data_destroy(&node->value);
            kvs_free_type(OBJ_HASH, node);
            hash->count--;
            return 0; // 成功
        }
        prev = node;
        node = node->next;
    }
    return 1; // 未找到
}

/**
 * 条件删除：只有当 key 存在且 expire_time 等于 expected_expire 时才真正删除
 * 返回 1 表示已删除，0 表示未删除（key 不存在或时间不匹配）
 */
int kvs_hash_del_if_expired(kvs_hash_t *hash, kv_data_t *key, int64_t expected_expire) {
    if (!hash || !key) return 0;
    unsigned long h = kv_data_hash(key, hash->max_slots);
    
    hashnode_t *node = hash->buckets[h];
    while (node) {
        if (kv_data_compare(&node->key, key) == 0) {
            // 关键校验：过期时间必须完全匹配，防止旧 timer 误删新值
            if (node->expire_time == expected_expire) {
                kvs_hash_del(hash, key);   // 调用原有的删除函数
                return 1;
            }
            // 找到了但时间不匹配（已被覆盖或删除），直接返回 0
            return 0;
        }
        node = node->next;
    }
    return 0; // 没找到
}


int kvs_hash_exist(kvs_hash_t *hash, kv_data_t *key) {
    if (!hash || !key) return 1;
    unsigned long h = kv_data_hash(key, hash->max_slots);
    
    hashnode_t *node = hash->buckets[h];
    while (node) {
        if (kv_data_compare(&node->key, key) == 0) {
            if (g_enable_ttl && node->expire_time > 0) {
                int64_t now = hash_now_if_ttl();
                if (now > node->expire_time) {
                    kvs_hash_del(hash, key);
                    return 1;
                }
            }
            return 0;
        }
        node = node->next;
    }
    return 1;
}


int kvs_hash_get_value_len(kvs_hash_t *hash, kv_data_t *key) {
    kv_data_t *val = kvs_hash_get(hash, key);
    return val ? val->len : -1;
}


void kvs_hash_foreach(kvs_hash_t *hash, void (*callback)(kv_data_t *key, kv_data_t *value, void *arg), void *arg) {
    if (!hash || !callback) return;
    
    int64_t now = 0;
    int check_expire = g_enable_ttl;
    if (check_expire) now = hash_now_if_ttl();

    for (int i = 0; i < hash->max_slots; i++) {
        hashnode_t *node = hash->buckets[i];
        while (node) {
            if (check_expire && node->expire_time > 0 && now > node->expire_time) {
                node = node->next;
                continue;
            }
            callback(&node->key, &node->value, arg);
            node = node->next;
        }
    }
}
#endif 