#include <stdlib.h>
#include <string.h>
#include <stdint.h>   
#include <sys/time.h> 
#include "kvstore.h" 

#if ENABLE_HASH

// 初始桶数量
#define INIT_HASH_SLOTS 16

kvs_hash_t global_hash = {0};

static inline int64_t hash_now_if_ttl(void) {
    if (!g_enable_ttl) return 0;
    struct timeval tv;
    gettimeofday(&tv, NULL);
    return (int64_t)tv.tv_sec * 1000 + tv.tv_usec / 1000;
}

/* 内部辅助函数：Rehash 动态扩容/缩容（零哈希计算开销） */
static int kvs_hash_resize(kvs_hash_t *hash, int new_slots) {
    if (!hash || new_slots < INIT_HASH_SLOTS) return -1;

    // 分配新的桶数组并清零
    hashnode_t **new_buckets = (hashnode_t **)kvs_calloc(new_slots, sizeof(hashnode_t *));
    if (!new_buckets) return -1;

    // 1. 预先计算位掩码 (要求 new_slots 必须是 2 的 N 次方)
    size_t mask = (size_t)new_slots - 1;

    // 2. 遍历旧桶重新挂载节点
    for (int i = 0; i < hash->max_slots; i++) {
        hashnode_t *node = hash->buckets[i];
        while (node) {
            hashnode_t *next = node->next;

            // ⭐ 核心优化：直接读取节点内缓存的完整 Hash 值，免去字符串重新哈希
            // 配合位与掩码运算，彻底消除开销昂贵的 CPU 取模 (%) 指令
            size_t h = (size_t)node->key.hash_cache & mask;

            // 头插法放入新桶
            node->next = new_buckets[h];
            new_buckets[h] = node;

            node = next;
        }
    }

    // 3. 释放旧桶，更新句柄
    kvs_free(hash->buckets);
    hash->buckets = new_buckets;
    hash->max_slots = new_slots;

    return 0;
}

/* 创建哈希表 */
int kvs_hash_create(kvs_hash_t *hash) {
    if (!hash) return -1;
    hash->max_slots = INIT_HASH_SLOTS;
    hash->count = 0;
    hash->buckets = (hashnode_t **)kvs_calloc(hash->max_slots, sizeof(hashnode_t *));
    if (hash->buckets == NULL) {
        return -1;
    }
    return 0;
}

/* 销毁哈希表 */
void kvs_hash_destroy(kvs_hash_t *hash) {
    if (!hash || !hash->buckets) return;
    for (int i = 0; i < hash->max_slots; i++) {
        hashnode_t *node = hash->buckets[i];
        while (node) {
            hashnode_t *next = node->next;
            kvs_free(node);
            node = next;
        }
    }
    kvs_free(hash->buckets);
    hash->buckets = NULL;
    hash->count = 0;
    hash->max_slots = 0;
}

/* 内部函数：创建内嵌节点（一次 malloc 零拷贝） */
static hashnode_t* hash_create_node(kv_data_t *key, kv_data_t *value, int64_t expire_time) {
    size_t total_size = sizeof(hashnode_t) + key->len + value->len;
    hashnode_t *node = (hashnode_t *)kvs_malloc(total_size);
    if (!node) return NULL;
    
    node->next = NULL;
    node->expire_time = expire_time;
    
    // 设置 key 的 kv_data_t
    node->key.data = node->data;
    node->key.len = key->len;
    node->key.hash_cache = key->hash_cache;
    memcpy(node->data, key->data, key->len);
    
    // 设置 value 的 kv_data_t
    node->value.data = node->data + key->len;
    node->value.len = value->len;
    node->value.hash_cache = 0;
    memcpy(node->value.data, value->data, value->len);
    
    return node;
}

/* SET / 更新 */
int kvs_hash_set(kvs_hash_t *hash, kv_data_t *key, kv_data_t *value, int64_t expire_time) {
    if (!hash || !key || !value) return -1;

    // 检查负载因子：当元素数量达到或超过当前桶数时触发 2 倍扩容
    if (hash->count >= hash->max_slots) {
        kvs_hash_resize(hash, hash->max_slots * 2);
    }

    unsigned long h = kv_data_hash(key, hash->max_slots);
    
    hashnode_t *node = hash->buckets[h];
    hashnode_t *prev = NULL;
    while (node) {
        if (kv_data_compare(&node->key, key) == 0) {
            break;
        }
        prev = node;
        node = node->next;
    }
    
    if (node) {
        // 更新现有 Key
        if (value->len <= node->value.len) {
            memcpy(node->value.data, value->data, value->len);
            node->value.len = value->len;
            node->expire_time = expire_time;
            return 0;
        } else {
            if (prev) {
                prev->next = node->next;
            } else {
                hash->buckets[h] = node->next;
            }
            kvs_free(node);
            hash->count--;
            
            hashnode_t *new_node = hash_create_node(key, value, expire_time);
            if (!new_node) return -1;
            new_node->next = hash->buckets[h];
            hash->buckets[h] = new_node;
            hash->count++;
            return 0;
        }
    }
    
    // Key 不存在，插入新节点
    hashnode_t *new_node = hash_create_node(key, value, expire_time);
    if (!new_node) return -1;
    new_node->next = hash->buckets[h];
    hash->buckets[h] = new_node;
    hash->count++;
    return 0;
}

/* GET */
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

/* MOD（直接复用 SET） */
int kvs_hash_mod(kvs_hash_t *hash, kv_data_t *key, kv_data_t *value, int64_t expire_time) {
    return kvs_hash_set(hash, key, value, expire_time);
}

/* DELETE */
int kvs_hash_del(kvs_hash_t *hash, kv_data_t *key) {
    if (!hash || !key) return -1;
    unsigned long h = kv_data_hash(key, hash->max_slots);
    
    hashnode_t *node = hash->buckets[h];
    hashnode_t *prev = NULL;
    
    while (node) {
        if (kv_data_compare(&node->key, key) == 0) {
            if (prev) {
                prev->next = node->next;
            } else {
                hash->buckets[h] = node->next;
            }
            kvs_free(node);
            hash->count--;

            // 触发动态缩容：当元素数量低于 25% 且大于初始值时，缩容 50%
            if (hash->count < hash->max_slots / 4 && hash->max_slots > INIT_HASH_SLOTS) {
                kvs_hash_resize(hash, hash->max_slots / 2);
            }

            return 0;
        }
        prev = node;
        node = node->next;
    }
    return 1; // 未找到
}

/* 条件删除（专用于过期定时器） */
int kvs_hash_del_if_expired(kvs_hash_t *hash, kv_data_t *key, int64_t expected_expire) {
    if (!hash || !key) return 0;
    unsigned long h = kv_data_hash(key, hash->max_slots);
    
    hashnode_t *node = hash->buckets[h];
    while (node) {
        if (kv_data_compare(&node->key, key) == 0) {
            if (node->expire_time == expected_expire) {
                kvs_hash_del(hash, key); // 内部会触发 count-- 及可能的缩容
                return 1;
            }
            return 0;
        }
        node = node->next;
    }
    return 0;
}

/* EXISTS */
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

/* 获取 value 长度 */
int kvs_hash_get_value_len(kvs_hash_t *hash, kv_data_t *key) {
    kv_data_t *val = kvs_hash_get(hash, key);
    return val ? (int)val->len : -1;
}

/* 遍历所有节点 */
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