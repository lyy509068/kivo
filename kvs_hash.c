#include <stdlib.h>
#include <string.h>
#include <stdint.h>   
#include <sys/time.h> 
#include "kvstore.h" 

#define DEFAULT_HASH_SLOTS 1024 

kvs_hash_t global_hash={0};

// ⏱️ 新增辅助函数：获取当前毫秒级时间戳
static int64_t get_current_ms_hash(void) {
    struct timeval tv;
    gettimeofday(&tv, NULL);
    return (int64_t)tv.tv_sec * 1000 + tv.tv_usec / 1000;
}

// 原样输出
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

// 原样输出
void kvs_hash_destroy(kvs_hash_t *hash) {
    if (!hash || !hash->buckets) return;
    for (int i = 0; i < hash->max_slots; i++) {
        hashnode_t *node = hash->buckets[i];
        while (node) {
            hashnode_t *next = node->next;
            // 使用辅助函数释放内部数据内存
            kv_data_destroy(&node->key);
            kv_data_destroy(&node->value);
            kvs_free(node);
            node = next;
        }
    }
    kvs_free(hash->buckets);
    hash->buckets = NULL;
    hash->count = 0;
}

// ⏱️ 修改：函数签名增加 expire_time
int kvs_hash_set(kvs_hash_t *hash, kv_data_t *key, kv_data_t *value, int64_t expire_time) {
    if (!hash || !key || !value) return -1;
    unsigned long h = kv_data_hash(key, hash->max_slots);
    
    // 检查是否存在（避免重复）
    hashnode_t *node = hash->buckets[h];
    while (node) {
        if (kv_data_cmp(&node->key, key) == 0) return 1; // 已存在
        node = node->next;
    }

    // 创建新节点
    hashnode_t *new_node = (hashnode_t *)kvs_malloc(sizeof(hashnode_t));
    if (!new_node) return -1;

    // 修复潜在隐患：显式将 new_node 内存清零，防止 dup 局部失败时 destroy 裸指针
    memset(new_node, 0, sizeof(hashnode_t));

    // 使用深拷贝辅助函数
    if (kv_data_dup(&new_node->key, key) != 0 || 
        kv_data_dup(&new_node->value, value) != 0) {
        kv_data_destroy(&new_node->key); // 失败时回滚已分配的 key 内存
        kvs_free(new_node);              // 释放节点本身
        return -1;
    }

    // ⏱️ 新增：写入绝对过期时间戳
    new_node->expire_time = expire_time;

    // 链表头插法
    new_node->next = hash->buckets[h];
    hash->buckets[h] = new_node;
    hash->count++;
    return 0;
}

// ⏱️ 修改：引入惰性删除拦截机制
kv_data_t *kvs_hash_get(kvs_hash_t *hash, kv_data_t *key) {
    if (!hash || !key) return NULL;
    unsigned long h = kv_data_hash(key, hash->max_slots);
    
    hashnode_t *node = hash->buckets[h];
    while (node) {
        if (kv_data_cmp(&node->key, key) == 0) {
            // ⏱️ 关键拦截：检查当前节点是否已超时过期
            if (node->expire_time > 0 && get_current_ms_hash() > node->expire_time) {
                kvs_hash_del(hash, key); // 惰性删除：将其从冲突链表中彻底剥离并释放
                return NULL;             // 假装数据不存在
            }
            return &node->value;
        }
        node = node->next;
    }
    return NULL;
}

// ⏱️ 修改：函数签名增加 expire_time
int kvs_hash_mod(kvs_hash_t *hash, kv_data_t *key, kv_data_t *value, int64_t expire_time) {
    if (!hash || !key || !value) return -1;
    unsigned long h = kv_data_hash(key, hash->max_slots);
    
    hashnode_t *node = hash->buckets[h];
    while (node) {
        if (kv_data_cmp(&node->key, key) == 0) {
            // 销毁旧值，利用 kv_data_create 更新新值
            kv_data_destroy(&node->value);
            int ret = kv_data_dup(&node->value, value);
            if (ret == 0) {
                // ⏱️ 新增：更新成功后同步覆盖新的过期时间
                node->expire_time = expire_time;
            }
            return ret;
        }
        node = node->next;
    }
    return 1; // 未找到
}

// 原样输出：删除键值对
int kvs_hash_del(kvs_hash_t *hash, kv_data_t *key) {
    if (!hash || !key) return -1;
    unsigned long h = kv_data_hash(key, hash->max_slots);
    
    hashnode_t *node = hash->buckets[h];
    hashnode_t *prev = NULL;
    
    while (node) {
        if (kv_data_cmp(&node->key, key) == 0) {
            if (prev) prev->next = node->next;
            else hash->buckets[h] = node->next;
            
            kv_data_destroy(&node->key);
            kv_data_destroy(&node->value);
            kvs_free(node);
            hash->count--;
            return 0; // 成功
        }
        prev = node;
        node = node->next;
    }
    return 1; // 未找到
}

// 原样输出
int kvs_hash_exist(kvs_hash_t *hash, kv_data_t *key) {
    return (kvs_hash_get(hash, key) != NULL) ? 0 : 1;
}

// 原样输出
int kvs_hash_get_value_len(kvs_hash_t *hash, kv_data_t *key) {
    kv_data_t *val = kvs_hash_get(hash, key);
    return val ? val->len : -1;
}

// 原样输出
void kvs_hash_foreach(kvs_hash_t *hash, void (*callback)(kv_data_t *key, kv_data_t *value, void *arg), void *arg) {
    if (!hash || !callback) return;
    for (int i = 0; i < hash->max_slots; i++) {
        hashnode_t *node = hash->buckets[i];
        while (node) {
            callback(&node->key, &node->value, arg);
            node = node->next;
        }
    }
}