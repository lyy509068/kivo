#include <stdlib.h>
#include <string.h>
#include <stdint.h>   
#include <sys/time.h> 
#include "kvstore.h" 
#if ENABLE_HASH
#define DEFAULT_HASH_SLOTS 131072 

kvs_hash_t global_hash={0};

static int64_t get_current_ms_hash(void) {
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
        if (kv_data_cmp(&node->key, key) == 0) {
            // 处理过期节点
            if (node->expire_time > 0 && get_current_ms_hash() > node->expire_time) {
                kvs_hash_del(hash, key);
                break;
            }
            
            // ✅ 修复：更新value和过期时间（而不是直接返回）
            kv_data_destroy(&node->value);
            if (kv_data_dup(&node->value, value) != 0) return -2;
            node->expire_time = expire_time;
            return 0;  // 更新成功
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

    // 链表头插法
    new_node->next = hash->buckets[h];
    hash->buckets[h] = new_node;
    hash->count++;
    return 0;
}

kv_data_t *kvs_hash_get(kvs_hash_t *hash, kv_data_t *key) {
    if (!hash || !key) return NULL;
    unsigned long h = kv_data_hash(key, hash->max_slots);
    //printf("[HASH_DEBUG] Key data ptr: %p, len: %zu, Slot: %lu\n", key->data, key->len, h);
    hashnode_t *node = hash->buckets[h];
    while (node) {
        if (kv_data_cmp(&node->key, key) == 0) {
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

int kvs_hash_mod(kvs_hash_t *hash, kv_data_t *key, kv_data_t *value, int64_t expire_time) {
    if (!hash || !key || !value) return -1;
    unsigned long h = kv_data_hash(key, hash->max_slots);
    
    hashnode_t *node = hash->buckets[h];
    while (node) {
        if (kv_data_cmp(&node->key, key) == 0) {
            // 如果虽然找到了，但数据已经过期了
            if (node->expire_time > 0 && get_current_ms_hash() > node->expire_time) {
                kvs_hash_del(hash, key); // 默默清理掉
                return 1;                // 视同“未找到”
            }

            // 销毁旧值，利用 kv_data_create 更新新值
            kv_data_destroy(&node->value);
            int ret = kv_data_dup(&node->value, value);
            if (ret == 0) {
                node->expire_time = expire_time;
            }
            return ret;
        }
        node = node->next;
    }
    return 1; // 未找到
}

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
            kvs_free_type(OBJ_HASH, node);
            hash->count--;
            return 0; // 成功
        }
        prev = node;
        node = node->next;
    }
    return 1; // 未找到
}


int kvs_hash_exist(kvs_hash_t *hash, kv_data_t *key) {
    if (!hash || !key) return 1; // 1 代表不存在
    unsigned long h = kv_data_hash(key, hash->max_slots);
    
    hashnode_t *node = hash->buckets[h];
    while (node) {
        if (kv_data_cmp(&node->key, key) == 0) {
            // 如果过期了，顺手做惰性删除
            if (node->expire_time > 0 && get_current_ms_hash() > node->expire_time) {
                kvs_hash_del(hash, key);
                return 1; 
            }
            return 0; // 0 代表存在
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
    int64_t now = get_current_ms_hash();

    for (int i = 0; i < hash->max_slots; i++) {
        hashnode_t *node = hash->buckets[i];
        hashnode_t *prev = NULL;

        while (node) {
            // 检查节点是否已过期
            if (node->expire_time > 0 && now > node->expire_time) {
                // 1. 从单链表中剔除该节点
                hashnode_t *next_node = node->next;
                if (prev) {
                    prev->next = next_node;
                } else {
                    hash->buckets[i] = next_node;
                }

                // 2. 释放物理内存
                kv_data_destroy(&node->key);
                kv_data_destroy(&node->value);
                kvs_free_type(OBJ_HASH, node);
                hash->count--;

                // 3. 转向下一个节点，不更新 prev
                node = next_node;
                continue;
            }

            // 正常未过期节点，调用回调
            callback(&node->key, &node->value, arg);
            
            prev = node;
            node = node->next;
        }
    }
}
#endif 