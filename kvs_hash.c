#include <stdlib.h>
#include <string.h>
#include <stdint.h>   
#include <sys/time.h> 
#include "kvstore.h" 

#if ENABLE_HASH

// 初始桶数量 (必须是 2 的 N 次方)
#define INIT_HASH_SLOTS 16

kvs_hash_t global_hash = {0}, global_hash1 = {0}, global_hash2 = {0}, global_hash3 = {0}, global_hash4 = {0};

static inline int64_t hash_now_if_ttl(void) {
    if (!g_enable_ttl) return 0;
    struct timeval tv;
    gettimeofday(&tv, NULL);
    return (int64_t)tv.tv_sec * 1000 + tv.tv_usec / 1000;
}

/* 内部辅助函数：Rehash 动态扩容/缩容（零哈希计算开销） */
static int kvs_hash_resize(kvs_hash_t *hash, int new_slots) {
    if (!hash || new_slots < INIT_HASH_SLOTS) return -1;

    size_t bytes = new_slots * sizeof(hashnode_t *);
    hashnode_t **new_buckets = (hashnode_t **)kvs_malloc(bytes);
    if (!new_buckets) return -1;
    memset(new_buckets, 0, bytes);

    size_t mask = (size_t)new_slots - 1;

    for (int i = 0; i < hash->max_slots; i++) {
        hashnode_t *node = hash->buckets[i];
        while (node) {
            hashnode_t *next = node->next;
            size_t h = (size_t)node->key.hash_cache & mask;
            node->next = new_buckets[h];
            new_buckets[h] = node;
            node = next;
        }
    }

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
    
    size_t bytes = hash->max_slots * sizeof(hashnode_t *);
    hash->buckets = (hashnode_t **)kvs_malloc(bytes);
    if (hash->buckets == NULL) {
        return -1;
    }
    memset(hash->buckets, 0, bytes);
    
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

/* 内部函数：创建内嵌节点（一次 malloc 零拷贝，并清零） */
static hashnode_t* hash_create_node(kv_data_t *key, kv_data_t *value, int64_t expire_time) {
    size_t total_size = sizeof(hashnode_t) + key->len + value->len;
    hashnode_t *node = (hashnode_t *)kvs_malloc(total_size);
    if (!node) return NULL;
    
    // 整个节点清零，包括柔性数组，防止内存池复用带来的残留指针
    memset(node, 0, total_size);
    
    node->next = NULL;
    node->expire_time = expire_time;
    
    node->key.data = node->data;
    node->key.len = key->len;
    node->key.hash_cache = key->hash_cache;
    if (key->len > 0 && key->data) {
        memcpy(node->data, key->data, key->len);
    }
    
    node->value.data = node->data + key->len;
    node->value.len = value->len;
    node->value.hash_cache = 0;
    if (value->len > 0 && value->data) {
        memcpy(node->value.data, value->data, value->len);
    }
    
    return node;
}

/* SET / 更新 */
int kvs_hash_set(kvs_hash_t *hash, kv_data_t *key, kv_data_t *value, int64_t expire_time) {
    if (!hash || !key || !value) return -1;
    if (!hash->buckets) return -1;

    // 检查负载因子：当元素数量达到或超过当前桶数时触发 2 倍扩容
    if (hash->count >= hash->max_slots) {
        if (kvs_hash_resize(hash, hash->max_slots * 2) != 0) {
            return -1;
        }
    }

    unsigned long h = kv_data_hash(key, hash->max_slots);
    // 防御：确保 h 在有效范围内
    if (h >= (unsigned long)hash->max_slots) {
        return -1;
    }
    
    hashnode_t *node = hash->buckets[h];
    hashnode_t *prev = NULL;
    while (node) {
        // 若节点指针异常（极小地址）则视为已损坏，直接退出
        if ((uintptr_t)node < 0x1000) {
            return -1;
        }
        if (kv_data_compare(&node->key, key) == 0) {
            break;
        }
        prev = node;
        node = node->next;
    }
    
    if (node) {
        // 更新现有 Key
        if (value->len <= node->value.len) {
            if (value->len > 0 && value->data) {
                memcpy(node->value.data, value->data, value->len);
            }
            node->value.len = value->len;
            node->expire_time = expire_time;
            return 0;
        } else {
            // 空间不足，替换旧节点
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
    if (!hash || !key || !hash->buckets) return NULL;
    unsigned long h = kv_data_hash(key, hash->max_slots);
    if (h >= (unsigned long)hash->max_slots) return NULL;
    
    hashnode_t *node = hash->buckets[h];
    while (node) {
        if ((uintptr_t)node < 0x1000) return NULL;
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
    if (!hash || !key || !hash->buckets) return -1;
    unsigned long h = kv_data_hash(key, hash->max_slots);
    if (h >= (unsigned long)hash->max_slots) return -1;
    
    hashnode_t *node = hash->buckets[h];
    hashnode_t *prev = NULL;
    
    while (node) {
        if ((uintptr_t)node < 0x1000) return -1;
        if (kv_data_compare(&node->key, key) == 0) {
            if (prev) {
                prev->next = node->next;
            } else {
                hash->buckets[h] = node->next;
            }
            kvs_free(node);
            hash->count--;

            // 缩容
            if (hash->count < hash->max_slots / 4 && hash->max_slots > INIT_HASH_SLOTS) {
                kvs_hash_resize(hash, hash->max_slots / 2);
            }

            return 0;
        }
        prev = node;
        node = node->next;
    }
    return 1;
}

/* 条件删除（专用于过期定时器） */
int kvs_hash_del_if_expired(kvs_hash_t *hash, kv_data_t *key, int64_t expected_expire) {
    if (!hash || !key || !hash->buckets) return 0;
    unsigned long h = kv_data_hash(key, hash->max_slots);
    if (h >= (unsigned long)hash->max_slots) return 0;
    
    hashnode_t *node = hash->buckets[h];
    while (node) {
        if ((uintptr_t)node < 0x1000) return 0;
        if (kv_data_compare(&node->key, key) == 0) {
            if (node->expire_time == expected_expire) {
                kvs_hash_del(hash, key);
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
    if (!hash || !key || !hash->buckets) return 1;
    unsigned long h = kv_data_hash(key, hash->max_slots);
    if (h >= (unsigned long)hash->max_slots) return 1;
    
    hashnode_t *node = hash->buckets[h];
    while (node) {
        if ((uintptr_t)node < 0x1000) break;
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
    if (!hash || !callback || !hash->buckets) return;
    
    int64_t now = 0;
    int check_expire = g_enable_ttl;
    if (check_expire) now = hash_now_if_ttl();

    for (int i = 0; i < hash->max_slots; i++) {
        hashnode_t *node = hash->buckets[i];
        while (node) {
            if ((uintptr_t)node < 0x1000) break;
            if (check_expire && node->expire_time > 0 && now > node->expire_time) {
                node = node->next;
                continue;
            }
            callback(&node->key, &node->value, arg);
            node = node->next;
        }
    }
}

// 追加接口： 首次写入：key → "ID" 再次写入：key → "ID1,ID2"
int kvs_hash_append(kvs_hash_t *hash, kv_data_t *key, kv_data_t *id) {
    if (!hash || !key || !id) return -1;
    if (!hash->buckets) return -1;
    if (id->len == 0 || id->data == NULL) return -1;

    // 查询现有值
    kv_data_t *existing = kvs_hash_get(hash, key);

    // 首次写入：直接 SET
    if (existing == NULL || existing->len == 0) {
        return kvs_hash_set(hash, key, id, 0);
    }

    // 去重扫描
    {
        char *edata = (char *)existing->data;
        size_t i = 0;
        while (i < existing->len) {
            size_t j = i;
            while (j < existing->len && edata[j] != ',') j++;

            size_t token_len = j - i;
            if (token_len == id->len &&
                memcmp(edata + i, id->data, id->len) == 0) {
                return 0;  // 已存在
            }
            i = j + 1;
        }
    }

    // 先拷贝旧数据（set 可能替换节点，原指针会悬空）
    size_t old_len = existing->len;
    char *old_buf = (char *)kvs_malloc(old_len);
    if (!old_buf) return -1;
    memcpy(old_buf, existing->data, old_len);

    // 拼接：old + "," + id
    size_t new_len = old_len + 1 + id->len;
    char *new_buf = (char *)kvs_malloc(new_len);
    if (!new_buf) {
        kvs_free(old_buf);
        return -1;
    }
    memcpy(new_buf, old_buf, old_len);
    new_buf[old_len] = ',';
    memcpy(new_buf + old_len + 1, id->data, id->len);
    kvs_free(old_buf);

    // 写回
    kv_data_t new_val = { new_buf, new_len, 0 };
    int ret = kvs_hash_set(hash, key, &new_val, 0);
    kvs_free(new_buf);
    return ret;
}

// 查询接口：读取 key 对应的所有 ID
int kvs_hash_get_list(kvs_hash_t *hash, kv_data_t *key,
                      char ***out_ids, int *out_count) {
    if (!hash || !key || !out_ids || !out_count) return -1;

    *out_ids = NULL;
    *out_count = 0;

    kv_data_t *value = kvs_hash_get(hash, key);
    if (value == NULL || value->len == 0) {
        return 0;
    }

    char *vdata = (char *)value->data;

    // 统计元素个数
    int count = 1;
    for (size_t i = 0; i < value->len; i++) {
        if (vdata[i] == ',') count++;
    }

    char **ids = (char **)kvs_malloc(sizeof(char *) * count);
    if (!ids) return -1;

    int idx = 0;
    size_t start = 0;
    for (size_t i = 0; i <= value->len; i++) {
        if (i == value->len || vdata[i] == ',') {
            size_t len = i - start;
            char *id = (char *)kvs_malloc(len + 1);
            if (!id) {
                for (int j = 0; j < idx; j++) kvs_free(ids[j]);
                kvs_free(ids);
                return -1;
            }
            memcpy(id, vdata + start, len);
            id[len] = '\0';
            ids[idx++] = id;
            start = i + 1;
        }
    }

    *out_ids = ids;
    *out_count = idx;
    return 0;
}

// 释放 ID 数组
void kvs_hash_free_list(char **ids, int count) {
    if (!ids) return;
    for (int i = 0; i < count; i++) {
        if (ids[i]) kvs_free(ids[i]);
    }
    kvs_free(ids);
}

#endif