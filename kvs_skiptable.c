// kvs_skiplist_binary.c
// 二进制安全版本的跳表存储引擎

#include "kvstore.h"
#include <string.h>
#include <stdlib.h>
#include <stdint.h>   // ⏱️ 新增：支持 int64_t
#include <sys/time.h> // ⏱️ 新增：支持 gettimeofday

#define MAX_LEVEL 6

// 全局跳表实例
kvs_skip_t global_skip = {0};

// ⏱️ 新增辅助函数：获取当前毫秒级时间戳
static int64_t get_current_ms_skip(void) {
    struct timeval tv;
    gettimeofday(&tv, NULL);
    return (int64_t)tv.tv_sec * 1000 + tv.tv_usec / 1000;
}

// 原样输出
static int random_level(void) {
    int level = 0;
    while (rand() < RAND_MAX / 2 && level < MAX_LEVEL) {
        level++;
    }
    return level;
}

// ⏱️ 修改：增加 expire_time 参数传入
static skipnode_binary_t* skipnode_create(int level, kv_data_t *key, kv_data_t *value, int64_t expire_time) {
    
    skipnode_binary_t *node = (skipnode_binary_t*)kvs_malloc(sizeof(skipnode_binary_t));
    if (!node) {
        printf("ERROR: skipnode_create - kvs_malloc node failed, size=%zu\n", sizeof(skipnode_binary_t));
        return NULL;
    }
    
    // 深拷贝 key
    if (kv_data_dup(&node->key, key) != 0) {
        kvs_free(node);
        return NULL;
    }
    
    // 深拷贝 value
    if (kv_data_dup(&node->value, value) != 0) {
        kv_data_destroy(&node->key);
        kvs_free(node);
        return NULL;
    }
    
    // ⏱️ 新增：写入绝对过期时间
    node->expire_time = expire_time;
    
    // 分配 forward 指针数组
    node->forward = (skipnode_binary_t**)kvs_malloc((level + 1) * sizeof(skipnode_binary_t*));
    if (!node->forward) {
        kv_data_destroy(&node->key);
        kv_data_destroy(&node->value);
        kvs_free(node);
        return NULL;
    }
   
    for (int i = 0; i <= level; i++) {
        node->forward[i] = NULL;
    }
    
    return node;
}

// 原样输出
int kvs_skip_create(kvs_skip_t *skip) {

    if (!skip) {
        printf("ERROR: kvs_skip_create - skip is NULL\n");
        return -1;
    }
    
    skip->level = 0;
    skip->count = 0;
    
    // 创建头节点（不存储实际数据）
    kv_data_t empty_key = {NULL, 0};
    kv_data_t empty_value = {NULL, 0};
    
    // 头节点没有过期概念，传入 0
    skip->header = skipnode_create(MAX_LEVEL, &empty_key, &empty_value, 0);
    if (!skip->header) {
        printf("ERROR: kvs_skip_create - skipnode_create failed\n");
        return -1;
    }
    
    return 0;
}

// 原样输出
static void skipnode_destroy(skipnode_binary_t *node) {
    if (!node) return;
    
    kv_data_destroy(&node->key);
    kv_data_destroy(&node->value);
    if (node->forward) {
        kvs_free(node->forward);
    }
    kvs_free(node);
}

// 原样输出
void kvs_skip_destroy(kvs_skip_t *skip) {
    if (!skip) return;
    
    skipnode_binary_t *current = skip->header->forward[0];
    skipnode_binary_t *next;
    
    while (current) {
        next = current->forward[0];
        skipnode_destroy(current);
        current = next;
    }
    
    skipnode_destroy(skip->header);
    skip->header = NULL;
    skip->level = 0;
    skip->count = 0;
}

// ⏱️ 修改：函数签名增加 expire_time
int kvs_skip_set(kvs_skip_t *skip, kv_data_t *key, kv_data_t *value, int64_t expire_time) {
    if (!skip)  { printf("DEBUG: Error, skip is NULL!\n"); return -1; }
    if (!key)   { printf("DEBUG: Error, key is NULL!\n"); return -1; }
    if (!value) { printf("DEBUG: Error, value is NULL!\n"); return -1; }
    
    if (!skip || !skip->header || !key || !value) return -1;
    skipnode_binary_t *update[MAX_LEVEL + 1];
    skipnode_binary_t *current = skip->header;
    
    // 查找插入位置，记录每层的前驱
    for (int i = skip->level; i >= 0; i--) {
        while (current->forward[i] && 
               kv_data_compare(&current->forward[i]->key, key) < 0) {
            current = current->forward[i];
        }
        update[i] = current;
    }
    
    current = current->forward[0];
    
    // 检查 key 是否已存在
    if (current && kv_data_compare(&current->key, key) == 0) {
        return 1;  // 已存在
    }
    
    // 随机层数
    int level = random_level();
    if (level > skip->level) {
        for (int i = skip->level + 1; i <= level; i++) {
            update[i] = skip->header;
        }
        skip->level = level;
    }
    
    // 创建新节点（传入 expire_time）
    skipnode_binary_t *new_node = skipnode_create(level, key, value, expire_time);
    if (!new_node) return -2;
    
    // 插入各层
    for (int i = 0; i <= level; i++) {
        new_node->forward[i] = update[i]->forward[i];
        update[i]->forward[i] = new_node;
    }
    
    skip->count++;
    return 0;
}

// ⏱️ 修改：加入惰性删除拦截机制
kv_data_t* kvs_skip_get(kvs_skip_t *skip, kv_data_t *key) {
    if (!skip || !key) return NULL;
    
    skipnode_binary_t *current = skip->header;
    
    for (int i = skip->level; i >= 0; i--) {
        while (current->forward[i] && 
               kv_data_compare(&current->forward[i]->key, key) < 0) {
            current = current->forward[i];
        }
    }
    
    current = current->forward[0];
    
    if (current && kv_data_compare(&current->key, key) == 0) {
        // ⏱️ 拦截：检查该跳表节点是否已超时过期
        if (current->expire_time > 0 && get_current_ms_skip() > current->expire_time) {
            kvs_skip_del(skip, key); // 惰性删除
            return NULL;             // 对上层返回不存在
        }
        return &current->value;
    }
    
    return NULL;
}

// 原样输出
int kvs_skip_del(kvs_skip_t *skip, kv_data_t *key) {
    if (!skip || !key) return -1;
    
    skipnode_binary_t *update[MAX_LEVEL + 1];
    skipnode_binary_t *current = skip->header;
    
    for (int i = skip->level; i >= 0; i--) {
        while (current->forward[i] && 
               kv_data_compare(&current->forward[i]->key, key) < 0) {
            current = current->forward[i];
        }
        update[i] = current;
    }
    
    current = current->forward[0];
    
    if (!current || kv_data_compare(&current->key, key) != 0) {
        return 1;  // 不存在
    }
    
    for (int i = 0; i <= skip->level; i++) {
        if (update[i]->forward[i] != current) {
            break;
        }
        update[i]->forward[i] = current->forward[i];
    }
    
    skipnode_destroy(current);
    skip->count--;
    
    // 降低跳表高度
    while (skip->level > 0 && skip->header->forward[skip->level] == NULL) {
        skip->level--;
    }
    
    return 0;
}

// ⏱️ 修改：函数签名增加 expire_time 参数
int kvs_skip_mod(kvs_skip_t *skip, kv_data_t *key, kv_data_t *value, int64_t expire_time) {
    if (!skip || !key || !value) return -1;
    
    skipnode_binary_t *current = skip->header;
    
    for (int i = skip->level; i >= 0; i--) {
        while (current->forward[i] && 
               kv_data_compare(&current->forward[i]->key, key) < 0) {
            current = current->forward[i];
        }
    }
    
    current = current->forward[0];
    
    if (current && kv_data_compare(&current->key, key) == 0) {
        // 释放旧 value，深拷贝新 value
        kv_data_destroy(&current->value);
        if (kv_data_dup(&current->value, value) != 0) return -2;
        
        // ⏱️ 新增：重置/更新过期时间戳
        current->expire_time = expire_time;
        return 0;
    }
    
    return 1;  // 不存在
}

// 原样输出
int kvs_skip_exist(kvs_skip_t *skip, kv_data_t *key) {
    if (!skip || !key) return -1;
    
    // 内部通过调用带惰性删除机制的 get 来进行存在性判定
    kv_data_t *value = kvs_skip_get(skip, key);
    return (value != NULL) ? 0 : 1;
}

// 原样输出
void kvs_skip_foreach(kvs_skip_t *skip, void (*callback)(kv_data_t *key, kv_data_t *value, void *arg), void *arg) {
    if (!skip || !callback) return;
    
    skipnode_binary_t *node = skip->header->forward[0];
    while (node) {
        callback(&node->key, &node->value, arg);
        node = node->forward[0];
    }
}

// 原样输出
int kvs_skip_get_value_len(kvs_skip_t *skip, kv_data_t *key) {
    // 内部调用 get 自动处理过期拦截
    kv_data_t *value = kvs_skip_get(skip, key);
    return value ? (int)value->len : -1;
}