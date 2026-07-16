#include "kvstore.h"
#include <string.h>
#include <stdlib.h>
#include <stdint.h>   
#include <sys/time.h> 
#include <unistd.h>

#if ENABLE_SKIPLIST


// 全局跳表实例
kvs_skip_t global_skip = {0};

static int64_t get_current_ms_skip(void) {
    struct timeval tv;
    gettimeofday(&tv, NULL);
    return (int64_t)tv.tv_sec * 1000 + tv.tv_usec / 1000;
}

// 修复1: 使用正确的概率分布 (1/4 晋升概率)
static int random_level(void) {
    int level = 0;
    // 使用位运算优化随机数生成
    // P(level >= k) = (1/4)^k
    while ((rand() & 0xFFFF) < (0xFFFF >> 2) && level < MAX_LEVEL) {
        level++;
    }
    return level;
}

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
    
    node->expire_time = expire_time;
    
    // 分配 forward 指针数组
    node->forward = (skipnode_binary_t**)kvs_malloc((level + 1) * sizeof(skipnode_binary_t*));
    if (!node->forward) {
        kv_data_destroy(&node->key);
        kv_data_destroy(&node->value);
        kvs_free(node);
        return NULL;
    }
    
    // 初始化所有forward指针为NULL
    for (int i = 0; i <= level; i++) {
        node->forward[i] = NULL;
    }
    
    return node;
}

int kvs_skip_create(kvs_skip_t *skip) {
    if (!skip) {
        printf("ERROR: kvs_skip_create - skip is NULL\n");
        return -1;
    }
    
    // 修复2: 初始化随机种子
    static int seeded = 0;
    if (!seeded) {
        srand((unsigned int)time(NULL) ^ (unsigned int)getpid());
        seeded = 1;
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

static void skipnode_destroy(skipnode_binary_t *node) {
    if (!node) return;
    
    kv_data_destroy(&node->key);
    kv_data_destroy(&node->value);
    if (node->forward) {
        kvs_free(node->forward);
    }
    kvs_free(node);
}

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

// 修复3: 提取公共查找函数，避免代码重复
static void skip_find_update(kvs_skip_t *skip, kv_data_t *key, 
                             skipnode_binary_t *update[MAX_LEVEL + 1],
                             skipnode_binary_t **found_node) {
    skipnode_binary_t *current = skip->header;
    
    for (int i = skip->level; i >= 0; i--) {
        while (current->forward[i] && 
               kv_data_compare(&current->forward[i]->key, key) < 0) {
            current = current->forward[i];
        }
        update[i] = current;
    }
    
    current = current->forward[0];
    
    if (found_node) {
        if (current && kv_data_compare(&current->key, key) == 0) {
            *found_node = current;
        } else {
            *found_node = NULL;
        }
    }
}

// 修复4: 处理过期节点的辅助函数
static int handle_expired_node(kvs_skip_t *skip, skipnode_binary_t *node,
                               skipnode_binary_t *update[MAX_LEVEL + 1]) {
    if (!node || node->expire_time <= 0) return 0;
    
    int64_t now = get_current_ms_skip();
    if (now <= node->expire_time) return 0;
    
    // 删除过期节点，使用已有的update数组
    for (int i = 0; i <= skip->level; i++) {
        if (update[i]->forward[i] == node) {
            update[i]->forward[i] = node->forward[i];
        }
    }
    
    skipnode_destroy(node);
    skip->count--;
    
    // 调整跳表高度
    while (skip->level > 0 && skip->header->forward[skip->level] == NULL) {
        skip->level--;
    }
    
    return 1;  // 表示节点已被删除
}

int kvs_skip_set(kvs_skip_t *skip, kv_data_t *key, kv_data_t *value, int64_t expire_time) {
    if (!skip || !skip->header || !key || !value) return -1;
    
    skipnode_binary_t *update[MAX_LEVEL + 1];
    skipnode_binary_t *current;
    
    // 使用公共查找函数
    skip_find_update(skip, key, update, &current);
    
    // 检查 key 是否已存在
    if (current) {
        // 处理过期节点
        if (handle_expired_node(skip, current, update)) {
            // 节点已删除，继续插入新节点
            current = NULL;
        } else {
            // 健康节点，更新值
            kv_data_destroy(&current->value);
            if (kv_data_dup(&current->value, value) != 0) return -2;
            current->expire_time = expire_time;
            return 1;  // 更新成功
        }
    }
    
    // 插入新节点
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

kv_data_t* kvs_skip_get(kvs_skip_t *skip, kv_data_t *key) {
    if (!skip || !skip->header || !key) return NULL;
    
    skipnode_binary_t *update[MAX_LEVEL + 1];
    skipnode_binary_t *current;
    
    // 使用公共查找函数
    skip_find_update(skip, key, update, &current);
    
    if (current) {
        // 检查是否过期
        if (current->expire_time > 0 && get_current_ms_skip() > current->expire_time) {
            // 惰性删除过期节点
            handle_expired_node(skip, current, update);
            return NULL;
        }
        return &current->value;
    }
    
    return NULL;
}

int kvs_skip_del(kvs_skip_t *skip, kv_data_t *key) {
    if (!skip || !skip->header || !key) return -1;
    
    skipnode_binary_t *update[MAX_LEVEL + 1];
    skipnode_binary_t *current;
    
    // 使用公共查找函数
    skip_find_update(skip, key, update, &current);
    
    if (!current) {
        return 1;  // 不存在
    }
    
    // 修复5: 正确的断链逻辑 - 不提前break
    for (int i = 0; i <= skip->level; i++) {
        if (update[i]->forward[i] == current) {
            update[i]->forward[i] = current->forward[i];
        }
        // 继续检查更高层，不要break
    }
    
    skipnode_destroy(current);
    skip->count--;
    
    // 降低跳表高度
    while (skip->level > 0 && skip->header->forward[skip->level] == NULL) {
        skip->level--;
    }
    
    return 0;
}

int kvs_skip_mod(kvs_skip_t *skip, kv_data_t *key, kv_data_t *value, int64_t expire_time) {
    if (!skip || !skip->header || !key || !value) return -1;
    
    // 修复6: 复用查找逻辑
    skipnode_binary_t *update[MAX_LEVEL + 1];
    skipnode_binary_t *current;
    
    skip_find_update(skip, key, update, &current);
    
    if (current) {
        // 处理过期节点
        if (current->expire_time > 0 && get_current_ms_skip() > current->expire_time) {
            handle_expired_node(skip, current, update);
            return 1;  // 键不存在（已过期）
        }
        
        // 释放旧 value，深拷贝新 value
        kv_data_destroy(&current->value);
        if (kv_data_dup(&current->value, value) != 0) return -2;
        
        current->expire_time = expire_time;
        return 0;
    }
    
    return 1;  // 不存在
}

int kvs_skip_exist(kvs_skip_t *skip, kv_data_t *key) {
    if (!skip || !key) return -1;
    
    // 内部通过调用带惰性删除机制的 get 来进行存在性判定
    kv_data_t *value = kvs_skip_get(skip, key);
    return (value != NULL) ? 0 : 1;
}

// 修复7: 添加批量清理过期节点函数
int kvs_skip_cleanup(kvs_skip_t *skip) {
    if (!skip || !skip->header) return -1;
    
    int64_t now = get_current_ms_skip();
    int removed = 0;
    skipnode_binary_t *node = skip->header->forward[0];
    skipnode_binary_t *next;
    
    while (node) {
        next = node->forward[0];
        if (node->expire_time > 0 && now > node->expire_time) {
            // 直接调用del，它会处理所有层的指针
            kvs_skip_del(skip, &node->key);
            removed++;
        }
        node = next;
    }
    
    return removed;
}

void kvs_skip_foreach(kvs_skip_t *skip, void (*callback)(kv_data_t *key, kv_data_t *value, void *arg), void *arg) {
    if (!skip || !callback) return;
    
    int64_t now = get_current_ms_skip();
    skipnode_binary_t *node = skip->header->forward[0];
    skipnode_binary_t *next;
    
    while (node) {
        next = node->forward[0];
        
        // 跳过过期节点（但不删除，避免影响遍历）
        if (node->expire_time > 0 && now > node->expire_time) {
            node = next;
            continue;
        }
        
        callback(&node->key, &node->value, arg);
        node = next;
    }
}

int kvs_skip_get_value_len(kvs_skip_t *skip, kv_data_t *key) {
    // 内部调用 get 自动处理过期拦截
    kv_data_t *value = kvs_skip_get(skip, key);
    return value ? (int)value->len : -1;
}

// 修复8: 添加获取跳表统计信息函数
void kvs_skip_stats(kvs_skip_t *skip, int *count, int *level) {
    if (count) *count = skip ? skip->count : 0;
    if (level) *level = skip ? skip->level : 0;
}

// 修复9: 添加批量删除函数，优化清空操作
int kvs_skip_clear(kvs_skip_t *skip) {
    if (!skip || !skip->header) return -1;
    
    skipnode_binary_t *current = skip->header->forward[0];
    skipnode_binary_t *next;
    
    // 删除所有数据节点
    while (current) {
        next = current->forward[0];
        skipnode_destroy(current);
        current = next;
    }
    
    // 重置header的forward指针
    for (int i = 0; i <= MAX_LEVEL; i++) {
        skip->header->forward[i] = NULL;
    }
    
    skip->level = 0;
    skip->count = 0;
    
    return 0;
}

#endif