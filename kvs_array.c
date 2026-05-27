// kvs_array.c
// 二进制安全版本的 Array 存储引擎

#include "kvstore.h"
#include <string.h>
#include <sys/time.h> 

kvs_array_t global_array = {0};

static int64_t get_current_ms_array(void) {
    struct timeval tv;
    gettimeofday(&tv, NULL);
    return (int64_t)tv.tv_sec * 1000 + tv.tv_usec / 1000;
}


int kvs_array_create(kvs_array_t *inst) {
    if (!inst) {
        return -1;
    }
    if (inst->table) {
        printf("table has alloc\n");
        return -1;
    }
    
    inst->table = kvs_malloc(KVS_ARRAY_SIZE * sizeof(kvs_array_item_t));
    if (!inst->table) return -1;
    
    memset(inst->table, 0, KVS_ARRAY_SIZE * sizeof(kvs_array_item_t));
    inst->total = 0;
    inst->idx = 0;
    
    return 0;
}


void kvs_array_destroy(kvs_array_t *inst) {
    if (!inst) return;
    
    if (inst->table) {
        // 释放每个槽位的 key 和 value
        for (int i = 0; i < inst->total; i++) {
            if (inst->table[i].key.data) {
                kv_data_destroy(&inst->table[i].key);
            }
            if (inst->table[i].value.data) {
                kv_data_destroy(&inst->table[i].value);
            }
        }
        kvs_free(inst->table);
        inst->table = NULL;
    }
    
    inst->total = 0;
    inst->idx = 0;
}

static int find_key_index(kvs_array_t *inst, kv_data_t *key) {
    for (int i = 0; i < inst->total; i++) {
        if (inst->table[i].key.data && kv_data_compare(&inst->table[i].key, key) == 0) {
            return i;
        }
    }
    return -1;
}

// 函数签名增加 expire_time，并移除原代码里不必要的空洞查询循环
int kvs_array_set(kvs_array_t *inst, kv_data_t *key, kv_data_t *value, int64_t expire_time) {
    if (!inst || !key || !value) return -1;
    if (inst->total >= KVS_ARRAY_SIZE) return -1;
    
    // 检查是否已存在
    int idx = find_key_index(inst, key);
    if (idx != -1) {
        return 1;  // 已存在
    }
    
    // 由于 del 保证了紧凑性，新元素直接追加在 inst->total 位置
    idx = inst->total;
    
    // 深拷贝 key 和 value
    if (kv_data_dup(&inst->table[idx].key, key) != 0) return -2;
    if (kv_data_dup(&inst->table[idx].value, value) != 0) {
        kv_data_destroy(&inst->table[idx].key);
        return -2;
    }
    
    inst->table[idx].expire_time = expire_time;
    
    inst->total++;
    inst->idx = inst->total; // 保持 idx 与 total 同步更新
    
    return 0;
}

// 增加惰性删除机制
kv_data_t* kvs_array_get(kvs_array_t *inst, kv_data_t *key) {
    if (!inst || !key) return NULL;
    
    int idx = find_key_index(inst, key);
    if (idx == -1) return NULL;
    
    if (inst->table[idx].expire_time > 0 && get_current_ms_array() > inst->table[idx].expire_time) {
        kvs_array_del(inst, key); // 惰性删除：当场剔除
        return NULL;              // 对上层假装不存在
    }
    
    return &inst->table[idx].value;
}


int kvs_array_del(kvs_array_t *inst, kv_data_t *key) {
    if (!inst || !key) return -1;
    
    int idx = find_key_index(inst, key);
    if (idx == -1) return 1;  // 不存在
    
    // 释放内存
    kv_data_destroy(&inst->table[idx].key);
    kv_data_destroy(&inst->table[idx].value);
    
    // 将最后一个元素移到当前位置（避免空洞）
    if (idx != inst->total - 1) {
        inst->table[idx] = inst->table[inst->total - 1];
        memset(&inst->table[inst->total - 1], 0, sizeof(kvs_array_item_t));
    } else {
        memset(&inst->table[idx], 0, sizeof(kvs_array_item_t));
    }
    
    inst->total--;
    inst->idx = inst->total;
    return 0;
}

// 函数签名增加 expire_time
int kvs_array_mod(kvs_array_t *inst, kv_data_t *key, kv_data_t *value, int64_t expire_time) {
    if (!inst || !key || !value) return -1;
    
    int idx = find_key_index(inst, key);
    if (idx == -1) return 1;  // 不存在
    
    // 释放旧 value，拷贝新 value
    kv_data_destroy(&inst->table[idx].value);
    if (kv_data_dup(&inst->table[idx].value, value) != 0) return -2;
    
    inst->table[idx].expire_time = expire_time;
    
    return 0;
}


int kvs_array_exist(kvs_array_t *inst, kv_data_t *key) {
    if (!inst || !key) return -1;
    
    // 这里内部调用修改后的 get 间接查找，可以顺便触发惰性删除逻辑
    kv_data_t *res = kvs_array_get(inst, key);
    return (res == NULL) ? 1 : 0;
}


int kvs_array_get_value_len(char *key_ptr, int key_len) {
    if (!key_ptr || key_len <= 0) return 0;

    // 1. 临时在栈上构建一个临时的 key 结构体（不需要分配内存，只是指针引用）
    kv_data_t tmp_key;
    tmp_key.data = key_ptr;
    tmp_key.len = key_len;

    // 2. 直接调用你原有的查询函数查找
    kv_data_t *res_val = kvs_array_get(&global_array, &tmp_key);
    if (res_val && res_val->data) {
        // 3. 找到了，直接返回服务器里这张图片/数据的实际字节数
        return res_val->len;
    }

    return 0; // 没找到返回 0
}


void kvs_array_foreach(kvs_array_t *inst, void (*callback)(kv_data_t *key, kv_data_t *value, void *arg), void *arg) {
    if (!inst || !callback || !inst->table) return;
    
    // 因为你的 del 保证了数组的紧凑性，直接线性遍历前 total 个元素即可
    for (int i = 0; i < inst->total; i++) {
        // 安全防御：确保该槽位确实存有有效的二进制 key 数据
        if (inst->table[i].key.data) {
            callback(&inst->table[i].key, &inst->table[i].value, arg);
        }
    }
}