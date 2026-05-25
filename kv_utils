// kv_utils.c
#include "kvstore.h"
#include <string.h>

// 1. 初始化/创建（移除 const，与 kvstore.h 的 void *src 严格对齐）
int kv_data_create(kv_data_t *data, void *src, size_t len) {
    if (!data) return -1;
    
    // 强制安全初始化，杜绝任何随机残留野值
    data->data = NULL;
    data->len = 0;
    
    if (len == 0) {
        return 0; // 有效空数据
    }
    
    if (!src) return -1;
    
    // 物理深拷贝：多分配 1 字节用于 '\0' 影子边界防御
    data->data = kvs_malloc(len + 1);
    if (!data->data) return -1;
    
    memcpy(data->data, src, len);
    
    // 通过临时指针强转，优雅绕过可修改左开销限制，强行封口
    char *p_modify = (char *)data->data;
    p_modify[len] = '\0'; 
    
    data->len = len;
    
    return 0;
}

// 2. 释放（保持不变，铁腕清除残渣）
void kv_data_destroy(kv_data_t *data) {
    if (!data) return;
    
    if (data->data) {
        kvs_free(data->data);
    }
    
    // 彻底抹平成绝对安全的纯零状态，防御任何后继的二次释放（Double Free）
    data->data = NULL;
    data->len = 0;
}

// 3. 深拷贝（移除 const，与 kvstore.h 的 kv_data_t *src 严格对齐）
int kv_data_dup(kv_data_t *dst, kv_data_t *src) {
    if (!dst) return -1;
    
    // 如果源对象本身就为空，或者内部指针无效，直接让目标对象安全初始化为空
    if (!src || src->len == 0 || !src->data) {
        dst->data = NULL;
        dst->len = 0;
        return 0;
    }
    
    return kv_data_create(dst, src->data, src->len);
}

// 比较两个 kv_data_t（二进制安全：先比长度，再比内容）
int kv_data_compare(kv_data_t *a, kv_data_t *b) {
    if (!a || !b) return 0; // 严谨起见增加安全防御
    if (a->len != b->len) {
        return (a->len > b->len) ? 1 : -1; // 修正直接相减可能导致的 int 溢出隐患
    }
    return memcmp(a->data, b->data, a->len);
}

// 哈希计算（DJB2 算法，支持二进制数据）
unsigned long kv_data_hash_func(kv_data_t *key, int size) {
    if (!key || !key->data || size <= 0) return 0;
    unsigned long hash = 5381;
    unsigned char *p = (unsigned char*)key->data;
    
    for (size_t i = 0; i < key->len; i++) {
        hash = ((hash << 5) + hash) + p[i];
    }
    return hash % size;
}

// 兼容旧命名的包裹函数 
int kv_data_cmp(kv_data_t *a, kv_data_t *b) {
    return kv_data_compare(a, b);
}

unsigned long kv_data_hash(kv_data_t *key, int size) {
    return kv_data_hash_func(key, size);
}

void kv_data_free(kv_data_t *data) {
    kv_data_destroy(data);
}

