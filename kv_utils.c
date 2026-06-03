// kv_utils.c
#include "kvstore.h"
#include <string.h>

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

void kv_data_destroy(kv_data_t *data) {
    if (!data) return;
    
    if (data->data) {
        kvs_free(data->data);
    }
    
    data->data = NULL;
    data->len = 0;
}

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

int kv_data_compare(const kv_data_t *a, const kv_data_t *b) {
    // 1. 安全拦截
    if (!a || !b) {
        return (a == b) ? 0 : (!a ? -1 : 1);
    }
    
    //printf("[CMP_TRACE] Comparing A: %.*s (len:%zu) with B: %.*s (len:%zu)\n", (int)a->len, (char *)a->data, a->len, (int)b->len, (char *)b->data, b->len);

    if (!a->data && !b->data) return 0;
    if (!a->data) return -1;
    if (!b->data) return 1;

    // 2. 取两个 Key 的最小共同长度进行 memcmp 比较
    size_t min_len = (a->len < b->len) ? a->len : b->len;
    int cmp = memcmp(a->data, b->data, min_len);
    
    // 3. 如果在共同长度内内容就已经不同，直接返回相对大小
    if (cmp != 0) {
        return cmp; 
    }
    
    // 4. 如果共同前缀完全一致，谁短谁小
    if (a->len < b->len) return -1;
    if (a->len > b->len) return 1;
    
    // 5. 长度和内容完全一致，返回 0
    return 0;
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

