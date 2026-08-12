// kv_utils.c
#include "kvstore.h"
#include <string.h>

// ============ 优化的哈希核心 ============
static inline uint64_t kv_hash_short(const void *key, size_t len) {
    if (len <= 16) {
        uint64_t h = 0;
        if (len <= 8) {
            uint64_t v = 0;
            memcpy(&v, key, len);
            h = v * 0x9e3779b97f4a7c15ULL;
        } else {
            uint64_t v1 = 0, v2 = 0;
            memcpy(&v1, key, 8);
            memcpy(&v2, (char*)key + len - 8, 8);
            h = (v1 * 0x9e3779b97f4a7c15ULL) ^ 
                (v2 * 0x85ebca6b3a6b3f4eULL);
        }
        h ^= h >> 33;
        h *= 0xff51afd7ed558ccdULL;
        h ^= h >> 33;
        h *= 0xc4ceb9fe1a85ec53ULL;
        h ^= h >> 33;
        return h;
    }
    
    // MurmurHash3 风格（安全非对齐读取）
    const uint64_t m = 0xc6a4a7935bd1e995ULL;
    const int r = 47;
    uint64_t h = 0x9e3779b97f4a7c15ULL ^ (len * m);
    
    const char *ptr = (const char *)key;
    const char *end = ptr + (len & ~7ULL);
    
    while (ptr != end) {
        uint64_t k;
        memcpy(&k, ptr, 8);
        ptr += 8;

        k *= m;
        k ^= k >> r;
        k *= m;
        h ^= k;
        h *= m;
    }

    const unsigned char *d = (const unsigned char *)ptr;
    switch (len & 7) {
        case 7: h ^= (uint64_t)d[6] << 48;
        case 6: h ^= (uint64_t)d[5] << 40;
        case 5: h ^= (uint64_t)d[4] << 32;
        case 4: h ^= (uint64_t)d[3] << 24;
        case 3: h ^= (uint64_t)d[2] << 16;
        case 2: h ^= (uint64_t)d[1] << 8;
        case 1: h ^= (uint64_t)d[0];
            h *= m;
    }
    h ^= h >> r;
    h *= m;
    h ^= h >> r;
    return h;
}

static inline uint64_t kv_data_hash_calc(kv_data_t *key) {
    if (key->hash_cache != 0) return key->hash_cache;
    uint64_t hash = kv_hash_short(key->data, key->len);
    key->hash_cache = hash;
    return hash;
}

// ============ 原有接口 ============
int kv_data_create(kv_data_t *data, void *src, size_t len) {
    if (!data) return -1;
    data->data = NULL;
    data->len = 0;
    data->hash_cache = 0;
    if (len == 0) return 0;
    if (!src) return -1;
    data->data = kvs_malloc(len + 1);
    if (!data->data) return -1;
    memcpy(data->data, src, len);
    ((char*)data->data)[len] = '\0';
    data->len = len;
    return 0;
}

void kv_data_destroy(kv_data_t *data) {
    if (!data) return;
    if (data->data) kvs_free(data->data);
    data->data = NULL;
    data->len = 0;
    data->hash_cache = 0;
}

int kv_data_dup(kv_data_t *dst, kv_data_t *src) {
    if (!dst) return -1;
    if (!src || src->len == 0 || !src->data) {
        dst->data = NULL;
        dst->len = 0;
        dst->hash_cache = 0;
        return 0;
    }
    int ret = kv_data_create(dst, src->data, src->len);
    if (ret == 0) {
        dst->hash_cache = src->hash_cache;
    }
    return ret;
}

int kv_data_equals(kv_data_t *a, kv_data_t *b) {
    if (a == b || (a->data == b->data && a->len == b->len)) return 1;
    if (a->len != b->len) return 0;
    return memcmp(a->data, b->data, a->len) == 0;
}

int kv_data_compare(kv_data_t *a, kv_data_t *b) {
    if (a == b || (a->data == b->data && a->len == b->len)) return 0;
    size_t min_len = (a->len < b->len) ? a->len : b->len;
    int cmp = memcmp(a->data, b->data, min_len);
    if (cmp != 0) return cmp;
    return (a->len < b->len) ? -1 : (a->len > b->len) ? 1 : 0;
}

unsigned long kv_data_hash(kv_data_t *key, int size) {
    if (!key || !key->data || size <= 0) return 0;
    return (unsigned long)(kv_data_hash_calc(key) & (uint64_t)(size - 1));
}

void kv_data_free(kv_data_t *data) {
    kv_data_destroy(data);
}