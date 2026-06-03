#include "kvstore.h"
#include <stdio.h>
#include <string.h>
#include <pthread.h>
#include <unistd.h>
#include <stdint.h>   
#include <sys/time.h> 
#include <stddef.h> // offsetof 宏

// 用于二进制快照的引擎标识
#define SNAP_TYPE_ARRAY    1
#define SNAP_TYPE_RBTREE   2
#define SNAP_TYPE_HASH     3
#define SNAP_TYPE_SKIPLIST 4

static int64_t get_current_ms_snapshot(void) {
    struct timeval tv;
    gettimeofday(&tv, NULL);
    return (int64_t)tv.tv_sec * 1000 + tv.tv_usec / 1000;
}

// 记录array的键值对，写进快照
#if ENABLE_ARRAY
static void snapshot_write_array_cb(kv_data_t *key, kv_data_t *value, void *arg) {
    FILE *fp = (FILE*)arg;
    int type = SNAP_TYPE_ARRAY;
    
    extern kvs_array_t global_array;
    int64_t expire_time = 0;
    //线性查找当前键值对的过期时间
    for(int i=0; i<global_array.idx; i++) {
        if(kv_data_compare(&global_array.table[i].key, key) == 0) {
            expire_time = global_array.table[i].expire_time;
            break;
        }
    }

    fwrite(&type, sizeof(int), 1, fp);// 写入存储结构类型
    fwrite(&expire_time, sizeof(int64_t), 1, fp); // 写入8字节过期时间
    fwrite(&key->len, sizeof(int), 1, fp);
    fwrite(key->data, 1, key->len, fp);
    fwrite(&value->len, sizeof(int), 1, fp);
    fwrite(value->data, 1, value->len, fp);
}
#endif 
#if ENABLE_RBTREE
static void snapshot_write_rbtree_cb(kv_data_t *key, kv_data_t *value, void *arg) {
    FILE *fp = (FILE*)arg;
    int type = SNAP_TYPE_RBTREE;
    
    // 通过 key 的指针，反推回输入节点的首地址
    rbtree_node_binary_t *node = (rbtree_node_binary_t *)((char *)key - offsetof(rbtree_node_binary_t, key));
    
    // 获取真实节点的过期时间
    int64_t expire_time = node->expire_time; 

    // 顺序写入快照二进制文件
    fwrite(&type, sizeof(int), 1, fp);
    fwrite(&expire_time, sizeof(int64_t), 1, fp); 
    fwrite(&key->len, sizeof(int), 1, fp);
    fwrite(key->data, 1, key->len, fp);
    fwrite(&value->len, sizeof(int), 1, fp);
    fwrite(value->data, 1, value->len, fp);
}
#endif 
#if ENABLE_HASH
static void snapshot_write_hash_cb(kv_data_t *key, kv_data_t *value, void *arg) {
    FILE *fp = (FILE*)arg;
    int type = SNAP_TYPE_HASH;
    
    int64_t expire_time = 0;
    extern kvs_hash_t global_hash;
    unsigned long slot = kv_data_hash_func(key, global_hash.max_slots);
    hashnode_t *node = global_hash.buckets[slot];
    while (node) {
        if (kv_data_compare(&node->key, key) == 0) {
            expire_time = node->expire_time;
            break;
        }
        node = node->next;
    }

    fwrite(&type, sizeof(int), 1, fp);
    fwrite(&expire_time, sizeof(int64_t), 1, fp); 
    fwrite(&key->len, sizeof(int), 1, fp);
    fwrite(key->data, 1, key->len, fp);
    fwrite(&value->len, sizeof(int), 1, fp);
    fwrite(value->data, 1, value->len, fp);
}
#endif 
#if ENABLE_SKIPLIST
static void snapshot_write_skip_cb(kv_data_t *key, kv_data_t *value, void *arg) {
    FILE *fp = (FILE*)arg;
    int type = SNAP_TYPE_SKIPLIST; 

    skipnode_binary_t *node = (skipnode_binary_t *)((char *)key - offsetof(skipnode_binary_t, key));
    int64_t expire_time = node->expire_time;

    fwrite(&type, sizeof(int), 1, fp);
    fwrite(&expire_time, sizeof(int64_t), 1, fp); 
    fwrite(&key->len, sizeof(int), 1, fp);
    fwrite(key->data, 1, key->len, fp);
    fwrite(&value->len, sizeof(int), 1, fp);
    fwrite(value->data, 1, value->len, fp);
}
#endif 
// 保存二进制快照：把存储结构的节点写进快照
int kvs_snapshot_save(void) {
    FILE *fp = fopen("kvstore.snap", "wb");
    if (!fp) return -1;
    
    #if ENABLE_ARRAY
        extern kvs_array_t global_array;
        kvs_array_foreach(&global_array, snapshot_write_array_cb, fp);
    #endif
    
    #if ENABLE_RBTREE
        extern kvs_rbtree_t global_rbtree;
        kvs_rbtree_foreach(&global_rbtree, snapshot_write_rbtree_cb, fp);
    #endif
    
    #if ENABLE_HASH
        extern kvs_hash_t global_hash;
        kvs_hash_foreach(&global_hash, snapshot_write_hash_cb, fp);
    #endif
    
    #if ENABLE_SKIPLIST
        extern kvs_skip_t global_skip;
        kvs_skip_foreach(&global_skip, snapshot_write_skip_cb, fp);
    #endif
    
    fclose(fp);
    return 0;
}

//从快照文件读取数据，恢复到存储结构中
int kvs_snapshot_load(void) {

    FILE *fp = fopen("kvstore.snap", "rb");
    if (!fp) {
        printf("No snapshot file found, starting fresh\n");
        return -1; 
    }
    
    int loaded_count = 0;
    int expired_cleanup_count = 0; 
    int type, key_len, val_len;
    int64_t expire_time;          
    int64_t now = get_current_ms_snapshot(); 
    
  
    while (fread(&type, sizeof(int), 1, fp) == 1) {
        
        
        if (fread(&expire_time, sizeof(int64_t), 1, fp) != 1) break;

        // 读 Key 长度
        if (fread(&key_len, sizeof(int), 1, fp) != 1) break;
        // 动态分配 Key 缓冲区
        void *k_buf = kvs_malloc((key_len + 7) & ~7);
        if (!k_buf || fread(k_buf, 1, key_len, fp) != key_len) {
            if (k_buf) kvs_free(k_buf);
            break;
        }
        
        // 读 Value 长度
        if (fread(&val_len, sizeof(int), 1, fp) != 1) {
            kvs_free(k_buf); 
            break; 
        }
        // 动态分配 Value 缓冲区
        void *v_buf = kvs_malloc((val_len + 7) & ~7);
        if (!v_buf || fread(v_buf, 1, val_len, fp) != val_len) {
            kvs_free(k_buf);
            if (v_buf) kvs_free(v_buf);
            break;
        }
        
        // 过期释放
        if (expire_time > 0 && now > expire_time) {
            kvs_free(k_buf);
            kvs_free(v_buf);
            expired_cleanup_count++;
            continue; 
        }

        // 组装统一的 kv_data_t 结构体
        kv_data_t kv_k = { .data = k_buf, .len = key_len };
        kv_data_t kv_v = { .data = v_buf, .len = val_len };
        
        // 分发到对应的存储引擎
        if (type == SNAP_TYPE_ARRAY) {
            #if ENABLE_ARRAY
            extern kvs_array_t global_array;
            kvs_array_set(&global_array, &kv_k, &kv_v, expire_time);
            #endif
        } 
        else if (type == SNAP_TYPE_RBTREE) {
            #if ENABLE_RBTREE
            extern kvs_rbtree_t global_rbtree;
            kvs_rbtree_set(&global_rbtree, &kv_k, &kv_v, expire_time);
            #endif
        } 
        else if (type == SNAP_TYPE_HASH) {
            #if ENABLE_HASH
            extern kvs_hash_t global_hash;
            kvs_hash_set(&global_hash, &kv_k, &kv_v, expire_time);
            #endif
        } 
        else if (type == SNAP_TYPE_SKIPLIST) {
            #if ENABLE_SKIPLIST
            extern kvs_skip_t global_skip;
            kvs_skip_set(&global_skip, &kv_k, &kv_v, expire_time);
            #endif
        }
        
        kvs_free(k_buf);
        kvs_free(v_buf);
        loaded_count++;
    }
    
    fclose(fp);
    printf("Snapshot loaded: %d entries (Purged %d expired entries on startup)\n", loaded_count, expired_cleanup_count);
    return 0;
}
