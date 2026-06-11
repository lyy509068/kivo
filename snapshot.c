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

#if ENABLE_ARRAY
static void snapshot_write_array_cb(kv_data_t *key, kv_data_t *value, void *arg) {
    FILE *fp = (FILE*)arg;
    int type = SNAP_TYPE_ARRAY;
    
    // ✨ 放弃 for 循环！利用内存偏移直接反推当前 array_item 的首地址来拿 expire_time
    kvs_array_item_t *item = (kvs_array_item_t *)((char *)key - offsetof(kvs_array_item_t, key));
    int64_t expire_time = item->expire_time;

    fwrite(&type, sizeof(int), 1, fp);
    fwrite(&expire_time, sizeof(int64_t), 1, fp); 
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
    
    // 精准反推，无循环
    rbtree_node_binary_t *node = (rbtree_node_binary_t *)((char *)key - offsetof(rbtree_node_binary_t, key));
    int64_t expire_time = node->expire_time; 

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
    
    // ✨ 放弃 while 循环！既然 key 是 hashnode_t 内部的成员，直接反推拿到当前节点！
    hashnode_t *node = (hashnode_t *)((char *)key - offsetof(hashnode_t, key));
    int64_t expire_time = node->expire_time;

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

    // 精准反推，无循环
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
    #endif
    #if ENABLE_RBTREE
    extern kvs_rbtree_t global_rbtree;
    #endif
    #if ENABLE_HASH
    extern kvs_hash_t global_hash;
    #endif
    #if ENABLE_SKIPLIST
    extern kvs_skip_t global_skip;
    #endif

    // 1. 只有 Array 真的有数据时，才允许调用 Array 的遍历和写入
    #if ENABLE_ARRAY
    if (global_array.total > 0 && global_array.table != NULL) {
        kvs_array_foreach(&global_array, snapshot_write_array_cb, fp);
    }
    #endif

    // 2. 只有红黑树真的有节点时，才允许进入
    #if ENABLE_RBTREE
    // 假设你的 rbtree 结构体里有 count 属性
    if (global_rbtree.nil != NULL && global_rbtree.root != global_rbtree.nil) {
        kvs_rbtree_foreach(&global_rbtree, snapshot_write_rbtree_cb, fp);
    }
    #endif

    // 3. 只有 Hash 真的有数据时，才调用 Hash 写入
    #if ENABLE_HASH
    if (global_hash.count > 0) {
        kvs_hash_foreach(&global_hash, snapshot_write_hash_cb, fp);
    }
    #endif

    // 4. 只有跳表真的有数据时，才调用跳表写入
    #if ENABLE_SKIPLIST
    if (global_skip.count > 0 && global_skip.header->forward[0] != NULL) {
        kvs_skip_foreach(&global_skip, snapshot_write_skip_cb, fp);
    }
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
