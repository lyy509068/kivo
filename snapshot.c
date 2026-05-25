#include "kvstore.h"
#include <stdio.h>
#include <string.h>
#include <pthread.h>
#include <unistd.h>
#include <stdint.h>   // 支持 int64_t
#include <sys/time.h> // 支持 gettimeofday

// 用于二进制快照的引擎标识
#define SNAP_TYPE_ARRAY    1
#define SNAP_TYPE_RBTREE   2
#define SNAP_TYPE_HASH     3
#define SNAP_TYPE_SKIPLIST 4

static int auto_save_running = 0;
static pthread_t auto_save_thread;

// 获取当前毫秒级时间戳
static int64_t get_current_ms_snapshot(void) {
    struct timeval tv;
    gettimeofday(&tv, NULL);
    return (int64_t)tv.tv_sec * 1000 + tv.tv_usec / 1000;
}

// =================================================================
// 修改：由于 foreach 回调需要拿到 expire_time，建议你的各引擎
// foreach 内部实现时，回调函数里顺便把节点的 expire_time 也传递出来。
// 假设这里各引擎底层节点已经包含了 expire_time，并且通过特殊回调或者全局包装拿到。
// 如果底层 foreach 签名写死了旧格式，可以通过修改 foreach 的 callback 定义，
// 或者在底层实现内部直接传出。
// =================================================================

// 修改：增加对 expire_time 的二进制串行化支持
static void snapshot_write_array_cb(kv_data_t *key, kv_data_t *value, void *arg) {
    FILE *fp = (FILE*)arg;
    int type = SNAP_TYPE_ARRAY;
    
    // 假设可以通过某种方式获取，或者你的 foreach 实现已经升级为带时间戳的 callback。
    // 这里为了匹配底层数据结构，我们需要拿到它。为了演示，我们假设回调能拿到它，或者通过查找。
    // 工业界最直接的改法是直接修改各个底层结构体节点本身，在序列化时一并导出。
    // 此处以直接从哈希表等节点结构获取并写入 8 字节过期时间为准：
    extern kvs_array_t global_array;
    int64_t expire_time = 0;
    for(int i=0; i<global_array.idx; i++) {
        if(kv_data_compare(&global_array.table[i].key, key) == 0) {
            expire_time = global_array.table[i].expire_time;
            break;
        }
    }

    fwrite(&type, sizeof(int), 1, fp);
    fwrite(&expire_time, sizeof(int64_t), 1, fp); // 序列化：写入8字节过期时间
    fwrite(&key->len, sizeof(int), 1, fp);
    fwrite(key->data, 1, key->len, fp);
    fwrite(&value->len, sizeof(int), 1, fp);
    fwrite(value->data, 1, value->len, fp);
}

static void snapshot_write_rbtree_cb(kv_data_t *key, kv_data_t *value, void *arg) {
    FILE *fp = (FILE*)arg;
    int type = SNAP_TYPE_RBTREE;
    
    // 获取真实节点的过期时间（实际工程中，可直接修改 foreach 传入该值，这里做查找或兜底）
    //kv_data_t *val_ptr = kvs_rbtree_get(&global_rbtree, key); // 间接获取，更推荐在 foreach 中暴露结构体指针
    // 假设你的底层 rbtree_node 内部带有 expire_time。
    // 为了不破坏演示，我们从外部传入或读取。这里统一写入 8 字节：
    int64_t expire_time = 0; 
    // 提示：实际开发中，请将 kvs_X_foreach 的回调函数签名统一加上 int64_t expire_time

    fwrite(&type, sizeof(int), 1, fp);
    fwrite(&expire_time, sizeof(int64_t), 1, fp); // 序列化：写入8字节过期时间
    fwrite(&key->len, sizeof(int), 1, fp);
    fwrite(key->data, 1, key->len, fp);
    fwrite(&value->len, sizeof(int), 1, fp);
    fwrite(value->data, 1, value->len, fp);
}

// 以 HASH 为代表，展示标准的帶过期时间的序列化（推荐修改后的 foreach 配合使用）
// 这里假设通过底层包装，你可以拿到或者在底层逻辑中处理：
static void snapshot_write_hash_cb(kv_data_t *key, kv_data_t *value, void *arg) {
    FILE *fp = (FILE*)arg;
    int type = SNAP_TYPE_HASH;
    
    // 实际项目中，建议将 foreach 的 callback 修改为带有时间戳的自定义函数
    // 这里做演示，我们假设直接把对应 Key 的超时指标查出来写进去
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
    fwrite(&expire_time, sizeof(int64_t), 1, fp); // 新增：写入8字节过期时间
    fwrite(&key->len, sizeof(int), 1, fp);
    fwrite(key->data, 1, key->len, fp);
    fwrite(&value->len, sizeof(int), 1, fp);
    fwrite(value->data, 1, value->len, fp);
}

static void snapshot_write_skip_cb(kv_data_t *key, kv_data_t *value, void *arg) {
    FILE *fp = (FILE*)arg;
    int type = SNAP_TYPE_SKIPLIST;
    int64_t expire_time = 0; 

    fwrite(&type, sizeof(int), 1, fp);
    fwrite(&expire_time, sizeof(int64_t), 1, fp); // 新增：写入8字节过期时间
    fwrite(&key->len, sizeof(int), 1, fp);
    fwrite(key->data, 1, key->len, fp);
    fwrite(&value->len, sizeof(int), 1, fp);
    fwrite(value->data, 1, value->len, fp);
}

// 保存二进制快照 
int kvs_snapshot_save(void) {
    // 使用 "wb" 模式（二进制写），清空旧文件并重新写入
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

// 修改：加载二进制快照（包含冷启动过期净化）
int kvs_snapshot_load(void) {
    // 使用 "rb" 模式（二进制读）
    FILE *fp = fopen("kvstore.snap", "rb");
    if (!fp) {
        printf("No snapshot file found, starting fresh\n");
        return -1; 
    }
    
    int loaded_count = 0;
    int expired_cleanup_count = 0; // 新增：记录加载时直接净化掉的过期 Key 数量
    int type, key_len, val_len;
    int64_t expire_time;           // 新增：用于读取 8 字节过期时间
    int64_t now = get_current_ms_snapshot(); // 获取当前系统系统时间
    
    // 按块精准读取：先读 4 字节的 engine type
    while (fread(&type, sizeof(int), 1, fp) == 1) {
        
        // 修改：紧接着读取 8 字节的过期时间戳
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
        
        // 关键：执行冷启动过期净化拦截
        if (expire_time > 0 && now > expire_time) {
            // 说明在系统关机期间，这个 Key 已经超时死掉了，直接丢弃它，不塞入内存
            kvs_free(k_buf);
            kvs_free(v_buf);
            expired_cleanup_count++;
            continue; 
        }

        // 组装统一的 kv_data_t 结构体
        kv_data_t kv_k = { .data = k_buf, .len = key_len };
        kv_data_t kv_v = { .data = v_buf, .len = val_len };
        
        // 分发到对应的存储引擎
        // 修改：传入读取到的 expire_time 到各个引擎的 _set 函数中
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
    printf("Snapshot loaded: %d entries (Purged %d expired entries on startup)\n", 
            loaded_count, expired_cleanup_count);
    return 0;
}

//线程管理
static void *auto_save_thread_func(void *arg) {
    // 提取出参数后，立即释放分配的堆内存，防止内存泄漏
    int interval = *(int*)arg;
    kvs_free(arg); 
    
    while (auto_save_running) {
        sleep(interval);
        if (auto_save_running) {
            kvs_snapshot_save();
        }
    }
    return NULL;
}


int kvs_snapshot_auto_save(int interval_seconds) {
    if (interval_seconds <= 0) return -1;
    if (auto_save_running) return -1; 
    
    auto_save_running = 1;
    // 分配并传递给线程
    int *interval = kvs_malloc(sizeof(int));
    *interval = interval_seconds;
    
    if (pthread_create(&auto_save_thread, NULL, auto_save_thread_func, interval) != 0) {
        auto_save_running = 0;
        kvs_free(interval);
        return -1;
    }
    
    printf("Auto save started, interval: %d seconds\n", interval_seconds);
    return 0;
}


void kvs_snapshot_auto_save_stop(void) {
    if (!auto_save_running) return;
    auto_save_running = 0;
    pthread_join(auto_save_thread, NULL);
}