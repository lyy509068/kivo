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

static uint64_t g_snapshot_crc = 0; // 全局计算器

static uint64_t kvs_crc64(uint64_t crc, const unsigned char *buf, size_t len) {
    // 如果是初始状态，设定 FNV offset basis
    if (crc == 0) {
        crc = 14695981039346656037ULL;
    }
    for (size_t i = 0; i < len; i++) {
        crc ^= buf[i];
        crc *= 1099511628211ULL; // FNV prime
    }
    return crc;
}

// 封装一个安全的带 CRC 累加的写入函数
static void fwrite_with_crc(const void *ptr, size_t size, size_t nmemb, FILE *stream) {
    size_t total_bytes = size * nmemb;
    if (total_bytes > 0) {
        g_snapshot_crc = kvs_crc64(g_snapshot_crc, (const unsigned char *)ptr, total_bytes);
        fwrite(ptr, size, nmemb, stream);
    }
}

static int64_t get_current_ms_snapshot(void) {
    struct timeval tv;
    gettimeofday(&tv, NULL);
    return (int64_t)tv.tv_sec * 1000 + tv.tv_usec / 1000;
}

#if ENABLE_ARRAY
static void snapshot_write_array_cb(kv_data_t *key, kv_data_t *value, void *arg) {
    FILE *fp = (FILE*)arg;
    int type = SNAP_TYPE_ARRAY;
    
    // 放弃 for 循环！利用内存偏移直接反推当前 array_item 的首地址来拿 expire_time
    kvs_array_item_t *item = (kvs_array_item_t *)((char *)key - offsetof(kvs_array_item_t, key));
    int64_t expire_time = item->expire_time;

    fwrite_with_crc(&type, sizeof(int), 1, fp);
    fwrite_with_crc(&expire_time, sizeof(int64_t), 1, fp); 
    fwrite_with_crc(&key->len, sizeof(int), 1, fp);
    fwrite_with_crc(key->data, 1, key->len, fp);
    fwrite_with_crc(&value->len, sizeof(int), 1, fp);
    fwrite_with_crc(value->data, 1, value->len, fp);
}
#endif 

#if ENABLE_RBTREE
static void snapshot_write_rbtree_cb(kv_data_t *key, kv_data_t *value, void *arg) {
    FILE *fp = (FILE*)arg;
    int type = SNAP_TYPE_RBTREE;
    
    // 精准反推，无循环
    rbtree_node_binary_t *node = (rbtree_node_binary_t *)((char *)key - offsetof(rbtree_node_binary_t, key));
    int64_t expire_time = node->expire_time; 

    fwrite_with_crc(&type, sizeof(int), 1, fp);
    fwrite_with_crc(&expire_time, sizeof(int64_t), 1, fp); 
    fwrite_with_crc(&key->len, sizeof(int), 1, fp);
    fwrite_with_crc(key->data, 1, key->len, fp);
    fwrite_with_crc(&value->len, sizeof(int), 1, fp);
    fwrite_with_crc(value->data, 1, value->len, fp);
}
#endif 

#if ENABLE_HASH
static void snapshot_write_hash_cb(kv_data_t *key, kv_data_t *value, void *arg) {
    FILE *fp = (FILE*)arg;
    int type = SNAP_TYPE_HASH;
    
    // 放弃 while 循环！既然 key 是 hashnode_t 内部的成员，直接反推拿到当前节点！
    hashnode_t *node = (hashnode_t *)((char *)key - offsetof(hashnode_t, key));
    int64_t expire_time = node->expire_time;

    fwrite_with_crc(&type, sizeof(int), 1, fp);
    fwrite_with_crc(&expire_time, sizeof(int64_t), 1, fp); 
    fwrite_with_crc(&key->len, sizeof(int), 1, fp);
    fwrite_with_crc(key->data, 1, key->len, fp);
    fwrite_with_crc(&value->len, sizeof(int), 1, fp);
    fwrite_with_crc(value->data, 1, value->len, fp);
}
#endif 

#if ENABLE_SKIPLIST
static void snapshot_write_skip_cb(kv_data_t *key, kv_data_t *value, void *arg) {
    FILE *fp = (FILE*)arg;
    int type = SNAP_TYPE_SKIPLIST; 

    // 精准反推，无循环
    skipnode_binary_t *node = (skipnode_binary_t *)((char *)key - offsetof(skipnode_binary_t, key));
    int64_t expire_time = node->expire_time;

    fwrite_with_crc(&type, sizeof(int), 1, fp);
    fwrite_with_crc(&expire_time, sizeof(int64_t), 1, fp); 
    fwrite_with_crc(&key->len, sizeof(int), 1, fp);
    fwrite_with_crc(key->data, 1, key->len, fp);
    fwrite_with_crc(&value->len, sizeof(int), 1, fp);
    fwrite_with_crc(value->data, 1, value->len, fp);
}
#endif
// 保存二进制快照：把存储结构的节点写进快照
int kvs_snapshot_save(void) {
    FILE *fp = fopen("kvstore.snap", "wb");
    if (!fp) return -1;

    g_snapshot_crc = 0; // 开始前将校验和清零


    // 1. 只有 Array 真的有数据时，才允许调用 Array 的遍历和写入
    #if ENABLE_ARRAY
    extern kvs_array_t global_array;
    if (global_array.total > 0 && global_array.table != NULL) {
        kvs_array_foreach(&global_array, snapshot_write_array_cb, fp);
    }
    #endif

    // 2. 只有红黑树真的有节点时，才允许进入
    #if ENABLE_RBTREE
    extern kvs_rbtree_t global_rbtree;
    if (global_rbtree.nil != NULL && global_rbtree.root != global_rbtree.nil) {
        kvs_rbtree_foreach(&global_rbtree, snapshot_write_rbtree_cb, fp);
    }
    #endif

    // 3. 只有 Hash 真的有数据时，才调用 Hash 写入
    #if ENABLE_HASH
    extern kvs_hash_t global_hash;
    if (global_hash.count > 0) {
        kvs_hash_foreach(&global_hash, snapshot_write_hash_cb, fp);
    }
    #endif

    // 4. 只有跳表真的有数据时，才调用跳表写入
    #if ENABLE_SKIPLIST
    extern kvs_skip_t global_skip;
    if (global_skip.count > 0 && global_skip.header->forward[0] != NULL) {
        kvs_skip_foreach(&global_skip, snapshot_write_skip_cb, fp);
    }
    #endif

    // 注意：这里用 fwrite 写入，因为校验值本身不计入校验计算
    fwrite(&g_snapshot_crc, sizeof(uint64_t), 1, fp);

    fclose(fp);

    printf("[SAVE] Snapshot saved safely with CRC64: %llu\n", (unsigned long long)g_snapshot_crc);

    return 0;
}

//从快照文件读取数据，恢复到存储结构中
int kvs_snapshot_load(void) {
    FILE *fp = fopen("kvstore.snap", "rb");
    if (!fp) {
        printf("No snapshot file found, starting fresh\n");
        return -1; 
    }
    
    // 算出文件的总长度，预留出末尾 8 字节的 CRC
    fseek(fp, 0, SEEK_END);
    long file_size = ftell(fp);
    if (file_size < (long)sizeof(uint64_t)) {
        printf("🚨 [CRITICAL] Snapshot file is corrupted (too small)!\n");
        fclose(fp);
        return -1;
    }
    long data_limit = file_size - (long)sizeof(uint64_t); // 数据截止边界
    fseek(fp, 0, SEEK_SET); // 重新回到文件头

    uint64_t my_calc_crc = 0; // 用于读取时现场计算的 CRC


    int loaded_count = 0;
    int expired_cleanup_count = 0; 
    int type, key_len, val_len;
    int64_t expire_time;          
    int64_t now = get_current_ms_snapshot(); 
    
  
    //  只有当前文件指针位置 ftell(fp) 没到边界时，才继续解析数据
    while (ftell(fp) < data_limit) {
        
        // 为了边读边计算 CRC，我们做个小宏或者手动更新 my_calc_crc
        #define FREAD_CRC(ptr, sz, n) { \
            if (fread(ptr, sz, n, fp) != n) break; \
            my_calc_crc = kvs_crc64(my_calc_crc, (const unsigned char *)(ptr), (sz)*(n)); \
        }

        FREAD_CRC(&type, sizeof(int), 1);
        FREAD_CRC(&expire_time, sizeof(int64_t), 1);
        FREAD_CRC(&key_len, sizeof(int), 1);

        void *k_buf = kvs_malloc((key_len + 7) & ~7);
        if (!k_buf) break;
        FREAD_CRC(k_buf, 1, key_len);
        
        FREAD_CRC(&val_len, sizeof(int), 1);
        void *v_buf = kvs_malloc((val_len + 7) & ~7);
        if (!v_buf) { kvs_free(k_buf); break; }
        FREAD_CRC(v_buf, 1, val_len);
        
        #undef FREAD_CRC
        
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

    // 3. 核心交卷时刻：读取文件末尾保存的原始 CRC
    uint64_t file_stored_crc = 0;
    fseek(fp, data_limit, SEEK_SET); // 明确把指针移到最后8字节
    if (fread(&file_stored_crc, sizeof(uint64_t), 1, fp) != 1) {
        printf("🚨 [CRITICAL] Failed to read CRC from file!\n");
        fclose(fp);
        return -1;
    }
    fclose(fp);

    // 4. 比对双方的校验值
    if (my_calc_crc != file_stored_crc) {
        printf("🚨 [CRITICAL] Snapshot CRC Mismatch!!!\n");
        printf("-> Expected (Calculated): %llu\n", (unsigned long long)my_calc_crc);
        printf("-> Got (File Stored):     %llu\n", (unsigned long long)file_stored_crc);
        printf("🔥 Data has been corrupted or tampered! Refusing to start server.\n");
        return -1; // 校验失败，决不妥协，保护系统
    }

    printf("🎉 [SUCCESS] Snapshot integrity checked OK (CRC: %llu). Loaded %d entries.\n", 
           (unsigned long long)file_stored_crc, loaded_count);
    return 0;
}
