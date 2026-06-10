// 增量持久化

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>   
#include <sys/time.h> 
#include "kvstore.h"
#include <pthread.h>


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

static FILE *aof_fp = NULL;


static pthread_mutex_t aof_write_mutex = PTHREAD_MUTEX_INITIALIZER;


static int64_t get_current_ms_aof(void) {
    struct timeval tv;
    gettimeofday(&tv, NULL);
    return (int64_t)tv.tv_sec * 1000 + tv.tv_usec / 1000;
}

// 初始化，以追加读写+二进制模式打开文件
int kvs_persistence_init(void) {
    aof_fp = fopen(PERSISTENCE_FILE, "a+b");
    if (!aof_fp) {
        printf("Failed to open persistence file!\n");
        return -1;
    }
    return 0;
}

//执行一次命令调用一次
void kvs_persistence_write(const void *data, int len) {
    if (aof_fp && data && len > 0) {
        pthread_mutex_lock(&aof_write_mutex); 
        fwrite(data, 1, len, aof_fp);
        fflush(aof_fp); 
        pthread_mutex_unlock(&aof_write_mutex); 
    }
}

//从日志文件恢复数据到引擎
void kvs_persistence_recover(void) {
    if (!aof_fp) return;
    
    fseek(aof_fp, 0, SEEK_SET);

    int recovered_count = 0;
    int expired_cleanup_count = 0; 
    int64_t now = get_current_ms_aof();

    while (1) {
        int cmd_len = 0, key_len = 0, val_len = 0;
        int64_t expire_time = 0; //从日志读真正的过期时间
        
        // 读取并校验 CMD 长度
        if (fread(&cmd_len, sizeof(int), 1, aof_fp) != 1) break;
        
        // 命令长度明显不合理安全退出
        if (cmd_len <= 0) {
            printf("[AOF Warning] Corrupted cmd_len detected: %d. Stopping recovery.\n", cmd_len);
            break;
        }

        char cmd[32] = {0}; 
        if (fread(cmd, 1, cmd_len, aof_fp) != (size_t)cmd_len) break;
        cmd[cmd_len] = '\0'; 

        // 无论什么命令，我们现在统一在 CMD 之后读取 8 字节的 expire_time
        // 因为我们在 log_binary_command 中固定了数据结构，所以这里必须按顺序读
        if (fread(&expire_time, sizeof(int64_t), 1, aof_fp) != 1) break;

        // 判定写命令 (仅用于判断后续是否需要检查过期拦截)
        int is_write_cmd = (strcmp(cmd, "SET") == 0 || strcmp(cmd, "MOD") == 0 || 
                            strcmp(cmd, "RSET") == 0 || strcmp(cmd, "RMOD") == 0 ||
                            strcmp(cmd, "HSET") == 0 || strcmp(cmd, "HMOD") == 0 || 
                            strcmp(cmd, "SSET") == 0 || strcmp(cmd, "SMOD") == 0);

        // 读取并校验 KEY
        if (fread(&key_len, sizeof(int), 1, aof_fp) != 1) break;
        // 限制 Key 最大 64KB
        if (key_len <= 0 || key_len > 1024 * 64) { 
            printf("[AOF Warning] Corrupted key_len detected: %d. Stopping recovery.\n", key_len);
            break;
        }

        void *key = kvs_malloc(key_len);
        if (!key) break;
        if (fread(key, 1, key_len, aof_fp) != (size_t)key_len) {
            kvs_free(key);
            break;
        }

        // 读取并校验 VALUE
        if (fread(&val_len, sizeof(int), 1, aof_fp) != 1) {
            kvs_free(key);
            break;
        }
        
        // 防止 Value 长度脏数据爆内存
        if (val_len < 0 || val_len > 1024 * 1024 * 10) { // 限制 Value 最大 10MB
            printf("[AOF Warning] Corrupted val_len detected: %d. Stopping recovery.\n", val_len);
            kvs_free(key);
            break;
        }

        void *val = NULL;
        if (val_len > 0) {
            val = kvs_malloc(val_len);
            if (!val) {
                kvs_free(key);
                break;
            }
            if (fread(val, 1, val_len, aof_fp) != (size_t)val_len) {
                kvs_free(key);
                kvs_free(val);
                break;
            }
        }

        // 过期拦截
        if (is_write_cmd && expire_time > 0 && now > expire_time) {//过期时间为0这里会 直接跳过 表示永不过期
            printf("[AOF Debug] CMD %s skipped because it expired! expire:%ld, now:%ld\n", cmd, expire_time, now);
            kvs_free(key);
            if (val) kvs_free(val);
            expired_cleanup_count++;
            continue; 
        }

        // 数据重放还原到各引擎
        kv_data_t kv_k = {key, key_len};
        kv_data_t kv_v = {val, val_len};

        #if ENABLE_ARRAY
        if (strcmp(cmd, "SET") == 0) kvs_array_set(&global_array, &kv_k, &kv_v, expire_time);
        else if (strcmp(cmd, "DEL") == 0) kvs_array_del(&global_array, &kv_k);
        else if (strcmp(cmd, "MOD") == 0) kvs_array_mod(&global_array, &kv_k, &kv_v, expire_time);
        #endif
        
        #if ENABLE_RBTREE
        if (strcmp(cmd, "RSET") == 0) kvs_rbtree_set(&global_rbtree, &kv_k, &kv_v, expire_time);
        else if (strcmp(cmd, "RDEL") == 0) kvs_rbtree_del(&global_rbtree, &kv_k);
        else if (strcmp(cmd, "RMOD") == 0) kvs_rbtree_mod(&global_rbtree, &kv_k, &kv_v, expire_time);
        #endif
        
        #if ENABLE_HASH
        if (strcmp(cmd, "HSET") == 0) kvs_hash_set(&global_hash, &kv_k, &kv_v, expire_time);
        else if (strcmp(cmd, "HDEL") == 0) kvs_hash_del(&global_hash, &kv_k);
        else if (strcmp(cmd, "HMOD") == 0) kvs_hash_mod(&global_hash, &kv_k, &kv_v, expire_time);
        #endif
        
        #if ENABLE_SKIPLIST
        if (strcmp(cmd, "SSET") == 0) kvs_skip_set(&global_skip, &kv_k, &kv_v, expire_time);
        else if (strcmp(cmd, "SDEL") == 0) kvs_skip_del(&global_skip, &kv_k);
        else if (strcmp(cmd, "SMOD") == 0) kvs_skip_mod(&global_skip, &kv_k, &kv_v, expire_time);
        #endif

        kvs_free(key);
        if (val) kvs_free(val);
        recovered_count++;
    }
    
    fseek(aof_fp, 0, SEEK_END);
    //printf("AOF recovery finished: %d commands replayed (Purged %d expired logs)\n", recovered_count, expired_cleanup_count);
}



void kvs_persistence_close(void) {
    if (aof_fp) {
        fclose(aof_fp);
        aof_fp = NULL;
    }
}