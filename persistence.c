// 增量持久化

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>   // 支持 int64_t
#include <sys/time.h> // 支持 gettimeofday
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

// ⏱️ 新增辅助函数：获取当前毫秒级时间戳
static int64_t get_current_ms_aof(void) {
    struct timeval tv;
    gettimeofday(&tv, NULL);
    return (int64_t)tv.tv_sec * 1000 + tv.tv_usec / 1000;
}

// 原样输出：初始化，以追加读写+二进制模式打开文件
int kvs_persistence_init(void) {
    aof_fp = fopen(PERSISTENCE_FILE, "a+b");
    if (!aof_fp) {
        printf("Failed to open persistence file!\n");
        return -1;
    }
    return 0;
}

// 原样输出：写入日志，直接将二进制块写入文件，并刷入磁盘
// 提示：网络层在调用此函数组装 AOF 缓冲时，需要按照新的协议（如果是 SET/MOD 需包含 8字节 expire_time）拼好后传入
void kvs_persistence_write(const void *data, int len) {
    if (aof_fp && data && len > 0) {
        fwrite(data, 1, len, aof_fp);
        fflush(aof_fp); // 确保数据立刻落盘，防止宕机丢失
    }
}

// ⏱️ 修改：恢复数据，读取包含 expire_time 的 AOF 文件
void kvs_persistence_recover(void) {
    if (!aof_fp) return;
    
    // 回到文件头部
    fseek(aof_fp, 0, SEEK_SET);

    int recovered_count = 0;
    int expired_cleanup_count = 0; // 记录加载时直接过滤掉的超时日志数量
    int64_t now = get_current_ms_aof();

    while (1) {
        int cmd_len = 0, key_len = 0, val_len = 0;
        int64_t expire_time = 0; // 存储从日志读出的过期时间戳
        
        // 读 CMD 长度，如果读不到说明文件结束(EOF)
        if (fread(&cmd_len, sizeof(int), 1, aof_fp) != 1) break;
        char cmd[32] = {0}; // 假设命令不会超过 32 字节
        fread(cmd, 1, cmd_len, aof_fp);
        cmd[cmd_len] = '\0'; // 方便做 strcmp

        // ⏱️ 修改：如果是写命令(SET/MOD)，日志协议里包含了 8 字节的 expire_time，必须先读出来
        int is_write_cmd = (strcmp(cmd, "SET") == 0 || strcmp(cmd, "MOD") == 0 ||
                            strcmp(cmd, "RSET") == 0 || strcmp(cmd, "RMOD") == 0 ||
                            strcmp(cmd, "HSET") == 0 || strcmp(cmd, "HMOD") == 0 ||
                            strcmp(cmd, "SSET") == 0 || strcmp(cmd, "SMOD") == 0);
        
        if (is_write_cmd) {
            if (fread(&expire_time, sizeof(int64_t), 1, aof_fp) != 1) break;
        }

        // 读 KEY
        if (fread(&key_len, sizeof(int), 1, aof_fp) != 1) break;
        void *key = kvs_malloc(key_len);
        fread(key, 1, key_len, aof_fp);

        // 读 VALUE
        if (fread(&val_len, sizeof(int), 1, aof_fp) != 1) {
            kvs_free(key);
            break;
        }
        void *val = NULL;
        if (val_len > 0) {
            val = kvs_malloc(val_len);
            fread(val, 1, val_len, aof_fp);
        }

        // ⏱️ 关键拦截：如果这条写日志已经超时过期，直接净化丢弃，不重放到内存中
        if (is_write_cmd && expire_time > 0 && now > expire_time) {
            kvs_free(key);
            if (val) kvs_free(val);
            expired_cleanup_count++;
            continue; // 跳过此条日志，继续还原下一条
        }

        kv_data_t kv_k;
        kv_k.data = key;
        kv_k.len = key_len;

        kv_data_t kv_v;
        kv_v.data = val;
        kv_v.len = val_len;

        #if ENABLE_ARRAY
        if (strcmp(cmd, "SET") == 0) {
            kvs_array_set(&global_array, &kv_k, &kv_v, expire_time); // ⏱️ 修改：传入时间戳
        } else if (strcmp(cmd, "DEL") == 0) {
            kvs_array_del(&global_array, &kv_k);
        } else if (strcmp(cmd, "MOD") == 0) {
            kvs_array_mod(&global_array, &kv_k, &kv_v, expire_time); // ⏱️ 修改：传入时间戳
        }
        #endif
        
        #if ENABLE_RBTREE
        if (strcmp(cmd, "RSET") == 0) {
            kvs_rbtree_set(&global_rbtree, &kv_k, &kv_v, expire_time); // ⏱️ 修改：传入时间戳
        } else if (strcmp(cmd, "RDEL") == 0) {
            kvs_rbtree_del(&global_rbtree, &kv_k);
        } else if (strcmp(cmd, "RMOD") == 0) {
            kvs_rbtree_mod(&global_rbtree, &kv_k, &kv_v, expire_time); // ⏱️ 修改：传入时间戳
        }
        #endif
        
        #if ENABLE_HASH
        if (strcmp(cmd, "HSET") == 0) {
            kvs_hash_set(&global_hash, &kv_k, &kv_v, expire_time); // ⏱️ 修改：传入时间戳
        } else if (strcmp(cmd, "HDEL") == 0) {
            kvs_hash_del(&global_hash, &kv_k);
        } else if (strcmp(cmd, "HMOD") == 0) {
            kvs_hash_mod(&global_hash, &kv_k, &kv_v, expire_time); // ⏱️ 修改：传入时间戳
        }
        #endif
        
        #if ENABLE_SKIPLIST
        if (strcmp(cmd, "SSET") == 0) {
            kvs_skip_set(&global_skip, &kv_k, &kv_v, expire_time); // ⏱️ 修改：传入时间戳
        } else if (strcmp(cmd, "SDEL") == 0) {
            kvs_skip_del(&global_skip, &kv_k);
        } else if (strcmp(cmd, "SMOD") == 0) {
            kvs_skip_mod(&global_skip, &kv_k, &kv_v, expire_time); // ⏱️ 修改：传入时间戳
        }
        #endif

        kvs_free(key);
        if (val) kvs_free(val);
        recovered_count++;
    }
    
    // 恢复完成后，把文件指针移到末尾，以便后续继续追加
    fseek(aof_fp, 0, SEEK_END);
    printf("AOF recovery finished: %d commands replayed (Purged %d expired logs)\n", 
            recovered_count, expired_cleanup_count);
}

// 原样输出：关闭文件
void kvs_persistence_close(void) {
    if (aof_fp) {
        fclose(aof_fp);
        aof_fp = NULL;
    }
}