#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <unistd.h>
#include <sys/time.h>
#include <pthread.h>
#include "kvstore.h"
#include "repl.h"

#include <pthread.h>

static volatile int expire_paused = 0;
static pthread_mutex_t expire_pause_mutex = PTHREAD_MUTEX_INITIALIZER;
static pthread_cond_t expire_pause_cond = PTHREAD_COND_INITIALIZER;

//暂停超时删除
void expire_thread_pause(void) {
    pthread_mutex_lock(&expire_pause_mutex);
    expire_paused = 1;
    pthread_mutex_unlock(&expire_pause_mutex);
    printf("[Expire] All expire threads paused.\n");
}
//恢复超时删除
void expire_thread_resume(void) {
    pthread_mutex_lock(&expire_pause_mutex);
    expire_paused = 0;
    pthread_cond_broadcast(&expire_pause_cond);
    pthread_mutex_unlock(&expire_pause_mutex);
    printf("[Expire] All expire threads resumed.\n");
}

extern int BEGIN_IN;

#if ENABLE_ARRAY
extern kvs_array_t   global_array;
#endif
#if ENABLE_RBTREE
extern kvs_rbtree_t  global_rbtree;
#endif
#if ENABLE_HASH
extern kvs_hash_t    global_hash;
#endif
#if ENABLE_SKIPLIST
extern kvs_skip_t    global_skip;
#endif

#if ENABLE_PERSISTENCE || ENABLE_REPLICATION_MASTER
extern void log_binary_command(const char *cmd, void *key, int key_len, void *value, int value_len, int64_t expire_time);
#endif

// 全局控制变量
volatile int expire_thread_running = 1;
pthread_t expire_tids[4];

// 获取当前系统绝对时间戳
int64_t get_current_ms(void) {
    struct timeval tv;
    gettimeofday(&tv, NULL);
    return (int64_t)tv.tv_sec * 1000 + tv.tv_usec / 1000;
}

// Array超时清理线程,基于元素移动与写锁
#if ENABLE_ARRAY
void* kvs_array_expire_worker(void* arg) {
    
    //printf("[Expire Thread] Array cleaner started.\n");
    while (expire_thread_running) {

        pthread_mutex_lock(&expire_pause_mutex);
        while (expire_paused && expire_thread_running) {
            pthread_cond_wait(&expire_pause_cond, &expire_pause_mutex);
        }
        pthread_mutex_unlock(&expire_pause_mutex);
        
        if (!expire_thread_running) break; 

        int64_t now = get_current_ms();
        
        // Array通常属于全局单一结构，这里默认使用分段锁的第0号锁保护整个数组
        pthread_rwlock_wrlock(&seg_locks[0]);
        
        for (int i = 0; i < global_array.idx; ) {
            if (global_array.table[i].expire_time > 0 && now > global_array.table[i].expire_time) {

                #if ENABLE_PERSISTENCE || ENABLE_REPLICATION_MASTER 
                log_binary_command("DEL", global_array.table[i].key.data, (int)global_array.table[i].key.len, NULL, 0, 0);
                #endif
                #if ENABLE_REPLICATION_MASTER
                if(BEGIN_IN){
                    repl_push_cmd("DEL", global_array.table[i].key.data, (int)global_array.table[i].key.len, NULL, 0);
                }
                #endif
                kv_data_destroy(&global_array.table[i].key);
                kv_data_destroy(&global_array.table[i].value);
                
                // 将尾部元素移到当前位置填补空缺
                if (i < global_array.idx - 1) {
                    global_array.table[i] = global_array.table[global_array.idx - 1];
                }
                global_array.idx--;
                global_array.total--;
                // 由于换过来了新元素，此时不需要 i++，继续检查当前位置
            } else {
                i++;
            }
        }
        
        pthread_rwlock_unlock(&seg_locks[0]);
        usleep(150000); // 适度休眠150ms，防止占满CPU
    }
    return NULL;
}
#endif

// Hash超时清理线程,链地址法单向链表并发删除
#if ENABLE_HASH
void* kvs_hash_expire_worker(void* arg) {
    
    //printf("[Expire Thread] Hash cleaner started.\n");
    while (expire_thread_running) {

        pthread_mutex_lock(&expire_pause_mutex);
        while (expire_paused && expire_thread_running) {
            pthread_cond_wait(&expire_pause_cond, &expire_pause_mutex);
        }
        pthread_mutex_unlock(&expire_pause_mutex);
        
        if (!expire_thread_running) break; 
        
        int64_t now = get_current_ms();
        
        // 遍历哈希表的所有桶
        for (int i = 0; i < global_hash.max_slots; i++) {
            // 根据桶的索引匹配对应的分段锁，实现极低冲突的细粒度锁
            int lock_idx = i % LOCK_SEGMENTS;
            
            pthread_rwlock_wrlock(&seg_locks[lock_idx]);
            
            hashnode_t *curr = global_hash.buckets[i];
            hashnode_t *prev = NULL;
            
            while (curr != NULL) {
                hashnode_t *next = curr->next;
                if (curr->expire_time > 0 && now > curr->expire_time) {
                    // 摘除节点
                    if (prev == NULL) {
                        global_hash.buckets[i] = next;
                    } else {
                        prev->next = next;
                    }

                    #if ENABLE_PERSISTENCE || ENABLE_REPLICATION_MASTER 
                    log_binary_command("HDEL", curr->key.data, (int)curr->key.len, NULL, 0, 0);
                    #endif
                    #if ENABLE_REPLICATION_MASTER
                    if(BEGIN_IN){
                        repl_push_cmd("HDEL", curr->key.data, (int)curr->key.len, NULL, 0);
                    }
                    #endif
                    kv_data_destroy(&curr->key);
                    kv_data_destroy(&curr->value);
                    
                #if ENABLE_MEM_POOL
                    // 如果启用了内存池，则还给哈希节点池
                    mem_pool_free(hash_node_pool, curr); 
                #else
                    kvs_free(curr);
                #endif
                    global_hash.count--;
                    curr = next; // 继续往后处理
                } else {
                    prev = curr;
                    curr = next;
                }
            }
            
            pthread_rwlock_unlock(&seg_locks[lock_idx]);
            
            // 阶段性让出 CPU，每扫描完一部分桶可以微小休眠，避免主线程 GET/SET 被长期阻塞
            if (i % 64 == 0) usleep(1000); 
        }
        usleep(100000); // 一轮全局扫描完成后，休眠 100ms
    }
    return NULL;
}
#endif

#if ENABLE_RBTREE
// 递归中序遍历，用读锁安全搜集最多 100 个过期 Key 的快照指针
static void rbtree_collect_expired(rbtree_node_binary_t *node, rbtree_node_binary_t *nil, int64_t now, kv_data_t *expired_keys, int *count, int max_count) {
    if (node == nil || *count >= max_count) return;
    
    rbtree_collect_expired(node->left, nil, now, expired_keys, count, max_count);
    
    if (node->expire_time > 0 && now > node->expire_time && *count < max_count) {
        expired_keys[*count] = node->key; // 浅拷贝保存 Key 索引
        (*count)++;
    }
    
    rbtree_collect_expired(node->right, nil, now, expired_keys, count, max_count);
}

void* kvs_rbtree_expire_worker(void* arg) {
    
    //printf("[Expire Thread] RB-Tree cleaner started.\n");
    kv_data_t expired_batch[100];
    
    while (expire_thread_running) {

        pthread_mutex_lock(&expire_pause_mutex);
        while (expire_paused && expire_thread_running) {
            pthread_cond_wait(&expire_pause_cond, &expire_pause_mutex);
        }
        pthread_mutex_unlock(&expire_pause_mutex);
        
        if (!expire_thread_running) break; 
        
        int64_t now = get_current_ms();
        int expired_count = 0;
        
        // 全局红黑树树共用1号分段锁
        pthread_rwlock_rdlock(&seg_locks[1]);
        rbtree_collect_expired(global_rbtree.root, global_rbtree.nil, now, expired_batch, &expired_count, 100);
        pthread_rwlock_unlock(&seg_locks[1]);
        
        if (expired_count > 0) {
            // 逐个加写锁安全安全摘除节点，防止树旋转破坏遍历
            for (int i = 0; i < expired_count; i++) {
                pthread_rwlock_wrlock(&seg_locks[1]);

                #if ENABLE_PERSISTENCE || ENABLE_REPLICATION_MASTER 
                log_binary_command("RDEL", expired_batch[i].data, (int)expired_batch[i].len, NULL, 0, 0);
                #endif
                #if ENABLE_REPLICATION_MASTER
                if(BEGIN_IN){
                    repl_push_cmd("RDEL", expired_batch[i].data, (int)expired_batch[i].len, NULL, 0);
                }
                #endif
                kvs_rbtree_del(&global_rbtree, &expired_batch[i]);
                pthread_rwlock_unlock(&seg_locks[1]);
            }
        }
        
        usleep(200000); // 树形结构调整较昂贵，周期设为 200ms
    }
    return NULL;
}
#endif

#if ENABLE_SKIPLIST
void* kvs_skiplist_expire_worker(void* arg) {
    
    //printf("[Expire Thread] Skiplist cleaner started.\n");
    kv_data_t expired_batch[100];
    
    while (expire_thread_running) {

        pthread_mutex_lock(&expire_pause_mutex);
        while (expire_paused && expire_thread_running) {
            pthread_cond_wait(&expire_pause_cond, &expire_pause_mutex);
        }
        pthread_mutex_unlock(&expire_pause_mutex);
        
        if (!expire_thread_running) break; 
        
        int64_t now = get_current_ms();
        int expired_count = 0;
        
        // 假设跳表共用2号分段锁，通过最底层的 Level 0 链表快速过滤过期 Key
        pthread_rwlock_rdlock(&seg_locks[2]);
        skipnode_binary_t *curr = global_skip.header->forward[0];
        while (curr != NULL && expired_count < 100) {
            if (curr->expire_time > 0 && now > curr->expire_time) {
                expired_batch[expired_count++] = curr->key;
            }
            curr = curr->forward[0];
        }
        pthread_rwlock_unlock(&seg_locks[2]);
        
        if (expired_count > 0) {
            // 释放读锁后，加写锁执行物理剔除
            for (int i = 0; i < expired_count; i++) {
                pthread_rwlock_wrlock(&seg_locks[2]);

                #if ENABLE_PERSISTENCE || ENABLE_REPLICATION_MASTER 
                log_binary_command("SDEL", expired_batch[i].data, (int)expired_batch[i].len, NULL, 0, 0);
                #endif
                #if ENABLE_REPLICATION_MASTER
                if(BEGIN_IN){
                    repl_push_cmd("SDEL", expired_batch[i].data, (int)expired_batch[i].len, NULL, 0);
                }
                #endif
                kvs_skip_del(&global_skip, &expired_batch[i]);
                pthread_rwlock_unlock(&seg_locks[2]);
            }
        }
        
        usleep(150000);
    }
    return NULL;
}
#endif

// 启动 4 个后台超时线程
int kvs_expire_thread_start(void) {
    expire_thread_running = 1;
    int res = 0;

#if ENABLE_ARRAY
    res = pthread_create(&expire_tids[0], NULL, kvs_array_expire_worker, NULL);
    if (res != 0) return -1;
#endif

#if ENABLE_HASH
    res = pthread_create(&expire_tids[1], NULL, kvs_hash_expire_worker, NULL);
    if (res != 0) return -1;
#endif

#if ENABLE_RBTREE
    res = pthread_create(&expire_tids[2], NULL, kvs_rbtree_expire_worker, NULL);
    if (res != 0) return -1;
#endif

#if ENABLE_SKIPLIST
    res = pthread_create(&expire_tids[3], NULL, kvs_skiplist_expire_worker, NULL);
    if (res != 0) return -1;
#endif

    //printf("[Expire System] All 4 background timeout threads initialized successfully.\n");
    return 0;
}

// 关闭并回收超时线程（在服务器关闭时调用）
void kvs_expire_thread_stop(void) {
    expire_thread_running = 0;
    
#if ENABLE_ARRAY
    pthread_join(expire_tids[0], NULL);
#endif
#if ENABLE_HASH
    pthread_join(expire_tids[1], NULL);
#endif
#if ENABLE_RBTREE
    pthread_join(expire_tids[2], NULL);
#endif
#if ENABLE_SKIPLIST
    pthread_join(expire_tids[3], NULL);
#endif

}