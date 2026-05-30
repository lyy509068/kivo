#ifndef __KV_STORE_H__
#define __KV_STORE_H__


#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <assert.h>
#include <stddef.h>
#include <stdint.h>     
#include <pthread.h>   
#include "mempool.h"


// 业务响应状态码,用来打包业务层回复
typedef enum {
    KVS_RESP_OK,           // "OK\r\n"
    KVS_RESP_EXIST,        // "EXIST\r\n"
    KVS_RESP_NO_EXIST,     // "NO EXIST\r\n"
    KVS_RESP_ERROR,        // "ERROR\r\n"
    KVS_RESP_PARSE_ERROR,  // "PARSE ERROR\r\n"
    KVS_RESP_UNKNOWN,      // "UNKNOWN COMMAND\r\n"
    KVS_RESP_SHUTDOWN,     // "SHUTDOWN\r\n"
    KVS_RESP_GET_OK        //  GET 命令成功找到数据（不固定回复）
} kvs_resp_status_t;
// 业务层返回给网络层的标准响应结构体
typedef struct {
    kvs_resp_status_t status; // 业务执行结果状态
    void *body;               // 仅在 GET_OK 时使用：指向查到的二进制数据副本
    size_t body_len;          // 仅在 GET_OK 时使用：二进制数据的绝对长度
} kvs_resp_t;



//业务层命令结构体
typedef struct {
    const char *cmd_name;
    int cmd_len;
    int cmd_enum;
} kvs_cmd_map_t;
void packet_build_batch(kvs_resp_t *resps, int cmd_count, char **wbuf, int *wcap, int *wlen);//打包函数



//超时删除
// 全局分段锁定义，用于多线程安全遍历删除
#define LOCK_SEGMENTS 32
extern pthread_rwlock_t seg_locks[LOCK_SEGMENTS];
// 后台超时线程启动声明
int kvs_expire_thread_start(void);
int64_t get_current_ms(void);




//内存池
#define ENABLE_MEM_POOL 0 
#if ENABLE_MEM_POOL
extern mem_pool_t *array_item_pool;
extern mem_pool_t *rbtree_node_pool;
extern mem_pool_t *hash_node_pool;
extern mem_pool_t *skip_node_pool;
#endif
void *kvs_malloc(size_t size);
void *kvs_calloc(size_t nmemb, size_t size);
void *kvs_realloc(void *ptr, size_t new_size);
void kvs_free(void *ptr);

//增量持久化
#define ENABLE_PERSISTENCE        0        
#define PERSISTENCE_FILE    "kvstore.aof"  
#if ENABLE_PERSISTENCE
int kvs_persistence_init(void);
void kvs_persistence_write(const void *data, int len);
void kvs_persistence_recover(void);
void kvs_persistence_close(void);
#endif

//全量持久化
#define ENABLE_SNAPSHOT           0        
#define SNAPSHOT_FILE     "kvstore.snap"  
#if ENABLE_SNAPSHOT
int kvs_snapshot_save(void);
int kvs_snapshot_load(void);
int kvs_snapshot_auto_save(int interval_seconds);
void kvs_snapshot_auto_save_stop(void);
#endif

//网络设置
#define NETWORK_REACTOR      0
#define NETWORK_PROACTOR     1
#define NETWORK_NTYCO        2

#define NETWORK_SELECT      NETWORK_REACTOR
#define KVS_MAX_TOKENS      128
//网络传输函数
typedef int (*binary_msg_handler)(void *msg, int length, kvs_resp_t *resp);
extern int reactor_start(unsigned short port, binary_msg_handler handler);
extern int proactor_start(unsigned short port, binary_msg_handler handler);
extern int ntyco_start(unsigned short port, binary_msg_handler handler);



// 二进制数据块
typedef struct {    
    void *data;     // 指向二进制数据
    size_t len;     // 数据长度
} kv_data_t;
// 底层数据结构二进制辅助函数
int kv_data_cmp(kv_data_t *a, kv_data_t *b);
unsigned long kv_data_hash(kv_data_t *key, int size);
int kv_data_dup(kv_data_t *dst, kv_data_t *src);
void kv_data_free(kv_data_t *data);
int kv_data_create(kv_data_t *data, void *src, size_t len);// 创建 kv_data_t
void kv_data_destroy(kv_data_t *data);// 释放 kv_data_t
int kv_data_compare(kv_data_t *a, kv_data_t *b);// 比较两个 kv_data_t
unsigned long kv_data_hash_func(kv_data_t *key, int size);// 哈希计算



//存储结构
#define ENABLE_ARRAY        1
#define ENABLE_RBTREE       1
#define ENABLE_HASH         1
#define ENABLE_SKIPLIST     1

#if ENABLE_ARRAY
typedef struct {
    kv_data_t key;
    kv_data_t value;
    int64_t expire_time; // 新增：绝对过期时间戳，0表示不过期
} kvs_array_item_t;

#define KVS_ARRAY_SIZE    1024

typedef struct kvs_array_s {//array结构体
    kvs_array_item_t *table;//指向动态分配的数组，这个数组用来存放键值
    int idx;//当前可插入元素的索引
    int total;//当前插入元素总数
} kvs_array_t;

int kvs_array_create(kvs_array_t *inst);
void kvs_array_destroy(kvs_array_t *inst);

int kvs_array_set(kvs_array_t *inst, kv_data_t *key, kv_data_t *value, int64_t expire_time);
kv_data_t* kvs_array_get(kvs_array_t *inst, kv_data_t *key);
int kvs_array_del(kvs_array_t *inst, kv_data_t *key);
int kvs_array_mod(kvs_array_t *inst, kv_data_t *key, kv_data_t *value, int64_t expire_time);
int kvs_array_exist(kvs_array_t *inst, kv_data_t *key);
void kvs_array_foreach(kvs_array_t *inst, void (*callback)(kv_data_t *key, kv_data_t *value, void *arg), void *arg);
int kvs_array_get_value_len(char *key_ptr, int key_len);

#endif

#if ENABLE_RBTREE

#define RED   0
#define BLACK 1

// 二进制安全红黑树节点
typedef struct _rbtree_node_binary {
    int color;
    struct _rbtree_node_binary *left;
    struct _rbtree_node_binary *right;
    struct _rbtree_node_binary *parent;

    kv_data_t key;                     
    kv_data_t value;                   
    int64_t expire_time;              
} rbtree_node_binary_t;

// 底层红黑树框架控制结构（内部转换使用）
typedef struct _rbtree_binary {
    rbtree_node_binary_t *root;        // 根节点指针
    rbtree_node_binary_t *nil;         // 哨兵边界节点（代替普通的 NULL，简化算法边界）
} rbtree_binary_t;

typedef rbtree_binary_t kvs_rbtree_t;// 面向公共接口的封装类型

int kvs_rbtree_create(kvs_rbtree_t *inst);
void kvs_rbtree_destroy(kvs_rbtree_t *inst);

// 修改：_set 和 _mod 增加 int64_t expire_time 参数
int kvs_rbtree_set(kvs_rbtree_t *inst, kv_data_t *key, kv_data_t *value, int64_t expire_time);
kv_data_t* kvs_rbtree_get(kvs_rbtree_t *inst, kv_data_t *key);
int kvs_rbtree_del(kvs_rbtree_t *inst, kv_data_t *key);
int kvs_rbtree_mod(kvs_rbtree_t *inst, kv_data_t *key, kv_data_t *value, int64_t expire_time);
int kvs_rbtree_exist(kvs_rbtree_t *inst, kv_data_t *key);
void kvs_rbtree_foreach(kvs_rbtree_t *inst,void (*callback)(kv_data_t *key, kv_data_t *value, void *arg),void *arg);
int kvs_rbtree_get_value_len(char *key_ptr, int key_len);

#endif

#if ENABLE_HASH

typedef struct hashnode_s {
    kv_data_t key;
    kv_data_t value;
    int64_t expire_time;     // 绝对过期时间戳(ms)，0表示不过期
    struct hashnode_s *next; // 链地址法处理哈希冲突
} hashnode_t;

typedef struct hashtable_s {
    hashnode_t **buckets; // 桶数组
    int max_slots;        // 桶的数量
    int count;            // 当前元素总数
} hashtable_t;
typedef hashtable_t kvs_hash_t;

int  kvs_hash_create(kvs_hash_t *hash);
void kvs_hash_destroy(kvs_hash_t *hash);

// 修改：_set 和 _mod 增加 int64_t expire_time 参数
int  kvs_hash_set(kvs_hash_t *hash, kv_data_t *key, kv_data_t *value, int64_t expire_time);
kv_data_t *kvs_hash_get(kvs_hash_t *hash, kv_data_t *key);
int  kvs_hash_mod(kvs_hash_t *hash, kv_data_t *key, kv_data_t *value, int64_t expire_time);
int  kvs_hash_del(kvs_hash_t *hash, kv_data_t *key);
int  kvs_hash_exist(kvs_hash_t *hash, kv_data_t *key);
int  kvs_hash_get_value_len(kvs_hash_t *hash, kv_data_t *key);
void kvs_hash_foreach(kvs_hash_t *hash, void (*callback)(kv_data_t *key, kv_data_t *value, void *arg), void *arg);

#endif


#if ENABLE_SKIPLIST

#define MAX_LEVEL 6

// 二进制安全跳表节点
typedef struct skipnode_binary_s {
    kv_data_t key;                     // 二进制安全键
    kv_data_t value;                   // 二进制安全值
    int64_t expire_time;               // 绝对过期时间戳(ms)，0表示不过期
    struct skipnode_binary_s **forward; // 向前指针数组
} skipnode_binary_t;

// 跳表结构体
typedef struct skiplist_binary_s {
    int level;                         // 当前最大层数
    skipnode_binary_t *header;         // 头节点
    int count;                         // 节点总数
} skiplist_binary_t;

// 对外类型
typedef skiplist_binary_t kvs_skip_t;

// 函数声明
int kvs_skip_create(kvs_skip_t *skip);
void kvs_skip_destroy(kvs_skip_t *skip);

// 修改：_set 和 _mod 增加 int64_t expire_time 参数
int kvs_skip_set(kvs_skip_t *skip, kv_data_t *key, kv_data_t *value, int64_t expire_time);
kv_data_t* kvs_skip_get(kvs_skip_t *skip, kv_data_t *key);
int kvs_skip_del(kvs_skip_t *skip, kv_data_t *key);
int kvs_skip_mod(kvs_skip_t *skip, kv_data_t *key, kv_data_t *value, int64_t expire_time);
int kvs_skip_exist(kvs_skip_t *skip, kv_data_t *key);

void kvs_skip_foreach(kvs_skip_t *skip, void (*callback)(kv_data_t *key, kv_data_t *value, void *arg), void *arg);
int kvs_skip_get_value_len(kvs_skip_t *skip, kv_data_t *key);

#endif

void *kvs_malloc(size_t size);
void kvs_free(void *ptr);

#endif