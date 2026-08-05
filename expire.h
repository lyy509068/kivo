#ifndef _EXPIRE_H_
#define _EXPIRE_H_

#include <stdint.h>
#include <pthread.h>
#include "kvstore.h"

/* ---------- 数据结构类型标记 ---------- */
#define EXPIRE_TYPE_ARRAY    0
#define EXPIRE_TYPE_HASH     1
#define EXPIRE_TYPE_RBTREE   2
#define EXPIRE_TYPE_SKIPLIST 3

/* 定时器堆节点：值拷贝 Key，附带类型和版本号 */
typedef struct {
    kv_data_t key;
    int64_t   expire_time;   // 过期时间戳(ms)
    uint64_t  version;       // 版本号，防止 SET EX DEL SET EX 的误删
    int       type;          // 数据结构类型，上面四个宏之一
} expire_item_t;

/* 队列大小必须是2的幂，便于位运算取模 */
#define EXPIRE_QUEUE_SIZE  65536
#define EXPIRE_QUEUE_MASK  (EXPIRE_QUEUE_SIZE - 1)

/* 无锁 SPSC 环形队列 (单生产者单消费者) */
typedef struct {
    expire_item_t items[EXPIRE_QUEUE_SIZE];
    volatile uint32_t head __attribute__((aligned(64)));
    volatile uint32_t tail __attribute__((aligned(64)));
} expire_queue_t;

/* 公开 API */
int64_t get_current_ms(void);
int  expire_system_init(void);
void expire_system_destroy(void);

/* 业务层在 SET EX 成功后调用，向 expire 线程注册一个到期任务 */
int  expire_push_cmd(int type, kv_data_t *key, int64_t expire_time, uint64_t version);

/* 主线程协程定期调用，消费 delete_queue，执行真正的删除、日志、复制 */
void expire_process_deletes(void);

#endif