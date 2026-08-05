#include "expire.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <sys/time.h>

/* ---------- 全局对象 ---------- */
static expire_queue_t g_cmd_queue = {0};
static expire_queue_t g_delete_queue = {0};
static pthread_t g_expire_tid = 0;
static volatile int g_expire_running = 0;

/* ---------- 外部存储引擎全局变量 ---------- */
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

/* 协议层内部删除日志接口 */
extern void protocol_handle_internal_del(const char *cmd, const void *key, int key_len);

/* ---------- 辅助函数 ---------- */
int64_t get_current_ms(void) {
    struct timeval tv;
    gettimeofday(&tv, NULL);
    return (int64_t)tv.tv_sec * 1000 + tv.tv_usec / 1000;
}

/* ========== 无锁 SPSC 队列操作 ========== */
static int queue_push(expire_queue_t *q, expire_item_t *item) {
    uint32_t head = __atomic_load_n(&q->head, __ATOMIC_RELAXED);
    uint32_t tail = __atomic_load_n(&q->tail, __ATOMIC_ACQUIRE);

    if (((head + 1) & EXPIRE_QUEUE_MASK) == tail) {
        return -1; // 队列满
    }

    q->items[head] = *item;  // 浅拷贝（key.data 指向的是堆内存，由调用方保证生命周期）
    __atomic_store_n(&q->head, (head + 1) & EXPIRE_QUEUE_MASK, __ATOMIC_RELEASE);
    return 0;
}

static int queue_pop(expire_queue_t *q, expire_item_t *item) {
    uint32_t tail = __atomic_load_n(&q->tail, __ATOMIC_RELAXED);
    uint32_t head = __atomic_load_n(&q->head, __ATOMIC_ACQUIRE);

    if (tail == head) {
        return -1; // 队列空
    }

    *item = q->items[tail];
    __atomic_store_n(&q->tail, (tail + 1) & EXPIRE_QUEUE_MASK, __ATOMIC_RELEASE);
    return 0;
}

/* ========== 最小堆（仅供 expire 线程操作） ========== */
typedef struct {
    expire_item_t *data;
    int size;
    int capacity;
} min_heap_t;

static min_heap_t g_heap = {NULL, 0, 0};

static void heap_init(int init_cap) {
    g_heap.capacity = init_cap > 1024 ? init_cap : 1024;
    g_heap.data = (expire_item_t *)malloc(sizeof(expire_item_t) * g_heap.capacity);
    g_heap.size = 0;
}

static void heap_swap(int i, int j) {
    expire_item_t tmp = g_heap.data[i];
    g_heap.data[i] = g_heap.data[j];
    g_heap.data[j] = tmp;
}

static void heap_push(expire_item_t *item) {
    if (g_heap.size >= g_heap.capacity) {
        g_heap.capacity *= 2;
        g_heap.data = (expire_item_t *)realloc(g_heap.data, sizeof(expire_item_t) * g_heap.capacity);
    }
    g_heap.data[g_heap.size] = *item;
    int curr = g_heap.size++;

    // 上浮
    while (curr > 0) {
        int parent = (curr - 1) / 2;
        if (g_heap.data[curr].expire_time >= g_heap.data[parent].expire_time) break;
        heap_swap(curr, parent);
        curr = parent;
    }
}

static int heap_pop(expire_item_t *out) {
    if (g_heap.size == 0) return -1;
    *out = g_heap.data[0];
    g_heap.data[0] = g_heap.data[--g_heap.size];

    // 下沉
    int curr = 0;
    while (1) {
        int left = 2 * curr + 1, right = 2 * curr + 2, smallest = curr;
        if (left < g_heap.size && g_heap.data[left].expire_time < g_heap.data[smallest].expire_time)
            smallest = left;
        if (right < g_heap.size && g_heap.data[right].expire_time < g_heap.data[smallest].expire_time)
            smallest = right;
        if (smallest == curr) break;
        heap_swap(curr, smallest);
        curr = smallest;
    }
    return 0;
}

/* ========== expire 后台线程主函数 ========== */
static void* expire_thread_routine(void *arg) {
    heap_init(4096);
    expire_item_t item;

    while (g_expire_running) {
        // 1. 从 cmd_queue 中消费全部新命令，放入最小堆
        while (queue_pop(&g_cmd_queue, &item) == 0) {
            heap_push(&item);
        }

        int64_t now = get_current_ms();
        int did_work = 0;

        // 2. 将堆顶所有到期项移入 delete_queue
        while (g_heap.size > 0 && g_heap.data[0].expire_time <= now) {
            heap_pop(&item);
            // 如果 delete_queue 满了，短暂等待后重试
            while (queue_push(&g_delete_queue, &item) != 0) {
                usleep(50);
            }
            did_work = 1;
        }

        // 空闲时休眠 1ms，避免 CPU 空转
        if (!did_work) {
            usleep(1000);
        }
    }
    return NULL;
}

/* ========== 公开 API ========== */

int expire_system_init(void) {
    g_expire_running = 1;
    return pthread_create(&g_expire_tid, NULL, expire_thread_routine, NULL);
}

void expire_system_destroy(void) {
    g_expire_running = 0;
    pthread_join(g_expire_tid, NULL);
    free(g_heap.data);
}

/* 业务层调用：向 cmd_queue 中推入一个到期任务 */
int expire_push_cmd(int type, kv_data_t *key, int64_t expire_time, uint64_t version) {
    expire_item_t item;
    item.expire_time = expire_time;
    item.version    = version;
    item.type       = type;

    // 深拷贝 key，因为调用方持有的内存可能马上失效
    item.key.len = key->len;
    item.key.data = (char *)kvs_malloc(key->len);
    if (!item.key.data) return -1;
    memcpy(item.key.data, key->data, key->len);

    if (queue_push(&g_cmd_queue, &item) != 0) {
        kvs_free(item.key.data);
        return -1; // 队列满
    }
    return 0;
}

/* 条件删除辅助函数：由 expire_process_deletes 内部使用
 * 返回 1 表示真正删除了，0 表示 key 不存在或 expire_time 不匹配
 */
static int engine_del_if_expired(int type, kv_data_t *key, int64_t expected_expire) {
    switch (type) {

        case EXPIRE_TYPE_ARRAY:
            return kvs_array_del_if_expired(&global_array, key, expected_expire);

        case EXPIRE_TYPE_HASH:
            return kvs_hash_del_if_expired(&global_hash, key, expected_expire);

        case EXPIRE_TYPE_RBTREE:
            return kvs_rbtree_del_if_expired(&global_rbtree, key, expected_expire);

        case EXPIRE_TYPE_SKIPLIST:
            return kvs_skip_del_if_expired(&global_skip, key, expected_expire);

        default:
            return 0;
    }
}

/* 主线程协程调用：消费 delete_queue，执行真正的删除 */
void expire_process_deletes(void) {
    expire_item_t item;
    int cnt = 0;
    // 每次最多处理 500 个，防止主线程阻塞太久
    while (cnt < 500 && queue_pop(&g_delete_queue, &item) == 0) {// 这里已经是删除队列的数据了 为什么还要在进行条件删除呢？？？不能直接删除呢
        cnt++;

        // 条件删除（防旧 timer 误删）
        int deleted = engine_del_if_expired(item.type, &item.key, item.expire_time);

        if (deleted) {
            // 根据引擎类型选择正确的命令字符串
            const char *cmd = NULL;
            switch (item.type) {
            case EXPIRE_TYPE_ARRAY:    cmd = "DEL";  break;
            case EXPIRE_TYPE_HASH:     cmd = "HDEL"; break;
            case EXPIRE_TYPE_RBTREE:   cmd = "RDEL"; break;
            case EXPIRE_TYPE_SKIPLIST: cmd = "SDEL"; break;
            default: cmd = "DEL"; break;
            }
            // 交给协议层写 AOF 和主从复制
            protocol_handle_internal_del(cmd, item.key.data, (int)item.key.len);
        }

        // 释放队列中 key 的内存
        kvs_free(item.key.data);
    }
}