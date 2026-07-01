
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>   
#include <sys/time.h> 
#include <pthread.h>
#include <unistd.h>
#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <liburing.h>
#include "kvstore.h"

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

// AOF 异步双缓冲与 io_uring 上下文
#define AOF_BUF_SIZE (4 * 1024 * 1024) 

typedef struct {
    char *buf;
    size_t len;
} aof_buffer_t;

static int aof_fd = -1;
static struct io_uring aof_ring;
static pthread_mutex_t aof_mutex = PTHREAD_MUTEX_INITIALIZER;

// 双缓冲区：一个负责接收当前写入(active)，另一个负责交给 io_uring 刷盘(flush)
static aof_buffer_t aof_buf_active;
static aof_buffer_t aof_buf_flush;
static int is_io_uring_busy = 0; // 标记 io_uring 是否正在刷盘
static off_t aof_file_offset = 0; // 记录当前文件追加的绝对偏移量

static int64_t get_current_ms_aof(void) {
    struct timeval tv;
    gettimeofday(&tv, NULL);
    return (int64_t)tv.tv_sec * 1000 + tv.tv_usec / 1000;
}

// 初始化 AOF 环境
int kvs_persistence_init(void) {
    // 1. 以读写、追加、创建模式打开文件描述符 fd
    aof_fd = open(PERSISTENCE_FILE, O_RDWR | O_CREAT | O_APPEND, 0644);
    if (aof_fd < 0) {
        printf("Failed to open persistence file via open()\n");
        return -1;
    }

    // 获取当前文件大小，作为 io_uring 写入的初始偏移量
    struct stat st;
    if (fstat(aof_fd, &st) == 0) {
        aof_file_offset = st.st_size;
    }else {
        aof_file_offset = 0;
    }

    is_io_uring_busy=0;

    // 2. 初始化双缓冲区
    aof_buf_active.buf = kvs_malloc(AOF_BUF_SIZE);
    aof_buf_active.len = 0;
    aof_buf_flush.buf = kvs_malloc(AOF_BUF_SIZE);
    aof_buf_flush.len = 0;

    // 3. 初始化 io_uring (深度为 4 足够 AOF 线性单发)
    if (io_uring_queue_init(4, &aof_ring, 0) < 0) {//identifier "of_ring" is undefined
        printf("Failed to init io_uring for AOF\n");
        close(aof_fd);
        return -1;
    }

    return 0;
}

// 内部函数：真正触发 io_uring 异步刷盘（调用时需持有 aof_mutex）
static void aof_trigger_flush_nolock(void) {
    // 如果没有数据需要刷，或者内核正在处理上一批数据，则直接返回
    if (aof_buf_active.len == 0 || is_io_uring_busy) {
        return;
    }

    // 交换双缓冲区 (Swap)
    aof_buffer_t temp = aof_buf_active;
    aof_buf_active = aof_buf_flush;
    aof_buf_flush = temp;

    // 重置激活缓冲区，准备接收新数据
    aof_buf_active.len = 0;
    is_io_uring_busy = 1;

    // 提交给 io_uring 异步写入
    struct io_uring_sqe *sqe = io_uring_get_sqe(&aof_ring);
    if (sqe) {
        io_uring_prep_write(sqe, aof_fd, aof_buf_flush.buf, aof_buf_flush.len, aof_file_offset);
        aof_file_offset += aof_buf_flush.len; // 推进文件偏移量
        io_uring_submit(&aof_ring);
    } else {
        // 退化处理：如果 SQE 获取失败，同步写入防止数据丢失
        write(aof_fd, aof_buf_flush.buf, aof_buf_flush.len);
        is_io_uring_busy = 0;
    }
}

// 检查 io_uring 是否完成刷盘，收割结果（非阻塞）
static void aof_check_cqe_nonblock(void) {
    if (!is_io_uring_busy) return;

    struct io_uring_cqe *cqe;
    // 使用 peek 检查，绝不阻塞主线程
    if (io_uring_peek_cqe(&aof_ring, &cqe) == 0 && cqe != NULL) {
        if (cqe->res < 0) {
            printf("[AOF io_uring] Async append failed: %d\n", cqe->res);
        }
        io_uring_cqe_seen(&aof_ring, cqe);
        is_io_uring_busy = 0; // 释放状态锁
        
        // 顺便检查刚刚在 busy 期间，active 缓冲区有没有积压数据，有的话连带刷出
        if (aof_buf_active.len > 0) {
            aof_trigger_flush_nolock();
        }
    }
}

// 执行一次写命令调用一次：写入内存 Buffer，必要时交由 io_uring 异步落盘
void kvs_persistence_write(const void *data, int len) {
    if (aof_fd < 0 || !data || len <= 0) return;

    pthread_mutex_lock(&aof_mutex);

    // 1. 顺便收割一下已经完成的异步 I/O 请求
    aof_check_cqe_nonblock();

    // 2. 如果单条命令大于缓冲区剩余空间，先强制把当前积压的刷出去
    if (aof_buf_active.len + len > AOF_BUF_SIZE) {
        // 如果 flush 缓冲区还没释放（内核还在写），主线程稍微等一下
        while (is_io_uring_busy) {
            pthread_mutex_unlock(&aof_mutex);
            usleep(100); // 微秒级微调等待
            pthread_mutex_lock(&aof_mutex);
            aof_check_cqe_nonblock();
        }
        aof_trigger_flush_nolock();
    }

    // 3. 将数据追加到活跃的用户态缓冲区
    memcpy(aof_buf_active.buf + aof_buf_active.len, data, len);
    aof_buf_active.len += len;

    // 4. 策略：Redis 默认的每一秒刷盘(everysec)或有积压就触发异步落盘
    // 这里采取：只要缓冲区有数据且 io_uring 空闲，就立刻推给内核异步去写
    if (!is_io_uring_busy) {
        aof_trigger_flush_nolock();
    } else {
        pwrite(aof_fd, aof_buf_active.buf, aof_buf_active.len, aof_file_offset);
        aof_file_offset += aof_buf_active.len;
        aof_buf_active.len = 0;
    }

    pthread_mutex_unlock(&aof_mutex);
}

// 加载，使用 mmap 零拷贝高速读取重放
void kvs_persistence_recover(void) {
    if (aof_fd < 0) return;

    pthread_mutex_lock(&aof_mutex);

    // 获取 AOF 文件大小
    struct stat st;
    if (fstat(aof_fd, &st) < 0 || st.st_size <= 0) {
        pthread_mutex_unlock(&aof_mutex);
        return; 
    }
    size_t file_size = st.st_size;

    // 将整个 AOF 日志映射到内存空间
    unsigned char *mmap_addr = mmap(NULL, file_size, PROT_READ, MAP_SHARED, aof_fd, 0);
    if (mmap_addr == MAP_FAILED) {
        printf("[AOF Critical] mmap failed during recovery!\n");
        pthread_mutex_unlock(&aof_mutex);
        return;
    }

    int recovered_count = 0;
    int expired_cleanup_count = 0; 
    int64_t now = get_current_ms_aof();
    size_t p = 0; // mmap 虚拟内存解析指针

    // 线性解析映射的内存空间
    while (p < file_size) {
        int cmd_len = 0, key_len = 0, val_len = 0;
        int64_t expire_time = 0;

        // 安全边界检查：防止读取越界
        if (p + sizeof(int) > file_size) break;
        cmd_len = *(int *)(mmap_addr + p);
        p += sizeof(int);
        
        if (cmd_len <= 0 || p + cmd_len + sizeof(int64_t) + sizeof(int) > file_size) {
            printf("[AOF Warning] Corrupted command structure or EOF. Stopping.\n");
            break;
        }

        // 直接拿到 cmd 字符串指针（零拷贝，注意尾部边界）
        char cmd[32] = {0};
        if (cmd_len < 32) {
            memcpy(cmd, mmap_addr + p, cmd_len);
        }
        p += cmd_len;

        // 读取过期时间
        expire_time = *(int64_t *)(mmap_addr + p);
        p += sizeof(int64_t);

        int is_write_cmd = (strcmp(cmd, "SET") == 0 || strcmp(cmd, "MOD") == 0 || 
                            strcmp(cmd, "RSET") == 0 || strcmp(cmd, "RMOD") == 0 ||
                            strcmp(cmd, "HSET") == 0 || strcmp(cmd, "HMOD") == 0 || 
                            strcmp(cmd, "SSET") == 0 || strcmp(cmd, "SMOD") == 0);

        // 读取 Key 长度
        key_len = *(int *)(mmap_addr + p);
        p += sizeof(int);

        if (key_len <= 0 || p + key_len + sizeof(int) > file_size) break;

        // 零拷贝：Key 直接指向 mmap 映射的内存
        void *key = (void *)(mmap_addr + p);
        p += key_len;

        // 读取 Value 长度
        val_len = *(int *)(mmap_addr + p);
        p += sizeof(int);

        if (val_len < 0 || p + val_len > file_size) break;

        // 零拷贝：Value 直接指向 mmap 映射的内存
        void *val = (val_len > 0) ? (void *)(mmap_addr + p) : NULL;
        p += val_len;

        // 过期拦截拦截
        if (is_write_cmd && expire_time > 0 && now > expire_time) {
            expired_cleanup_count++;
            continue; 
        }

        void *safe_key = kvs_malloc(key_len);
        memcpy(safe_key, key, key_len);

        void *safe_val = NULL;
        if (val_len > 0 && val) {
            safe_val = kvs_malloc(val_len);
            memcpy(safe_val, val, val_len);
        }

        kv_data_t kv_k = {safe_key, key_len};
        kv_data_t kv_v = {safe_val, val_len};

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

        recovered_count++;
    }

    // 恢复完成，解除映射
    munmap(mmap_addr, file_size);

    aof_file_offset = file_size;

    printf("[AOF mmap] Recovery finished: %d commands replayed (Purged %d expired logs)\n", 
           recovered_count, expired_cleanup_count);

    pthread_mutex_unlock(&aof_mutex);
}

// 关闭并销毁 AOF 资源
void kvs_persistence_close(void) {
    pthread_mutex_lock(&aof_mutex);

    // 1. 如果 active 缓冲区有数据，先触发刷盘
    if (aof_buf_active.len > 0) {
        aof_trigger_flush_nolock();
    }

    // 2. 等待所有 io_uring 请求完成
    while (is_io_uring_busy) {
        struct io_uring_cqe *cqe;
        int ret = io_uring_wait_cqe(&aof_ring, &cqe);
        if (ret == 0 && cqe != NULL) {
            if (cqe->res < 0) {
                printf("[AOF] Async write error during close: %d\n", cqe->res);
            }
            io_uring_cqe_seen(&aof_ring, cqe);
            is_io_uring_busy = 0;
        } else {
            break;
        }
    }

    // 3. 双重保险：同步写入剩余数据
    if (aof_buf_active.len > 0) {
        pwrite(aof_fd, aof_buf_active.buf, aof_buf_active.len, aof_file_offset);
        aof_buf_active.len = 0;
    }

    // 4. 强制刷新到磁盘
    if (aof_fd >= 0) {
        fdatasync(aof_fd);
    }

    // 5. 清理
    if (aof_buf_active.buf) kvs_free(aof_buf_active.buf);
    if (aof_buf_flush.buf) kvs_free(aof_buf_flush.buf);
    io_uring_queue_exit(&aof_ring);
    if (aof_fd >= 0) {
        close(aof_fd);
        aof_fd = -1;
    }
    pthread_mutex_unlock(&aof_mutex);
}
