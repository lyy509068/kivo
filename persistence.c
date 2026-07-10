// 修改：整体要写成resp格式
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

    // 收割已经完成的异步 I/O 请求
    aof_check_cqe_nonblock();

    // 如果单条命令大于缓冲区剩余空间，先强制把当前积压的刷出
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

    // 将数据追加到活跃的用户态缓冲区
    memcpy(aof_buf_active.buf + aof_buf_active.len, data, len);
    aof_buf_active.len += len;

    // 只要缓冲区有数据且 io_uring 空闲，就立刻推给内核异步去写
    if (!is_io_uring_busy) {
        aof_trigger_flush_nolock();
    } else {
        pwrite(aof_fd, aof_buf_active.buf, aof_buf_active.len, aof_file_offset);
        aof_file_offset += aof_buf_active.len;
        aof_buf_active.len = 0;
    }

    pthread_mutex_unlock(&aof_mutex);
}

void kvs_persistence_recover(void) {
    if (aof_fd < 0) return;

    pthread_mutex_lock(&aof_mutex);

    struct stat st;
    if (fstat(aof_fd, &st) < 0 || st.st_size <= 0) {
        printf("[AOF] No data to recover or file error\n");
        pthread_mutex_unlock(&aof_mutex);
        return; 
    }
    size_t file_size = st.st_size;

    // mmap 零拷贝映射整个 AOF 文件
    unsigned char *data = mmap(NULL, file_size, PROT_READ, MAP_SHARED, aof_fd, 0);
    if (data == MAP_FAILED) {
        printf("[AOF Critical] mmap failed during recovery!\n");
        pthread_mutex_unlock(&aof_mutex);
        return;
    }

    printf("[AOF] Starting recovery, file size: %zu bytes\n", file_size);

    // 调用协议层的恢复函数处理所有命令
    int processed = protocol_process_recover((const char *)data, file_size);
    
    munmap(data, file_size);
    aof_file_offset = file_size;

    printf("[AOF] Recovery completed: processed %d bytes\n", processed);
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
