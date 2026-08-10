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
#include <errno.h>
#include "kvstore.h"
#include "expire.h"

#define AOF_BUF_SIZE        (64 * 1024 * 1024)     // 64MB 异步双缓冲区
#define IO_URING_QUEUE_DEPTH 128                   // SQ 队列深度，应对高并发批处理
#define AOF_MIN_FLUSH_SIZE  (64 * 1024)            // 64KB 批处理阈值
#define AOF_FLUSH_TIMEOUT_MS 20                    // 超时刷盘控制

static uint64_t g_last_flush_time_ms = 0; // 上次提交刷盘的时间戳


typedef struct {
    char *buf;
    size_t len;
} aof_buffer_t;

// 持久化上下文
static int aof_fd = -1;
static struct io_uring aof_ring;
// 双缓冲区架构
static aof_buffer_t aof_buf_active;
static aof_buffer_t aof_buf_flush;
static volatile int is_io_uring_busy = 0; 
static off_t aof_file_offset = 0;

// 前置声明
static void aof_trigger_flush_nolock(void);
static void aof_check_cqe_nonblock(void);

// AOF 初始化
int kvs_persistence_init(void) {
    aof_fd = open(PERSISTENCE_FILE, O_RDWR | O_CREAT | O_APPEND, 0644);
    if (aof_fd < 0) {
        fprintf(stderr, "[AOF ERROR] Failed to open persistence file: %s\n", strerror(errno));
        return -1;
    }

    struct stat st;
    if (fstat(aof_fd, &st) == 0) {
        aof_file_offset = st.st_size;
    } else {
        aof_file_offset = 0;
    }

    is_io_uring_busy = 0;

    // 分配双缓冲区内存
    aof_buf_active.buf = (char *)kvs_malloc(AOF_BUF_SIZE);
    aof_buf_active.len = 0;
    aof_buf_flush.buf = (char *)kvs_malloc(AOF_BUF_SIZE);
    aof_buf_flush.len = 0;

    if (!aof_buf_active.buf || !aof_buf_flush.buf) {
        fprintf(stderr, "[AOF ERROR] Failed to allocate AOF buffers\n");
        if (aof_buf_active.buf) kvs_free(aof_buf_active.buf);
        if (aof_buf_flush.buf) kvs_free(aof_buf_flush.buf);
        close(aof_fd);
        return -1;
    }

    // 初始化 io_uring 内核异步队列
    if (io_uring_queue_init(IO_URING_QUEUE_DEPTH, &aof_ring, 0) < 0) {
        fprintf(stderr, "[AOF ERROR] Failed to init io_uring: %s\n", strerror(errno));
        kvs_free(aof_buf_active.buf);
        kvs_free(aof_buf_flush.buf);
        close(aof_fd);
        return -1;
    }

    return 0;
}

// 触发双缓冲交换与 io_uring 刷盘
static void aof_trigger_flush_nolock(void) {
    if (aof_buf_active.len == 0 || is_io_uring_busy) {
        return;
    }

    // 1. 原子交换双缓冲区 (Swap)
    aof_buffer_t temp = aof_buf_active;
    aof_buf_active = aof_buf_flush;
    aof_buf_flush = temp;

    size_t flush_len = aof_buf_flush.len;
    aof_buf_active.len = 0; // 重置 active 缓冲区以接收新日志

    // 2. 设置 busy 标志位
    __sync_lock_test_and_set(&is_io_uring_busy, 1);

    // 3. 获取内核 SQE 槽位投递异步追加任务
    struct io_uring_sqe *sqe = io_uring_get_sqe(&aof_ring);
    if (sqe) {
        io_uring_prep_write(sqe, aof_fd, aof_buf_flush.buf, flush_len, aof_file_offset);
        io_uring_sqe_set_data(sqe, (void *)(uintptr_t)flush_len);
        aof_file_offset += flush_len; // 提前推进文件偏移
        io_uring_submit(&aof_ring);
    } else {
        // SQE 满时清除 busy 标志，下一次调用这个函数再次尝试提交
        __sync_lock_release(&is_io_uring_busy);
    }
}

// 收割 CQE 后顺延刷盘，64KB 阀值控制
static void aof_check_cqe_nonblock(void) {
    if (!is_io_uring_busy) return;

    struct io_uring_cqe *cqe;
    int found = 0;
    while (io_uring_peek_cqe(&aof_ring, &cqe) == 0 && cqe != NULL) {
        if (cqe->res < 0) {
            fprintf(stderr, "[AOF ERROR] Async write failed: %s\n", strerror(-cqe->res));
        }
        io_uring_cqe_seen(&aof_ring, cqe);
        found = 1;
    }

    if (found) {
        __sync_lock_release(&is_io_uring_busy);
        aof_buf_flush.len = 0;

        // 增加阀值判断，防止 CQE 完成后立刻平刷几字节的 active 数据
        if (aof_buf_active.len >= AOF_MIN_FLUSH_SIZE) {
            aof_trigger_flush_nolock();
        }
    }
}

// 写日志函数：纯内存追加
void kvs_persistence_write(const void *data, int len) {
    if (aof_fd < 0 || !data || len <= 0) return;

    // 如果 active 缓冲区装满了，尝试触发一次刷盘
    if (aof_buf_active.len + len > AOF_BUF_SIZE) {
        aof_check_cqe_nonblock();
        if (!is_io_uring_busy) {
            aof_trigger_flush_nolock();
        }
    }

    // 纯内存拷贝，耗时小于 50ns
    memcpy(aof_buf_active.buf + aof_buf_active.len, data, len);
    aof_buf_active.len += len;

    
}

// 批量刷盘（容量 + 超时双控制）
void kvs_persistence_flush_pending(void) {
    if (aof_fd < 0) return;

    // 1. 先收割可能完成的 CQE
    aof_check_cqe_nonblock();

    uint64_t now = get_current_ms();

    // 2. 判断触发刷盘的条件：
    // 条件 A：积攒数据超过 64KB (高并发场景)
    // 条件 B：当前有数据，且距离上次刷盘已经超过 10ms (低并发或压测尾部场景)
    int need_flush_by_size = (aof_buf_active.len >= AOF_MIN_FLUSH_SIZE);
    int need_flush_by_time = (aof_buf_active.len > 0 && (now - g_last_flush_time_ms >= AOF_FLUSH_TIMEOUT_MS));

    if (!is_io_uring_busy && (need_flush_by_size || need_flush_by_time)) {
        aof_trigger_flush_nolock();
        g_last_flush_time_ms = now; // 更新刷盘时间
    }
}


void kvs_persistence_recover(void) {
    if (aof_fd < 0) {
        aof_fd = open(PERSISTENCE_FILE, O_RDONLY);
        if (aof_fd < 0) {
            printf("[AOF] No AOF file found, skip recovery\n");
            return;
        }
    }

    struct stat st;
    if (fstat(aof_fd, &st) < 0 || st.st_size <= 0) {
        printf("[AOF] AOF file empty (size=%ld), skip recovery\n", st.st_size);
        return;
    }

    size_t file_size = st.st_size;
    printf("[AOF] AOF file size: %zu bytes\n", file_size);

    unsigned char *data = (unsigned char *)mmap(NULL, file_size, PROT_READ | PROT_WRITE, MAP_PRIVATE, aof_fd, 0);
    if (data == MAP_FAILED) {
        fprintf(stderr, "[AOF ERROR] mmap failed during recovery: %s\n", strerror(errno));
        return;
    }

    int processed = protocol_process_recover((char *)data, file_size);

    munmap(data, file_size);
    aof_file_offset = file_size;

    fprintf(stdout, "[AOF] Log recovery completed. Total processed: %d bytes\n", processed);
    fflush(stdout);

    close(aof_fd);
    aof_fd = open(PERSISTENCE_FILE, O_RDWR | O_CREAT | O_APPEND, 0644);
    if (aof_fd < 0) {
        fprintf(stderr, "[AOF ERROR] Failed to reopen AOF file after recovery: %s\n", strerror(errno));
    }
}


// 强制刷盘
void kvs_persistence_force_flush(void) {
    if (aof_fd < 0) return;

    aof_check_cqe_nonblock();

    if (!is_io_uring_busy && aof_buf_active.len > 0) {
        aof_trigger_flush_nolock();
        g_last_flush_time_ms = get_current_ms();
    }
    
}

// 销毁与收尾落盘 
void kvs_persistence_close(void) {
    
    // 步骤 1：收割所有已完成的 CQE
    aof_check_cqe_nonblock();

    // 步骤 2：如果 io_uring 空闲且 active 有数据，触发交换
    if (!is_io_uring_busy && aof_buf_active.len > 0) {
        aof_trigger_flush_nolock();
    }

    // 步骤 3：等待所有正在进行的 io_uring 操作完成
    while (is_io_uring_busy) {
        struct io_uring_cqe *cqe;
        int ret = io_uring_wait_cqe(&aof_ring, &cqe);
        if (ret == 0 && cqe != NULL) {
            if (cqe->res < 0) {
                fprintf(stderr, "[AOF ERROR] Async write error during close: %s\n", 
                        strerror(-cqe->res));
            }
            io_uring_cqe_seen(&aof_ring, cqe);
        } else {
            break;
        }
    }
    is_io_uring_busy = 0;
    aof_buf_flush.len = 0;

    // 步骤 4：处理 active 缓冲区中的剩余数据
    if (aof_buf_active.len > 0) {
        struct io_uring_sqe *sqe = io_uring_get_sqe(&aof_ring);
        if (sqe) {
            io_uring_prep_write(sqe, aof_fd, aof_buf_active.buf, 
                               aof_buf_active.len, aof_file_offset);
            aof_file_offset += aof_buf_active.len;
            io_uring_submit(&aof_ring);
            
            struct io_uring_cqe *cqe;
            int ret = io_uring_wait_cqe(&aof_ring, &cqe);
            if (ret == 0 && cqe != NULL) {
                if (cqe->res < 0) {
                    fprintf(stderr, "[AOF ERROR] Final write failed: %s\n", 
                            strerror(-cqe->res));
                    ssize_t written = pwrite(aof_fd, aof_buf_active.buf, 
                                             aof_buf_active.len, 
                                             aof_file_offset - aof_buf_active.len);
                    if (written < 0) {
                        fprintf(stderr, "[AOF ERROR] Final sync fallback also failed: %s\n", 
                                strerror(errno));
                    }
                }
                io_uring_cqe_seen(&aof_ring, cqe);
            }
        } else {
            ssize_t written = pwrite(aof_fd, aof_buf_active.buf, 
                                     aof_buf_active.len, aof_file_offset);
            if (written < 0) {
                fprintf(stderr, "[AOF ERROR] Final sync write failed: %s\n", 
                        strerror(errno));
            } else {
                aof_file_offset += written;
            }
        }
        aof_buf_active.len = 0;
    }

    // 步骤 5：强制同步到磁盘
    if (aof_fd >= 0) {
        if (fdatasync(aof_fd) < 0) {
            fprintf(stderr, "[AOF ERROR] fdatasync failed: %s\n", strerror(errno));
        }
    }

    // 步骤 6：资源清理
    if (aof_buf_active.buf) {
        kvs_free(aof_buf_active.buf);
        aof_buf_active.buf = NULL;
    }
    if (aof_buf_flush.buf) {
        kvs_free(aof_buf_flush.buf);
        aof_buf_flush.buf = NULL;
    }
    io_uring_queue_exit(&aof_ring);
    if (aof_fd >= 0) {
        close(aof_fd);
        aof_fd = -1;
    }
}