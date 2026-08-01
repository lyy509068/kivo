#include "kvstore.h"
#include <stdio.h>
#include <string.h>
#include <pthread.h>
#include <unistd.h>
#include <stdint.h>   
#include <sys/time.h> 
#include <stddef.h> 
#include <liburing.h>
#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>

// 用于二进制快照的引擎标识
#define SNAP_TYPE_ARRAY    1
#define SNAP_TYPE_RBTREE   2
#define SNAP_TYPE_HASH     3
#define SNAP_TYPE_SKIPLIST 4

typedef struct {
    unsigned char *data;
    size_t capacity;
    size_t offset;
} snapshot_buffer_t;

static uint64_t kvs_crc64(uint64_t crc, const unsigned char *buf, size_t len) {
    if (crc == 0) {
        crc = 14695981039346656037ULL;
    }
    for (size_t i = 0; i < len; i++) {
        crc ^= buf[i];
        crc *= 1099511628211ULL;
    }
    return crc;
}

// 统一的内存追加函数
static void buffer_append(snapshot_buffer_t *buf, const void *ptr, size_t size) {
    if (buf->offset + size > buf->capacity) {
        // 确保扩容后的容量一定能装下新数据
        while (buf->offset + size > buf->capacity) {
            buf->capacity *= 2;
        }
        buf->data = kvs_realloc(buf->data, buf->capacity);
    }
    // 拷贝到缓冲区
    memcpy(buf->data + buf->offset, ptr, size);
    buf->offset += size;
}

static int64_t get_current_ms_snapshot(void) {
    struct timeval tv;
    gettimeofday(&tv, NULL);
    return (int64_t)tv.tv_sec * 1000 + tv.tv_usec / 1000;
}

#if ENABLE_ARRAY
static void snapshot_write_array_cb(kv_data_t *key, kv_data_t *value, void *arg) {
    snapshot_buffer_t *buf = (snapshot_buffer_t*)arg;
    int type = SNAP_TYPE_ARRAY;
    
    kvs_array_item_t *item = (kvs_array_item_t *)((char *)key - offsetof(kvs_array_item_t, key));
    int64_t expire_time = item->expire_time;

    buffer_append(buf, &type, sizeof(int));
    buffer_append(buf, &expire_time, sizeof(int64_t)); 
    buffer_append(buf, &key->len, sizeof(int));
    buffer_append(buf, key->data, key->len);
    buffer_append(buf, &value->len, sizeof(int));
    buffer_append(buf, value->data, value->len);
}
#endif 

#if ENABLE_RBTREE
static void snapshot_write_rbtree_cb(kv_data_t *key, kv_data_t *value, void *arg) {
    snapshot_buffer_t *buf = (snapshot_buffer_t*)arg;
    int type = SNAP_TYPE_RBTREE;
    
    rbtree_node_binary_t *node = (rbtree_node_binary_t *)((char *)key - offsetof(rbtree_node_binary_t, key));
    int64_t expire_time = node->expire_time; 

    buffer_append(buf, &type, sizeof(int));
    buffer_append(buf, &expire_time, sizeof(int64_t)); 
    buffer_append(buf, &key->len, sizeof(int));
    buffer_append(buf, key->data, key->len);
    buffer_append(buf, &value->len, sizeof(int));
    buffer_append(buf, value->data, value->len);
}
#endif 

#if ENABLE_HASH
static void snapshot_write_hash_cb(kv_data_t *key, kv_data_t *value, void *arg) {
    snapshot_buffer_t *buf = (snapshot_buffer_t*)arg;
    int type = SNAP_TYPE_HASH;
    
    hashnode_t *node = (hashnode_t *)((char *)key - offsetof(hashnode_t, key));
    int64_t expire_time = node->expire_time;

    buffer_append(buf, &type, sizeof(int));
    buffer_append(buf, &expire_time, sizeof(int64_t)); 
    buffer_append(buf, &key->len, sizeof(int));
    buffer_append(buf, key->data, key->len);
    buffer_append(buf, &value->len, sizeof(int));
    buffer_append(buf, value->data, value->len);
}
#endif 

#if ENABLE_SKIPLIST
static void snapshot_write_skip_cb(kv_data_t *key, kv_data_t *value, void *arg) {
    snapshot_buffer_t *buf = (snapshot_buffer_t*)arg;
    int type = SNAP_TYPE_SKIPLIST; 

    skipnode_binary_t *node = (skipnode_binary_t *)((char *)key - offsetof(skipnode_binary_t, key));
    int64_t expire_time = node->expire_time;

    buffer_append(buf, &type, sizeof(int));
    buffer_append(buf, &expire_time, sizeof(int64_t)); 
    buffer_append(buf, &key->len, sizeof(int));
    buffer_append(buf, key->data, key->len);
    buffer_append(buf, &value->len, sizeof(int));
    buffer_append(buf, value->data, value->len);
}
#endif

// 保存二进制快照：使用 io_uring 异步落盘 + 严格的同步防护
int kvs_snapshot_save(void) {
    // 1. 使用 O_TRUNC 且不使用任何可能带来脏写缓存的标志
    int fd = open("kvstore.snap", O_WRONLY | O_CREAT | O_TRUNC, 0644);
    if (fd < 0) {
        printf("Failed to open snapshot file for writing\n");
        return -1;
    }
    
    fchmod(fd, 0666);

    // 初始化内存 Buffer
    snapshot_buffer_t snap_buf;
    snap_buf.capacity = 4 * 1024 * 1024;
    snap_buf.offset = 0;
    snap_buf.data = kvs_malloc(snap_buf.capacity);
    if (!snap_buf.data) {
        close(fd);
        return -1;
    }

    for (int i = 0; i < LOCK_SEGMENTS; i++) {
        pthread_rwlock_wrlock(&seg_locks[i]);
    }

    #if ENABLE_ARRAY
    extern kvs_array_t global_array;
    if (global_array.total > 0 && global_array.table != NULL) {
        kvs_array_foreach(&global_array, snapshot_write_array_cb, &snap_buf);
    }
    #endif

    #if ENABLE_RBTREE
    extern kvs_rbtree_t global_rbtree;
    if (global_rbtree.nil != NULL && global_rbtree.root != global_rbtree.nil) {
        kvs_rbtree_foreach(&global_rbtree, snapshot_write_rbtree_cb, &snap_buf);
    }
    #endif

    #if ENABLE_HASH
    extern kvs_hash_t global_hash;
    if (global_hash.count > 0) {
        kvs_hash_foreach(&global_hash, snapshot_write_hash_cb, &snap_buf);
    }
    #endif

    #if ENABLE_SKIPLIST
    extern kvs_skip_t global_skip;
    if (global_skip.count > 0 && global_skip.header->forward[0] != NULL) {
        kvs_skip_foreach(&global_skip, snapshot_write_skip_cb, &snap_buf);
    }
    #endif

    // 单次高效计算数据区的 CRC
    uint64_t calc_crc = kvs_crc64(0, snap_buf.data, snap_buf.offset);

    // 安全地追加 CRC 到缓冲区末尾
    if (snap_buf.offset + sizeof(uint64_t) > snap_buf.capacity) {
        while (snap_buf.offset + sizeof(uint64_t) > snap_buf.capacity) {
            snap_buf.capacity *= 2;
        }
        snap_buf.data = kvs_realloc(snap_buf.data, snap_buf.capacity);
    }
    memcpy(snap_buf.data + snap_buf.offset, &calc_crc, sizeof(uint64_t));
    snap_buf.offset += sizeof(uint64_t);

    for (int i = 0; i < LOCK_SEGMENTS; i++) {
        pthread_rwlock_unlock(&seg_locks[i]);
    }

    // 2. 将计算结果放入堆栈临时变量，彻底杜绝 io_uring 异步期间原 Buffer 被销毁或踩内存
    size_t total_write_bytes = snap_buf.offset;

    // io_uring 异步落盘
    struct io_uring ring;
    if (io_uring_queue_init(8, &ring, 0) < 0) {
        kvs_free(snap_buf.data);
        close(fd);
        return -1;
    }

    struct io_uring_sqe *sqe = io_uring_get_sqe(&ring);
    if (!sqe) {
        io_uring_queue_exit(&ring);
        kvs_free(snap_buf.data);
        close(fd);
        return -1;
    }

    // 提交异步写请求
    io_uring_prep_write(sqe, fd, snap_buf.data, total_write_bytes, 0);
    io_uring_submit(&ring);

    struct io_uring_cqe *cqe;
    // int write_res = -1;
    
    // 挂起等待内核完全写完此段内存，此时 snap_buf.data 严禁被释放
    if (io_uring_wait_cqe(&ring, &cqe) < 0 || cqe->res < 0) {
        printf("[io_uring] Async write failed: %d\n", cqe ? cqe->res : -1);
    } else {
        // write_res = cqe->res;
        // printf("[SAVE] io_uring safely dumped %d bytes. CRC64: %llu\n", write_res, (unsigned long long)calc_crc);
    }

    if (cqe) {
        io_uring_cqe_seen(&ring, cqe);
    }
    
    // printf("[DEBUG SAVE] Buffer Offset: %zu, io_uring Res: %d\n", total_write_bytes, write_res);

    // 销毁异步队列
    io_uring_queue_exit(&ring);
    
    // 3. 释放写完的内存，并调用 fsync 强刷文件系统缓存页到磁盘
    kvs_free(snap_buf.data);
    fsync(fd);
    close(fd);

    return 0;
}

int kvs_snapshot_load(void) {
    int fd = open("kvstore.snap", O_RDONLY);
    if (fd < 0) {
        printf("No snapshot file found, starting fresh\n");
        return -1; 
    }
    
    struct stat st;
    if (fstat(fd, &st) < 0 || st.st_size < (off_t)sizeof(uint64_t)) {
        printf("[CRITICAL] Snapshot file is corrupted or too small!\n");
        close(fd);
        return -1;
    }
    size_t file_size = st.st_size;
    size_t data_limit = file_size - sizeof(uint64_t); 

    // printf("[DEBUG LOAD] File Size: %zu, Data Limit: %zu\n", file_size, data_limit);

    unsigned char *mmap_addr = mmap(NULL, file_size, PROT_READ, MAP_PRIVATE, fd, 0);
    close(fd); 

    if (mmap_addr == MAP_FAILED) {
        printf("[CRITICAL] mmap failed!\n");
        return -1;
    }

    // 提取保存在末尾的 CRC
    uint64_t file_stored_crc = *(uint64_t *)(mmap_addr + data_limit);
    
    // 单次高效校验 CRC（不解析数据结构）
    uint64_t calc_crc = kvs_crc64(0, mmap_addr, data_limit);
    
    // printf("[SNAP] CRC Check - Calculated: %llu, Stored: %llu\n", (unsigned long long)calc_crc, (unsigned long long)file_stored_crc);
    
    if (calc_crc != file_stored_crc) {
        printf("[CRITICAL] Snapshot CRC Mismatch! File may be corrupted.\n");
        munmap(mmap_addr, file_size);
        return -1; 
    }
    
    // printf("[SNAP] CRC verified OK, loading data...\n");

    // CRC 通过后逐条解析加载
    size_t p = 0; 
    int loaded_count = 0;
    int expired_cleanup_count = 0; 
    int64_t now = get_current_ms_snapshot(); 

    while (p + sizeof(int) <= data_limit) {
        int type = *(int *)(mmap_addr + p);
        p += sizeof(int);

        if (type < SNAP_TYPE_ARRAY || type > SNAP_TYPE_SKIPLIST) {
            printf("[SNAP] Invalid type %d at offset %zu\n", type, p - sizeof(int));
            break;
        }

        if (p + sizeof(int64_t) > data_limit) break;
        int64_t expire_time = *(int64_t *)(mmap_addr + p);
        p += sizeof(int64_t);

        if (p + sizeof(int) > data_limit) break;
        int key_len = *(int *)(mmap_addr + p);
        p += sizeof(int);

        if (key_len <= 0 || key_len > 1024 * 1024) {
            printf("[SNAP] Invalid key_len %d at offset %zu\n", key_len, p - sizeof(int));
            break;
        }

        if (p + key_len > data_limit) break;
        void *k_buf = (void *)(mmap_addr + p);
        p += key_len;

        if (p + sizeof(int) > data_limit) break;
        int val_len = *(int *)(mmap_addr + p);
        p += sizeof(int);

        if (val_len < 0 || val_len > 10 * 1024 * 1024) {
            printf("[SNAP] Invalid val_len %d at offset %zu\n", val_len, p - sizeof(int));
            break;
        }

        if (p + val_len > data_limit) break;
        void *v_buf = (void *)(mmap_addr + p);
        p += val_len;

        if (expire_time > 0 && now > expire_time) {
            expired_cleanup_count++;
            continue; 
        }

        kv_data_t kv_k = { .data = k_buf, .len = key_len };
        kv_data_t kv_v = { .data = v_buf, .len = val_len };
        
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
        
        loaded_count++;
    }

    munmap(mmap_addr, file_size);

    printf("[SNAP] Load complete: %d loaded, %d expired skipped\n", 
           loaded_count, expired_cleanup_count);
    return 0;
}