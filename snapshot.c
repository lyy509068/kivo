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

static uint64_t g_snapshot_crc = 0; // 全局计算器

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
    // 拷贝到缓冲区并累加 CRC
    memcpy(buf->data + buf->offset, ptr, size);
    g_snapshot_crc = kvs_crc64(g_snapshot_crc, (const unsigned char *)ptr, size);
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

// 保存二进制快照：使用 io_uring 异步落盘
int kvs_snapshot_save(void) {
    // 修复点：在这里通过系统调用打开磁盘文件，拿到 fd
    int fd = open("kvstore.snap", O_WRONLY | O_CREAT | O_TRUNC, 0644);
    if (fd < 0) {
        printf("Failed to open snapshot file for writing\n");
        return -1;
    }

    g_snapshot_crc = 0; // 开始前将校验和清零

    // 初始化内存 Buffer
    snapshot_buffer_t snap_buf;
    snap_buf.capacity = 4 * 1024 * 1024;
    snap_buf.offset = 0;
    snap_buf.data = kvs_malloc(snap_buf.capacity);
    if (!snap_buf.data) {
        close(fd);
        return -1;
    }

    // 1. 只有 Array 真的有数据时，才允许调用 Array 的遍历和写入
    #if ENABLE_ARRAY
    extern kvs_array_t global_array;
    if (global_array.total > 0 && global_array.table != NULL) {
        kvs_array_foreach(&global_array, snapshot_write_array_cb, &snap_buf);
    }
    #endif

    // 2. 只有红黑树真的有节点时，才允许进入
    #if ENABLE_RBTREE
    extern kvs_rbtree_t global_rbtree;
    if (global_rbtree.nil != NULL && global_rbtree.root != global_rbtree.nil) {
        kvs_rbtree_foreach(&global_rbtree, snapshot_write_rbtree_cb, &snap_buf);
    }
    #endif

    // 3. 只有 Hash 真的有数据时，才调用 Hash 写入
    #if ENABLE_HASH
    extern kvs_hash_t global_hash;
    if (global_hash.count > 0) {
        kvs_hash_foreach(&global_hash, snapshot_write_hash_cb, &snap_buf);
    }
    #endif

    // 4. 只有跳表真的有数据时，才调用跳表写入
    #if ENABLE_SKIPLIST
    extern kvs_skip_t global_skip;
    if (global_skip.count > 0 && global_skip.header->forward[0] != NULL) {
        kvs_skip_foreach(&global_skip, snapshot_write_skip_cb, &snap_buf);
    }
    #endif

    // 最后把最终计算出的 CRC 追加入 Buffer
    uint64_t final_crc = g_snapshot_crc;
    if (snap_buf.offset + sizeof(uint64_t) > snap_buf.capacity) {
        snap_buf.capacity += sizeof(uint64_t);
        snap_buf.data = kvs_realloc(snap_buf.data, snap_buf.capacity);
    }
    memcpy(snap_buf.data + snap_buf.offset, &final_crc, sizeof(uint64_t));
    snap_buf.offset += sizeof(uint64_t);

    // ==========================================
    // io_uring 异步落盘逻辑
    // ==========================================
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

    // 投递异步写任务到内核
    io_uring_prep_write(sqe, fd, snap_buf.data, snap_buf.offset, 0);
    io_uring_submit(&ring);

    // 等待内核落盘完成的事件
    struct io_uring_cqe *cqe;
    if (io_uring_wait_cqe(&ring, &cqe) < 0 || cqe->res < 0) {
        printf("[io_uring] Async write failed: %d\n", cqe ? cqe->res : -1);
    } else {
        printf("[SAVE] io_uring safely dumped %d bytes. CRC64: %llu\n", 
               cqe->res, (unsigned long long)final_crc);
    }

    // 清理收尾
    io_uring_cqe_seen(&ring, cqe);
    io_uring_queue_exit(&ring);
    kvs_free(snap_buf.data);
    close(fd);

    return 0;
}

// 保持不变：从快照文件读取数据（mmap 零拷贝）
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

    unsigned char *mmap_addr = mmap(NULL, file_size, PROT_READ, MAP_SHARED, fd, 0);
    close(fd); 

    if (mmap_addr == MAP_FAILED) {
        printf("[CRITICAL] mmap failed!\n");
        return -1;
    }

    uint64_t my_calc_crc = 0; 
    size_t p = 0; 

    int loaded_count = 0;
    int expired_cleanup_count = 0; 
    int64_t now = get_current_ms_snapshot(); 

    while (p < data_limit) {
        int type = *(int *)(mmap_addr + p);
        my_calc_crc = kvs_crc64(my_calc_crc, mmap_addr + p, sizeof(int));
        p += sizeof(int);

        int64_t expire_time = *(int64_t *)(mmap_addr + p);
        my_calc_crc = kvs_crc64(my_calc_crc, mmap_addr + p, sizeof(int64_t));
        p += sizeof(int64_t);

        int key_len = *(int *)(mmap_addr + p);
        my_calc_crc = kvs_crc64(my_calc_crc, mmap_addr + p, sizeof(int));
        p += sizeof(int);

        void *k_buf = (void *)(mmap_addr + p);
        my_calc_crc = kvs_crc64(my_calc_crc, mmap_addr + p, key_len);
        p += key_len;
        
        int val_len = *(int *)(mmap_addr + p);
        my_calc_crc = kvs_crc64(my_calc_crc, mmap_addr + p, sizeof(int));
        p += sizeof(int);

        void *v_buf = (void *)(mmap_addr + p);
        my_calc_crc = kvs_crc64(my_calc_crc, mmap_addr + p, val_len);
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

    uint64_t file_stored_crc = *(uint64_t *)(mmap_addr + data_limit);
    munmap(mmap_addr, file_size);

    if (my_calc_crc != file_stored_crc) {
        printf("[CRITICAL] mmap Snapshot CRC Mismatch!!!\n");
        printf("-> Expected (Calculated): %llu\n", (unsigned long long)my_calc_crc);
        printf("-> Got (File Stored):     %llu\n", (unsigned long long)file_stored_crc);
        return -1; 
    }

    printf("[SUCCESS] mmap Snapshot checked OK! Loaded %d entries (Purged %d expired)\n", 
           loaded_count, expired_cleanup_count);
    return 0;
}