#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif

#include "repl.h"
#include "resp.h"
#include "rdma.h"
#include "network.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
#include <fcntl.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <sys/epoll.h>
#include <sys/mman.h>
#include <sys/sendfile.h>
#include <netinet/tcp.h>
#include <endian.h>
#include <errno.h>

#define REPL_INIT_BUFFER_SIZE 4096
#define DEFAULT_RDMA_DEVICE "rxe0"

volatile int g_slave_fd = -1;

volatile int g_sync_file_done = 0;

static pthread_t repl_slave_tid; 
static bool g_repl_ctx_ready = false;
static struct repl_context g_repl_ctx = { .fd = -1, .wbuffer = NULL, .wcapacity = 0, .wlength = 0 };

struct rdma_ring_ctx *g_rdma_ctx = NULL; 

repl_backlog_node_t g_repl_backlog[REPL_BACKLOG_MAX];
int g_repl_backlog_head = 0;
int g_repl_backlog_tail = 0;
int g_repl_backlog_count = 0;
volatile int g_repl_backlog_enabled = 0;

// 动态嗅探 RoCEv2 IPv4 GID
static int get_rocev2_ipv4_gid(struct ibv_context *ctx, int port_num, union ibv_gid *out_gid, int *out_index) {
    for (int i = 0; i < 16; i++) {
        union ibv_gid tg;
        if (ibv_query_gid(ctx, port_num, i, &tg) != 0) break;
        
        if (tg.global.interface_id == 0 && tg.global.subnet_prefix == 0) continue;

        if (tg.raw[10] == 0xff && tg.raw[11] == 0xff) {
            *out_gid = tg;
            *out_index = i;
            return 0;
        }
    }
    return -1;
}

// 两端RDMA都用：初始化RDMA
int repl_init(const char *rdma_dev) {
    const char *final_dev = rdma_dev ? rdma_dev : DEFAULT_RDMA_DEVICE;

    g_rdma_ctx = rdma_ring_init(final_dev);
    if (!g_rdma_ctx) {
        printf("[Repl Error] Failed to init RDMA hardware on device: %s\n", final_dev);
        g_repl_ctx_ready = false; 
        return -1;
    }

    g_repl_ctx_ready = true;
    return 0;
}

// 从端RDMA专用：打开接收线程 
int repl_start_slave_engine(void) {
    if (!g_rdma_ctx) return -1;
    for (int i = 0; i < 8; i++) {
        rdma_slave_post_recv_envelope(g_rdma_ctx, i); 
    }
    if (pthread_create(&repl_slave_tid, NULL, pure_rdma_repl_slave_thread, NULL) != 0) return -1;
    return 0;
}

// 从端RDMA专用：发CONNECT 等待接收ACK 发送SYNC  
int repl_connect_to_master(const char *master_ip, unsigned short master_port) {
    g_repl_ctx.fd = socket(AF_INET, SOCK_STREAM, 0);
    if (g_repl_ctx.fd < 0) return -1;

    struct sockaddr_in master_addr;
    memset(&master_addr, 0, sizeof(master_addr));
    master_addr.sin_family = AF_INET;
    master_addr.sin_port = htons(master_port);
    master_addr.sin_addr.s_addr = inet_addr(master_ip);

    if (connect(g_repl_ctx.fd, (struct sockaddr*)&master_addr, sizeof(master_addr)) < 0) {
        close(g_repl_ctx.fd);
        g_repl_ctx.fd = -1;
        return -1; 
    }

    if (g_repl_ctx.wbuffer == NULL) {
        g_repl_ctx.wcapacity = REPL_INIT_BUFFER_SIZE;
        g_repl_ctx.wbuffer = (char *)kvs_malloc(g_repl_ctx.wcapacity);
    }
    g_repl_ctx.wlength = 0;
    
    // 发送 RDMA_CONNECT
    uint32_t my_rkey = g_rdma_ctx->mr_buf->rkey;
    uint64_t my_vaddr = (uint64_t)(uintptr_t)g_rdma_ctx->buffer;
    uint32_t my_qpn = g_rdma_ctx->qp->qp_num;

    union ibv_gid my_gid;
    int gid_idx;
    memset(&my_gid, 0, sizeof(my_gid));
    if (get_rocev2_ipv4_gid(g_rdma_ctx->ctx, 1, &my_gid, &gid_idx) == 0) {
    } else {
        ibv_query_gid(g_rdma_ctx->ctx, 1, 0, &my_gid);
    }

    char rkey_str[32], vaddr_str[64], qpn_str[32];
    char gid_str[64] = {0}; 
    sprintf(rkey_str, "%u", my_rkey);
    sprintf(vaddr_str, "%lu", my_vaddr);
    sprintf(qpn_str, "%u", my_qpn);

    char *p = gid_str;
    for (int i = 0; i < 16; i++) p += sprintf(p, "%02x", my_gid.raw[i]);

    char sync_cmd[512];
    int cmd_len = sprintf(sync_cmd, "*5\r\n$12\r\nRDMA_CONNECT\r\n$%zu\r\n%s\r\n$%zu\r\n%s\r\n$%zu\r\n%s\r\n$%zu\r\n%s\r\n", 
                          strlen(rkey_str), rkey_str, strlen(vaddr_str), vaddr_str,
                          strlen(qpn_str), qpn_str, strlen(gid_str), gid_str);
    
    if (g_repl_ctx.wlength + cmd_len <= g_repl_ctx.wcapacity) {
        memcpy(g_repl_ctx.wbuffer + g_repl_ctx.wlength, sync_cmd, cmd_len);
        g_repl_ctx.wlength += cmd_len;
    }

    int total_sent = 0;
    while (total_sent < g_repl_ctx.wlength) {
        int sent = send(g_repl_ctx.fd, g_repl_ctx.wbuffer + total_sent, g_repl_ctx.wlength - total_sent, 0);
        if (sent <= 0) return -1;
        total_sent += sent;
    }
    g_repl_ctx.wlength = 0; 

    printf("[Slave] RDMA_CONNECT sent. Waiting for ACK...\n");

    // 阻塞接收 ACK
    char ack_buf[512];
    int ack_total = 0;
    while (ack_total < (int)sizeof(ack_buf) - 1) {
        int n = recv(g_repl_ctx.fd, ack_buf + ack_total, sizeof(ack_buf) - 1 - ack_total, 0);
        if (n <= 0) break;
        ack_total += n;
        ack_buf[ack_total] = '\0';
        if (strstr(ack_buf, "\r\n")) break;
    }

    // 解析 ACK，配置 QP 连接
    if (ack_total > 0 && strstr(ack_buf, "RDMA_CONNECT_ACK")) {
        printf("[Slave] Received RDMA_CONNECT_ACK.\n");

        struct ring_meta master_meta;
        char *lines[16];
        int line_idx = 0;
        char *token = strtok(ack_buf, "\r\n");
        while (token && line_idx < 16) {
            lines[line_idx++] = token;
            token = strtok(NULL, "\r\n");
        }

        if (line_idx >= 11) {
            master_meta.rkey   = (uint32_t)strtoul(lines[4], NULL, 10);
            master_meta.buf_va = (uint64_t)strtoull(lines[6], NULL, 10);
            master_meta.qpn    = (uint32_t)strtoul(lines[8], NULL, 10);

            char *gid_str = lines[10];
            for (int i = 0; i < 16; i++) {
                unsigned int b;
                sscanf(gid_str + i * 2, "%02x", &b);
                master_meta.gid.raw[i] = (uint8_t)b;
            }

            if (rdma_ring_configure(g_rdma_ctx, &master_meta) != 0) {
                fprintf(stderr, "[Slave Error] Failed to configure QP!\n");
                close(g_repl_ctx.fd);
                g_repl_ctx.fd = -1;
                return -1;
            }

            // 发送 SYNC
            const char *sync = "*1\r\n$4\r\nSYNC\r\n";
            send(g_repl_ctx.fd, sync, strlen(sync), 0);
            printf("[Slave] SYNC sent.\n");

            // 启动 RDMA 引擎
            repl_start_slave_engine();
        }
    }

    return g_repl_ctx.fd;
}

// 主端RDMA专用：接收CONNECT 配置QP连接 发送ACK
void handle_slave_rdma_connect(resp_request_t *req, char **wbuf, int *wcap, int *wlen, int fd) {
    g_slave_fd = fd;
    
    printf("[Master] Recieved RDMA_CONNECT.\n");
    
    struct ring_meta slave_meta;
    slave_meta.rkey = (uint32_t)strtoul(req->argv[1], NULL, 10);
    slave_meta.buf_va = (uint64_t)strtoull(req->argv[2], NULL, 10);
    slave_meta.qpn = (uint32_t)strtoul(req->argv[3], NULL, 10);
    for (int i = 0; i < 16; i++) {
        unsigned int b;
        sscanf(req->argv[4] + i * 2, "%02x", &b);
        slave_meta.gid.raw[i] = (uint8_t)b;
    }

    if (rdma_ring_configure(g_rdma_ctx, &slave_meta) != 0) {
        fprintf(stderr, "[Protocol Master Error] Failed to configure Master QP to RTS!\n");
    }

    uint32_t my_rkey = g_rdma_ctx->mr_buf->rkey;
    uint64_t my_vaddr = (uint64_t)(uintptr_t)g_rdma_ctx->buffer;
    uint32_t my_qpn = g_rdma_ctx->qp->qp_num;
    
    union ibv_gid my_gid;
    int gid_idx;
    memset(&my_gid, 0, sizeof(my_gid));

    if (get_rocev2_ipv4_gid(g_rdma_ctx->ctx, 1, &my_gid, &gid_idx) == 0) {
    } else {
        ibv_query_gid(g_rdma_ctx->ctx, 1, 0, &my_gid);
    }

    char rkey_str[32], vaddr_str[64], qpn_str[32], gid_str[64];
    sprintf(rkey_str, "%u", my_rkey);
    sprintf(vaddr_str, "%lu", my_vaddr);
    sprintf(qpn_str, "%u", my_qpn);
    char *p = gid_str;
    for (int i = 0; i < 16; i++) {
        p += sprintf(p, "%02x", my_gid.raw[i]);
    }

    char ack_cmd[512];
    int ack_len = sprintf(ack_cmd, "*5\r\n$16\r\nRDMA_CONNECT_ACK\r\n$%zu\r\n%s\r\n$%zu\r\n%s\r\n$%zu\r\n%s\r\n$%zu\r\n%s\r\n", 
                          strlen(rkey_str), rkey_str, strlen(vaddr_str), vaddr_str,
                          strlen(qpn_str), qpn_str, strlen(gid_str), gid_str);

    int total_sent = 0;
    while (total_sent < ack_len) {
        int sent = send(fd, ack_cmd + total_sent, ack_len - total_sent, 0);
        if (sent <= 0) return;
        total_sent += sent;
    }
    printf("[Master] RDMA_CONNECT_ACK sent.\n");
}

// 主端 RDMA 专用：分片发送文件
int repl_sync_log_via_rdma(void) {
    if (!g_rdma_ctx) return -1;

    struct stat st;
    int file_exists = (stat(PERSISTENCE_FILE, &st) == 0);

    if (!file_exists || st.st_size == 0) {
        rdma_slave_post_recv_envelope(g_rdma_ctx, 1);
        int ret = rdma_master_write_log_imm(g_rdma_ctx, 0, 0);
        if (ret < 0) {
            fprintf(stderr, "[RDMA] Failed to send empty header\n");
            return -1;
        }
        uint32_t ack_signal = 0;
        rdma_slave_block_and_get_imm(g_rdma_ctx, &ack_signal);
        printf("[RDMA] Empty sync completed\n");
        fflush(stdout); // 强制立刻刷盘
        return 0;
    }

    int src_fd = open(PERSISTENCE_FILE, O_RDONLY);
    if (src_fd < 0) return -1;

    size_t file_size = st.st_size;
    void *mapped = mmap(NULL, file_size, PROT_READ, MAP_SHARED, src_fd, 0);
    close(src_fd);
    if (mapped == MAP_FAILED) return -1;

    struct timespec t_start, t_end;
    clock_gettime(CLOCK_MONOTONIC, &t_start);

    rdma_slave_post_recv_envelope(g_rdma_ctx, 1);
    int ret = rdma_master_write_log_imm(g_rdma_ctx, 0, (uint32_t)file_size);
    if (ret < 0) {
        fprintf(stderr, "[RDMA] Failed to send file size header\n");
        munmap(mapped, file_size);
        return -1;
    }

    uint32_t ack_signal = 0;
    rdma_slave_block_and_get_imm(g_rdma_ctx, &ack_signal);

    size_t remaining = file_size;
    size_t offset = 0;
    while (remaining > 0) {
        size_t chunk_size = (remaining > RDMA_CHUNK_SIZE) ? RDMA_CHUNK_SIZE : remaining;
        memcpy(g_rdma_ctx->buffer, (char *)mapped + offset, chunk_size);

        rdma_slave_post_recv_envelope(g_rdma_ctx, 1);
        ret = rdma_master_write_log_imm(g_rdma_ctx, (uint32_t)chunk_size, (uint32_t)chunk_size);
        if (ret < 0) {
            fprintf(stderr, "[RDMA] Failed to send chunk at offset %zu\n", offset);
            break;
        }
        rdma_slave_block_and_get_imm(g_rdma_ctx, &ack_signal);

        offset += chunk_size;
        remaining -= chunk_size;
    }

    clock_gettime(CLOCK_MONOTONIC, &t_end);
    double elapsed = (t_end.tv_sec - t_start.tv_sec) + (t_end.tv_nsec - t_start.tv_nsec) / 1e9;
    double throughput = file_size / elapsed / (1024.0 * 1024.0);
    printf("[RDMA] Sent %zu bytes in %.3f seconds, throughput: %.2f MB/s\n", file_size, elapsed, throughput);
    fflush(stdout); // 强制立刻刷盘

    munmap(mapped, file_size);
    return ret;
}

// 从端 RDMA 专用：接收线程
static volatile int g_slave_rdma_running = 0;
void* pure_rdma_repl_slave_thread(void *arg) {
    if (g_slave_rdma_running) return NULL;
    g_slave_rdma_running = 1;

    struct timespec t_start, t_end;

    rdma_slave_post_recv_envelope(g_rdma_ctx, 1);
    uint32_t total_file_size = 0;
    int ret = rdma_slave_block_and_get_imm(g_rdma_ctx, &total_file_size);
    if (ret < 0) {
        fprintf(stderr, "[RDMA] Failed to receive file size header\n");
        g_slave_rdma_running = 0;
        return NULL;
    }

    if (total_file_size == 0) {
        int empty_fd = open(PERSISTENCE_FILE, O_WRONLY | O_CREAT | O_TRUNC, 0644);
        if (empty_fd >= 0) close(empty_fd);

        rdma_slave_post_recv_envelope(g_rdma_ctx, 1);
        rdma_master_write_log_imm(g_rdma_ctx, 0, 1);

        if (g_repl_ctx.fd >= 0) {
            const char *done_cmd = "*1\r\n$9\r\nSYNC_DONE\r\n";
            send(g_repl_ctx.fd, done_cmd, strlen(done_cmd), 0);
        }
        printf("[RDMA] Empty sync completed\n");
        fflush(stdout); // 强制立刻刷盘
        g_slave_rdma_running = 0;
        return NULL;
    }

    int local_fd = open(PERSISTENCE_FILE, O_WRONLY | O_CREAT | O_TRUNC, 0644);
    if (local_fd < 0) {
        g_slave_rdma_running = 0;
        return NULL;
    }

    clock_gettime(CLOCK_MONOTONIC, &t_start);

    rdma_slave_post_recv_envelope(g_rdma_ctx, 1);
    rdma_master_write_log_imm(g_rdma_ctx, 0, 1);

    size_t total_received = 0;
    while (total_received < total_file_size) {
        uint32_t incoming_chunk_size = 0;
        ret = rdma_slave_block_and_get_imm(g_rdma_ctx, &incoming_chunk_size);
        if (ret < 0 || incoming_chunk_size == 0) {
            fprintf(stderr, "[RDMA] Failed to receive data chunk\n");
            close(local_fd);
            g_slave_rdma_running = 0;
            return NULL;
        }

        ssize_t written = write(local_fd, g_rdma_ctx->buffer, incoming_chunk_size);
        if (written < 0 || (size_t)written != incoming_chunk_size) {
            close(local_fd);
            g_slave_rdma_running = 0;
            return NULL;
        }
        total_received += incoming_chunk_size;

        if (total_received < total_file_size) {
            rdma_slave_post_recv_envelope(g_rdma_ctx, 1);
        }
        rdma_master_write_log_imm(g_rdma_ctx, 0, 1);
    }

    clock_gettime(CLOCK_MONOTONIC, &t_end);
    fsync(local_fd);
    close(local_fd);

    double elapsed = (t_end.tv_sec - t_start.tv_sec) + (t_end.tv_nsec - t_start.tv_nsec) / 1e9;
    double throughput = total_received / elapsed / (1024.0 * 1024.0);
    printf("[RDMA] Received %zu bytes in %.3f seconds, throughput: %.2f MB/s\n", total_received, elapsed, throughput);
    fflush(stdout); // 强制立刻刷盘

    if (g_repl_ctx.fd >= 0) {
        const char *done_cmd = "*1\r\n$9\r\nSYNC_DONE\r\n";
        send(g_repl_ctx.fd, done_cmd, strlen(done_cmd), 0);
    }

    kvs_persistence_recover();
    g_slave_rdma_running = 0;
    return NULL;
}

// 主端专用：TCP 传输
int repl_sync_log_via_tcp(void) {
    if (g_slave_fd < 0) return -1;

    struct stat st;
    int file_exists = (stat(PERSISTENCE_FILE, &st) == 0);

    if (!file_exists || st.st_size == 0) {
        uint64_t net_size = htobe64(0);
        if (send(g_slave_fd, &net_size, sizeof(net_size), MSG_NOSIGNAL) != sizeof(net_size)) {
            fprintf(stderr, "[TCP] Failed to send empty header\n");
            return -1;
        }
        printf("[TCP] Empty sync completed\n");
        fflush(stdout); // 强制立刻刷盘
        return 0;
    }

    int src_fd = open(PERSISTENCE_FILE, O_RDONLY);
    if (src_fd < 0) return -1;
    uint64_t file_size = st.st_size;

    struct timespec t_start, t_end;
    clock_gettime(CLOCK_MONOTONIC, &t_start);

    int flags = fcntl(g_slave_fd, F_GETFL, 0);
    if (flags >= 0) fcntl(g_slave_fd, F_SETFL, flags & ~O_NONBLOCK);

    struct timeval tv = {.tv_sec = 10, .tv_usec = 0};
    setsockopt(g_slave_fd, SOL_SOCKET, SO_SNDTIMEO, &tv, sizeof(tv));

    int state = 1;
    setsockopt(g_slave_fd, IPPROTO_TCP, TCP_CORK, &state, sizeof(state));

    uint64_t net_size = htobe64(file_size);
    if (send(g_slave_fd, &net_size, sizeof(net_size), MSG_NOSIGNAL) != sizeof(net_size)) {
        close(src_fd);
        return -1;
    }

    off_t offset = 0;
    size_t remaining = file_size;
    while (remaining > 0) {
        ssize_t n = sendfile(g_slave_fd, src_fd, &offset, remaining);
        if (n < 0) {
            if (errno == EAGAIN || errno == EINTR) continue;
            close(src_fd);
            return -1;
        }
        if (n == 0) {
            close(src_fd);
            return -1;
        }
        remaining -= n;
    }

    state = 0;
    setsockopt(g_slave_fd, IPPROTO_TCP, TCP_CORK, &state, sizeof(state));

    clock_gettime(CLOCK_MONOTONIC, &t_end);
    double elapsed = (t_end.tv_sec - t_start.tv_sec) + (t_end.tv_nsec - t_start.tv_nsec) / 1e9;
    double throughput = file_size / elapsed / (1024.0 * 1024.0);
    printf("[TCP] Sent %zu bytes in %.3f seconds, throughput: %.2f MB/s\n", file_size, elapsed, throughput);
    fflush(stdout); // 强制立刻刷盘

    close(src_fd);
    return 0;
}

// 从端专用：接收sendfile文件线程
void* tcp_sendfile_recv_thread(void *arg) {
    int fd = *(int*)arg;
    kvs_free(arg);

    int flags = fcntl(fd, F_GETFL, 0);
    if (flags >= 0) fcntl(fd, F_SETFL, flags & ~O_NONBLOCK);

    struct timeval tv = {.tv_sec = 10, .tv_usec = 0};
    setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));

    uint64_t net_size = 0;
    ssize_t n = recv(fd, &net_size, sizeof(net_size), MSG_WAITALL);
    if (n != sizeof(net_size)) {
        close(fd);
        return NULL;
    }
    size_t file_size = be64toh(net_size);

    if (file_size == 0) {
        int local_fd = open(PERSISTENCE_FILE, O_WRONLY | O_CREAT | O_TRUNC, 0644);
        if (local_fd >= 0) close(local_fd);
        printf("[TCP] Empty sync completed\n");
        fflush(stdout); // 强制立刻刷盘
        flags = fcntl(fd, F_GETFL, 0);
        if (flags >= 0) fcntl(fd, F_SETFL, flags | O_NONBLOCK);
        return NULL;
    }

    int local_fd = open(PERSISTENCE_FILE, O_WRONLY | O_CREAT | O_TRUNC, 0644);
    if (local_fd < 0) {
        close(fd);
        return NULL;
    }

    int pipefd[2];
    if (pipe(pipefd) < 0) {
        close(fd);
        close(local_fd);
        return NULL;
    }
    fcntl(pipefd[1], F_SETPIPE_SZ, 1024 * 1024);

    struct timespec t_start, t_end;
    clock_gettime(CLOCK_MONOTONIC, &t_start);

    size_t remaining = file_size;
    while (remaining > 0) {
        ssize_t n_spliced = splice(fd, NULL, pipefd[1], NULL, remaining, SPLICE_F_MOVE | SPLICE_F_MORE);
        if (n_spliced <= 0) {
            if (n_spliced < 0 && (errno == EAGAIN || errno == EINTR)) continue;
            break;
        }

        size_t pipe_rem = n_spliced;
        while (pipe_rem > 0) {
            ssize_t n_written = splice(pipefd[0], NULL, local_fd, NULL, pipe_rem, SPLICE_F_MOVE | SPLICE_F_MORE);
            if (n_written <= 0) {
                if (n_written < 0 && (errno == EAGAIN || errno == EINTR)) continue;
                goto cleanup;
            }
            pipe_rem -= n_written;
        }
        remaining -= n_spliced;
    }

cleanup:
    clock_gettime(CLOCK_MONOTONIC, &t_end);

    close(pipefd[0]);
    close(pipefd[1]);
    fsync(local_fd);
    close(local_fd);

    size_t received = file_size - remaining;
    double elapsed = (t_end.tv_sec - t_start.tv_sec) + (t_end.tv_nsec - t_start.tv_nsec) / 1e9;
    double throughput = elapsed > 0 ? (received / elapsed / (1024.0 * 1024.0)) : 0;
    printf("[TCP] Received %zu bytes in %.3f seconds, throughput: %.2f MB/s\n", received, elapsed, throughput);
    fflush(stdout); // 强制立刻刷盘

    if (received == file_size) {
        kvs_persistence_recover();
        const char *done_cmd = "*1\r\n$9\r\nSYNC_DONE\r\n";
        send(fd, done_cmd, strlen(done_cmd), 0);
    }

    flags = fcntl(fd, F_GETFL, 0);
    if (flags >= 0) fcntl(fd, F_SETFL, flags | O_NONBLOCK);

    return NULL;
}


// 通用函数
void repl_destroy(void) {

    if (g_rdma_ctx) {
        rdma_ring_destroy(g_rdma_ctx);
        g_rdma_ctx = NULL;
    }

    if (g_enable_repl_slave){
        if (repl_slave_tid) {
            pthread_join(repl_slave_tid, NULL);
            repl_slave_tid = 0;
        }
    }

    if (g_repl_ctx.wbuffer) {
        kvs_free(g_repl_ctx.wbuffer);
        g_repl_ctx.wbuffer = NULL;
    }
    if (g_repl_ctx.fd >= 0) {
        close(g_repl_ctx.fd);
        g_repl_ctx.fd = -1;
    }
}

void repl_push_cmd(const char *cmd, void *key, int key_len, void *value, int value_len) {
    if (g_slave_fd < 0) return;

    char buf[512];
    int len;
    if (value && value_len > 0) {
        len = snprintf(buf, sizeof(buf), "*3\r\n$%zu\r\n%s\r\n$%d\r\n%.*s\r\n$%d\r\n%.*s\r\n",
                       strlen(cmd), cmd,
                       key_len, key_len, (char*)key,
                       value_len, value_len, (char*)value);
    } else {
        len = snprintf(buf, sizeof(buf), "*2\r\n$%zu\r\n%s\r\n$%d\r\n%.*s\r\n",
                       strlen(cmd), cmd,
                       key_len, key_len, (char*)key);
    }
    send(g_slave_fd, buf, len, MSG_DONTWAIT);
}