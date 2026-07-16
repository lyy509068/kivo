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

#define REPL_INIT_BUFFER_SIZE 4096
#define DEFAULT_RDMA_DEVICE "rxe0"

volatile int g_running = 1; 
static int g_slave_fd = -1;

static pthread_t repl_slave_tid; 
static bool g_repl_ctx_ready = false;
static struct repl_context g_repl_ctx = { .fd = -1, .wbuffer = NULL, .wcapacity = 0, .wlength = 0 };


repl_backlog_item_t g_repl_backlog[REPL_BACKLOG_MAX];
int g_repl_backlog_count;
volatile int g_repl_backlog_enabled;  // 1=追加中, 0=关闭


struct rdma_ring_ctx *g_rdma_ctx = NULL; 

extern int start_replica_udp_server_coroutine(int listen_port);

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


int repl_init(const char *rdma_dev) {
    const char *final_dev = rdma_dev ? rdma_dev : DEFAULT_RDMA_DEVICE;
    // printf("[Repl] Initializing RDMA engine on device: %s\n", final_dev);

    g_rdma_ctx = rdma_ring_init(final_dev);
    if (!g_rdma_ctx) {
        printf("[Repl Error] Failed to init RDMA hardware on device: %s\n", final_dev);
        g_repl_ctx_ready = false; 
        return -1;
    }

    g_repl_ctx_ready = true;
    return 0;
}

int repl_start_slave_engine(void) {
    if (!g_rdma_ctx) return -1;
    for (int i = 0; i < 8; i++) {
        rdma_slave_post_recv_envelope(g_rdma_ctx, i); 
    }
    g_running = 1;
    if (pthread_create(&repl_slave_tid, NULL, pure_rdma_repl_slave_thread, NULL) != 0) return -1;
    // printf("[Repl Slave] Pure RDMA background replication engine IS RUNNING.\n");
    return 0;
}

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
    
    // printf("Slave: Successfully connected to Master at %s:%d\n", master_ip, master_port);
    
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

    printf("[Repl Slave] RDMA_CONNECT sent. Waiting for ACK...\n");

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

    // 解析 ACK，配置 QP
    if (ack_total > 0 && strstr(ack_buf, "RDMA_CONNECT_ACK")) {
        printf("[Repl Slave] Received RDMA_CONNECT_ACK.\n");

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
            printf("[Repl Slave] SYNC sent. Handshake complete.\n");

            // 启动 RDMA 引擎
            repl_start_slave_engine();
        }
    }

    return g_repl_ctx.fd;
}

void handle_slave_rdma_connect(resp_request_t *req, char **wbuf, int *wcap, int *wlen, int fd) {
    g_slave_fd = fd;
    
    printf("[Repl Master] Recieved RDMA_CONNECT.\n");
    
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
        //printf("[Protocol Master] Dynamically matched RoCEv2 IPv4 GID at Index: %d\n", gid_idx);
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
    printf("[Repl Master] RDMA_CONNECT_ACK sent successfully.\n");

}

int repl_sync_log_via_rdma(void) {
    if (!g_rdma_ctx) {
        printf("[Repl Error] RDMA context not ready for log sync.\n");
        return -1;
    }

    // 打开源文件
    int src_fd = open(PERSISTENCE_FILE, O_RDONLY);
    if (src_fd < 0) {
        perror("[Repl Error] Failed to open persistence file");
        return -1;
    }
    // 获取文件大小
    struct stat st;
    if (fstat(src_fd, &st) < 0) { close(src_fd); return -1; }
    size_t file_size = st.st_size;
    if (file_size == 0) { close(src_fd); return 0; }
    // 检查文件大小有没有超出上限
    if (file_size > RING_BUFFER_SIZE) {
        fprintf(stderr, "[Repl Error] Log file size %zu exceeds limit\n", file_size);
        close(src_fd); return -1;
    }

    // mmap 源文件
    void *mapped = mmap(NULL, file_size, PROT_READ, MAP_SHARED, src_fd, 0);
    close(src_fd);
    if (mapped == MAP_FAILED) {
        perror("[Repl Error] mmap source failed");
        return -1;
    }

    // 生成临时文件名
    char tmp_file[256];
    snprintf(tmp_file, sizeof(tmp_file), "%s.sync.%d", PERSISTENCE_FILE, getpid());

    // 把 mmap 写入临时文件
    int tmp_fd = open(tmp_file, O_WRONLY | O_CREAT | O_TRUNC, 0644);
    if (tmp_fd < 0) {
        perror("[Repl Error] Failed to create temp file");
        munmap(mapped, file_size); return -1;
    }
    ssize_t written = write(tmp_fd, mapped, file_size);
    close(tmp_fd);
    munmap(mapped, file_size);

    if (written != (ssize_t)file_size) {
        perror("[Repl Error] Failed to write temp file");
        unlink(tmp_file); return -1;
    }

    // mmap 临时文件，拷贝到 RDMA 缓冲区
    tmp_fd = open(tmp_file, O_RDONLY); 
    if (tmp_fd < 0) { unlink(tmp_file); return -1; }

    void *tmp_mapped = mmap(NULL, file_size, PROT_READ, MAP_SHARED, tmp_fd, 0);
    close(tmp_fd);
    if (tmp_mapped == MAP_FAILED) { unlink(tmp_file); return -1; }

    memcpy(g_rdma_ctx->buffer, tmp_mapped, file_size);
    munmap(tmp_mapped, file_size);

    // RDMA 发送
    int ret = rdma_master_write_log_imm(g_rdma_ctx, (uint32_t)file_size);
    if (ret != 0) { unlink(tmp_file); return -1; }

    // 删除临时文件
    unlink(tmp_file);

    printf("Master : Successfully pushed %zu bytes to slave.\n", file_size);
    return 0;
}


// 补发缓冲区
int repl_flush_backlog_via_rdma(void) {
    if (!g_rdma_ctx) {
        fprintf(stderr, "[Repl Error] g_rdma_ctx is NULL, cannot flush backlog\n");
        return -1;
    }

    size_t total = 0;
    for (int i = 0; i < g_repl_backlog_count; i++) {
        if (total + g_repl_backlog[i].len > RING_BUFFER_SIZE) {
            fprintf(stderr, "[Repl Error] Backlog overflow, truncated\n");
            break;
        }
        memcpy(g_rdma_ctx->buffer + total, g_repl_backlog[i].data, g_repl_backlog[i].len);
        total += g_repl_backlog[i].len;
        kvs_free(g_repl_backlog[i].data);
    }
    g_repl_backlog_count = 0;
    g_repl_backlog_enabled = 0;

    if (total == 0){
        fprintf(stderr, "Backlog flushed 0 bytes to slave via RDMA.\n");
        return 0;
    } 

    int ret = rdma_master_write_log_imm(g_rdma_ctx, (uint32_t)total);
    if (ret != 0) {
        fprintf(stderr, "[Repl Error] Backlog RDMA send failed\n");
        return -1;
    }

    printf("Master : Backlog flushed %zu bytes to slave via RDMA.\n", total);
    return 0;
}


void* pure_rdma_repl_slave_thread(void *arg) {
    int first_sync = 1;  

    while (g_running) {
        uint32_t incoming_log_size = 0;
        int ret = rdma_slave_block_and_get_imm(g_rdma_ctx, &incoming_log_size);
        if (ret == 1) {
            // 写入 AOF
            int local_fd = open(PERSISTENCE_FILE, O_WRONLY | O_CREAT | O_APPEND, 0644);
            if (local_fd >= 0) {
                write(local_fd, g_rdma_ctx->buffer, incoming_log_size);
                fsync(local_fd);
                close(local_fd);
            }

            // 重新加载引擎
            kvs_persistence_recover();
            printf("[Repl Slave] AOF reload successfully!\n");

            if (first_sync) {                
                if (g_repl_ctx.fd >= 0) {
                    const char *done_cmd = "*1\r\n$9\r\nSYNC_DONE\r\n";
                    ssize_t send_ret = send(g_repl_ctx.fd, done_cmd, strlen(done_cmd), 0);
                    if (send_ret < 0) {
                        printf("[Repl Slave] SYNC_DONE send FAILED: errno=%d (%s)\n", errno, strerror(errno));
                    } else {
                        printf("[Repl Slave] SYNC_DONE report sent to Master.\n");
                    }                    
                }
                first_sync = 0;
            }    
        }
    }
    return NULL;
}

void repl_destroy(void) {
    if (g_running) g_running = 0;

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