#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif

#include "repl.h"
#include "resp.h"
#include "rdma.h"
#include "ebpf.h"
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

#define REPL_INIT_BUFFER_SIZE 4096
#define DEFAULT_RDMA_DEVICE "rxe0"

volatile int g_running = 1; 

static pthread_t repl_slave_tid; 
static bool g_repl_ctx_ready = false;
static struct repl_context g_repl_ctx = { .fd = -1, .wbuffer = NULL, .wcapacity = 0, .wlength = 0 };

struct rdma_ring_ctx *g_rdma_ctx = NULL; 

extern void recv_cb(int fd); 
extern struct conn conn_list[];
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

// ✅ 与 repl.h 保持一致的签名
int repl_init(const char *rdma_dev, const char *ebpf_obj_path) {
    const char *final_dev = rdma_dev ? rdma_dev : DEFAULT_RDMA_DEVICE;
    printf("[Repl] Initializing RDMA engine on device: %s\n", final_dev);

    g_rdma_ctx = rdma_ring_init(final_dev);
    if (!g_rdma_ctx) {
        printf("[Repl Error] Failed to init RDMA hardware on device: %s\n", final_dev);
        g_repl_ctx_ready = false; 
        return -1;
    }

    #if ENABLE_REPLICATION_MASTER
    if (ebpf_obj_path) {
        if (ebpf_init_loader(ebpf_obj_path) < 0) {
            printf("[Repl Error] Failed to load eBPF byte code\n");
            g_repl_ctx_ready = false;
            return -1;
        }
    }
    ebpf_register_slave();
    printf("[Repl Master] eBPF TC metadata registered.\n");
    #endif

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
    printf("[Repl Slave] Handshake complete. Pure RDMA background replication engine IS RUNNING.\n");
    return 0;
}

void slave_rdma_handshake_read_cb(int fd) {
    struct conn *c = &conn_list[fd];
    int total_new_bytes = 0;
    while (1) {
        if (c->rcapacity - c->rlength < 1024) {
            int new_capacity = c->rcapacity * 2;
            if (new_capacity < 4096) new_capacity = 4096;
            char *new_buf = (char *)kvs_realloc(c->rbuffer, new_capacity);
            if (!new_buf) { close_and_free_connection(fd); return; }
            c->rbuffer = new_buf;
            c->rcapacity = new_capacity;
        }
        int remaining_space = c->rcapacity - c->rlength;
        int count = recv(fd, c->rbuffer + c->rlength, remaining_space, MSG_DONTWAIT);
        if (count < 0) {
            if (errno == EAGAIN || errno == EWOULDBLOCK) break;
            close_and_free_connection(fd); return;
        }
        if (count == 0) { close_and_free_connection(fd); return; }
        c->rlength += count;
        total_new_bytes += count;
    }

    if (total_new_bytes == 0 && c->rlength == 0) return;

    if (strstr(c->rbuffer, "RDMA_CONNECT_ACK") != NULL) {
        printf("[Protocol Slave] Intercepted RDMA_CONNECT_ACK successfully!\n");
        struct ring_meta master_meta;
        char *lines[16];
        int line_idx = 0;
        char *token = strtok(c->rbuffer, "\r\n");
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
                fprintf(stderr, "[Protocol Slave Error] Failed to configure Slave QP to RTS!\n");
                close_and_free_connection(fd);
                return;
            }
            printf("[Protocol Slave] RDMA Pipeline is now RTS.\n");

            const char *sync_cmd = "*1\r\n$4\r\nSYNC\r\n";
            send(fd, sync_cmd, strlen(sync_cmd), 0);
            printf("[Protocol Slave] SYNC request injected into TCP channel. Handshake over.\n");

            repl_start_slave_engine();

            c->rlength = 0;                                  
            c->read_callback = recv_cb;          
            reactor_set_event(fd, EPOLLIN, 0);
        } else {
            close_and_free_connection(fd);
        }
    }
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
    
    printf("Slave: Successfully connected to Master at %s:%d\n", master_ip, master_port);
    
    uint32_t my_rkey = g_rdma_ctx->mr_buf->rkey;
    uint64_t my_vaddr = (uint64_t)(uintptr_t)g_rdma_ctx->buffer;
    uint32_t my_qpn = g_rdma_ctx->qp->qp_num;

    union ibv_gid my_gid;
    int gid_idx;
    memset(&my_gid, 0, sizeof(my_gid));
    if (get_rocev2_ipv4_gid(g_rdma_ctx->ctx, 1, &my_gid, &gid_idx) == 0) {
        printf("[Repl] Dynamically resolved RoCEv2 IPv4 GID at index %d\n", gid_idx);
    } else {
        printf("[Repl Warning] Cannot find RoCEv2 IPv4 GID, fallback to Index 0\n");
        ibv_query_gid(g_rdma_ctx->ctx, 1, 0, &my_gid);
    }

    char rkey_str[32], vaddr_str[64], qpn_str[32];
    char gid_str[64] = {0}; 

    sprintf(rkey_str, "%u", my_rkey);
    sprintf(vaddr_str, "%lu", my_vaddr);
    sprintf(qpn_str, "%u", my_qpn);

    char *p = gid_str;
    for (int i = 0; i < 16; i++) {
        p += sprintf(p, "%02x", my_gid.raw[i]);
    }

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

    int flags = fcntl(g_repl_ctx.fd, F_GETFL, 0);
    if (flags >= 0) fcntl(g_repl_ctx.fd, F_SETFL, flags | O_NONBLOCK);

    struct conn *c = reactor_host_slave_connection(g_repl_ctx.fd, g_repl_ctx.wbuffer, g_repl_ctx.wcapacity, g_repl_ctx.wlength);
    if (!c) return -1;

    rdma_init_context(c);
    conn_list[g_repl_ctx.fd].read_callback = slave_rdma_handshake_read_cb;

    printf("[Repl Slave] Pure RDMA Handshake sent. Waiting for Master metadata...\n");
    return g_repl_ctx.fd;
}

// ✅ 与 repl.h 保持一致的签名
void handle_master_rdma_connect(resp_request_t *req, char **wbuf, int *wcap, int *wlen, int fd) {
    printf("[Protocol Master] Intercepted RDMA_CONNECT. Shaking hands with slave...\n");
    
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
    } else {
        printf("[Protocol Master] RDMA Pipeline is now RTS (Ready to Send).\n");
    }

    uint32_t my_rkey = g_rdma_ctx->mr_buf->rkey;
    uint64_t my_vaddr = (uint64_t)(uintptr_t)g_rdma_ctx->buffer;
    uint32_t my_qpn = g_rdma_ctx->qp->qp_num;
    
    union ibv_gid my_gid;
    int gid_idx;
    memset(&my_gid, 0, sizeof(my_gid));
    if (get_rocev2_ipv4_gid(g_rdma_ctx->ctx, 1, &my_gid, &gid_idx) == 0) {
        printf("[Protocol Master] Dynamically matched RoCEv2 IPv4 GID at Index: %d\n", gid_idx);
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
    printf("[Protocol Master] RDMA_CONNECT_ACK sent successfully (%d bytes).\n", total_sent);

    if (wbuf && wcap && wlen) {
        int needed = *wlen + ack_len + 1;
        if (needed > *wcap) {
            int new_cap = *wcap * 2;
            if (new_cap < needed) new_cap = needed;
            char *new_buf = (char *)kvs_realloc(*wbuf, new_cap);
            if (new_buf) { *wbuf = new_buf; *wcap = new_cap; }
        }
        memcpy(*wbuf + *wlen, ack_cmd, ack_len);
        *wlen += ack_len;
    }
}

int repl_sync_log_via_rdma(void) {
    if (!g_rdma_ctx) {
        printf("[Repl Error] RDMA context not ready for log sync.\n");
        return -1;
    }
    printf("[Repl] Intercepted SYNC command. Starting RDMA Zero-Copy with IMM...\n");

    int fd = open(PERSISTENCE_FILE, O_RDONLY);
    if (fd < 0) {
        perror("[Repl Error] Failed to open persistence file");
        return -1;
    }

    struct stat st;
    if (fstat(fd, &st) < 0) {
        close(fd);
        return -1;
    }
    size_t file_size = st.st_size;
    if (file_size == 0) { close(fd); return 0; }

    if (file_size > RING_BUFFER_SIZE) {
        fprintf(stderr, "[Repl Error] Log file size %zu exceeds limit\n", file_size);
        close(fd);
        return -1;
    }

    memset(g_rdma_ctx->buffer, 0, RING_BUFFER_SIZE);
    ssize_t read_bytes = read(fd, g_rdma_ctx->buffer, file_size);
    close(fd); 
    if (read_bytes != (ssize_t)file_size) return -1;

    int ret = rdma_master_write_log_imm(g_rdma_ctx, (uint32_t)file_size);
    if (ret != 0) return -1;

    printf("[Repl] Successfully pushed %zu bytes to slave and triggered slave hardware event.\n", file_size);
    return 0;
}

void* pure_rdma_repl_slave_thread(void *arg) {
    printf("[Repl Slave Thread] Pure RDMA replication engine started.\n");
    
    while (g_running) {
        uint32_t incoming_log_size = 0;
        int ret = rdma_slave_block_and_get_imm(g_rdma_ctx, &incoming_log_size);
        if (ret == 1) {
            printf("[Repl Slave] Hardware DMA Sync Done. Size: %u bytes. Dumping...\n", incoming_log_size);

            int local_fd = open(PERSISTENCE_FILE, O_WRONLY | O_CREAT | O_APPEND, 0644);
            if (local_fd >= 0) {
                write(local_fd, g_rdma_ctx->buffer, incoming_log_size);
                fsync(local_fd);
                close(local_fd);
            }

            kvs_persistence_recover();
            printf("[Repl Slave] Reload successfully!\n");

             start_replica_udp_server_coroutine(3000);

            const char *done_cmd = "*1\r\n$9\r\nSYNC_DONE\r\n";
            send(g_repl_ctx.fd, done_cmd, strlen(done_cmd), 0);
            printf("[Repl Slave] SYNC_DONE report sent to Master.\n");
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

    #if ENABLE_REPLICATION_SLAVE
    if (repl_slave_tid) {
        pthread_join(repl_slave_tid, NULL);
        repl_slave_tid = 0;
    }
    #endif

    #if ENABLE_REPLICATION_MASTER
    ebpf_cleanup(); 
    #endif

    if (g_repl_ctx.wbuffer) {
        free(g_repl_ctx.wbuffer);
        g_repl_ctx.wbuffer = NULL;
    }
    if (g_repl_ctx.fd >= 0) {
        close(g_repl_ctx.fd);
        g_repl_ctx.fd = -1;
    }
}