// sync_filter.bpf.c
#include "vmlinux.h"
#include <bpf/bpf_helpers.h>
#include <bpf/bpf_tracing.h>
#include <bpf/bpf_endian.h>
#include <bpf/bpf_core_read.h>

#ifndef offsetof
#define offsetof(TYPE, MEMBER) __builtin_offsetof(TYPE, MEMBER)
#endif

#define SERVER_PORT         2000
#define MAX_PAYLOAD_SIZE    4096   // 使用较小的固定大小，减少 verifier 压力

#ifndef AF_INET
#define AF_INET 2
#endif

/* 固定大小的事件结构，避免变长数组导致的 verifier 难题 */
struct event_t {
    __u32 payload_len;
    __u32 pid;
    __u32 tid;
    __u32 saddr;
    __u32 daddr;
    __u16 sport;
    __u16 dport;
    __u16 reserved;
    char payload[MAX_PAYLOAD_SIZE];
};

/* kprobe 入口保存参数 */
struct recv_ctx_t {
    struct sock *sk;
    struct msghdr *msg;
};

struct {
    __uint(type, BPF_MAP_TYPE_HASH);
    __uint(max_entries, 16384);
    __type(key, __u64);
    __type(value, struct recv_ctx_t);
} recv_ctx_map SEC(".maps");

struct {
    __uint(type, BPF_MAP_TYPE_RINGBUF);
    __uint(max_entries, 8 * 1024 * 1024);
} payload_ringbuf SEC(".maps");

static __always_inline __u64 get_tid_key(void) {
    return bpf_get_current_pid_tgid();
}

static __always_inline int get_connection_info(struct sock *sk,
                                               __u16 *sport,
                                               __u16 *dport,
                                               __u32 *saddr,
                                               __u32 *daddr) {
    __u16 num = 0, dport_val = 0;
    __u32 saddr_val = 0, daddr_val = 0;
    if (!sk) return -1;

    bpf_core_read(&num, sizeof(num), &sk->__sk_common.skc_num);
    bpf_core_read(&dport_val, sizeof(dport_val), &sk->__sk_common.skc_dport);
    bpf_core_read(&saddr_val, sizeof(saddr_val), &sk->__sk_common.skc_rcv_saddr);
    bpf_core_read(&daddr_val, sizeof(daddr_val), &sk->__sk_common.skc_daddr);

    *sport = bpf_ntohs(num);
    *dport = bpf_ntohs(dport_val);
    *saddr = saddr_val;
    *daddr = daddr_val;
    return 0;
}

SEC("kprobe/tcp_recvmsg")
int BPF_KPROBE(trace_tcp_recvmsg_entry) {
    struct sock *sk = (struct sock *)PT_REGS_PARM1(ctx);
    struct msghdr *msg = (struct msghdr *)PT_REGS_PARM2(ctx);
    if (!sk || !msg) return 0;

    __u16 family = 0;
    bpf_core_read(&family, sizeof(family), &sk->__sk_common.skc_family);
    if (family != AF_INET) return 0;

    __u16 sport = 0, dport = 0;
    __u32 saddr = 0, daddr = 0;
    if (get_connection_info(sk, &sport, &dport, &saddr, &daddr) != 0) return 0;

    if (dport != SERVER_PORT) return 0;

    struct recv_ctx_t info = {.sk = sk, .msg = msg};
    __u64 tid = get_tid_key();
    bpf_map_update_elem(&recv_ctx_map, &tid, &info, BPF_ANY);
    return 0;
}

SEC("kretprobe/tcp_recvmsg")
int BPF_KRETPROBE(trace_tcp_recvmsg_exit, int ret) {
    if (ret <= 0) return 0;

    __u64 tid = get_tid_key();
    struct recv_ctx_t *ctx_info = bpf_map_lookup_elem(&recv_ctx_map, &tid);
    if (!ctx_info) return 0;

    struct sock *sk = ctx_info->sk;
    struct msghdr *msg = ctx_info->msg;
    if (!sk || !msg) goto cleanup;

    __u16 sport = 0, dport = 0;
    __u32 saddr = 0, daddr = 0;
    if (get_connection_info(sk, &sport, &dport, &saddr, &daddr) != 0) goto cleanup;
    if (dport != SERVER_PORT) goto cleanup;

    struct iov_iter iter = {};
    bpf_core_read(&iter, sizeof(iter), &msg->msg_iter);
    if (iter.count == 0) goto cleanup;

    struct iovec iov = {};
    struct iovec *iov_ptr = NULL;
    bpf_core_read(&iov_ptr, sizeof(iov_ptr), &iter.iov);
    if (!iov_ptr) goto cleanup;
    bpf_probe_read_kernel(&iov, sizeof(iov), &iov_ptr[0]);  // 只取第一个 iovec

    if (!iov.iov_base || iov.iov_len == 0) goto cleanup;

    // 限制拷贝长度
    __u32 copy_len = (__u32)ret;
    if (copy_len > MAX_PAYLOAD_SIZE)
        copy_len = MAX_PAYLOAD_SIZE;
    if (copy_len > iov.iov_len)
        copy_len = (__u32)iov.iov_len;
    if (copy_len == 0) goto cleanup;

    // 预留 ringbuf 空间（预留整个结构体，简化 verifier 分析）
    struct event_t *e = bpf_ringbuf_reserve(&payload_ringbuf, sizeof(*e), 0);
    if (!e) goto cleanup;

    // 填充头部
    e->payload_len = copy_len;
    e->pid = (__u32)(tid >> 32);
    e->tid = (__u32)tid;
    e->saddr = saddr;
    e->daddr = daddr;
    e->sport = sport;
    e->dport = dport;
    e->reserved = 0;

    // 只拷贝一次，绝不越界
    if (bpf_probe_read_user(e->payload, copy_len, iov.iov_base) != 0) {
        bpf_ringbuf_discard(e, 0);
        goto cleanup;
    }

    bpf_ringbuf_submit(e, 0);

cleanup:
    bpf_map_delete_elem(&recv_ctx_map, &tid);
    return 0;
}

char _license[] SEC("license") = "GPL";














// ebpf_relay.c
#define _GNU_SOURCE
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <stdint.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <bpf/libbpf.h>
#include <bpf/bpf.h>
#include <errno.h>
#include <fcntl.h>
#include <signal.h>

#define SLAVE_IP "192.168.88.130"
#define SLAVE_PORT 2000
#define MAX_PAYLOAD_SIZE 4096   // 与内核态一致

struct event_t {
    uint32_t payload_len;
    uint32_t pid;
    uint32_t tid;
    uint32_t saddr;
    uint32_t daddr;
    uint16_t sport;
    uint16_t dport;
    uint16_t reserved;
    char payload[MAX_PAYLOAD_SIZE];
};

int slave_sock = -1;
volatile int running = 1;

// 前向声明，解决编译错误
static int handle_event(void *ctx, void *data, size_t data_sz);

static void sig_handler(int sig) {
    running = 0;
}

int main(void) {
    signal(SIGINT, sig_handler);
    signal(SIGTERM, sig_handler);

    slave_sock = socket(AF_INET, SOCK_STREAM, 0);
    struct sockaddr_in sa = {.sin_family = AF_INET, .sin_port = htons(SLAVE_PORT)};
    inet_pton(AF_INET, SLAVE_IP, &sa.sin_addr);
    if (connect(slave_sock, (struct sockaddr *)&sa, sizeof(sa)) < 0) {
        perror("connect");
        return 1;
    }

    int map_fd = bpf_obj_get("/sys/fs/bpf/payload_ringbuf");
    if (map_fd < 0) {
        perror("bpf_obj_get");
        close(slave_sock);
        return 1;
    }

    struct ring_buffer *rb = ring_buffer__new(map_fd, handle_event, NULL, NULL);
    if (!rb) {
        fprintf(stderr, "ring_buffer__new failed\n");
        close(map_fd);
        close(slave_sock);
        return 1;
    }

    printf("[Relay] Ready, polling events...\n");

    while (running) {
        ring_buffer__poll(rb, 100);
    }

    ring_buffer__free(rb);
    close(map_fd);
    close(slave_sock);
    return 0;
}

static int handle_event(void *ctx, void *data, size_t data_sz) {
    (void)ctx;
    (void)data_sz;

    struct event_t *e = (struct event_t *)data;
    if (e->payload_len == 0 || e->payload_len > MAX_PAYLOAD_SIZE)
        return 0;

    // 直接转发 payload 给从机
    ssize_t sent = send(slave_sock, e->payload, e->payload_len, MSG_NOSIGNAL);
    if (sent < 0) {
        if (errno == EAGAIN || errno == EWOULDBLOCK) {
            usleep(1000);
            sent = send(slave_sock, e->payload, e->payload_len, MSG_NOSIGNAL);
        }
        if (sent < 0) {
            perror("send");
            running = 0;
        }
    }
    return 0;
}













CC = gcc
CLANG = clang

CFLAGS = -Wall -g -O2 -fno-omit-frame-pointer -I ./NtyCo/core/

LDFLAGS = -L ./NtyCo/ -lntyco -lpthread -luring -ldl -libverbs -lrdmacm

# ★ 链接 libbpf 必须加上 -lbpf -lelf -lz
BPF_LDFLAGS = -lbpf -lelf -lz

# ★ 简化 BPF 编译选项，避免与 vmlinux.h 冲突
BPF_CFLAGS = -target bpf -D__TARGET_ARCH_x86 -I. -I/usr/include -g -O2 -Wall

# ★ 保留原文件名 sync_filter.bpf.c
BPF_KERN_SRC = sync_filter.bpf.c
BPF_KERN_OBJ = sync_filter.bpf.o

SRCS = server.c kvstore.c mempool.c persistence.c snapshot.c reactor.c proactor.c ntyco.c resp.c \
       kvs_array.c kvs_rbtree.c kvs_hash.c kvs_skiptable.c kv_utils.c config.c \
       rdma.c repl.c expire.c

TARGET = server
RELAY_TARGET = ebpf_relay

TESTCASES = test_fullpersistence1 test_fullpersistence2 test_incrementpersistence1 \
            test_incrementpersistence2 test_mempool test_mempool_slab test_master test_slave test_TTL \
            test_batchcommand test_batchcommand_verify test_specialchars test_save \
            test_1G test_1G_verify test_transport test_transport_verify

SUBDIR = ./NtyCo/
OBJS = server.o kvstore.o mempool.o persistence.o snapshot.o reactor.o proactor.o ntyco.o resp.o \
       kvs_array.o kvs_rbtree.o kvs_hash.o kvs_skiptable.o kv_utils.o config.o \
       rdma.o repl.o expire.o

.PHONY: all clean ECHO $(SUBDIR) load_bpf unload_bpf

# ========== 默认编译 ==========
all: $(SUBDIR) $(BPF_KERN_OBJ) $(TARGET) $(RELAY_TARGET) $(TESTCASES)

$(SUBDIR): ECHO
	make -C $@

ECHO:
	@echo $(SUBDIR)

# eBPF 内核态字节码编译（kprobe/kretprobe 版本）
$(BPF_KERN_OBJ): $(BPF_KERN_SRC)
	$(CLANG) $(BPF_CFLAGS) -c $< -o $@
	@echo "✨ BPF kprobe bytecode compiled."

# 主服务器程序
$(TARGET): $(OBJS)
	$(CC) -o $@ $(OBJS) $(LDFLAGS)

# eBPF 用户态中继程序（★ 链接 libbpf）
$(RELAY_TARGET): ebpf_relay.o
	$(CC) -o $@ $< $(BPF_LDFLAGS)

ebpf_relay.o: ebpf_relay.c
	$(CC) $(CFLAGS) -c $< -o $@

# 测试用例
$(TESTCASES): %: %.c
	$(CC) $(CFLAGS) -o $@ $<

# 通用 .c -> .o 编译规则
%.o: %.c
	$(CC) $(CFLAGS) -c $< -o $@

IFACE = ens33

# 加载 eBPF kprobe/kretprobe 程序
load_bpf: $(BPF_KERN_OBJ)
	@echo "加载 eBPF kprobe/kretprobe 程序并 Pin Maps..."
	@sudo rm -rf /sys/fs/bpf/* 2>/dev/null || true
	sudo bpftool prog load $(BPF_KERN_OBJ) /sys/fs/bpf/tcp_recvmsg_prog type kprobe pinmaps /sys/fs/bpf/
	sudo bpftool attach kretprobe /sys/fs/bpf/tcp_recvmsg_prog tcp_recvmsg
	@echo "kprobe/tcp_recvmsg 已就绪！"

# 卸载 eBPF 程序
unload_bpf:
	@echo "清理内核 eBPF 残留..."
	sudo bpftool detach kretprobe /sys/fs/bpf/tcp_recvmsg_prog tcp_recvmsg 2>/dev/null || true
	sudo rm -rf /sys/fs/bpf/* 2>/dev/null || true
	@echo "内核环境已恢复"

# 其他目标保留不变
batchcommand:
	@if [ -f "./batchcommand.sh" ]; then \
		chmod +x ./batchcommand.sh; \
		./batchcommand.sh; \
	else \
		echo "❌ 错误: 当前目录下未找到 batchcommand.sh 脚本！"; \
	fi

incrementpersistence:
	@if [ -f "./incrementpersistence.sh" ]; then \
		chmod +x ./incrementpersistence.sh; \
		./incrementpersistence.sh; \
	else \
		echo "❌ 错误: 当前目录下未找到 incrementpersistence.sh 脚本！"; \
	fi

save:
	@if [ -f "./save.sh" ]; then \
		chmod +x ./save.sh; \
		./save.sh; \
	else \
		echo "❌ 错误: 当前目录下未找到 save.sh 脚本！"; \
	fi

auto_test:
	@if [ -f "./auto_test.sh" ]; then \
		chmod +x ./auto_test.sh; \
		./auto_test.sh; \
	else \
		echo "❌ 错误: 当前目录下未找到 auto_test.sh 脚本！"; \
	fi

fire_test:
	@if [ -f "./fire_test.sh" ]; then \
		chmod +x ./fire_test.sh; \
		./fire_test.sh; \
	else \
		echo "❌ 错误: 当前目录下未找到 fire_test.sh 脚本！"; \
	fi

clean:
	rm -rf $(TARGET) $(RELAY_TARGET) $(TESTCASES) kvstore.aof kvstore.snap kvstore.snap.tmp $(BPF_KERN_OBJ) \
	       ebpf_relay.o server.o kvstore.o mempool.o persistence.o snapshot.o \
	       reactor.o proactor.o ntyco.o resp.o \
	       kvs_array.o kvs_rbtree.o kvs_hash.o kvs_skiptable.o kv_utils.o \
	       config.o rdma.o repl.o expire.o
	rm -rf perf.data out.folded out.perf kvstore.svg *.svg
	make -C $(SUBDIR) clean



