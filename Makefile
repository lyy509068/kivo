CC = gcc
CLANG = clang
CFLAGS = -Wall -g -D_GNU_SOURCE -I ./NtyCo/core/
LDFLAGS = -L ./NtyCo/ -lntyco -lpthread -luring -ldl -libverbs -lrdmacm -lbpf -lelf -lz

BPF_CFLAGS = -target bpf -D__TARGET_ARCH_x86 -I/usr/include/x86_64-linux-gnu -I/usr/include -g -O2 -Wall

SRCS = server.c kvstore.c mempool.c persistence.c snapshot.c reactor.c resp.c udp.c\
       kvs_array.c kvs_rbtree.c kvs_hash.c kvs_skiptable.c kv_utils.c \
       rdma.c ebpf.c repl.c

TARGET = server
TESTCASES = test_fullpersistence1 test_fullpersistence2 test_incrementpersistence1 \
            test_incrementpersistence2 test_mempool test_master test_slave test_TTL
SUBDIR = ./NtyCo/

OBJS = server.o kvstore.o mempool.o persistence.o snapshot.o reactor.o resp.o udp.o\
       kvs_array.o kvs_rbtree.o kvs_hash.o kvs_skiptable.o kv_utils.o \
       rdma.o ebpf.o repl.o

.PHONY: all clean ECHO $(SUBDIR) load_bpf unload_bpf

# 默认只编译代码，不进行任何内核挂载
all: $(SUBDIR) sync_filter.bpf.o $(TARGET) $(TESTCASES)

$(SUBDIR): ECHO
	make -C $@

ECHO:
	@echo $(SUBDIR)

sync_filter.bpf.o: sync_filter.bpf.c
	$(CLANG) $(BPF_CFLAGS) -c $< -o $@
	@echo "✨ BPF Kernel bytes bytecode compiled."

$(TARGET): $(OBJS)
	$(CC) -o $@ $(OBJS) $(LDFLAGS)

$(TESTCASES): %: %.c
	$(CC) $(CFLAGS) -o $@ $<

%.o: %.c
	$(CC) $(CFLAGS) -c $< -o $@

# eBPF 部署与卸载规则
IFACE = ens33

load_bpf: sync_filter.bpf.o
	@echo "加载 eBPF 程序并强行 Pin Maps..."
	@sudo rm -rf /sys/fs/bpf/* 2>/dev/null || true
	# 1. 使用 bpftool 加载程序，并将内部所有带 pinning=1 的 maps 自动钉到 /sys/fs/bpf/
	sudo bpftool prog load sync_filter.bpf.o /sys/fs/bpf/handle_tc_dual_write type classifier pinmaps /sys/fs/bpf/
	# 2. 挂载 tc 队列
	sudo tc qdisc add dev $(IFACE) clsact 2>/dev/null || true
	# 3. 将已经钉选在 BPF FS 里的程序直接绑定到网卡 Ingress 上
	sudo tc filter replace dev $(IFACE) ingress bpf pinned /sys/fs/bpf/handle_tc_dual_write direct-action
	@echo "eBPF 全局管道和 Maps 已在内核就绪！"

unload_bpf:
	@echo "清理内核 eBPF 残留..."
	sudo tc filter del dev $(IFACE) ingress 2>/dev/null || true
	sudo tc qdisc del dev $(IFACE) clsact 2>/dev/null || true
	sudo rm -rf /sys/fs/bpf/* 2>/dev/null || true
	@echo "内核环境已恢复"

clean: 
	rm -rf $(OBJS) $(TARGET) $(TESTCASES) kvstore.aof kvstore.snap sync_filter.bpf.o
	make -C $(SUBDIR) clean