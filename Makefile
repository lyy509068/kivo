CC = gcc
CLANG = clang

CFLAGS = -Wall -g -O2 -fno-omit-frame-pointer -I ./NtyCo/core/ -mavx2 -mfma

LDFLAGS = -L ./NtyCo/ -lntyco -lpthread -luring -ldl -libverbs -lrdmacm -lcjson -lm -lcurl

BPF_LDFLAGS = -lbpf -lelf -lz

BPF_CFLAGS = -target bpf -D__TARGET_ARCH_x86 -I/usr/include/x86_64-linux-gnu -I/usr/include -g -O2 -Wall

SRCS = server.c kvstore.c mempool.c persistence.c snapshot.c reactor.c proactor.c ntyco.c resp.c \
       kvs_array.c kvs_rbtree.c kvs_hash.c kvs_skiptable.c kv_utils.c config.c\
       rdma.c repl.c expire.c ai_chat.c

TARGET = server

RELAY_TARGET = ebpf_relay

BPF_KERN_OBJ = sync_filter.bpf.o

TESTCASES = test_fullpersistence1 test_fullpersistence2 test_incrementpersistence1 \
            test_incrementpersistence2 test_mempool test_mempool_slab test_master test_slave test_TTL test_ttl\
            test_batchcommand test_batchcommand_verify test_specialchars test_save \
            test_1G test_1G_verify test_transport test_transport_verify \

SUBDIR = ./NtyCo/

OBJS = server.o kvstore.o mempool.o persistence.o snapshot.o reactor.o proactor.o ntyco.o resp.o \
       kvs_array.o kvs_rbtree.o kvs_hash.o kvs_skiptable.o kv_utils.o config.o\
       rdma.o repl.o expire.o ai_chat.o

.PHONY: all clean ECHO $(SUBDIR) load_bpf unload_bpf

# ========== 默认编译 ==========
all: $(SUBDIR) $(BPF_KERN_OBJ) $(TARGET) $(RELAY_TARGET) $(TESTCASES)

$(SUBDIR): ECHO
	make -C $@

ECHO:
	@echo $(SUBDIR)

# eBPF 内核态字节码编译
$(BPF_KERN_OBJ): sync_filter.bpf.c
	$(CLANG) $(BPF_CFLAGS) -c $< -o $@
	@echo "✨ BPF Kernel bytes bytecode compiled."

# 主服务器程序
$(TARGET): $(OBJS)
	$(CC) -o $@ $(OBJS) $(LDFLAGS)

# eBPF 用户态中继程序
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

rdma:
	@if [ -f "./rdma.sh" ]; then \
		chmod +x ./rdma.sh; \
		./rdma.sh; \
	else \
		echo "❌ 错误: 当前目录下未找到 rdma.sh 脚本！"; \
	fi

load_bpf: $(BPF_KERN_OBJ)
	@echo "加载 eBPF 程序并强行 Pin Maps..."
	@sudo rm -rf /sys/fs/bpf/* 2>/dev/null || true
	sudo bpftool prog load $(BPF_KERN_OBJ) /sys/fs/bpf/handle_tc_dual_write type classifier pinmaps /sys/fs/bpf/
	sudo tc qdisc add dev $(IFACE) clsact 2>/dev/null || true
	sudo tc filter replace dev $(IFACE) ingress bpf pinned /sys/fs/bpf/handle_tc_dual_write direct-action
	@echo "eBPF 全局管道和 Maps 已在内核就绪！"

unload_bpf:
	@echo "清理内核 eBPF 残留..."
	sudo tc filter del dev $(IFACE) ingress 2>/dev/null || true
	sudo tc qdisc del dev $(IFACE) clsact 2>/dev/null || true
	sudo rm -rf /sys/fs/bpf/* 2>/dev/null || true
	@echo "内核环境已恢复"

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

auto_incre_test:
	@if [ -f "./auto_incre_test.sh" ]; then \
		chmod +x ./auto_incre_test.sh; \
		./auto_incre_test.sh; \
	else \
		echo "❌ 错误: 当前目录下未找到 auto_incre_test.sh 脚本！"; \
	fi

auto_full_test:
	@if [ -f "./auto_full_test.sh" ]; then \
		chmod +x ./auto_full_test.sh; \
		./auto_full_test.sh; \
	else \
		echo "❌ 错误: 当前目录下未找到 auto_full_test.sh 脚本！"; \
	fi

fire_test:
	@if [ -f "./fire_test.sh" ]; then \
		chmod +x ./fire_test.sh; \
		./fire_test.sh; \
	else \
		echo "❌ 错误: 当前目录下未找到 fire_test.sh 脚本！"; \
	fi

clean:
	rm -rf $(TARGET) $(RELAY_TARGET) $(TESTCASES) kvstore.aof kvstore.snap kvstore.snap.tmp $(BPF_KERN_OBJ) ebpf_relay.o server.o kvstore.o mempool.o persistence.o snapshot.o \
	                                          reactor.o proactor.o ntyco.o resp.o \
	                                          kvs_array.o kvs_rbtree.o kvs_hash.o kvs_skiptable.o kv_utils.o \
	                                          config.o rdma.o repl.o expire.o ai_chat.o
	rm -rf perf.data out.folded out.perf kvstore.svg *.svg
	make -C $(SUBDIR) clean