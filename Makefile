CC = gcc
CLANG = clang
# 在 CFLAGS 中加入了 -D_GNU_SOURCE 选项
CFLAGS = -Wall -g -D_GNU_SOURCE -I ./NtyCo/core/
LDFLAGS = -L ./NtyCo/ -lntyco -lpthread -luring -ldl -libverbs -lrdmacm -lbpf -lelf -lz

# BPF 编译选项
BPF_CFLAGS = -target bpf \
             -D__TARGET_ARCH_x86 \
             -I/usr/include/x86_64-linux-gnu \
             -I/usr/include \
             -g -O2 -Wall

SRCS = server.c kvstore.c mempool.c persistence.c snapshot.c reactor.c resp.c\
       kvs_array.c kvs_rbtree.c kvs_hash.c kvs_skiptable.c kv_utils.c \
       rdma.c ebpf.c repl.c

TARGET = server
TESTCASES = test_fullpersistence1 test_fullpersistence2 test_incrementpersistence1 \
            test_incrementpersistence2 test_mempool test_master test_slave test_TTL
SUBDIR = ./NtyCo/


OBJS = server.o kvstore.o mempool.o persistence.o snapshot.o reactor.o resp.o\
       kvs_array.o kvs_rbtree.o kvs_hash.o kvs_skiptable.o kv_utils.o \
       rdma.o ebpf.o repl.o

.PHONY: all clean ECHO $(SUBDIR)

all: $(SUBDIR) sync_filter.bpf.o $(TARGET) $(TESTCASES)

$(SUBDIR): ECHO
	make -C $@

ECHO:
	@echo $(SUBDIR)

# BPF 编译
sync_filter.bpf.o: sync_filter.bpf.c
	$(CLANG) $(BPF_CFLAGS) -c $< -o $@
	@echo "BPF program compiled (will be loaded at runtime)"

# server 不再链接 sync_filter.bpf.o
$(TARGET): $(OBJS)
	$(CC) -o $@ $(OBJS) $(LDFLAGS)

$(TESTCASES): %: %.c
	$(CC) $(CFLAGS) -o $@ $<

%.o: %.c
	$(CC) $(CFLAGS) -c $< -o $@

clean: 
	rm -rf $(OBJS) $(TARGET) $(TESTCASES) kvstore.aof kvstore.snap sync_filter.bpf.o
	make -C $(SUBDIR) clean