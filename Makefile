CC = gcc
CFLAGS = -Wall -g -I ./NtyCo/core/
LDFLAGS = -L ./NtyCo/ -lntyco -lpthread -luring -ldl

SRCS = server.c kvstore.c mempool.c persistence.c snapshot.c reactor.c resp.c\
       kvs_array.c kvs_rbtree.c kvs_hash.c kvs_skiptable.c kv_utils.c expire_thread.c replication.c

TARGET = server
TESTCASES = client test_fullpersistence test_incrementpersistence test_mempool
SUBDIR = ./NtyCo/

OBJS = server.o kvstore.o mempool.o persistence.o snapshot.o reactor.o resp.o\
       kvs_array.o kvs_rbtree.o kvs_hash.o kvs_skiptable.o kv_utils.o expire_thread.o replication.o

.PHONY: all clean ECHO $(SUBDIR)

# all 依赖所有的测试客户端
all: $(SUBDIR) $(TARGET) $(TESTCASES)

$(SUBDIR): ECHO
	make -C $@

ECHO:
	@echo $(SUBDIR)

$(TARGET): $(OBJS) 
	$(CC) -o $@ $(OBJS) $(LDFLAGS)

$(TESTCASES): %: %.c
	$(CC) $(CFLAGS) -o $@ $<

%.o: %.c
	$(CC) $(CFLAGS) -c $< -o $@

clean: 
	rm -rf $(OBJS) $(TARGET) $(TESTCASES) kvstore.aof kvstore.snap
	make -C $(SUBDIR) clean