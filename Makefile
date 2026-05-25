CC = gcc
CFLAGS = -Wall -g -I ./NtyCo/core/
LDFLAGS = -L ./NtyCo/ -lntyco -lpthread -luring -ldl

SRCS = kvstore.c mempool.c persistence.c snapshot.c reactor.c proactor.c ntyco.c\
       kvs_array.c kvs_rbtree.c kvs_hash.c kvs_skiplist.c kv_utils.c expire_thread.c replication.c

TESTCASE_SRCS = test1.c

TARGET = kvstore
TESTCASE = test1
SUBDIR = ./NtyCo/

OBJS = kvstore.o mempool.o persistence.o snapshot.o reactor.o proactor.o ntyco.o\
       kvs_array.o kvs_rbtree.o kvs_hash.o kvs_skiplist.o kv_utils.o expire_thread.o replication.o

.PHONY: all clean ECHO $(SUBDIR)

all: $(SUBDIR) $(TARGET) $(TESTCASE)

$(SUBDIR): ECHO
	make -C $@

ECHO:
	@echo $(SUBDIR)

$(TARGET): $(OBJS) 
	$(CC) -o $@ $(OBJS) $(LDFLAGS)

$(TESTCASE): $(TESTCASE_SRCS)
	$(CC) $(CFLAGS) -o $@ $^

%.o: %.c
	$(CC) $(CFLAGS) -c $< -o $@

clean: 
	rm -rf $(OBJS) $(TARGET) $(TESTCASE) kvstore.aof kvstore.snap
	make -C $(SUBDIR) clean