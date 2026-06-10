# Kvstore

启动服务器./server 2000 

# 特殊字符和批量命令测试 
窗口模式：
        redis-cli -p 2000 --raw 插入命令在本地文件1.txt中(不能超过redis 窗口限制字符数量)
文件模式：
        特殊字符 
        redis-cli -p 2000 -x SET io_multiplexing_article < 本地文件2.txt
        redis-cli -p 2000 --raw GET io_multiplexing_article | head -n 20
        cat test_cmd.txt | redis-cli -p 2000 --pipe
        批量命令 
        redis-cli -p 2000 < test_cmd.txt 
        

# 压力测试：-p 端口，-c 50个并发连接，-n 总共发送10000条命令，-t 只测试 set和get命令
redis-benchmark -p 2000 -c 50 -n 10000 -t set,get

redis-benchmark -p 2000 -c 50 -n 10000 -r 10000 SET key:__rand_int__ value:__rand_int__
redis-benchmark -p 2000 -c 50 -n 10000 -r 10000 GET key:__rand_int__ value:__rand_int__

redis-benchmark -p 2000 -c 50 -n 10000 -r 10000 RSET key:__rand_int__ value:__rand_int__
redis-benchmark -p 2000 -c 50 -n 10000 -r 10000 RGET key:__rand_int__ value:__rand_int__

redis-benchmark -p 2000 -c 50 -n 10000 -r 10000 HSET key:__rand_int__ value:__rand_int__
redis-benchmark -p 2000 -c 50 -n 10000 -r 10000 HGET key:__rand_int__ value:__rand_int__

redis-benchmark -p 2000 -c 50 -n 10000 -r 10000 SSET key:__rand_int__ value:__rand_int__
redis-benchmark -p 2000 -c 50 -n 10000 -r 10000 SGET key:__rand_int__ value:__rand_int__

# 测试结果
  数据结构	    命令	  QPS (请求/秒)	 平均延迟
Array (基础)	SET	    14,450 ops/sec	~2-3ms
RBTREE (红黑树)	RSET	29,850 ops/sec	~1-2ms
HASH (哈希表)	HSET	25,316 ops/sec	~1-2ms
SKIPLIST (跳表)	SSET	29,411 ops/sec	~1-2ms

# 全量持久化测试 test_fullpersistence
一共四种模式 ./test_fullpersistence (1 2 3 4)
客户端：连接服务器->插入10w条数据->SAVE保存快照->SHUTDOWN关闭服务器->重新打开服务器->重新连接服务器->获取10w条数据并校验->清除快照文件（不影响下次测试）->SHUTDOWN关闭服务器

# 增量持久化测试
一共四种模式 ./test_incrementpersistence (1 2 3 4)
客户端：连接服务器->插入10w条数据->SHUTDOWN关闭服务器->重新打开服务器->重新连接服务器->获取10w条数据并校验->清除日志文件（不影响下次测试）->SHUTDOWN关闭服务器

# 超时功能测试
./test_TTL (1 2 3 4)
如果不传超时时间，默认永不超时；
底层存储结构执行SET GET EXISTS 遍历时会进行惰性删除，从而保证快照实现超时删除；
后台超时清理线程不停存储结构，如果发现某个节点被删掉，就追加一条删除命令的日志，并向从端发送一条删除命令；
测试流程：连接服务器->插入3s超时的数据->立即读取，此时数据都存在->等待4s读取数据全部被删掉

# 分段锁
一种数据结构用同一套读写锁，对这个数据结构在一个时间只能进行增删改查的一种操作，增删改加写锁，查加读锁；
对哈希表来说，有多个哈希桶，只要不是在同一个桶中的操作可以同时进行，就可以每个桶一套锁，提升操作效率；

# 内存池测试
./test_mempool (0 1 2) (1 2 3 4) 100000
# 测试结果
内存分配器	      存储引擎	      总操作数	  耗时(ms)	QPS	    VmSize增量  VmRSS增量(KB)
Glibc_Malloc	Array (1)	    100,000	    40,520	 2,467	        0	        0
                RBTREE (2)	    100,000	    6,259	15,976	    +6,468	    +6,704
                HASH (3)	    100,000	    6,125	16,326	    +5,676	    +7,056
                SKIPLIST (4)	100,000	    7,463	13,399	    +7,260	    +7,384
Jemalloc	    Array (1)	    100,000	    37,761	 2,648	    +6,144	    +3,332
                RBTREE (2)	    100,000	    6,296	15,883	    +6,144	    +7,004
                HASH (3)	    100,000	    6,136	16,297	    +6,144	    +6,508
                SKIPLIST (4)	100,000	    7,219	13,852	    +6,144	    +6,252
Custom_Mempool	Array (1)	    100,000	    36,044	 2,774	    +4,224	    +4,776
                RBTREE (2)	    100,000	    6,188	16,160	    +8,448	    +8,740
                HASH (3)	    100,000	    6,184	16,170	    +7,128	    +7,760
                SKIPLIST (4)	100,000	    7,900	12,658	    +9,108	    +9,504
                
# 主从同步测试
需要配合日志
打开主端服务器./server 2000 插入5w条数据 ./test_master (1 2 3 4) 1
打开从端服务器,.server 2000 reactor_start启动后，从端服务器主动连接主端服务器，向主端发送获取日志命令
主端服务器收到获取日志命令后，把日志文件发过去
从端收到日志文件后，在从端恢复日志
继续向主端插入5w条数据 ./test_master (1 2 3 4) 2
从端同步完第二轮的5w条数据后，通过客户端验证 ./test_slave (1 2 3 4)

不太稳定，会丢包，reactor和proactor都会出现这个问题

### 面试题
1. 为什么会实现kvstore，使用场景在哪里？
2. reactor, ntyco, io_uring的三种网络模型的性能差异？
3. 多线程的kvstore该如何改进？
4. 私有协议如何设计会更加安全可靠？
5. 协议改进以后，对已有的代码有哪些改变？
6. kv引擎实现了哪些？
7. 每个kv引擎的使用场景，以及性能差异？
8. 测试用例如何实现？并且保证代码覆盖率超过90%
9. 网络并发量如何？qps如何？
10. 能够跟哪些系统交互使用？


### 架构设计
![image](https://disk.0voice.com/p/py)



