# Kvstore

启动服务器sudo ./server config.conf 

# 特殊字符和批量命令测试 
redis窗口模式：
        redis-cli -p 2000 --raw 插入命令在本地文件1.txt中(不能超过redis 窗口限制字符数量)
redis文件模式：
        特殊字符 
        redis-cli -p 2000 -x SET io_multiplexing_article < 本地文件2.txt
        redis-cli -p 2000 --raw GET io_multiplexing_article | head -n 20
        批量命令 
        redis-cli -p 2000 < test_cmd.txt 

./test_batchcommand 一次性插入100条命令并验证回复，然后获取，重复1000次

# Redis pipeline 测试（160条一批）
redis-benchmark -h 127.0.0.1 -p 6379 -t set -n 100000 -P 160 -q


./test_specialchars 四种数据结构都插入5个特殊字符串，由本地五个文件作为value构建resp命令，先插入并验证回复，然后获取并逐字对比和本地文件是否相同

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
  数据结构	命令	  QPS (请求/秒)	 
Array (基础)	SET	  21k ops/sec	
RBTREE (红黑树)	RSET	  108k ops/sec	
HASH (哈希表)	HSET	  121k ops/sec	
SKIPLIST (跳表)	SSET	  101k ops/sec	

# 全量持久化测试 test_fullpersistence
./test_fullpersistence1 插入10w条数据    
./test_fullpersistence2 获得10w条数据
手动打开服务器
客户端：连接服务器->插入10w条数据->SAVE保存快照->断开连接
手动关闭服务器再重新打开
客户端：重新连接服务器->获取10w条数据并校验->清除日志文件（不影响下次测试）->断开连接

每条日志：4字节引擎标志+8字节过期时间+4字节key长度+10字节key（可变）+4字节value长度+10字节value（可变）

# 增量持久化测试
./test_incrementpersistence1 插入10w条数据
array数据恢复时间较长，等待数据恢复完成后再获取数据
./test_incrementpersistence2 获得10w条数据
手动打开服务器
客户端：连接服务器->插入10w条数据->断开连接
手动关闭服务器再重新打开
客户端：重新连接服务器->获取10w条数据并校验->清除日志文件（不影响下次测试）->断开连接

每条日志：4字节命令长度+4字节命令+8字节过期时间+4字节key长度+10字节key（可变）+4字节value长度+10字节value（可变）

# 超时功能测试
./test_TTL a b 插入as超时的数据->立即读取，此时数据都存在->等待bs读取数据全部被删掉

如果不传超时时间，默认永不超时；
底层存储结构执行SET GET EXISTS 遍历时会进行惰性删除，从而保证快照实现超时删除；
后台超时清理线程不停存储结构，如果发现某个节点被删掉，就追加一条删除命令的日志，并向从端发送一条删除的增量命令；

# 分段锁
一种数据结构用同一套读写锁，对这个数据结构在一个时间只能进行增删改查的一种操作，增删改加写锁，查加读锁；
对哈希表来说，有多个哈希桶，只要不是在同一个桶中的操作可以同时进行，就可以每个桶一套锁，提升操作效率；

# 内存池测试
./test_mempool (0 1 2) (1 2 3 4) 100000  选择内存管理方式(不使用内存池 jemalloc mempool) 数据结构(array rbtree hash skiptable) 插入10w条数据

sudo LD_PRELOAD=/usr/lib/x86_64-linux-gnu/libjemalloc.so.2 ./server config.conf
                
# 主从同步测试

打开主端服务器./server 2000 插入5w条数据 ./test_master (1 2 3 4) 1
打开从端服务器,.server 2000 reactor_start启动后，从端服务器主动连接主端服务器，向主端发送获取日志命令
主端服务器收到获取日志命令后，把日志文件发过去
从端收到日志文件后，在从端恢复日志
发送日志期间，如果有新的命令，就放进临时缓冲区中，发完日志再发一次
继续向主端插入5w条数据 ./test_master (1 2 3 4) 2
从端同步完第二轮的5w条数据后，通过客户端验证 ./test_slave (1 2 3 4)

# 全量同步测试
服务端（接收方）：iperf3 -s
客户端（发送方）：iperf3 -c 192.168.1.100 -t 10
转发方式     发送速度         时间        带宽(无传输时379MB/s)
  RDMA      32.31MB/ms      31.693s     194MB/s
  TCP       258.7MB/ms      3.958s      341MB/s
# 增量同步测试
 转发方式	  QPS	 时间(s)	
基准（无同步）	 10,784	  9.273
eBPF 转发       10,171	 9.831	
TCP 网络转发	 7,513	 13.310	

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



