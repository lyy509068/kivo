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
./test_specialchars 四种数据结构都插入5个特殊字符串，由本地五个文件作为value构建resp命令，先插入并验证回复，然后获取并逐字对比和本地文件是否相同

Redis                                                                                              
redis-benchmark -p 6379 -n 1000000 -r 100000000 -P 1  -c 1 -q SET key:__rand_int__ value:__rand_int__   
redis-benchmark -p 6379 -n 1000000 -r 100000000 -P 10 -c 1 -q SET key:__rand_int__ value:__rand_int__    
redis-benchmark -p 6379 -n 1000000 -r 100000000 -P 20 -c 1 -q SET key:__rand_int__ value:__rand_int__    
redis-benchmark -p 6379 -n 1000000 -r 100000000 -P 40 -c 1 -q SET key:__rand_int__ value:__rand_int__   
redis-benchmark -p 6379 -n 1000000 -r 100000000 -P 80 -c 1 -q SET key:__rand_int__ value:__rand_int__    
redis-benchmark -p 6379 -n 1000000 -r 100000000 -P 160 -c 1 -q SET key:__rand_int__ value:__rand_int__   

KVstore                                                                                              
redis-benchmark -p 2000 -n 1000000 -r 100000000 -P 1  -c 1 -q SET key:__rand_int__ value:__rand_int__     
redis-benchmark -p 2000 -n 1000000 -r 100000000 -P 10 -c 1 -q SET key:__rand_int__ value:__rand_int__   
redis-benchmark -p 2000 -n 1000000 -r 100000000 -P 20 -c 1 -q SET key:__rand_int__ value:__rand_int__   
redis-benchmark -p 2000 -n 1000000 -r 100000000 -P 40 -c 1 -q SET key:__rand_int__ value:__rand_int__    
redis-benchmark -p 2000 -n 1000000 -r 100000000 -P 80 -c 1 -q SET key:__rand_int__ value:__rand_int__    
redis-benchmark -p 2000 -n 1000000 -r 100000000 -P 160 -c 1 -q SET key:__rand_int__ value:__rand_int__   
                                                          

======================================================================================================================================
                                Redis vs KVstore 三大引擎独立测试 (1000000 条)                                                         
======================================================================================================================================
Pipeline   | Redis (PING)   | KV (PING)      | Redis (SET)    | KV (RBTree)     | KV (Hash)      | KV (SkipList)   
--------------------------------------------------------------------------------------------------------------------------------------
-P 1       | 15074          | 15465          | 14323          | 14373           | 15069          | 14060           
-P 10      | 145053         | 150037         | 117702         | 103745          | 130701         | 95858           
-P 20      | 274649         | 293513         | 197784         | 162733          | 232072         | 146584          
-P 40      | 504795         | 553097         | 300932         | 232991          | 378931         | 199282          
-P 80      | 864304         | 996016         | 394944         | 274197          | 554016         | 232342          
-P 160     | 1371742        | 1655629        | 503778         | 327761          | 726744         | 266737          
======================================================================================================================================

# 全量持久化功能测试
./test_fullpersistence1 插入10w条数据    
./test_fullpersistence2 获得10w条数据
手动打开服务器
客户端：连接服务器->插入10w条数据->SAVE保存快照->断开连接
手动关闭服务器再重新打开
客户端：重新连接服务器->获取10w条数据并校验->清除日志文件（不影响下次测试）->断开连接
每条日志：4字节引擎标志+8字节过期时间+4字节key长度+10字节key（可变）+4字节value长度+10字节value（可变）
# 性能测试：save指令的性能影响
===============================================================================
测试时间            间隔命令条数    SAVE次数   时间(s)    QPS        最后一次SAVE耗时
------------------ -------------- ---------- --------- ---------- ----------------
2026-08-13 08:48:17 1000000        1          62.80      15923      2.13ms
2026-08-13 08:49:19 100000         10         62.02      16125      2.00ms
2026-08-13 08:50:22 10000          100        62.74      15941      1.92ms
2026-08-13 08:51:26 1000           1000       62.85      15927      0.02ms
=================================================================================

# 增量持久化功能测试
./test_incrementpersistence1 插入10w条数据
array数据恢复时间较长，等待数据恢复完成后再获取数据
./test_incrementpersistence2 获得10w条数据
手动打开服务器
客户端：连接服务器->插入10w条数据->断开连接
手动关闭服务器再重新打开
客户端：重新连接服务器->获取10w条数据并校验->清除日志文件（不影响下次测试）->断开连接
每条日志：*3\r\n$3\r\nSET\r\n$10\r\nkey_015000\r\n$12\r\nvalue_015000\r\n
# 性能测试
redis测试命令   
sudo systemctl stop redis-server
sudo rm -f /var/lib/redis/appendonly.aof
sudo rm -f /var/lib/redis/dump.rdb
sudo systemctl restart redis-server
redis-cli -p 6379 CONFIG SET appendonly yes 
redis-cli -p 6379 CONFIG SET appendfsync everysec 
redis-cli -p 6379 CONFIG SET appendonly no
redis-benchmark -p 6379 -t ping -n 100000 -q

redis-benchmark -p 6379 -n 1000000 -r 100000000 -c 1 -q PING                                                        
redis-benchmark -p 6379 -n 1000000 -r 100000000 -c 1 -q SET key:__rand_int__ value:__rand_int__                      

KVstore测试命令
redis-benchmark -p 2000 -n 1000000 -r 100000000 -c 1 -q PING                                                         
redis-benchmark -p 2000 -n 10000 -r 100000000 -c 1 -q SET key:__rand_int__ value:__rand_int__                                 
redis-benchmark -p 2000 -n 1000000 -r 100000000 -c 1 -q RSET key:__rand_int__ value:__rand_int__                      
redis-benchmark -p 2000 -n 1000000 -r 100000000 -c 1 -q HSET key:__rand_int__ value:__rand_int__                      
redis-benchmark -p 2000 -n 1000000 -r 100000000 -c 1 -q SSET key:__rand_int__ value:__rand_int__ 

=========================================================================
                     日志开关性能影响对比测试汇总                       
=========================================================================
测试命令 / 数据结构         | 关闭日志 QPS | 打开日志 QPS
------------------------------------------------------------------------- 
Redis PING                          | 14837           | 14849          
Redis SET                           | 14196           | 13038          
KVstore PING                        | 15092           | 16131          
KVstore SET (Array)                 | 9532            | 8795           
KVstore RSET (Red-Black)            | 13970           | 14670          
KVstore HSET (Hash)                 | 14631           | 15261          
KVstore SSET (SkipList)             | 13704           | 14241          
=========================================================================


# 超时功能测试
./test_TTL a b 插入as超时的数据->立即读取，此时数据都存在->等待bs读取数据全部被删掉

如果不传超时时间，默认永不超时；
底层存储结构执行SET GET EXISTS 遍历时会进行惰性删除，从而保证快照实现超时删除；
后台超时清理线程不停存储结构，如果发现某个节点被删掉，就追加一条删除命令的日志，并向从端发送一条删除的增量命令；

# 分段锁
一种数据结构用同一套读写锁，对这个数据结构在一个时间只能进行增删改查的一种操作，增删改加写锁，查加读锁；
对哈希表来说，有多个哈希桶，只要不是在同一个桶中的操作可以同时进行，就可以每个桶一套锁，提升操作效率；

# 内存池测试
./test_mempool (0 1 2)  选择内存管理方式(不使用内存池 jemalloc mempool) 插入100w条数据

sudo LD_PRELOAD=/usr/lib/x86_64-linux-gnu/libjemalloc.so.2 ./server config.conf
===================================================================================================================
Strategy       |    VmSz1 |    VmSz2 |    VmSz3 |   VmRSS1 |   VmRSS2 |   VmRSS3 |  Time_ms |      QPS |      Ops
----------------|----------|----------|----------|----------|----------|----------|----------|----------|----------
Glibc_Malloc   |    29200 |    33028 |    33028 |     8352 |    12376 |    12376 |     2756 |   362844 |  1000000
Jemalloc       |    60932 |    60932 |    60932 |    10776 |    14712 |    14712 |     2500 |   400000 |  1000000
Custom_Mempool |    29332 |    33112 |    33112 |     8472 |    12232 |    12232 |     2368 |   422297 |  1000000
===================================================================================================================
                
# 主从同步测试

打开主端服务器./server 2000 插入5w条数据 ./test_master  1
打开从端服务器,.server 2000 reactor_start启动后，从端服务器主动连接主端服务器，向主端发送获取日志命令
主端服务器收到获取日志命令后，把日志文件发过去
从端收到日志文件后，在从端恢复日志
发送日志期间，如果有新的命令，就放进临时缓冲区中，发完日志再发一次
继续向主端插入5w条数据 ./test_master  2
从端同步完第二轮的5w条数据后，通过客户端验证 ./test_slave 

# 全量同步性能测试
服务端（接收方）：iperf3 -s
客户端（发送方）：iperf3 -c 192.168.37.129 -t 10
[Repl RDMA] Synchronized EXACT 1073745556 / 1073745556 bytes in 15.674 seconds, throughput: 65.33 MB/s MTU 8000  
              [Perf TCP] Received 1073749448/1073749448 bytes in 3.117 seconds, throughput: 328.53 MB/s


# 增量同步性能测试
 转发方式	  QPS	    	
基准（无同步）	 347726	 
eBPF 转发       351767 	 	
TCP 网络转发	403577

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



