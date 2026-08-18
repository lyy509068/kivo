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
-P 1       | 15460          | 33146          | 14938          | 23367           | 26943          | 22365           
-P 10      | 147297         | 286944         | 123472         | 160000          | 201369         | 137419          
-P 20      | 282885         | 469043         | 208811         | 246002          | 343878         | 194628          
-P 40      | 514668         | 874890         | 320204         | 333333          | 537056         | 251004          
-P 80      | 877193         | 1483679        | 431778         | 414765          | 751314         | 296559          
-P 160     | 1396648        | 2444987        | 546746         | 454959          | 944287         | 320410          
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
2026-08-16 12:05:22 1000000        1          37.55      26632      1.53ms
2026-08-16 12:06:00 100000         10         37.38      26754      1.45ms
2026-08-16 12:06:40 10000          100        39.14      25550      1.51ms
2026-08-16 12:07:18 1000           1000       38.10      26272      1.02ms
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
测试命令 / 数据结构                  | 关闭日志 QPS | 开启日志 QPS
------------------------------------------------------------------------- 
Redis PING                          | 15342           | 15417          
Redis SET                           | 14793           | 13618          
KVstore PING                        | 32930           | 32823          
KVstore SET (Array)                 | 12004           | 11737          
KVstore RSET (Red-Black)            | 22532           | 22386          
KVstore HSET (Hash)                 | 26432           | 25095          
KVstore SSET (SkipList)             | 21802           | 21662          
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
Glibc_Malloc   |    30660 |    98448 |    96312 |     7840 |    75648 |     8176 |    40548 |    24662 |  1000000
Glibc_Malloc   |    30660 |    98208 |    96148 |     7828 |    75400 |     8164 |    41040 |    24366 |  1000000
Glibc_Malloc   |    30660 |    98332 |    96244 |     7836 |    75644 |     8140 |    41380 |    24166 |  1000000
Jemalloc       |    51744 |   113184 |   133664 |    10244 |    74272 |    48236 |    38378 |    26056 |  1000000
Jemalloc       |    51744 |   113184 |   133664 |    10252 |    74500 |    44336 |    38725 |    25823 |  1000000
Jemalloc       |    51744 |   113184 |   133664 |    10132 |    74328 |    45888 |    39221 |    25496 |  1000000
Custom_Mempool |    30924 |   165988 |   151136 |     7944 |    84568 |     8960 |    39076 |    25591 |  1000000
Custom_Mempool |    30924 |   166124 |   151668 |     7912 |    84636 |     8944 |    39366 |    25402 |  1000000
Custom_Mempool |    30924 |   165896 |   151304 |     7896 |    84516 |     8912 |    39353 |    25411 |  1000000
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
[TCP] Received 1073745705 bytes in 3.942 seconds, throughput: 259.74 MB/s
lyy@myubuntu:~/course/project/KVstore2/9.1-kvstore$ iperf3 -c 192.168.37.129 -t 20
Connecting to host 192.168.37.129, port 5201
[  5] local 192.168.37.128 port 59646 connected to 192.168.37.129 port 5201
[ ID] Interval           Transfer     Bitrate         Retr  Cwnd
[  5]   0.00-1.00   sec   320 MBytes  2.68 Gbits/sec   10   1.55 MBytes       
[  5]   1.00-2.00   sec   278 MBytes  2.33 Gbits/sec   15   1.55 MBytes       
[  5]   2.00-3.00   sec   378 MBytes  3.17 Gbits/sec  136   1.06 MBytes       
[  5]   3.00-4.00   sec   445 MBytes  3.73 Gbits/sec   78    874 KBytes       
[  5]   4.00-5.00   sec   422 MBytes  3.54 Gbits/sec  132    769 KBytes       
[  5]   5.00-6.00   sec   385 MBytes  3.23 Gbits/sec   37   1.18 MBytes       
[  5]   6.00-7.00   sec   432 MBytes  3.63 Gbits/sec   90    961 KBytes       
[  5]   7.00-8.00   sec   496 MBytes  4.16 Gbits/sec  109   1.13 MBytes       
[  5]   8.00-9.00   sec   358 MBytes  3.00 Gbits/sec   35    935 KBytes       
[  5]   9.00-10.00  sec   365 MBytes  3.06 Gbits/sec   24    996 KBytes       
[  5]  10.00-11.00  sec   388 MBytes  3.26 Gbits/sec   57   1014 KBytes       
[  5]  11.00-12.00  sec   426 MBytes  3.57 Gbits/sec   37    751 KBytes       
[  5]  12.00-13.00  sec   366 MBytes  3.07 Gbits/sec   39    856 KBytes       
[  5]  13.00-14.00  sec   465 MBytes  3.90 Gbits/sec   73   1.17 MBytes       
[  5]  14.00-15.00  sec   440 MBytes  3.69 Gbits/sec   96    682 KBytes       
[  5]  15.00-16.00  sec   405 MBytes  3.40 Gbits/sec   32   1.07 MBytes       
[  5]  16.00-17.00  sec   418 MBytes  3.50 Gbits/sec   49   1.14 MBytes       
[  5]  17.00-18.00  sec   341 MBytes  2.86 Gbits/sec    1   1.27 MBytes       
[  5]  18.00-19.00  sec   425 MBytes  3.56 Gbits/sec   31   1.19 MBytes       
[  5]  19.00-20.00  sec   391 MBytes  3.28 Gbits/sec   29   1.04 MBytes       
- - - - - - - - - - - - - - - - - - - - - - - - -
[ ID] Interval           Transfer     Bitrate         Retr
[  5]   0.00-20.00  sec  7.76 GBytes  3.33 Gbits/sec  1110             sender
[  5]   0.00-20.03  sec  7.76 GBytes  3.33 Gbits/sec                  receiver

iperf Done.


[RDMA] Received 1073745705 bytes in 8.668 seconds, throughput: 118.14 MB/s
lyy@myubuntu:~/course/project/KVstore2/9.1-kvstore$ iperf3 -c 192.168.37.129 -t 20
Connecting to host 192.168.37.129, port 5201
[  5] local 192.168.37.128 port 47370 connected to 192.168.37.129 port 5201
[ ID] Interval           Transfer     Bitrate         Retr  Cwnd
[  5]   0.00-1.00   sec   409 MBytes  3.43 Gbits/sec   42    918 KBytes       
[  5]   1.00-2.00   sec   369 MBytes  3.09 Gbits/sec   53    437 KBytes       
[  5]   2.00-3.00   sec   298 MBytes  2.50 Gbits/sec   50    655 KBytes       
[  5]   3.00-4.00   sec   202 MBytes  1.70 Gbits/sec   79    821 KBytes       
[  5]   4.00-5.00   sec   404 MBytes  3.39 Gbits/sec   33    647 KBytes       
[  5]   5.00-6.00   sec   380 MBytes  3.19 Gbits/sec   28    874 KBytes       
[  5]   6.00-7.00   sec   382 MBytes  3.20 Gbits/sec   35    918 KBytes       
[  5]   7.00-8.00   sec   425 MBytes  3.57 Gbits/sec   55   1.14 MBytes       
[  5]   8.00-9.00   sec   545 MBytes  4.57 Gbits/sec   59    900 KBytes       
[  5]   9.00-10.00  sec   572 MBytes  4.80 Gbits/sec   34   1.14 MBytes       
[  5]  10.00-11.00  sec   581 MBytes  4.87 Gbits/sec   46   1.19 MBytes       
[  5]  11.00-12.00  sec   596 MBytes  5.01 Gbits/sec   35   1.14 MBytes       
[  5]  12.00-13.00  sec   568 MBytes  4.76 Gbits/sec   19   1.06 MBytes       
[  5]  13.00-14.00  sec   559 MBytes  4.68 Gbits/sec   98   1.13 MBytes       
[  5]  14.00-15.00  sec   591 MBytes  4.96 Gbits/sec   42   1.11 MBytes       
[  5]  15.00-16.00  sec   559 MBytes  4.70 Gbits/sec   49   1.05 MBytes       
[  5]  16.00-17.00  sec   591 MBytes  4.95 Gbits/sec   34   1.01 MBytes       
[  5]  17.00-18.00  sec   580 MBytes  4.85 Gbits/sec   72   1.11 MBytes       
[  5]  18.00-19.00  sec   576 MBytes  4.84 Gbits/sec   15   1.15 MBytes       
[  5]  19.00-20.00  sec   476 MBytes  4.01 Gbits/sec   44   1.13 MBytes       
- - - - - - - - - - - - - - - - - - - - - - - - -
[ ID] Interval           Transfer     Bitrate         Retr
[  5]   0.00-20.00  sec  9.44 GBytes  4.05 Gbits/sec  922             sender
[  5]   0.00-20.04  sec  9.44 GBytes  4.04 Gbits/sec                  receiver

iperf Done.


# 增量同步性能测试
 转发方式	  QPS	    	
基准    	 2022	 
eBPF 转发        1816 	 	
TCP 网络转发	  1598

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



