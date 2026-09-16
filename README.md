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
        redis-cli -p 2000 < test_vector.txt 

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
测试命令 / 数据结构         | 关闭日志 QPS | 开启日志 QPS
------------------------------------------------------------------------- 
Redis PING                          | 14189           | 14183          
Redis SET                           | 13578           | 12860          
KVstore PING                        | 23830           | 23023          
KVstore SET (Array)                 | 17574           | 17921          
KVstore RSET (Red-Black)            | 19754           | 19126          
KVstore HSET (Hash)                 | 20643           | 20361          
KVstore SSET (SkipList)             | 19293           | 18970          
=========================================================================


# 超时功能测试
./test_TTL a b 插入as超时的数据->立即读取，此时数据都存在->等待bs读取数据全部被删掉
如果不传超时时间，默认永不超时；
底层存储结构执行SET GET EXISTS 遍历时会进行惰性删除，从而保证快照实现超时删除；
===========================================================================
| 测试场景       |   数据量   | TTL |   QPS  |   相对基准 |    过期验证             
| 无 TTL        | 1,000,000  |   — | 21,905 |     100%  | 100 万条均存在        
| 开启 TTL      | 1,000,000  | 30s | 21,618 |    98.69% | 等待 35s 后过期 
===========================================================================

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
客户端（发送方）：iperf3 -c 192.168.88.130 -t 10
=================== 第一轮：基准带宽 ===================
时间: 2026-09-02 07:43:37
--- 基准网络带宽 (iperf3 20s) ---
Connecting to host 192.168.88.130, port 5201
[  5] local 192.168.88.128 port 42168 connected to 192.168.88.130 port 5201
[ ID] Interval           Transfer     Bitrate         Retr  Cwnd
[  5]   0.00-1.00   sec   658 MBytes  5.51 Gbits/sec   50   1.33 MBytes       
[  5]   1.00-2.00   sec   664 MBytes  5.58 Gbits/sec   24    918 KBytes       
[  5]   2.00-3.00   sec   669 MBytes  5.60 Gbits/sec   37   1.05 MBytes       
[  5]   3.00-4.00   sec   620 MBytes  5.20 Gbits/sec   41    926 KBytes       
[  5]   4.00-5.00   sec   645 MBytes  5.41 Gbits/sec   22    769 KBytes       
[  5]   5.00-6.00   sec   626 MBytes  5.25 Gbits/sec   16   1.07 MBytes       
[  5]   6.00-7.00   sec   655 MBytes  5.49 Gbits/sec    1   1.04 MBytes       
[  5]   7.00-8.00   sec   649 MBytes  5.44 Gbits/sec   40   1.04 MBytes       
[  5]   8.00-9.00   sec   658 MBytes  5.51 Gbits/sec   21    987 KBytes       
[  5]   9.00-10.00  sec   672 MBytes  5.64 Gbits/sec   17   1005 KBytes       
[  5]  10.00-11.00  sec   654 MBytes  5.49 Gbits/sec   29    690 KBytes       
[  5]  11.00-12.00  sec   649 MBytes  5.44 Gbits/sec   23    821 KBytes       
[  5]  12.00-13.00  sec   642 MBytes  5.39 Gbits/sec   49    874 KBytes       
[  5]  13.00-14.00  sec   626 MBytes  5.25 Gbits/sec   11    891 KBytes       
[  5]  14.00-15.00  sec   658 MBytes  5.52 Gbits/sec   33    909 KBytes       
[  5]  15.00-16.00  sec   646 MBytes  5.42 Gbits/sec   28   1.02 MBytes       
[  5]  16.00-17.00  sec   646 MBytes  5.43 Gbits/sec   18    900 KBytes       
[  5]  17.00-18.00  sec   636 MBytes  5.34 Gbits/sec    6    900 KBytes       
[  5]  18.00-19.00  sec   638 MBytes  5.35 Gbits/sec    5    778 KBytes       
[  5]  19.00-20.00  sec   635 MBytes  5.32 Gbits/sec   30   1.16 MBytes       
- - - - - - - - - - - - - - - - - - - - - - - - -
[ ID] Interval           Transfer     Bitrate         Retr
[  5]   0.00-20.00  sec  12.6 GBytes  5.43 Gbits/sec  501             sender
[  5]   0.00-20.04  sec  12.6 GBytes  5.42 Gbits/sec                  receiver
iperf Done.
=================== 第二轮：RDMA 全量同步 ===================
时间: 2026-09-02 07:44:05
--- 主机 RDMA 全量同步输出 ---
[RDMA] Sent 1073745705 bytes in 7.277 seconds, throughput: 140.72 MB/s
--- 从机 AOF 文件大小 ---
-rw-r--r-- 1 root root 1.1G Sep  2 07:45 /home/c2/project/KVstore8/9.1-kvstore/kvstore.aof
=================== 第三轮：TCP 全量同步 ===================
时间: 2026-09-02 07:45:16
--- 主机 TCP 全量同步输出 ---
[TCP] Sent 1073745705 bytes in 2.352 seconds, throughput: 435.41 MB/s
--- 从机 AOF 文件大小 ---
-rw-r--r-- 1 root root 1.1G Sep  2 07:46 /home/c2/project/KVstore8/9.1-kvstore/kvstore.aof

# 增量同步性能测试
 转发方式	  QPS	    	
基准    	 2025	 
eBPF 转发        1967 	 	
TCP 网络转发	 1924

# chat_service接口测试
python3 test_data.py --mode ### --count 1000
============================================================
向量数据库(ai_text_to_vector连接开源模型)
------------------------------------------------------------
压测结果 (KEEP)：
总请求数:     1000
总耗时:       76.69 秒
QPS:          13.04
平均延迟:     76.68 ms
P50 延迟:     72.06 ms
P95 延迟:     130.75 ms
P99 延迟:     164.37 ms
最大延迟:     255.71 ms

压测结果 (MATCH)：
总请求数:     1000
总耗时:       70.96 秒
QPS:          14.09
平均延迟:     70.95 ms
P50 延迟:     67.00 ms
P95 延迟:     120.70 ms
P99 延迟:     149.65 ms
最大延迟:     252.32 ms
------------------------------------------------------------
向量数据库(ai_text_to_vector空实现)
------------------------------------------------------------
压测结果 (KEEP):
总请求数:     1000
总耗时:       0.07 秒
QPS:          13345.84
平均延迟:     0.07 ms
P50 延迟:     0.06 ms
P95 延迟:     0.11 ms
P99 延迟:     0.20 ms
最大延迟:     2.24 ms

压测结果 (MATCH)
总请求数:     1000
总耗时:       0.15 秒
QPS:          6554.70
平均延迟:     0.15 ms
P50 延迟:     0.14 ms
P95 延迟:     0.23 ms
P99 延迟:     0.33 ms
最大延迟:     0.89 ms
============================================================


============================================================
上下文和全量数据（10w次）   QPS
------------------------------------------------------------
上下文：
setctx                  16540.25 
getctx                  16977.57
全量数据：
setrec                  16702.89
getrec                  16815.01
向量索引：
setidx                  371.42
getidx                  257.16
时间索引：
zadd                    16292.27
zrange                  390.02
============================================================






