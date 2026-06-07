# 9.1 Kvstore

启动服务器./server 2000 

1.特殊字符和批量命令测试 
窗口模式：redis-cli -p 2000 一次只能发送一条命令
文件模式：cat test_cmd.txt | redis-cli -p 2000
redis-cli -p 2000 -x SET io_multiplexing_article < 本地文件.txt
redis-cli -p 2000 --raw GET io_multiplexing_article | head -n 20

管道模式：

压力测试：-p 端口，-c 50个并发连接，-n 总共发送10000条命令，-t 只测试 set和get命令
redis-benchmark -p 2000 -c 50 -n 10000 -t set,get

2.全量持久化测试 test_fullpersistence
一共四种模式 ./test_fullpersistence 1 2 3 4
客户端：连接服务器->插入10w条数据->SAVE保存快照->SHUTDOWN关闭服务器->重新打开服务器->重新连接服务器->获取10w条数据并校验->清除快照文件（不影响下次测试）->SHUTDOWN关闭服务器

3.增量持久化测试
一共四种模式 ./test_incrementpersistence 1 2 3 4
客户端：连接服务器->插入10w条数据->SHUTDOWN关闭服务器->重新打开服务器->重新连接服务器->获取10w条数据并校验->清除日志文件（不影响下次测试）->SHUTDOWN关闭服务器

4.超时功能测试

5.内存池测试
./test_mempool (0 1 2) (1 2 3 4) 100000
array不能用大量数据测试，rbtree最快，hash比rbtree慢一点，skiptable只有前两个的一半
malloc>jemalloc>mempool(为什么内存池反而比不用内存池慢？)

6.主从同步测试
打开主端服务器./server 2000 插入5w条数据 ./test_master (1 2 3 4) 1
打开从端服务器,.server 2000 reactor_start启动后，从端服务器主动连接主端服务器，向主端发送获取日志命令
主端服务器收到获取日志命令后，把日志文件发过去
从端收到日志文件后，在从端恢复日志
继续向主端插入5w条数据 ./test_master (1 2 3 4) 2
从端同步完第二轮的5w条数据后，通过客户端验证 ./test_slave (1 2 3 4)

rbtree和skiplist都能测试成功 10w条数据对array来说太多了不能测试成功 hash不能测试成功，从端的日志只有98810这么多数据，可能是什么原因？？？
不太稳定，会丢包

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



