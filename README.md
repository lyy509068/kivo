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
客户端：连接服务器->插入10w条数据->SAVE保存快照->对比快照和预期文件是否相同->SHUTDOWN关闭服务器->重新打开服务器->重新连接服务器->获取10w条数据并校验->清除快照文件（不影响下次测试）->SHUTDOWN关闭服务器

3.增量持久化测试
一共四种模式 ./test_incrementpersistence 1 2 3 4
客户端：连接服务器->插入10w条数据->SHUTDOWN关闭服务器->重新打开服务器->重新连接服务器->获取10w条数据并校验->清除日志文件（不影响下次测试）->SHUTDOWN关闭服务器

4.超时功能测试

5.内存池测试
array不能用大量数据测试，rbtree最快，hash比rbtree慢一点，skiptable只有前两个的一半
malloc>jemalloc>mempool(为什么内存池反而比不用内存池慢？)

6.主从同步测试

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



