# 9.1 Kvstore
1.使用redis-cli测试
# 自定义文件测试 
cat test_cmd.txt | redis-cli -p 2000
redis-cli -p 2000 -x SET io_multiplexing_article < 本地文件.txt
redis-cli -p 2000 --raw GET io_multiplexing_article | head -n 20
# -p 端口，-c 50个并发连接，-n 总共发送10000条命令，-t 只测试 set和get命令
redis-benchmark -p 2000 -c 50 -n 10000 -t set,get

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



