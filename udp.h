#ifndef REPLICATION_UDP_SERVER_H
#define REPLICATION_UDP_SERVER_H

// 启动从端 UDP 增量监听协程
// listen_port: 接收 eBPF 弹射的目的端口（如 3000）
int start_replica_udp_server_coroutine(int listen_port);

#endif // REPLICATION_UDP_SERVER_H