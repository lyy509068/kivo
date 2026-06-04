#ifndef _REPLICATION_H_
#define _REPLICATION_H_

#include <stdint.h>
#include <stddef.h>

#define REPL_INIT_BUFFER_SIZE 4096

#define ENABLE_REPLICATION_MASTER 1
#define ENABLE_REPLICATION_SLAVE  0

// 建立连接
int repl_connect_to_master(const char *master_ip, unsigned short master_port);
// 发送命令
int repl_push_cmd(const char *cmd_name, const char *key, int key_len, const char *value, int value_len);
// 刷新缓冲区
int repl_flush();
// 关闭线程
void repl_close();

#endif