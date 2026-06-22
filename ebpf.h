#ifndef EBPF_CONTROL_H
#define EBPF_CONTROL_H

#include <stdint.h>

// 初始化 eBPF 环境，加载编译好的内核字节码
int ebpf_init_loader(const char *bpf_object_path);

// 将从端的 fd 注册到内核路由表中
int ebpf_register_slave(int slave_fd);

// 同步层调用的控制开关函数
int ebpf_set_forward_switch(int enable); 

int ebpf_add_whitelist_packet(uint32_t tcp_seq);

// 卸载与清理
void ebpf_cleanup(void);

#endif