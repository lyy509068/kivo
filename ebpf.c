#include "ebpf.h"
#include <stdio.h>
#include <bpf/libbpf.h>
#include <bpf/bpf.h>
#include <unistd.h>
#include <stdint.h> 

static struct bpf_object *obj = NULL;
static int switch_map_fd = -1;
static int routing_map_fd = -1;
static int whitelist_map_fd = -1; 

int ebpf_init_loader(const char *bpf_object_path) {
    obj = bpf_object__open_file(bpf_object_path, NULL);
    if (!obj) return -1;

    if (bpf_object__load(obj)) return -1;

    switch_map_fd = bpf_object__find_map_fd_by_name(obj, "sock_switch_map");
    routing_map_fd = bpf_object__find_map_fd_by_name(obj, "sock_routing_map");
    
    // 获取内核中定义的白名单 Map 句柄
    whitelist_map_fd = bpf_object__find_map_fd_by_name(obj, "write_whitelist_map");
    if (whitelist_map_fd < 0) {
        printf("[eBPF Error] Failed to find write_whitelist_map\n");
    }

    return 0;
}

int ebpf_register_slave(int slave_fd) {
    int key = 0;
    return bpf_map_update_elem(routing_map_fd, &key, &slave_fd, BPF_ANY);
}

int ebpf_set_forward_switch(int enable) {
    int key = 0;
    int value = enable; 
    return bpf_map_update_elem(switch_map_fd, &key, &value, BPF_ANY);
}

/**
 * 💡 向内核追加成功的写命令 TCP 序列号通行证
 * @param tcp_seq: 当前成功执行的请求在内核中所对应的 TCP Sequence Number
 */
int ebpf_add_whitelist_packet(uint32_t tcp_seq) {
    if (whitelist_map_fd < 0) return -1;

    uint8_t allow_forward = 1; // 1 代表允许转发
    
    // 将序列号作为 Key 写入内核，内核 eBPF 见票放行
    return bpf_map_update_elem(whitelist_map_fd, &tcp_seq, &allow_forward, BPF_ANY);
}

void ebpf_cleanup(void) {
    if (obj) bpf_object__close(obj);
}