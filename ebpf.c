#include "ebpf.h"
#include <stdio.h>
#include <bpf/libbpf.h>
#include <bpf/bpf.h>
#include <net/if.h>
#include <stdlib.h>    
#include <unistd.h>    

static struct bpf_object *obj = NULL;
static int tc_map_fd = -1;
static int switch_map_fd = -1; 

int ebpf_init_loader(const char *bpf_object_path) {
    char cmd[512];
    int ret;

    // 1. 自动加载 TC 程序到 ens33 网卡
    snprintf(cmd, sizeof(cmd),
        "tc qdisc add dev ens33 clsact 2>/dev/null; "
        "tc filter replace dev ens33 ingress bpf obj %s sec tc direct-action 2>&1",
        bpf_object_path);

    ret = system(cmd);
    if (ret != 0) {
        fprintf(stderr, "[eBPF TC] Failed to attach: %s\n", cmd);
        return -1;
    }
    printf("[eBPF TC] Program attached to ens33.\n");

    // 2. 短暂等待 map 创建
    usleep(50000);

    // 3. 通过 bpftool 获取 map id 并 pin 到 bpffs
    snprintf(cmd, sizeof(cmd),
        "bpftool map show | grep -E 'slave_ifindex_map|clone_switch_map' | "
        "awk '{print $1}' | tr -d ':' | "
        "while read id; do "
        "  name=$(bpftool map show id $id | grep name | awk '{print $2}'); "
        "  bpftool map pin id $id /sys/fs/bpf/$name 2>/dev/null; "
        "done");
    system(cmd);

    // 4. 获取 map fd
    tc_map_fd = bpf_obj_get("/sys/fs/bpf/slave_ifindex_map");
    switch_map_fd = bpf_obj_get("/sys/fs/bpf/clone_switch_map");

    if (tc_map_fd < 0 || switch_map_fd < 0) {
        fprintf(stderr, "[eBPF TC] Failed to get maps. tc_fd=%d, switch_fd=%d\n",
                tc_map_fd, switch_map_fd);
        return -1;
    }

    printf("[eBPF TC] Ready. tc_map_fd=%d, switch_fd=%d\n", tc_map_fd, switch_map_fd);
    return 0;
}

int ebpf_register_slave(int slave_fd) {
    __u32 key = 0;
    __u32 ifindex = (__u32)if_nametoindex("ens33");
    return bpf_map_update_elem(tc_map_fd, &key, &ifindex, BPF_ANY);
}

// 开关函数
int ebpf_set_forward_switch(int enable) {
    if (switch_map_fd < 0) return -1;
    __u32 key = 0;
    __u32 value = enable ? 1 : 0;
    return bpf_map_update_elem(switch_map_fd, &key, &value, BPF_ANY);
}

int ebpf_register_client(int client_fd) { (void)client_fd; return 0; }
void ebpf_cleanup(void) { if (obj) bpf_object__close(obj); }