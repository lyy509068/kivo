#include <linux/bpf.h>
#include <bpf/bpf_helpers.h>

// 1. 定义一个共享的 Map，用来当做控制开关
struct {
    __uint(type, BPF_MAP_TYPE_HASH);
    __uint(max_entries, 1024);
    __type(key, int);   // 键：从端的 slave_fd
    __type(value, int); // 值：0 = 关闭转发(拦截放行)，1 = 开启转发
} sock_switch_map SEC(".maps");

// 2. 定义一个 Sockmap，用来存放从端的套接字路由
struct {
    __uint(type, BPF_MAP_TYPE_SOCKMAP);
    __uint(max_entries, 1024);
    __type(key, int);
    __type(value, int);
} sock_routing_map SEC(".maps");

SEC("sk_msg")
int handle_master_traffic(struct sk_msg_md *msg) {
    int slave_key = 0; // 假设从端固定在路由表的 0 号槽位
    
    // 查询同步层下发的开关状态
    int *switch_status = bpf_map_lookup_elem(&sock_switch_map, &slave_key);
    
    // 如果同步层没有显式开启开关，或者关掉了开关(0)，则不转发，直接放行让用户态读取
    if (!switch_status || *switch_status == 0) {
        return SK_PASS; 
    }

    // 如果开关为 1，说明主端业务层已经确认成功，触发内核高速零拷贝转发
    bpf_msg_redirect_map(msg, &sock_routing_map, 0, BPF_F_INGRESS);
    
    return SK_PASS;
}

char _license[] SEC("license") = "GPL";