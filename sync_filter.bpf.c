#include <linux/bpf.h>
#include <linux/pkt_cls.h>
#include <linux/if_ether.h>
#include <linux/ip.h>
#include <linux/tcp.h>
#include <bpf/bpf_helpers.h>
#include <bpf/bpf_endian.h>

// 从端网卡 ifindex
struct {
    __uint(type, BPF_MAP_TYPE_ARRAY);
    __uint(max_entries, 1);
    __type(key, __u32);
    __type(value, __u32);
} slave_ifindex_map SEC(".maps");

// 全局转发开关
struct {
    __uint(type, BPF_MAP_TYPE_ARRAY);
    __uint(max_entries, 1);
    __type(key, __u32);
    __type(value, __u32);  // 0=关闭, 1=开启
} clone_switch_map SEC(".maps");

SEC("tc")
int handle_tc_dual_write(struct __sk_buff *skb) {
    void *data_end = (void *)(long)skb->data_end;
    void *data = (void *)(long)skb->data;

    struct ethhdr *eth = data;
    if ((void *)(eth + 1) > data_end) return TC_ACT_OK;
    if (eth->h_proto != bpf_htons(ETH_P_IP)) return TC_ACT_OK;

    struct iphdr *iph = (void *)(eth + 1);
    if ((void *)(iph + 1) > data_end) return TC_ACT_OK;
    if (iph->protocol != 6) return TC_ACT_OK;

    struct tcphdr *tcph = (void *)(iph + 1);
    if ((void *)(tcph + 1) > data_end) return TC_ACT_OK;
    if (tcph->dest != bpf_htons(2000)) return TC_ACT_OK;

    // 检查开关
    __u32 key = 0;
    __u32 *enabled = bpf_map_lookup_elem(&clone_switch_map, &key);
    if (!enabled || *enabled == 0) {
        return TC_ACT_OK;  
    }

    __u32 *ifindex = bpf_map_lookup_elem(&slave_ifindex_map, &key);
    if (ifindex && *ifindex > 0) {
        bpf_clone_redirect(skb, *ifindex, 0);
    }

    return TC_ACT_OK;
}

char _license[] SEC("license") = "GPL";