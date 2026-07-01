#include <linux/bpf.h>
#include <linux/pkt_cls.h>
#include <linux/if_ether.h>
#include <linux/ip.h>
#include <linux/tcp.h>
#include <bpf/bpf_helpers.h>
#include <bpf/bpf_endian.h>

// 存储从端 MAC 地址
struct {
    __uint(type, BPF_MAP_TYPE_ARRAY);
    __uint(max_entries, 1);
    __type(key, __u32);
    __type(value, char[6]); 
} slave_mac_map SEC(".maps");

// 存储主端自己的 MAC 地址（用于包恢复）
struct {
    __uint(type, BPF_MAP_TYPE_ARRAY);
    __uint(max_entries, 1);
    __type(key, __u32);
    __type(value, char[6]); 
} master_mac_map SEC(".maps");

struct {
    __uint(type, BPF_MAP_TYPE_ARRAY);
    __uint(max_entries, 1);
    __type(key, __u32);
    __type(value, __u32);
} slave_ifindex_map SEC(".maps");

struct {
    __uint(type, BPF_MAP_TYPE_ARRAY);
    __uint(max_entries, 1);
    __type(key, __u32);
    __type(value, __u32);
} slave_ip_map SEC(".maps");

// 添加自动钉住（Pinning）声明，使其在 BPF 文件系统中全局唯一固定
struct {
    __uint(type, BPF_MAP_TYPE_ARRAY);
    __uint(max_entries, 1);
    __type(key, __u32);
    __type(value, __u32);
    __uint(pinning, 1); 
} clone_switch_map SEC(".maps");

SEC("classifier")
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
    
    // 只针对目标端口 2000 的包进行打印和处理
    if (tcph->dest != bpf_htons(2000)) return TC_ACT_OK;

    // 抓包成功
    bpf_printk("[eBPF] === Caught TCP Dest Port 2000 Ingress ===\n");

    __u32 key = 0;
    __u32 *enabled = bpf_map_lookup_elem(&clone_switch_map, &key);
    
    if (!enabled) {
        bpf_printk("[eBPF] Error: clone_switch_map lookup returned NULL\n");
        return TC_ACT_OK;
    }
    
    // 确认开关状态
    bpf_printk("[eBPF] clone_switch_map value: %d\n", *enabled);
    if (*enabled == 0) return TC_ACT_OK;

    __u32 *slave_ip = bpf_map_lookup_elem(&slave_ip_map, &key);
    if (!slave_ip) {
        bpf_printk("[eBPF] Error: slave_ip_map lookup returned NULL\n");
        return TC_ACT_OK;
    }
    
    // 确认 IP
    bpf_printk("[eBPF] slave_ip_map value: %u\n", *slave_ip);
    if (*slave_ip == 0) return TC_ACT_OK;

    __u32 *ifindex = bpf_map_lookup_elem(&slave_ifindex_map, &key);
    char *slave_mac = bpf_map_lookup_elem(&slave_mac_map, &key);
    char *master_mac = bpf_map_lookup_elem(&master_mac_map, &key);
    
    if (!ifindex || !slave_mac || !master_mac) {
        bpf_printk("[eBPF] Error: Metadata map lookup failed! (ifindex/smac/mmac)\n");
        return TC_ACT_OK;
    }
    if (*ifindex == 0) {
        bpf_printk("[eBPF] Error: slave_ifindex is 0!\n");
        return TC_ACT_OK;
    }

    // 准备弹射
    bpf_printk("[eBPF] All Maps OK. Forging packet... ifindex: %u\n", *ifindex);

    __u32 old_daddr = iph->daddr;
    __u32 new_daddr = *slave_ip;

    // 伪造二三层信息并克隆弹射
    bpf_skb_store_bytes(skb, offsetof(struct ethhdr, h_dest), slave_mac, 6, 0);
    bpf_skb_store_bytes(skb, ETH_HLEN + offsetof(struct iphdr, daddr), &new_daddr, 4, 0);

    // 修正校验和 (L3 IP Checksum)
    bpf_l3_csum_replace(skb, ETH_HLEN + offsetof(struct iphdr, check), old_daddr, new_daddr, sizeof(__u32));
    
    // 修正校验和 (L4 TCP Pseudo-header Checksum)
    bpf_l4_csum_replace(skb, ETH_HLEN + sizeof(struct iphdr) + offsetof(struct tcphdr, check), 
                        old_daddr, new_daddr, BPF_F_PSEUDO_HDR | sizeof(__u32));

    // 将携带了从端 MAC/IP 的克隆包，强行推入网卡发送队列
    int clone_ret = bpf_clone_redirect(skb, *ifindex, 0);
    
    // --- [Debug 5: 查看弹射返回值] ---
    bpf_printk("[eBPF] bpf_clone_redirect returned: %d\n", clone_ret);

    // 还原当前原始数据包，交还主端协议栈
    bpf_skb_store_bytes(skb, offsetof(struct ethhdr, h_dest), master_mac, 6, 0);
    bpf_skb_store_bytes(skb, ETH_HLEN + offsetof(struct iphdr, daddr), &old_daddr, 4, 0);
    
    bpf_l3_csum_replace(skb, ETH_HLEN + offsetof(struct iphdr, check), new_daddr, old_daddr, sizeof(__u32));
    bpf_l4_csum_replace(skb, ETH_HLEN + sizeof(struct iphdr) + offsetof(struct tcphdr, check), 
                        new_daddr, old_daddr, BPF_F_PSEUDO_HDR | sizeof(__u32));

    
    bpf_printk("[eBPF] Packet restored and passed to local stack (TC_ACT_OK)\n");

    return TC_ACT_OK; 
}

char _license[] SEC("license") = "GPL";