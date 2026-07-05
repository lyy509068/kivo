#include <linux/bpf.h>
#include <linux/pkt_cls.h>
#include <linux/if_ether.h>
#include <linux/ip.h>
#include <linux/tcp.h>
#include <linux/udp.h>
#include <bpf/bpf_helpers.h>
#include <bpf/bpf_endian.h>
#include <stddef.h>

// 存储从端 MAC 地址
struct {
    __uint(type, BPF_MAP_TYPE_ARRAY);
    __uint(max_entries, 1);
    __type(key, __u32);
    __type(value, char[6]); 
    __uint(pinning, 1); 
} slave_mac_map SEC(".maps");

// 存储主端自己的 MAC 地址
struct {
    __uint(type, BPF_MAP_TYPE_ARRAY);
    __uint(max_entries, 1);
    __type(key, __u32);
    __type(value, char[6]); 
    __uint(pinning, 1); 
} master_mac_map SEC(".maps");

struct {
    __uint(type, BPF_MAP_TYPE_ARRAY);
    __uint(max_entries, 1);
    __type(key, __u32);
    __type(value, __u32);
    __uint(pinning, 1); 
} slave_ifindex_map SEC(".maps");

struct {
    __uint(type, BPF_MAP_TYPE_ARRAY);
    __uint(max_entries, 1);
    __type(key, __u32);
    __type(value, __u32);
    __uint(pinning, 1); 
} slave_ip_map SEC(".maps");


struct {
    __uint(type, BPF_MAP_TYPE_ARRAY);
    __uint(max_entries, 1);
    __type(key, __u32);
    __type(value, __u32);
    __uint(pinning, 1); 
} clone_switch_map SEC(".maps");

// 用于临时备份 TCP 头部前 8 字节的结构体
struct tcp_hdr_backup {
    __be16 source;
    __be16 dest;
    __be32 seq;
};

SEC("classifier")
int handle_tc_dual_write(struct __sk_buff *skb) {
    void *data_end = (void *)(long)skb->data_end;
    void *data = (void *)(long)skb->data;

    // 探测点 0：证明 TC 挂载成功，且网卡收到了数据包
    bpf_printk("[eBPF Debug 0] Packet entered TC Ingress.\n");

    struct ethhdr *eth = data;
    if ((void *)(eth + 1) > data_end) return TC_ACT_OK;
    
    if (eth->h_proto != bpf_htons(ETH_P_IP)) {
        // 如果走的是 VLAN 报文或者 IPv6，会死在这里
        bpf_printk("[eBPF Debug 1] Dropped: Not IPv4. proto=0x%x\n", bpf_ntohs(eth->h_proto));
        return TC_ACT_OK;
    }

    struct iphdr *iph = (void *)(eth + 1);
    if ((void *)(iph + 1) > data_end) return TC_ACT_OK;
    
    if (iph->protocol != 6) {
        // 如果不是 TCP 报文（例如是 UDP 或 ICMP 乒乓包），死在这里
        bpf_printk("[eBPF Debug 2] Dropped: Not TCP. proto=%d\n", iph->protocol);
        return TC_ACT_OK;
    }

    struct tcphdr *tcph = (void *)(iph + 1);
    if ((void *)(tcph + 1) > data_end) return TC_ACT_OK;

    if (tcph->dest != bpf_htons(2000)) {
        return TC_ACT_OK; 
    }

    // 看看抓到的 TCP 目的端口到底是多少
    bpf_printk("[eBPF Debug 3] TCP packet caught. Dest Port=%d\n", bpf_ntohs(tcph->dest));

    __u16 ip_total_len = bpf_ntohs(iph->tot_len);
    __u16 ip_hdr_len = iph->ihl * 4;
    __u16 tcp_hdr_len = tcph->doff * 4;
    
    if (ip_total_len < (ip_hdr_len + tcp_hdr_len)) return TC_ACT_OK;
    __u16 payload_len = ip_total_len - ip_hdr_len - tcp_hdr_len;
    
    if (payload_len == 0 || tcph->rst) {
        bpf_printk("[eBPF Debug 4] Dropped: Pure ACK/SYN or RST packet.\n");
        return TC_ACT_OK;
    }

    __u32 key = 0;
    // 探测点 5：开始检查核心 Map 里的控制开关
    __u32 *enabled = bpf_map_lookup_elem(&clone_switch_map, &key);
    if (!enabled) {
        bpf_printk("[eBPF Debug 5] ERROR: clone_switch_map lookup failed (NULL)!\n");
        return TC_ACT_OK;
    }
    if (*enabled == 0) {
        bpf_printk("[eBPF Debug 6] Dropped: Replication switch is OFF (*enabled == 0).\n");
        return TC_ACT_OK;
    }

    __u32 *slave_ip = bpf_map_lookup_elem(&slave_ip_map, &key);
    __u32 *ifindex = bpf_map_lookup_elem(&slave_ifindex_map, &key);
    char *slave_mac = bpf_map_lookup_elem(&slave_mac_map, &key);
    char *master_mac = bpf_map_lookup_elem(&master_mac_map, &key);
    
    if (!slave_ip || !ifindex || !slave_mac || !master_mac) {
        bpf_printk("[eBPF Debug 7] ERROR: One of the metadata maps is NULL!\n");
        return TC_ACT_OK;
    }

    if (*ifindex == 0 || *slave_ip == 0) {
        bpf_printk("[eBPF Debug 8] ERROR: Metadata invalid. ifindex=%d, slave_ip=%d\n", *ifindex, *slave_ip);
        return TC_ACT_OK;
    }

    bpf_printk("[eBPF] ALL checks passed. Executing clone_redirect to ifindex: %d...\n", *ifindex);

    
    // 提取并备份主端原包核心字段
    __u32 old_daddr = iph->daddr;
    __u32 new_daddr = *slave_ip;
    __u8 old_proto = 6;
    __u8 new_proto = 17;

    // 备份即将被 UDP 头部覆盖的原 TCP 前 8 字节
    struct tcp_hdr_backup orig_tcp_head;
    if (bpf_skb_load_bytes(skb, ETH_HLEN + ip_hdr_len, &orig_tcp_head, sizeof(orig_tcp_head)) < 0) {
        return TC_ACT_OK;
    }

    // 同时也把原本 TCP 包的完整 L4 校验和读出来，待会儿直接强行写回恢复
    __be16 orig_tcp_check;
    if (bpf_skb_load_bytes(skb, ETH_HLEN + ip_hdr_len + offsetof(struct tcphdr, check), &orig_tcp_check, sizeof(orig_tcp_check)) < 0) {
        return TC_ACT_OK;
    }

    // 第一阶段：原地篡改包结构，迎合从端（变成 UDP）
    
    // 1. 修改二层目标 MAC 地址
    bpf_skb_store_bytes(skb, offsetof(struct ethhdr, h_dest), slave_mac, 6, 0);

    // 2. 修改三层目标 IP 和 协议字段 (TCP 变 UDP)
    bpf_skb_store_bytes(skb, ETH_HLEN + offsetof(struct iphdr, daddr), &new_daddr, 4, 0);
    bpf_skb_store_bytes(skb, ETH_HLEN + offsetof(struct iphdr, protocol), &new_proto, 1, 0);

    // 3. 构建伪造的 UDP 头部并覆盖在原 TCP 头的前 8 字节上
    struct udphdr udp_hdr;
    udp_hdr.source = bpf_htons(2000);
    udp_hdr.dest = bpf_htons(3000); 
    
    // 伪造的 UDP 数据包在内核看来，它的 Payload 包含了“未被覆盖的残余 TCP 报头选项”以及“真正的应用层数据”。
    // 因而其总长度等同于原包除去 IP 头部后的全部字节大小：ip_total_len - ip_hdr_len
    udp_hdr.len = bpf_htons(ip_total_len - ip_hdr_len);
    udp_hdr.check = 0; // 克隆包设为 0，绕过从端内核 UDP 校验

    bpf_skb_store_bytes(skb, ETH_HLEN + ip_hdr_len, &udp_hdr, sizeof(struct udphdr), 0);

    // 4. 动态计算更新伪造包的 L3 IP 校验和
    bpf_l3_csum_replace(skb, ETH_HLEN + offsetof(struct iphdr, check), old_daddr, new_daddr, sizeof(__u32));
    bpf_l3_csum_replace(skb, ETH_HLEN + offsetof(struct iphdr, check), bpf_htons(old_proto), bpf_htons(new_proto), sizeof(__u16));

    //  第二阶段：抓取当前瞬间快照并弹射
    int clone_ret = bpf_clone_redirect(skb, *ifindex, 0);
    bpf_printk("[eBPF] UDP injection via clone_redirect returned: %d\n", clone_ret);

    
    // 第三阶段：瞒天过海，100% 还原主端现场
    
    // 1. 还原二层 MAC 目标地址为主端自己
    bpf_skb_store_bytes(skb, offsetof(struct ethhdr, h_dest), master_mac, 6, 0);
    
    // 2. 还原三层 IP 和 协议号为原本的 TCP 状态
    bpf_skb_store_bytes(skb, ETH_HLEN + offsetof(struct iphdr, daddr), &old_daddr, 4, 0);
    bpf_skb_store_bytes(skb, ETH_HLEN + offsetof(struct iphdr, protocol), &old_proto, 1, 0);
    
    // 3. 彻底复原被 UDP 覆盖掉的原 TCP 头前 8 字节
    bpf_skb_store_bytes(skb, ETH_HLEN + ip_hdr_len, &orig_tcp_head, sizeof(orig_tcp_head), 0);

    // 4. 将原汁原味的原始 TCP Checksum 重新写回 TCP 头的校验和位置
    bpf_skb_store_bytes(skb, ETH_HLEN + ip_hdr_len + offsetof(struct tcphdr, check), &orig_tcp_check, sizeof(orig_tcp_check), 0);

    // 5. 反向更新 L3 IP 校验和，使其完全倒回客户端发送时的初始状态
    bpf_l3_csum_replace(skb, ETH_HLEN + offsetof(struct iphdr, check), new_daddr, old_daddr, sizeof(__u32));
    bpf_l3_csum_replace(skb, ETH_HLEN + offsetof(struct iphdr, check), bpf_htons(new_proto), bpf_htons(old_proto), sizeof(__u16));

    // 完美放行：因为所有报头字节（L2, L3, L4）和原 IP 校验和都被我们强行百分之百恢复了，
    // 包状态和刚进入网卡时没有任何区别，主端协议栈会畅通无阻地接收。
    return TC_ACT_OK; 
}

char _license[] SEC("license") = "GPL";