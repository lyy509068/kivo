#include <linux/bpf.h>
#include <linux/pkt_cls.h>
#include <linux/if_ether.h>
#include <linux/ip.h>
#include <linux/tcp.h>
#include <bpf/bpf_helpers.h>
#include <bpf/bpf_endian.h>

#define MAX_PAYLOAD_SIZE 16384

// 定义一个固定大小的结构体，用于在暂存区(Scratch Buffer)中分配空间
struct event_t {
    __u32 payload_len;
    char payload[MAX_PAYLOAD_SIZE];
};

// 1. 用于向用户态发送变长数据的 RingBuffer
struct {
    __uint(type, BPF_MAP_TYPE_RINGBUF);
    __uint(max_entries, 64 * 1024 * 1024); // 64MB
} payload_ringbuf SEC(".maps");

// 2. Per-CPU Array 暂存区 (Scratch Map)
// 由于 eBPF 栈大小限制(512B)，8KB 的临时缓冲区必须放在 Map 中
struct {
    __uint(type, BPF_MAP_TYPE_PERCPU_ARRAY);
    __type(key, __u32);
    __type(value, struct event_t);
    __uint(max_entries, 1);
} scratch_map SEC(".maps");

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

    if (tcph->dest != bpf_htons(2000)) return TC_ACT_OK;

    __u32 src_ip = iph->saddr;
    if (src_ip == bpf_htonl(0xc0a85c82)) return TC_ACT_OK;

    if (tcph->rst || tcph->syn || tcph->fin) return TC_ACT_OK;

    int total = (int)bpf_ntohs(iph->tot_len);
    int ihl   = (int)(iph->ihl * 4);
    int thl   = (int)(tcph->doff * 4);
    
    int len = total - ihl - thl;
    if (len < 1) return TC_ACT_OK;
    if (len > MAX_PAYLOAD_SIZE) len = MAX_PAYLOAD_SIZE;

    __u32 offset = ETH_HLEN + ihl + thl;

    // 1：获取当前 CPU 的独立暂存区 (避免并发冲突)
    __u32 key = 0;
    struct event_t *e = bpf_map_lookup_elem(&scratch_map, &key);
    if (!e) return TC_ACT_OK;

    // 2：把数据先写入暂存区
    e->payload_len = len;
    if (bpf_skb_load_bytes(skb, offset, e->payload, len) != 0) {
        return TC_ACT_OK;
    }

    // 3：计算实际要发送的动态长度
    // 强制转为 unsigned long，并增加显式边界检查，帮助 Verifier 确认不会越界
    __u64 send_size = sizeof(__u32) + len;
    if (send_size > sizeof(struct event_t)) {
        return TC_ACT_OK; // 兜底防御，防止 Verifier 报错
    }

    // 4：使用 bpf_ringbuf_output 提交变长数据
    // 它会从 e 的地址拷贝 send_size 个字节到 RingBuffer 中
    bpf_ringbuf_output(&payload_ringbuf, e, send_size, 0);

    return TC_ACT_OK;
}

char _license[] SEC("license") = "GPL";