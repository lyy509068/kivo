#include <linux/bpf.h>
#include <linux/pkt_cls.h>
#include <linux/if_ether.h>
#include <linux/ip.h>
#include <linux/tcp.h>
#include <bpf/bpf_helpers.h>
#include <bpf/bpf_endian.h>
#include <stddef.h>   // for offsetof

#define MAX_PAYLOAD_SIZE 16384

// 事件结构：携带 TCP 元数据和 payload
struct event_t {
    __u32 src_ip;
    __u16 src_port;
    __u32 seq;                // TCP 序列号（主机字节序）
    __u32 payload_len;
    char payload[MAX_PAYLOAD_SIZE];
};

// RingBuffer：用户态读取
struct {
    __uint(type, BPF_MAP_TYPE_RINGBUF);
    __uint(max_entries, 64 * 1024 * 1024); // 64MB
} payload_ringbuf SEC(".maps");

// PerCPU 暂存区，避免栈溢出
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

    // 只捕获目的端口为 2000 的 TCP 包（客户端 -> 主机）
    if (tcph->dest != bpf_htons(2000)) return TC_ACT_OK;

    // 可选：过滤掉特定源 IP（如从机 IP），防止自环
    // 根据实际环境修改，这里假设从机 IP 为 192.168.37.129
    // 如果不需要过滤，可删除下面的判断
    __u32 src_ip = iph->saddr;
    if (src_ip == bpf_htonl(0xc0a82581)) return TC_ACT_OK; // 192.168.37.129

    // 忽略握手、挥手和 RST 包
    if (tcph->rst || tcph->syn || tcph->fin) return TC_ACT_OK;

    int total = (int)bpf_ntohs(iph->tot_len);
    int ihl   = (int)(iph->ihl * 4);
    int thl   = (int)(tcph->doff * 4);
    int len = total - ihl - thl;
    if (len < 1) return TC_ACT_OK;
    if (len > MAX_PAYLOAD_SIZE) len = MAX_PAYLOAD_SIZE;

    __u32 offset = ETH_HLEN + ihl + thl;

    __u32 key = 0;
    struct event_t *e = bpf_map_lookup_elem(&scratch_map, &key);
    if (!e) return TC_ACT_OK;

    // 填充元数据
    e->src_ip = iph->saddr;
    e->src_port = tcph->source;
    e->seq = bpf_ntohl(tcph->seq);   // 转为主机字节序，便于用户态比较
    e->payload_len = len;

    // 拷贝 payload
    if (bpf_skb_load_bytes(skb, offset, e->payload, len) != 0) {
        return TC_ACT_OK;
    }

    // 计算实际要发送的字节数（只发送实际 payload，避免拷贝整个 MAX_PAYLOAD_SIZE）
    __u64 send_size = offsetof(struct event_t, payload) + len;
    if (send_size > sizeof(struct event_t)) {
        return TC_ACT_OK;
    }

    // 提交到 RingBuffer
    bpf_ringbuf_output(&payload_ringbuf, e, send_size, 0);

    return TC_ACT_OK;
}

char _license[] SEC("license") = "GPL";