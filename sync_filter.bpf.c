#include <linux/bpf.h>
#include <linux/pkt_cls.h>
#include <linux/if_ether.h>
#include <linux/ip.h>
#include <linux/tcp.h>
#include <bpf/bpf_helpers.h>
#include <bpf/bpf_endian.h>

#define MAX_PAYLOAD_SIZE 1024

struct event_t {
    char payload[MAX_PAYLOAD_SIZE];
    __u32 payload_len;
};

struct {
    __uint(type, BPF_MAP_TYPE_RINGBUF);
    __uint(max_entries, 256 * 1024);
} payload_ringbuf SEC(".maps");

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
    if (src_ip == bpf_htonl(0xc0a85c82)) {
        return TC_ACT_OK;
    }

    __u16 ip_hdr_len = iph->ihl * 4;
    __u16 tcp_hdr_len = tcph->doff * 4;
    __u16 ip_total_len = bpf_ntohs(iph->tot_len);

    if (ip_total_len <= ip_hdr_len + tcp_hdr_len) return TC_ACT_OK;
    if (tcph->rst || tcph->syn || tcph->fin) return TC_ACT_OK;
    
    // 仅抓取有 PSH 标志的包（TCP 头偏移 13 字节处是 flags）
    __u8 tcp_flags = ((__u8 *)tcph)[13];
    if (!(tcp_flags & 0x08)) return TC_ACT_OK;  // 0x08 是 PSH

    __u32 offset = ETH_HLEN + ip_hdr_len + tcp_hdr_len;
    __u16 payload_len = ip_total_len - ip_hdr_len - tcp_hdr_len;

    if (payload_len < 5) return TC_ACT_OK; // 至少 5 字节才可能是 Redis 命令

    // 检查第一个字节
    __u8 first_char = 0;
    if (bpf_skb_load_bytes(skb, offset, &first_char, 1) != 0) return TC_ACT_OK;
    if (first_char != '*') return TC_ACT_OK;

    __u32 copy_len = payload_len;

    // 1. 强行截断超过 1024 的部分。Verifier 此时确信: copy_len <= 1024
    if (copy_len > MAX_PAYLOAD_SIZE) {
        copy_len = MAX_PAYLOAD_SIZE;
    }

    // 2. 显式剔除等于 0 的情况。
    // 由于内部只有一句简单的 return，Clang 会生成标准的 BPF_JEQ (==0) 汇编。
    // 使得后续的主拷贝流程留在“直行 Fallthrough 分支”，Verifier 此时 100% 确信: 1 <= copy_len <= 1024
    if (copy_len == 0) {
        return TC_ACT_OK;
    }

    // 此时验证器已经对 copy_len 的动态区间彻底放行
    struct event_t *e = bpf_ringbuf_reserve(&payload_ringbuf, sizeof(*e), 0);
    if (!e) return TC_ACT_OK;

    // 使用已经过完全校验的动态长度 copy_len，绝不会触发 size=0 的越界报错
    if (bpf_skb_load_bytes(skb, offset, e->payload, copy_len) != 0) {
        bpf_ringbuf_discard(e, 0);
        return TC_ACT_OK;
    }

    // 记录真实长度并提交
    e->payload_len = copy_len;
    bpf_ringbuf_submit(e, 0);

    return TC_ACT_OK;
}
char _license[] SEC("license") = "GPL";