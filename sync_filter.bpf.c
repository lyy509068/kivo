#include <linux/bpf.h>
#include <linux/pkt_cls.h>
#include <linux/if_ether.h>
#include <linux/ip.h>
#include <linux/tcp.h>
#include <bpf/bpf_helpers.h>
#include <bpf/bpf_endian.h>
#include <stddef.h>

#define MAX_PAYLOAD_SIZE 16384

struct event_t {
    __u32 src_ip;
    __u16 src_port;
    __u32 seq;
    __u32 payload_len;
    char payload[MAX_PAYLOAD_SIZE];   // 固定大小，便于 ringbuf reserve
};

struct {
    __uint(type, BPF_MAP_TYPE_RINGBUF);
    __uint(max_entries, 64 * 1024 * 1024);
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
    if (src_ip == bpf_htonl(0xc0a82581)) return TC_ACT_OK; // 过滤从机
    if (tcph->rst || tcph->syn || tcph->fin) return TC_ACT_OK;

    int total = (int)bpf_ntohs(iph->tot_len);
    int ihl = (int)(iph->ihl * 4);
    int thl = (int)(tcph->doff * 4);
    int len = total - ihl - thl;
    if (len < 1) return TC_ACT_OK;
    if (len > MAX_PAYLOAD_SIZE) len = MAX_PAYLOAD_SIZE;

    __u32 offset = ETH_HLEN + ihl + thl;

    // 直接在 ringbuf 中预留空间
    struct event_t *e = bpf_ringbuf_reserve(&payload_ringbuf, sizeof(*e), 0);
    if (!e) return TC_ACT_OK;

    e->src_ip = iph->saddr;
    e->src_port = tcph->source;
    e->seq = bpf_ntohl(tcph->seq);
    e->payload_len = len;

    if (bpf_skb_load_bytes(skb, offset, e->payload, len) != 0) {
        bpf_ringbuf_discard(e, 0);
        return TC_ACT_OK;
    }

    bpf_ringbuf_submit(e, 0);
    return TC_ACT_OK;
}

char _license[] SEC("license") = "GPL";