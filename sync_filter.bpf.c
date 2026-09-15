#include <linux/bpf.h>
#include <linux/pkt_cls.h>
#include <linux/if_ether.h>
#include <linux/ip.h>
#include <linux/tcp.h>
#include <bpf/bpf_helpers.h>
#include <bpf/bpf_endian.h>
#include <stddef.h>

#define MAX_PAYLOAD_SIZE 1024

struct event_t {
    __u32 src_ip;
    __u16 src_port;
    __u32 seq;
    __u32 payload_len;
    char payload[MAX_PAYLOAD_SIZE];
};

/* 指定 NUMA node 0，如网卡在别的 node 改成对应编号 */
struct {
    __uint(type, BPF_MAP_TYPE_RINGBUF);
    __uint(max_entries, 64 * 1024 * 1024);
    __uint(map_flags, BPF_F_NUMA_NODE);
    __uint(numa_node, 0);
} payload_ringbuf SEC(".maps");

/* 丢弃计数：ringbuf 满时累加，用于诊断 */
struct {
    __uint(type, BPF_MAP_TYPE_PERCPU_ARRAY);
    __uint(max_entries, 1);
    __type(key, __u32);
    __type(value, __u64);
} drop_counter SEC(".maps");

SEC("classifier")
int handle_tc_dual_write(struct __sk_buff *skb) {
    void *data_end = (void *)(long)skb->data_end;
    void *data     = (void *)(long)skb->data;

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
    if (src_ip == bpf_htonl(0xc0a82581)) return TC_ACT_OK;   /* 过滤从机 */
    if (tcph->rst || tcph->syn || tcph->fin) return TC_ACT_OK;

    int total = (int)bpf_ntohs(iph->tot_len);
    int ihl   = (int)(iph->ihl * 4);
    int thl   = (int)(tcph->doff * 4);
    int len   = total - ihl - thl;
    if (len < 1) return TC_ACT_OK;
    if (len > MAX_PAYLOAD_SIZE) len = MAX_PAYLOAD_SIZE;

    __u32 offset = ETH_HLEN + ihl + thl;

    struct event_t *e = bpf_ringbuf_reserve(&payload_ringbuf, sizeof(*e), 0);
    if (!e) {
        /* ringbuf 溢出，累加丢弃计数 */
        __u32 key = 0;
        __u64 *cnt = bpf_map_lookup_elem(&drop_counter, &key);
        if (cnt) (*cnt)++;
        return TC_ACT_OK;
    }

    e->src_ip      = iph->saddr;
    e->src_port    = tcph->source;
    e->seq         = bpf_ntohl(tcph->seq);
    e->payload_len = len;

    if (bpf_skb_load_bytes(skb, offset, e->payload, len) != 0) {
        bpf_ringbuf_discard(e, 0);
        return TC_ACT_OK;
    }

    /* 【优化点 1】NO_WAKEUP：不主动唤醒用户态，由用户态 1ms poll 拉取 */
    bpf_ringbuf_submit(e, 0);
    return TC_ACT_OK;
}

char _license[] SEC("license") = "GPL";