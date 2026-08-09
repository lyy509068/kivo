#include <linux/bpf.h>
#include <linux/pkt_cls.h>
#include <linux/if_ether.h>
#include <linux/ip.h>
#include <linux/tcp.h>
#include <bpf/bpf_helpers.h>
#include <bpf/bpf_endian.h>

#define MAX_PAYLOAD_SIZE 24 * 1024

struct event_t {
    __u64 seq_num;
    __u32 cpu_id;
    __u32 payload_len;
    char payload[MAX_PAYLOAD_SIZE];
};

// 全局序列号计数器
struct {
    __uint(type, BPF_MAP_TYPE_ARRAY);
    __type(key, __u32);
    __type(value, __u64);
    __uint(max_entries, 1);
} seq_counter SEC(".maps");

// RingBuffer满导致的丢弃计数
struct {
    __uint(type, BPF_MAP_TYPE_ARRAY);
    __type(key, __u32);
    __type(value, __u64);
    __uint(max_entries, 1);
} ringbuf_drop_counter SEC(".maps");

// 总丢弃计数器
struct {
    __uint(type, BPF_MAP_TYPE_ARRAY);
    __type(key, __u32);
    __type(value, __u64);
    __uint(max_entries, 1);
} drop_counter SEC(".maps");

// 处理事件计数器
struct {
    __uint(type, BPF_MAP_TYPE_ARRAY);
    __type(key, __u32);
    __type(value, __u64);
    __uint(max_entries, 1);
} event_counter SEC(".maps");

struct {
    __uint(type, BPF_MAP_TYPE_RINGBUF);
    __uint(max_entries, 64 * 1024 * 1024);
} payload_ringbuf SEC(".maps");

struct {
    __uint(type, BPF_MAP_TYPE_PERCPU_ARRAY);
    __type(key, __u32);
    __type(value, struct event_t);
    __uint(max_entries, 1);
} scratch_map SEC(".maps");

struct {
    __uint(type, BPF_MAP_TYPE_ARRAY);
    __type(key, __u32);
    __type(value, __u32);
    __uint(max_entries, 1);
} filter_config SEC(".maps");

SEC("classifier")
int ebpf_relay_handler(struct __sk_buff *skb) {
    void *data_end = (void *)(long)skb->data_end;
    void *data = (void *)(long)skb->data;

    struct ethhdr *eth = data;
    if ((void *)(eth + 1) > data_end) 
        return TC_ACT_OK;
    
    if (eth->h_proto != bpf_htons(ETH_P_IP)) 
        return TC_ACT_OK;

    struct iphdr *iph = (void *)(eth + 1);
    if ((void *)(iph + 1) > data_end) 
        return TC_ACT_OK;
    
    if (iph->protocol != 6) 
        return TC_ACT_OK;

    struct tcphdr *tcph = (void *)(iph + 1);
    if ((void *)(tcph + 1) > data_end) 
        return TC_ACT_OK;

    if (tcph->dest != bpf_htons(2000)) 
        return TC_ACT_OK;

    __u32 src_ip = iph->saddr;
    __u32 filter_key = 0;
    __u32 *filter_ip = bpf_map_lookup_elem(&filter_config, &filter_key);
    if (filter_ip && *filter_ip != 0) {
        if (src_ip != *filter_ip) 
            return TC_ACT_OK;
    }

    if (tcph->rst || tcph->syn || tcph->fin) 
        return TC_ACT_OK;

    int total = (int)bpf_ntohs(iph->tot_len);
    int ihl   = (int)(iph->ihl * 4);
    int thl   = (int)(tcph->doff * 4);
    
    int len = total - ihl - thl;
    if (len < 1) 
        return TC_ACT_OK;
    if (len > MAX_PAYLOAD_SIZE) 
       len = MAX_PAYLOAD_SIZE;

    __u32 offset = ETH_HLEN + ihl + thl;

    __u32 seq_key = 0;
    __u64 *seq_ptr = bpf_map_lookup_elem(&seq_counter, &seq_key);
    if (!seq_ptr) 
        return TC_ACT_OK;
    
    __u64 current_seq = __sync_fetch_and_add(seq_ptr, 1);

    __u64 *event_count = bpf_map_lookup_elem(&event_counter, &seq_key);
    if (event_count) {
        __sync_fetch_and_add(event_count, 1);
    }

    __u32 scratch_key = 0;
    struct event_t *e = bpf_map_lookup_elem(&scratch_map, &scratch_key);
    if (!e) 
        return TC_ACT_OK;

    e->seq_num = current_seq;
    e->cpu_id = bpf_get_smp_processor_id();
    e->payload_len = len;
    
    if (bpf_skb_load_bytes(skb, offset, e->payload, len) != 0) {
        return TC_ACT_OK;
    }

    __u64 send_size = sizeof(__u64) + sizeof(__u32) + sizeof(__u32) + len;
    if (send_size > sizeof(struct event_t)) {
        return TC_ACT_OK;
    }

    // 更新丢弃计数器
    long ret = bpf_ringbuf_output(&payload_ringbuf, e, send_size, 0);
    
    if (ret < 0) {
        // RingBuffer满导致的丢弃
        __u64 *ringbuf_drop = bpf_map_lookup_elem(&ringbuf_drop_counter, &seq_key);
        if (ringbuf_drop) {
            __sync_fetch_and_add(ringbuf_drop, 1);
        }
        
        // 总丢弃计数
        __u64 *drop = bpf_map_lookup_elem(&drop_counter, &seq_key);
        if (drop) {
            __sync_fetch_and_add(drop, 1);
        }
    }

    return TC_ACT_OK;
}

char _license[] SEC("license") = "GPL";