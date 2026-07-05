#include "ebpf.h"
#include <stdio.h>
#include <bpf/bpf.h>
#include <net/if.h>
#include <unistd.h>    
#include <arpa/inet.h>
#include <sys/socket.h>
#include <netinet/in.h>

static int tc_map_fd      = -1;
static int slave_ip_fd   = -1;
static int slave_mac_fd  = -1;
static int master_mac_fd = -1;
static int switch_fd     = -1;

int ebpf_init_loader(const char *bpf_object_path) {
    (void)bpf_object_path;

    tc_map_fd     = bpf_obj_get("/sys/fs/bpf/slave_ifindex_map");
    slave_ip_fd   = bpf_obj_get("/sys/fs/bpf/slave_ip_map");
    slave_mac_fd  = bpf_obj_get("/sys/fs/bpf/slave_mac_map");
    master_mac_fd = bpf_obj_get("/sys/fs/bpf/master_mac_map");
    switch_fd     = bpf_obj_get("/sys/fs/bpf/clone_switch_map");

    if (tc_map_fd < 0 || slave_ip_fd < 0 || slave_mac_fd < 0 || master_mac_fd < 0 || switch_fd < 0) {
        fprintf(stderr, "[eBPF] Maps not found. Run: make load_bpf\n");
        return -1;
    }

    printf("[eBPF] All maps loaded from /sys/fs/bpf/\n");

    return 0;
}

int ebpf_register_slave(void) {
    __u32 key = 0;

    __u32 ifindex = (__u32)if_nametoindex("ens33");
    if (bpf_map_update_elem(tc_map_fd, &key, &ifindex, BPF_ANY) < 0) return -1;

    __u32 slave_ip = htonl(0xC0A85C82);
    if (bpf_map_update_elem(slave_ip_fd, &key, &slave_ip, BPF_ANY) < 0) return -1;

    unsigned char slave_mac[6] = {0x00, 0x0c, 0x29, 0xfc, 0x21, 0xcd};
    if (bpf_map_update_elem(slave_mac_fd, &key, slave_mac, BPF_ANY) < 0) return -1;

    unsigned char master_mac[6] = {0x00, 0x0c, 0x29, 0x6d, 0x32, 0x1f};
    if (bpf_map_update_elem(master_mac_fd, &key, master_mac, BPF_ANY) < 0) return -1;

    printf("[eBPF] Slave metadata registered.\n");

    return 0;
}

int ebpf_set_forward_switch(int enable) {
    __u32 key = 0;
    __u32 value = enable ? 1 : 0;
    return bpf_map_update_elem(switch_fd, &key, &value, BPF_ANY);
}

void ebpf_cleanup(void) {
    if (tc_map_fd >= 0)     close(tc_map_fd);
    if (slave_ip_fd >= 0)   close(slave_ip_fd);
    if (slave_mac_fd >= 0)  close(slave_mac_fd);
    if (master_mac_fd >= 0) close(master_mac_fd);
    if (switch_fd >= 0)     close(switch_fd);
}