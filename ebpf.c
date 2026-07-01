#include "ebpf.h"
#include <stdio.h>
#include <bpf/libbpf.h>
#include <bpf/bpf.h>
#include <net/if.h>
#include <stdlib.h>    
#include <unistd.h>    
#include <arpa/inet.h>

static struct bpf_object *obj = NULL;
static int tc_map_fd = -1;
static int switch_map_fd = -1; 
static int slave_ip_map_fd = -1; 
static int slave_mac_map_fd = -1;
static int master_mac_map_fd = -1;

// 修改后的初始化加载函数
int ebpf_init_loader(const char *bpf_object_path) {
    char cmd[512];
    int ret;

    // 🌟 1. 提前清理历史残留的 Pin 文件，防止因为文件已存在 (EEXIST) 导致加载失败
    unlink("/sys/fs/bpf/clone_switch_map");
    unlink("/sys/fs/bpf/handle_tc_dual_write");

    // 2. 利用 libbpf 打开 eBPF 字节码
    obj = bpf_object__open_file(bpf_object_path, NULL);
    if (!obj) {
        fprintf(stderr, "[eBPF TC] Failed to open BPF object file: %s\n", bpf_object_path);
        return -1;
    }

    // 3. 真正加载到内核（此时在内核中创建了第一套专属的 Map 和 Prog 实例）
    if (bpf_object__load(obj)) {
        fprintf(stderr, "[eBPF TC] Failed to load BPF object into kernel\n");
        bpf_object__close(obj);
        obj = NULL;
        return -1;
    }

    // 🌟 4. 【核心修改点】将运行中的 Map 和 Program 强行钉到 BPF 文件系统中
    struct bpf_map *map_obj = bpf_object__find_map_by_name(obj, "clone_switch_map");
    if (map_obj) {
        bpf_map__pin(map_obj, "/sys/fs/bpf/clone_switch_map");
    }

    struct bpf_program *prog_obj = bpf_object__find_program_by_name(obj, "handle_tc_dual_write");
    if (prog_obj) {
        bpf_program__pin(prog_obj, "/sys/fs/bpf/handle_tc_dual_write");
    }

    // 5. 从当前唯一的 obj 中提取 Map 的描述符
    tc_map_fd         = bpf_object__find_map_fd_by_name(obj, "slave_ifindex_map");
    switch_map_fd     = bpf_object__find_map_fd_by_name(obj, "clone_switch_map");
    slave_ip_map_fd   = bpf_object__find_map_fd_by_name(obj, "slave_ip_map");   
    slave_mac_map_fd  = bpf_object__find_map_fd_by_name(obj, "slave_mac_map");
    master_mac_map_fd = bpf_object__find_map_fd_by_name(obj, "master_mac_map");

    if (tc_map_fd < 0 || switch_map_fd < 0 || slave_ip_map_fd < 0 || slave_mac_map_fd < 0 || master_mac_map_fd < 0) {
        fprintf(stderr, "[eBPF TC] Failed to locate map FDs within compiled object.\n");
        bpf_object__close(obj);
        obj = NULL;
        return -1;
    }

    // 🌟 6. 【核心修改点】让 tc 命令直接去挂载已经载入内核的、BPF FS 里的那个程序！
    // 绝不允许 tc 重新加载 obj 文件，从而在根源上掐死“平行 Map”的产生。
    system("tc qdisc del dev ens33 clsact 2>/dev/null");
    snprintf(cmd, sizeof(cmd),
        "tc qdisc add dev ens33 clsact 2>/dev/null; "
        "tc filter replace dev ens33 ingress bpf pinned /sys/fs/bpf/handle_tc_dual_write direct-action 2>&1");

    ret = system(cmd);
    if (ret != 0) {
        fprintf(stderr, "[eBPF TC] Failed to attach filter via tc command.\n");
        bpf_object__close(obj);
        obj = NULL;
        return -1;
    }

    printf("[eBPF TC] Program and Maps unified and anchored into BPF FS successfully.\n");
    return 0;
}

int ebpf_register_slave(int slave_fd) {
    __u32 key = 0;

    if (tc_map_fd < 0 || slave_ip_map_fd < 0 || slave_mac_map_fd < 0 || master_mac_map_fd < 0) {
        return -1;
    }

    // 设置物理网卡接口索引
    __u32 ifindex = (__u32)if_nametoindex("ens33");
    bpf_map_update_elem(tc_map_fd, &key, &ifindex, BPF_ANY);

    // 设置从端（c2）物理 IP：192.168.92.130
    __u32 slave_ip = htonl(0xC0A85C82);  
    bpf_map_update_elem(slave_ip_map_fd, &key, &slave_ip, BPF_ANY);

    // 注入从端（c2）真实 MAC：00:0c:29:fc:21:cd
    unsigned char slave_mac[6] = {0x00, 0x0c, 0x29, 0xfc, 0x21, 0xcd};
    bpf_map_update_elem(slave_mac_map_fd, &key, slave_mac, BPF_ANY);

    // 注入主端（lyy）真实 MAC：00:0c:29:6d:32:1f
    unsigned char master_mac[6] = {0x00, 0x0c, 0x29, 0x6d, 0x32, 0x1f};
    bpf_map_update_elem(master_mac_map_fd, &key, master_mac, BPF_ANY);

    printf("[eBPF TC] Slave Metadata registered via safe memory descriptor.\n");
    return 0;
}

int ebpf_set_forward_switch(int enable) {
    __u32 key = 0;
    __u32 value = enable ? 1 : 0;
    int map_fd = -1;

    // 路径 1：尝试获取 libbpf 默认固定的全局全局全局 Map 路径
    const char *map_path = "/sys/fs/bpf/clone_switch_map";
    map_fd = bpf_obj_get(map_path);

    // 路径 2：如果路径 1 失败，尝试 iproute2 常见的 tc 专属固定路径
    if (map_fd < 0) {
        map_path = "/sys/fs/bpf/tc/globals/clone_switch_map";
        map_fd = bpf_obj_get(map_path);
    }

    // 如果成功通过 BPF 文件系统获取到了真正运行中的内核 Map FD
    if (map_fd >= 0) {
        int ret = bpf_map_update_elem(map_fd, &key, &value, BPF_ANY);
        if (ret == 0) {
            printf("[Master] Successfully updated WALKING-IN-KERNEL map via BPF FS (%s)!\n", map_path);
        } else {
            perror("[Repl Error] Failed to update pinned map");
        }
        close(map_fd); // 记得关闭通过 bpf_obj_get 获取的临时文件描述符
        return ret;
    }

    // 兜底路径：如果系统未正确挂载 BPF FS，则降级到原有的全局变量逻辑
    printf("[Master Warning] BPF FS map not found. Falling back to global switch_map_fd...\n");
    if (switch_map_fd >= 0) {
        return bpf_map_update_elem(switch_map_fd, &key, &value, BPF_ANY);
    }

    fprintf(stderr, "[Repl Error] All paths failed! Cannot locate clone_switch_map.\n");
    return -1;
}

void ebpf_cleanup(void) { 
    if (obj) {
        bpf_object__close(obj); 
        obj = NULL;
    }
    system("tc qdisc del dev ens33 clsact 2>/dev/null");
    // 清理掉固定文件，保持系统干净
    unlink("/sys/fs/bpf/handle_tc_dual_write");
    unlink("/sys/fs/bpf/clone_switch_map");
}