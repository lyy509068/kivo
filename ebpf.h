#ifndef EBPF_H
#define EBPF_H

#include <linux/types.h>

#define MAP_KEY_SOLE 0

int ebpf_init_loader(const char *bpf_object_path);
int ebpf_register_slave(int slave_fd);
int ebpf_set_forward_switch(int enable);
void ebpf_cleanup(void);

#endif