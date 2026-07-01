#ifndef EBPF_CONTROL_H
#define EBPF_CONTROL_H

int ebpf_init_loader(const char *bpf_object_path);
int ebpf_register_slave(int slave_fd);
int ebpf_set_forward_switch(int enable);
void ebpf_cleanup(void);

#endif