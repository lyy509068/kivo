#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <fcntl.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <sys/stat.h>
#include <errno.h>
#include <sys/epoll.h>
#include "replication.h"    
#include "network.h"
#include "kvstore.h"      


struct repl_conn g_repl = { -1, NULL, 0, 0 };

static int ensure_wbuffer_capacity(int needed_space) {
    if (g_repl.wcapacity - g_repl.wlength >= needed_space) {
        return 0; 
    }
    // 如果当前容量为 0，给它一个保底的初始容量；否则乘以 2
    int new_capacity = (g_repl.wcapacity == 0) ? REPL_INIT_BUFFER_SIZE : (g_repl.wcapacity * 2);
    
    // 继续乘以 2 寻找足够大的容量
    while (new_capacity - g_repl.wlength < needed_space) {
        new_capacity *= 2;
    }

    char *new_buf = (char *)kvs_realloc(g_repl.wbuffer, new_capacity);
    if (!new_buf) {
        perror("[Repl-Push] replication wbuffer kvs_realloc failed");
        return -1;
    }

    g_repl.wbuffer = new_buf;
    g_repl.wcapacity = new_capacity;
    
    return 0;
}



int repl_connect_to_master(const char *master_ip, unsigned short master_port) {
    g_repl.fd = socket(AF_INET, SOCK_STREAM, 0);// 创建套接字
    if (g_repl.fd < 0) {
        perror("Slave: Create socket failed");
        return -1;
    }
    struct sockaddr_in master_addr; // 配置主端的服务器地址
    memset(&master_addr, 0, sizeof(master_addr));
    master_addr.sin_family = AF_INET;
    master_addr.sin_port = htons(master_port);
    master_addr.sin_addr.s_addr = inet_addr(master_ip);
    // 向主端发起连接
    if (connect(g_repl.fd, (struct sockaddr*)&master_addr, sizeof(master_addr)) < 0) {
        perror("Slave: Connect to master failed");
        close(g_repl.fd);
        g_repl.fd = -1;
        return -1; 
    }
    // 连接成功，初始化接收/发送缓冲区
    if (g_repl.wbuffer == NULL) {
        g_repl.wcapacity = REPL_INIT_BUFFER_SIZE;
        g_repl.wbuffer = (char *)kvs_malloc(g_repl.wcapacity);
        if (!g_repl.wbuffer) {
            perror("Slave: malloc wbuffer failed");
            close(g_repl.fd);
            g_repl.fd = -1;
            return -1;
        }
    }
    g_repl.wlength = 0;
    
    int flags = fcntl(g_repl.fd, F_GETFL, 0);
    if (flags >= 0) {
        fcntl(g_repl.fd, F_SETFL, flags | O_NONBLOCK);
    }
    printf("Slave: Successfully connected to Master at %s:%d\n", master_ip, master_port);
    
    // 打包resp命令
    const char *sync_cmd = "*1\r\n$4\r\nSYNC\r\n"; 
    int cmd_len = strlen(sync_cmd);

    // 灌入主从专用的全局写缓冲区 g_repl.wbuffer
    if (g_repl.wlength + cmd_len <= g_repl.wcapacity) {
        memcpy(g_repl.wbuffer + g_repl.wlength, sync_cmd, cmd_len);
        g_repl.wlength += cmd_len;
    }
    // 写缓冲区里的数据全量送给主端
    int sent = send(g_repl.fd, g_repl.wbuffer, g_repl.wlength, 0);
    if (sent > 0) {
        g_repl.wlength -= sent; 
    }
    // 调用网络层的封装接口，安全完成 Reactor 托孤
    if (reactor_host_slave_connection(g_repl.fd, g_repl.wbuffer, g_repl.wcapacity, g_repl.wlength) < 0) {
        
        close(g_repl.fd);
        g_repl.fd = -1;
        return -1;
    }

    return 0;
}


int repl_push_cmd(const char *cmd_name, const char *key, int key_len, const char *value, int value_len) {
    if (g_repl.fd < 0) {
        //printf("[Repl-Push] Error: Replication FD is invalid (%d)\n", g_repl.fd);
        return -1;
    }
    if (!cmd_name) {
        return -1;
    }

    int cmd_name_len = strlen(cmd_name);
    int total_needed = 64 + cmd_name_len + key_len + value_len; 

    if (ensure_wbuffer_capacity(total_needed) != 0) {
        return -1;
    }

    char *p = g_repl.wbuffer + g_repl.wlength;
    int written = 0;

    if (value_len > 0 && value != NULL) {
        written = sprintf(p, "*3\r\n$%d\r\n%s\r\n$%d\r\n", cmd_name_len, cmd_name, key_len);
        p += written;
        
        memcpy(p, key, key_len);
        p += key_len;
        
        int tail_written = sprintf(p, "\r\n$%d\r\n", value_len);
        p += tail_written;
        
        memcpy(p, value, value_len);
        p += value_len;
        
        memcpy(p, "\r\n", 2);
        p += 2;
        
        g_repl.wlength += (written + key_len + tail_written + value_len + 2);
    } else {
        written = sprintf(p, "*2\r\n$%d\r\n%s\r\n$%d\r\n", cmd_name_len, cmd_name, key_len);
        p += written;
        
        memcpy(p, key, key_len);
        p += key_len;
        
        memcpy(p, "\r\n", 2);
        p += 2;
        
        g_repl.wlength += (written + key_len + 2);
    }
    
    repl_flush(); 

    return 0;
}

int repl_flush() {
    if (g_repl.fd < 0) return 0;
    if (g_repl.wlength == 0) {
        fprintf(stderr, "[Repl-Push] Error: Replication FD is invalid (%d)\n", g_repl.fd);
        return 0;
    }

    int ret = send(g_repl.fd, g_repl.wbuffer, g_repl.wlength, MSG_DONTWAIT);
    
    if (ret > 0) {
        
        if (ret < g_repl.wlength) {
            // 没发完，平移残包
            int remaining = g_repl.wlength - ret;
            memmove(g_repl.wbuffer, g_repl.wbuffer + ret, remaining);
            g_repl.wlength = remaining;
            
            // 托管给写事件
            set_event(g_repl.fd, EPOLLOUT, 0);
        } else {
            // 全部发完
            g_repl.wlength = 0;
            set_event(g_repl.fd, 0, 0);
            
            // 按需缩容
            if (g_repl.wcapacity > REPL_INIT_BUFFER_SIZE * 4) {
                char *shrunk_buf = (char *)kvs_realloc(g_repl.wbuffer, REPL_INIT_BUFFER_SIZE);
                if (shrunk_buf) {
                    g_repl.wbuffer = shrunk_buf;
                    g_repl.wcapacity = REPL_INIT_BUFFER_SIZE;
                }
            }
        }
        return ret;
    } 
    else if (ret < 0) {
        if (errno == EAGAIN || errno == EWOULDBLOCK) {
            set_event(g_repl.fd, EPOLLOUT, 0);
            return 0;
        }
        repl_close();
        return -1;
    }
    return 0;
}

void repl_close() {
    if (g_repl.fd >= 0) {
        close(g_repl.fd);
        g_repl.fd = -1;
    }
    if (g_repl.wbuffer) {
        kvs_free(g_repl.wbuffer);
        g_repl.wbuffer = NULL;
    }
    g_repl.wcapacity = 0;
    g_repl.wlength = 0;
} 
