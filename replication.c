#include "replication.h"
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
#include "kvstore.h"


// 连接与缓冲区状态
struct repl_conn {
    int fd;
    char *wbuffer;
    int wcapacity;
    int wlength;
};

static struct repl_conn g_repl = { -1, NULL, 0, 0 };

//动态扩容
static int ensure_wbuffer_capacity(int needed_space) {
    if (g_repl.wcapacity - g_repl.wlength >= needed_space) {
        return 0; 
    }
    int new_capacity = g_repl.wcapacity * 2;
    while (new_capacity - g_repl.wlength < needed_space) {
        new_capacity *= 2;
    }

    char *new_buf = (char *)kvs_realloc(g_repl.wbuffer, new_capacity);
    if (!new_buf) {
        perror("replication wbuffer kvs_realloc failed");
        return -1;
    }

    g_repl.wbuffer = new_buf;
    g_repl.wcapacity = new_capacity;
    return 0;
}

int repl_connect_to_slave(const char *slave_ip, unsigned short slave_port) {
    g_repl.fd = socket(AF_INET, SOCK_STREAM, 0);
    if (g_repl.fd < 0) return -1;

    struct sockaddr_in slave_addr;
    memset(&slave_addr, 0, sizeof(slave_addr));
    slave_addr.sin_family = AF_INET;
    slave_addr.sin_port = htons(slave_port);
    slave_addr.sin_addr.s_addr = inet_addr(slave_ip);

    if (connect(g_repl.fd, (struct sockaddr*)&slave_addr, sizeof(slave_addr)) < 0) {
        perror("Connect to slave failed");
        close(g_repl.fd);
        g_repl.fd = -1;
        return -1;
    }

    // 初始化发送缓冲区
    g_repl.wcapacity = REPL_INIT_BUFFER_SIZE;
    g_repl.wbuffer = (char *)kvs_malloc(g_repl.wcapacity);
    g_repl.wlength = 0;

    // 设置为非阻塞模式,防止发包时卡死Master主线程
    int flags = fcntl(g_repl.fd, F_GETFL, 0);
    fcntl(g_repl.fd, F_SETFL, flags | O_NONBLOCK);

    printf("Successfully connected to Slave at %s:%d\n", slave_ip, slave_port);
    return 0;
}

int repl_push_cmd(const char *cmd_name, const char *key, int key_len, const char *value, int value_len) {
    if (g_repl.fd < 0) return -1;
    if (!cmd_name) return -1;

    int cmd_len = strlen(cmd_name);

    // 对齐从端
    int single_cmd_len = 4 + cmd_len + 4 + key_len + 4 + value_len;
    int total_needed = single_cmd_len;
    
    // 如果当前发送缓冲区是空的，说明我们要开启一个全新大包，必须算上全局头 cmd_count 的 4 字节
    if (g_repl.wlength == 0) {
        total_needed += 4; 
    }

    // 动态扩容网络缓冲区
    if (ensure_wbuffer_capacity(total_needed) != 0) return -1;

    char *p = g_repl.wbuffer + g_repl.wlength;

    // 处理全局大包头 cmd_count
    if (g_repl.wlength == 0) {
        int cmd_count = 1;
        memcpy(p, &cmd_count, 4); 
        p += 4;
    } else {
        int *batch_cmd_count = (int*)g_repl.wbuffer;
        (*batch_cmd_count)++;
    }

    // 严格按照从端状态机的交替顺序写入内存 [Len][Data][Len][Data]
    // 写入命令
    memcpy(p, &cmd_len, 4);       p += 4;
    memcpy(p, cmd_name, cmd_len); p += cmd_len;

    // 写入Key
    memcpy(p, &key_len, 4);       p += 4;
    memcpy(p, key, key_len);       p += key_len;

    // 写入Value
    memcpy(p, &value_len, 4);     p += 4;
    if (value_len > 0 && value != NULL) {
        memcpy(p, value, value_len); p += value_len;
    }

    // 推进发送缓冲区有效数据长度
    g_repl.wlength += total_needed;
    
    // 触发非阻塞发送
    repl_flush(); 
    return 0;
}

int repl_flush() {
    if (g_repl.fd < 0 || g_repl.wlength == 0) return 0;

    int total_sent = 0;
    
    // 非阻塞下尽量排空缓冲区
    while (g_repl.wlength > 0) {
        int ret = send(g_repl.fd, g_repl.wbuffer, g_repl.wlength, 0);
        if (ret > 0) {
            total_sent += ret;
            if (ret < g_repl.wlength) {
                memmove(g_repl.wbuffer, g_repl.wbuffer + ret, g_repl.wlength - ret);
            }
            g_repl.wlength -= ret;
        } else if (ret < 0) {
            if (errno == EINTR) {
                continue; // 被信号中断，继续尝试发送
            }
            if (errno == EAGAIN || errno == EWOULDBLOCK) {
                // 内核缓冲区满了，主线程不再死等，优雅退出。
                // 没发完的残包会留在 wbuffer 中，等下一次 repl_push_cmd 进来时一起带走
                break; 
            }
            // 发生严重网络错误
            perror("Replication send error, slave disconnected");
            repl_close();
            return -1;
        }
    }

    // 按需缩容
    if (g_repl.wlength == 0 && g_repl.wcapacity > REPL_INIT_BUFFER_SIZE * 4) {
        char *shrunk_buf = (char *)kvs_realloc(g_repl.wbuffer, REPL_INIT_BUFFER_SIZE);
        if (shrunk_buf) {
            g_repl.wbuffer = shrunk_buf;
            g_repl.wcapacity = REPL_INIT_BUFFER_SIZE;
        }
    }

    return total_sent;
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