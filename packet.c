#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <arpa/inet.h> 
#include "kvstore.h"

/**
 * @brief 批量打包函数
 * @param resps 业务层返回的响应数组
 * @param cmd_count 总条数
 * @param wbuf 网络层写缓冲区指针的指针（方便 kvs_realloc 修改原指针）
 * @param wcap 网络层写缓冲区总容量指针
 * @param wlen 网络层写缓冲区当前已用长度指针
 */
void packet_build_batch(kvs_resp_t *resps, int cmd_count, char **wbuf, int *wcap, int *wlen) {
    if (cmd_count <= 0 || resps == NULL) return;
    // 基础头部：总条数占用 4 字节
    size_t total_needed_bytes = 4; 

    for (int i = 0; i < cmd_count; i++) {
        total_needed_bytes += 4; // 状态码 status (4字节)
        total_needed_bytes += 4; // 回复内容长度 body_len (4字节)
        
        // 只有当状态码为成功且有实际包体时，才计算包体长度
        // 这里的 7 对应你的 KVS_RESP_GET_OK 枚举值（假设其整型值为 7）!!!!!!增加回复类型要改这里
        if (resps[i].status == 7 && resps[i].body_len > 0 && resps[i].body != NULL) {
            total_needed_bytes += resps[i].body_len;
        }
    }

    // 在网络层缓冲区中进行“一次性”安全扩容
    if ((*wlen) + total_needed_bytes > (size_t)(*wcap)) {
        int new_cap = (*wcap) == 0 ? 1024 : (*wcap);
        // 动态翻倍，直到能完全放下新数据包
        while ((*wlen) + total_needed_bytes > (size_t)new_cap) {
            new_cap *= 2;
        }
        
        char *new_buf = (char *)kvs_realloc(*wbuf, new_cap);
        if (new_buf == NULL) {
            printf("[ERROR-PACKET] Out of memory during kvs_realloc!\n");
            return; // 线上环境应有更严谨的写失败保护
        }
        *wbuf = new_buf;
        *wcap = new_cap;
        printf("[DEBUG-PACKET] Buffer expanded successfully to %d bytes\n", new_cap);
    }

    // 定位到当前缓冲区可写的末尾位置
    char *ptr = (*wbuf) + (*wlen);

    // A. 写入总条数 (转网络大端序)
    int cmd_count_net = htonl(cmd_count);
    memcpy(ptr, &cmd_count_net, 4);
    ptr += 4;

    // B. 循环写入每一条命令的回复
    for (int i = 0; i < cmd_count; i++) {
        // a. 写入状态码
        int status_net = htonl(resps[i].status);
        memcpy(ptr, &status_net, 4);
        ptr += 4;

        // b. 计算并写入回复内容长度
        int current_body_len = 0;
        if (resps[i].status == 7 && resps[i].body_len > 0) {
            current_body_len = (int)resps[i].body_len;
        }
        
        int body_len_net = htonl(current_body_len);
        memcpy(ptr, &body_len_net, 4);
        ptr += 4;

        // c. 写入回复内容自身
        if (current_body_len > 0 && resps[i].body != NULL) {
            memcpy(ptr, resps[i].body, current_body_len);
            ptr += current_body_len;
        }
    }

    // 更新网络层的总写入长度
    *wlen += total_needed_bytes;
    printf("[DEBUG-PACKET] Batch packet built. Total written size updated to %d\n", *wlen);
}