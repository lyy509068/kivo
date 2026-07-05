#include "udp.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <pthread.h> 

extern int protocol_process_udp_silent(const char *in_buf, int in_len);

static void *replica_udp_server_worker(void *arg) {
    int port = *(int *)arg;
    free(arg);

    int fd = socket(AF_INET, SOCK_DGRAM, 0);
    if (fd < 0) return NULL;

    int rcvbuf = 256 * 1024;
    setsockopt(fd, SOL_SOCKET, SO_RCVBUF, &rcvbuf, sizeof(rcvbuf));

    struct sockaddr_in addr;
    memset(&addr, 0, sizeof(addr));
    addr.sin_family = AF_INET;
    addr.sin_port = htons(port);
    addr.sin_addr.s_addr = htonl(INADDR_ANY);

    if (bind(fd, (struct sockaddr *)&addr, sizeof(addr)) < 0) {
        close(fd);
        return NULL;
    }

    char *recv_buf = malloc(65535);
    if (!recv_buf) { close(fd); return NULL; }

    printf("[+] [UDP Server] Smart Scan UDP Listener running on port %d...\n", port);


    while (1) {
        struct sockaddr_in client_addr;
        socklen_t addr_len = sizeof(client_addr);
        
        int len = recvfrom(fd, recv_buf, 65535, 0, (struct sockaddr *)&client_addr, &addr_len);
        if (len > 0) {
            protocol_process_udp_silent(recv_buf, len);
        }
    }

    free(recv_buf);
    close(fd);
    return NULL;
}

int start_replica_udp_server_coroutine(int listen_port) {
    int *port_ptr = malloc(sizeof(int));
    if (!port_ptr) return -1;
    *port_ptr = listen_port;

    pthread_t tid;
    if (pthread_create(&tid, NULL, replica_udp_server_worker, port_ptr) != 0) {
        free(port_ptr);
        return -1;
    }
    pthread_detach(tid);
    return 0;
}