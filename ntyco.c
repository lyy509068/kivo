#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <string.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <arpa/inet.h>

#include "nty_coroutine.h" 

#include "network.h"
#include "kvstore.h"
#include "resp.h"
#include "repl.h"
#include "rdma.h"

static stream_handler_t g_stream_handler = NULL;

struct conn ntyco_conn_list[CONNECTION_SIZE] = {0};

void ntyco_client_co(void *arg);
void ntyco_server_co(void *arg);

void ntyco_close_and_free_connection(int fd) {
    close(fd);
    if (ntyco_conn_list[fd].rbuffer) kvs_free(ntyco_conn_list[fd].rbuffer);
    if (ntyco_conn_list[fd].wbuffer) kvs_free(ntyco_conn_list[fd].wbuffer);
    memset(&ntyco_conn_list[fd], 0, sizeof(struct conn));
}

void ntyco_client_co(void *arg) {
    int fd = (int)(long)arg;
    struct conn *c = &ntyco_conn_list[fd];

    c->rcapacity = INIT_BUFFER_SIZE;
    c->rbuffer = (char*)kvs_malloc(INIT_BUFFER_SIZE);
    c->wcapacity = INIT_BUFFER_SIZE;
    c->wbuffer = (char*)kvs_malloc(INIT_BUFFER_SIZE);
    c->rlength = 0;
    c->wlength = 0;

    if (!c->rbuffer || !c->wbuffer) { ntyco_close_and_free_connection(fd); return; }

    while (1) {
        if (c->rcapacity - c->rlength < 65536) {
            int new_capacity = c->rcapacity * 2;
            if (new_capacity < 65536) new_capacity = 65536;
            c->rbuffer = (char *)kvs_realloc(c->rbuffer, new_capacity);
            c->rcapacity = new_capacity;
        }

        int count = recv(fd, c->rbuffer + c->rlength, c->rcapacity - c->rlength, 0);
        if (count <= 0) break;
        c->rlength += count;

        int total_parsed_bytes = 0;
        while (c->rlength > total_parsed_bytes) {
            int parsed_bytes = 0;
            int is_master = (c->role == CONN_MASTER);

            int status = g_stream_handler(
                c->rbuffer + total_parsed_bytes, 
                c->rlength - total_parsed_bytes, 
                &parsed_bytes,
                is_master ? NULL : &c->wbuffer,   
                is_master ? NULL : &c->wcapacity,
                is_master ? NULL : &c->wlength, 
                fd 
            );

            if (status == 1) break;
            if (status < 0) { ntyco_close_and_free_connection(fd); return; }
            total_parsed_bytes += parsed_bytes;
        }

        if (total_parsed_bytes > 0) {
            int remaining = c->rlength - total_parsed_bytes;
            if (remaining > 0) memmove(c->rbuffer, c->rbuffer + total_parsed_bytes, remaining);
            c->rlength = remaining;
        }

        if (c->wlength > 0) {
            send(fd, c->wbuffer, c->wlength, 0);
            c->wlength = 0;
        }
    }

    ntyco_close_and_free_connection(fd);
}

void ntyco_server_co(void *arg) {
    int listen_fd = (int)(long)arg;
    while (1) {
        struct sockaddr_in clientaddr;
        socklen_t len = sizeof(clientaddr);
        int clientfd = accept(listen_fd, (struct sockaddr*)&clientaddr, &len);
        if (clientfd < 0) continue;
        if (clientfd >= CONNECTION_SIZE) { close(clientfd); continue; }

        ntyco_conn_list[clientfd].fd = clientfd;
        ntyco_conn_list[clientfd].role = CONN_CLIENT;

        nty_coroutine *client_co = NULL;
        nty_coroutine_create(&client_co, ntyco_client_co, (void*)(long)clientfd);
    }
}

static int init_listen_socket(unsigned short port) {
    int sockfd = socket(AF_INET, SOCK_STREAM, 0);
    if (sockfd < 0) return -1;
    int opt = 1;
    setsockopt(sockfd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));
    struct sockaddr_in servaddr;
    memset(&servaddr, 0, sizeof(servaddr));
    servaddr.sin_family = AF_INET;
    servaddr.sin_addr.s_addr = htonl(INADDR_ANY);
    servaddr.sin_port = htons(port);
    if (bind(sockfd, (struct sockaddr*)&servaddr, sizeof(servaddr)) < 0) return -1;
    if (listen(sockfd, 10) < 0) return -1;
    return sockfd;
}

void ntyco_slave_init_co(void *arg) {
    const char *master_ip = "192.168.37.128";
    unsigned short master_port = 2000;
    
    if (g_use_tcp_sync) {
        int fd = socket(AF_INET, SOCK_STREAM, 0);
        struct sockaddr_in addr;
        memset(&addr, 0, sizeof(addr));
        addr.sin_family = AF_INET;
        addr.sin_port = htons(master_port);
        inet_pton(AF_INET, master_ip, &addr.sin_addr);
        if (connect(fd, (struct sockaddr*)&addr, sizeof(addr)) < 0) {
            close(fd);
            return;
        }
        const char *sync = "*1\r\n$4\r\nSYNC\r\n";
        send(fd, sync, strlen(sync), 0);
        
        pthread_t tid;
        int *pfd = malloc(sizeof(int));
        *pfd = fd;
        pthread_create(&tid, NULL, tcp_sendfile_recv_thread, pfd);
        pthread_detach(tid);
        
    } else {
        repl_connect_to_master(master_ip, master_port);
    }
}

int ntyco_start(unsigned short port, stream_handler_t handler) {
    g_stream_handler = handler;
    int listen_fd = init_listen_socket(port);
    if (listen_fd < 0) return -1;

    ntyco_conn_list[listen_fd].fd = listen_fd;

    nty_coroutine *server_co = NULL;
    nty_coroutine_create(&server_co, ntyco_server_co, (void*)(long)listen_fd);

    if (g_enable_repl_slave){
        nty_coroutine *slave_init_co = NULL;
        nty_coroutine_create(&slave_init_co, ntyco_slave_init_co, NULL);
    }

    nty_schedule_run();
    return 0;
}

struct conn* ntyco_host_slave_connection(int fd, char *wbuf, int wcap, int wlen) {
    if (fd < 0 || fd >= CONNECTION_SIZE) return NULL;
    struct conn *c = &ntyco_conn_list[fd];
    c->fd = fd;
    c->role = CONN_MASTER;
    c->wbuffer = wbuf;
    c->wcapacity = wcap;
    c->wlength = wlen;
    c->rbuffer = (char *)kvs_malloc(65536);
    c->rcapacity = 65536;
    c->rlength = 0;

    nty_coroutine *client_co = NULL;
    nty_coroutine_create(&client_co, ntyco_client_co, (void*)(long)fd);

    return c;
}