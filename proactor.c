#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <string.h>
#include <unistd.h>
#include <sys/time.h>
#include <arpa/inet.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <liburing.h> 

#include "network.h"
#include "kvstore.h"
#include "resp.h"
#include "repl.h"
#include "rdma.h"

static stream_handler_t g_stream_handler = NULL;
static struct io_uring ring; 

struct conn proactor_conn_list[CONNECTION_SIZE] = {0};

static struct timeval begin;

void submit_accept(int listen_fd);
void submit_recv(int fd);
void submit_send(int fd);

void proactor_close_and_free_connection(int fd) {
    close(fd);
    if (proactor_conn_list[fd].rbuffer) kvs_free(proactor_conn_list[fd].rbuffer);
    if (proactor_conn_list[fd].wbuffer) kvs_free(proactor_conn_list[fd].wbuffer);
    memset(&proactor_conn_list[fd], 0, sizeof(struct conn));
}

void submit_accept(int listen_fd) {
    struct io_uring_sqe *sqe = io_uring_get_sqe(&ring);
    struct conn *c = &proactor_conn_list[listen_fd];
    c->accept_ctx.fd = listen_fd;
    c->accept_ctx.type = OP_ACCEPT;
    io_uring_prep_accept(sqe, listen_fd, (struct sockaddr*)&c->clientaddr, &c->clientlen, 0);
    io_uring_sqe_set_data(sqe, &c->accept_ctx);
}

void submit_recv(int fd) {
    struct conn *c = &proactor_conn_list[fd];
    if (c->rcapacity - c->rlength < 4096) {
        int new_capacity = c->rcapacity * 2;
        if (new_capacity < 4096) new_capacity = 4096;
        char *new_buf = (char *)kvs_realloc(c->rbuffer, new_capacity);
        if (!new_buf) { proactor_close_and_free_connection(fd); return; }
        c->rbuffer = new_buf;
        c->rcapacity = new_capacity;
    }
    struct io_uring_sqe *sqe = io_uring_get_sqe(&ring);
    c->recv_ctx.fd = fd;
    c->recv_ctx.type = OP_RECV;
    int remaining_space = c->rcapacity - c->rlength;
    io_uring_prep_recv(sqe, fd, c->rbuffer + c->rlength, remaining_space, 0);
    io_uring_sqe_set_data(sqe, &c->recv_ctx);
}

void submit_send(int fd) {
    struct conn *c = &proactor_conn_list[fd];
    if (c->wlength <= 0) return;
    struct io_uring_sqe *sqe = io_uring_get_sqe(&ring);
    c->send_ctx.fd = fd;
    c->send_ctx.type = OP_SEND;
    io_uring_prep_send(sqe, fd, c->wbuffer, c->wlength, 0);
    io_uring_sqe_set_data(sqe, &c->send_ctx);
}

void on_recv_completed(int fd, int res) {
    struct conn *c = &proactor_conn_list[fd];
    if (res <= 0) { proactor_close_and_free_connection(fd); return; }

    c->rlength += res;
    if (!g_stream_handler) return;

    int total_parsed_bytes = 0;
    while (c->rlength > total_parsed_bytes) {
        int parsed_bytes = 0;
        long long expect_file_size = 0;
        int is_master = (c->role == CONN_MASTER);

        int status = g_stream_handler(
            c->rbuffer + total_parsed_bytes,
            c->rlength - total_parsed_bytes,
            &parsed_bytes,
            is_master ? NULL : &c->wbuffer,
            is_master ? NULL : &c->wcapacity,
            is_master ? NULL : &c->wlength,
            &expect_file_size,
            fd
        );

        if (status == 1) break;
        if (status < 0) {
            if (c->role == CONN_MASTER) c->wlength = 0;
            proactor_close_and_free_connection(fd); return;
        }
        total_parsed_bytes += parsed_bytes;
    }

    if (total_parsed_bytes > 0) {
        int remaining = c->rlength - total_parsed_bytes;
        if (remaining > 0) memmove(c->rbuffer, c->rbuffer + total_parsed_bytes, remaining);
        c->rlength = remaining;
    }

    if (c->wlength > 0) submit_send(fd);
    else submit_recv(fd);
}

void on_send_completed(int fd, int res) {
    struct conn *c = &proactor_conn_list[fd];
    if (res < 0) { proactor_close_and_free_connection(fd); return; }

    if (c->wlength > 0) {
        if (res < c->wlength) {
            memmove(c->wbuffer, c->wbuffer + res, c->wlength - res);
            c->wlength -= res;
            submit_send(fd);
            return;
        }
        c->wlength = 0;
    }
    submit_recv(fd);
}

void on_accept_completed(int listen_fd, int clientfd) {
    if (clientfd < 0) return;
    submit_accept(listen_fd);
    if (clientfd >= CONNECTION_SIZE) { close(clientfd); return; }

    proactor_conn_list[clientfd].fd = clientfd;
    proactor_conn_list[clientfd].rcapacity = INIT_BUFFER_SIZE;
    proactor_conn_list[clientfd].rbuffer = (char*)kvs_malloc(INIT_BUFFER_SIZE);
    proactor_conn_list[clientfd].wcapacity = INIT_BUFFER_SIZE;
    proactor_conn_list[clientfd].wbuffer = (char*)kvs_malloc(INIT_BUFFER_SIZE);
    proactor_conn_list[clientfd].rlength = 0;
    proactor_conn_list[clientfd].wlength = 0;
    proactor_conn_list[clientfd].role = CONN_CLIENT;

    if (!proactor_conn_list[clientfd].rbuffer || !proactor_conn_list[clientfd].wbuffer) {
        proactor_close_and_free_connection(clientfd); return;
    }
    submit_recv(clientfd);
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
    if (bind(sockfd, (struct sockaddr*)&servaddr, sizeof(servaddr)) < 0) { close(sockfd); return -1; }
    if (listen(sockfd, 10) < 0) { close(sockfd); return -1; }
    return sockfd;
}

int proactor_start(unsigned short port, stream_handler_t handler) {
    g_stream_handler = handler;
    if (io_uring_queue_init(256, &ring, 0) < 0) return -1;

    int listen_fd = init_listen_socket(port);
    if (listen_fd < 0) return -1;

    proactor_conn_list[listen_fd].fd = listen_fd;
    proactor_conn_list[listen_fd].clientlen = sizeof(struct sockaddr_in);
    submit_accept(listen_fd);
    gettimeofday(&begin, NULL);

#if ENABLE_REPLICATION_SLAVE
    const char *master_ip = "192.168.92.128";
    unsigned short master_port = 2000;
    int master_fd = repl_connect_to_master(master_ip, master_port);
    if (master_fd < 0) {
        fprintf(stderr, "[Proactor Error] Slave failed to establish replication link.\n");
    }
#endif

    struct io_uring_cqe *cqe;
    while (1) {
        io_uring_submit_and_wait(&ring, 1);
        unsigned head;
        int count = 0;
        io_uring_for_each_cqe(&ring, head, cqe) {
            count++;
            io_ctx_t *ctx = (io_ctx_t *)io_uring_cqe_get_data(cqe);
            if (!ctx) continue;
            int res = cqe->res;
            int fd = ctx->fd;
            switch (ctx->type) {
                case OP_ACCEPT: on_accept_completed(fd, res); break;
                case OP_RECV:   on_recv_completed(fd, res);   break;
                case OP_SEND:   on_send_completed(fd, res);   break;
                default: break;
            }
        }
        io_uring_cq_advance(&ring, count);
    }
    return 0;
}

struct conn* proactor_host_slave_connection(int fd, char *wbuf, int wcap, int wlen) {
    if (fd < 0 || fd >= CONNECTION_SIZE) return NULL;

    struct conn *c = &proactor_conn_list[fd];
    c->fd = fd;
    c->role = CONN_MASTER;
    c->rbuffer = (char *)kvs_malloc(4096);
    if (!c->rbuffer) return NULL;
    c->rcapacity = 4096;
    c->rlength = 0;
    c->wbuffer = wbuf;
    c->wcapacity = wcap;
    c->wlength = wlen;
    submit_recv(fd);
    return c;
}