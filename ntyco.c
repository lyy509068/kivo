#define _GNU_SOURCE
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
#include <sys/syscall.h> 
#include "nty_coroutine.h" 
#include <errno.h>

#include "network.h"
#include "kvstore.h"
#include "resp.h"
#include "repl.h"
#include "rdma.h"
#include "expire.h"

#ifndef SPLICE_F_MOVE
#define SPLICE_F_MOVE 1
#endif

#define MAX_RBUFFER_SIZE (16 * 1024 * 1024) 

extern int g_slave_fd; 

static stream_handler_t g_stream_handler = NULL;

struct conn ntyco_conn_list[CONNECTION_SIZE] = {0};

void ntyco_client_co(void *arg);
void ntyco_server_co(void *arg);

void ntyco_close_and_free_connection(int fd) {
    if (fd < 0 || fd >= CONNECTION_SIZE) return;
    close(fd);
    if (ntyco_conn_list[fd].rbuffer) kvs_free(ntyco_conn_list[fd].rbuffer);
    if (ntyco_conn_list[fd].wbuffer) kvs_free(ntyco_conn_list[fd].wbuffer);
    memset(&ntyco_conn_list[fd], 0, sizeof(struct conn));
}

void ntyco_expire_co(void *arg) {
    while (1) {
        expire_process_deletes();
        nty_coroutine_sleep(1);
    }
}

void ntyco_persistence_co(void *arg) {
    while (1) {
        if (g_enable_persistence || g_enable_repl_master || g_enable_repl_slave) {
            kvs_persistence_flush_pending();
        }
        nty_coroutine_sleep(1);
    }
}

void ntyco_client_co(void *arg) {
    int fd = (int)(long)arg;
    struct conn *c = &ntyco_conn_list[fd];
    
    extern int g_enable_persistence, g_enable_repl_master, g_enable_repl_slave; 

    c->rcapacity = INIT_BUFFER_SIZE;
    c->rbuffer = (char*)kvs_malloc(INIT_BUFFER_SIZE);
    c->wcapacity = INIT_BUFFER_SIZE;
    c->wbuffer = (char*)kvs_malloc(INIT_BUFFER_SIZE);
    c->rlength = 0;
    c->wlength = 0;

    if (!c->rbuffer || !c->wbuffer) { 
        ntyco_close_and_free_connection(fd); 
        return; 
    }

    while (1) {
        if (c->rlength >= MAX_RBUFFER_SIZE) {
            nty_coroutine_sleep(0); 
            continue;
        }

        if (c->rcapacity - c->rlength < 65536 && c->rcapacity < MAX_RBUFFER_SIZE) { 
            int new_capacity = c->rcapacity * 2;
            if (new_capacity < 65536) new_capacity = 65536;
            char *new_buf = (char *)kvs_realloc(c->rbuffer, new_capacity);
            if (!new_buf) {
                ntyco_close_and_free_connection(fd);
                return;
            }
            c->rbuffer = new_buf;
            c->rcapacity = new_capacity;
        }

        int count = recv(fd, c->rbuffer + c->rlength, c->rcapacity - c->rlength, 0);
        
        if (count < 0) {
            if (errno == EAGAIN || errno == EWOULDBLOCK) {
                nty_coroutine_sleep(1);
                continue;
            }
            break;
        } else if (count == 0) {
            break;
        }
        
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
            
            if (status == 1 || parsed_bytes == 0) break;
            if (status < 0) { 
                ntyco_close_and_free_connection(fd); 
                return; 
            }
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

        nty_coroutine_sleep(0); 
    }

    if (g_enable_persistence || g_enable_repl_master || g_enable_repl_slave) {
        kvs_persistence_force_flush();
    }

    ntyco_close_and_free_connection(fd);
}

void ntyco_master_repl_send_co(void *arg) {
    while (1) {
        if (g_slave_fd > 0 && g_repl_backlog_count > 0) {
            int max_per_loop = 100;
            int sent_count = 0;
            
            while (g_repl_backlog_count > 0 && sent_count < max_per_loop) {
                repl_backlog_node_t *node = &g_repl_backlog[g_repl_backlog_head];
                ssize_t sent = send(g_slave_fd, node->data, node->len, MSG_DONTWAIT);

                if (sent > 0) {
                    if ((size_t)sent == node->len) {
                        kvs_free(node->data);
                        g_repl_backlog_head = (g_repl_backlog_head + 1) % REPL_BACKLOG_MAX;
                        g_repl_backlog_count--;
                        sent_count++;
                    } else {
                        memmove(node->data, node->data + sent, node->len - sent);
                        node->len -= sent;
                        break;
                    }
                } else if (sent < 0) {
                    if (errno == EAGAIN || errno == EWOULDBLOCK) {
                        break;
                    } else {
                        ntyco_close_and_free_connection(g_slave_fd);
                        g_slave_fd = -1; 
                        break;
                    }
                }
            }
            nty_coroutine_sleep(0);              
        } else {
            nty_coroutine_sleep(10);
        }
    }
}


void ntyco_server_co(void *arg) {
    int listen_fd = (int)(long)arg;
    
    while (1) {
        struct sockaddr_in clientaddr;
        socklen_t len = sizeof(clientaddr);
        int clientfd = accept(listen_fd, (struct sockaddr*)&clientaddr, &len);
        
        if (clientfd < 0) { 
            if (errno != EAGAIN && errno != EWOULDBLOCK) {
                perror("accept failed");
            }
            nty_coroutine_sleep(10);
            continue; 
        }
        if (clientfd >= CONNECTION_SIZE) { 
            close(clientfd); 
            continue; 
        }

        ntyco_conn_list[clientfd].fd = clientfd;
        ntyco_conn_list[clientfd].role = CONN_CLIENT;

        nty_coroutine *client_co = NULL;
        if (nty_coroutine_create(&client_co, ntyco_client_co, (void*)(long)clientfd) < 0) {
            close(clientfd);
            continue;
        }
        
        nty_coroutine_sleep(0);
    }
}

static int init_listen_socket(unsigned short port) {
    int sockfd = socket(AF_INET, SOCK_STREAM, 0);
    if (sockfd < 0) return -1;

    int flags = fcntl(sockfd, F_GETFL, 0);
    if (flags < 0 || fcntl(sockfd, F_SETFL, flags | O_NONBLOCK) < 0) {
        close(sockfd);
        return -1;
    }

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
    const char *master_ip = "192.168.88.128";
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
        printf("[Slave] SYNC sent.\n");

        pthread_t tid;
        int *pfd = kvs_malloc(sizeof(int));
        *pfd = fd;
        pthread_create(&tid, NULL, tcp_sendfile_recv_thread, pfd);
        pthread_join(tid, NULL);

        ntyco_conn_list[fd].fd = fd;
        ntyco_conn_list[fd].role = CONN_MASTER;
        ntyco_conn_list[fd].rcapacity = 4*1024*1024;
        ntyco_conn_list[fd].rbuffer = (char *)kvs_malloc(4*1024*1024);
        ntyco_conn_list[fd].rlength = 0;
        ntyco_conn_list[fd].wcapacity = 0;
        ntyco_conn_list[fd].wbuffer = NULL;
        ntyco_conn_list[fd].wlength = 0;

        nty_coroutine *co = NULL;
        nty_coroutine_create(&co, ntyco_client_co, (void*)(long)fd);
    } else if(g_use_rdma_sync) {
        repl_connect_to_master(master_ip, master_port);
    }
}

int ntyco_start(unsigned short port, stream_handler_t handler) {
    g_stream_handler = handler;
    int listen_fd = init_listen_socket(port);
    if (listen_fd < 0) return -1;

    ntyco_conn_list[listen_fd].fd = listen_fd;

    nty_coroutine *server_co = NULL;
    if (nty_coroutine_create(&server_co, ntyco_server_co, (void*)(long)listen_fd) < 0) {
        close(listen_fd);
        return -1;
    }

    if (g_enable_ttl) {
        nty_coroutine *expire_co = NULL;
        nty_coroutine_create(&expire_co, ntyco_expire_co, NULL);
    }
    
    nty_coroutine *persistence_co = NULL;
    nty_coroutine_create(&persistence_co, ntyco_persistence_co, NULL);

    if (g_enable_repl_master) {
        nty_coroutine *send_co = NULL;
        nty_coroutine_create(&send_co, ntyco_master_repl_send_co, NULL);
    }

    if (g_enable_repl_slave) {
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
    if (!c->rbuffer) return NULL;
    c->rcapacity = 65536;
    c->rlength = 0;

    nty_coroutine *client_co = NULL;
    nty_coroutine_create(&client_co, ntyco_client_co, (void*)(long)fd);

    return c;
}