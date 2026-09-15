#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <arpa/inet.h>
#include <sys/time.h>
#include <sys/socket.h>
#include <stdint.h>

#define SERVER_IP   "127.0.0.1"
#define SERVER_PORT 2000

static double now_sec(void) {
    struct timeval tv;
    gettimeofday(&tv, NULL);
    return tv.tv_sec + tv.tv_usec / 1e6;
}

static int connect_server(void) {
    int sock = socket(AF_INET, SOCK_STREAM, 0);
    if (sock < 0) return -1;
    struct sockaddr_in addr;
    addr.sin_family = AF_INET;
    addr.sin_port   = htons(SERVER_PORT);
    inet_pton(AF_INET, SERVER_IP, &addr.sin_addr);
    if (connect(sock, (struct sockaddr *)&addr, sizeof(addr)) < 0) {
        close(sock);
        return -1;
    }
    return sock;
}

static int send_all(int sock, const char *buf, int len) {
    int t = 0;
    while (t < len) {
        int n = send(sock, buf + t, len - t, 0);
        if (n <= 0) return -1;
        t += n;
    }
    return t;
}

static int recv_resp(int sock, char *buf, int buflen) {
    int total = 0;
    while (total < buflen - 1) {
        int n = recv(sock, buf + total, 1, 0);
        if (n <= 0) return -1;
        total += n;
        if (total >= 2 && buf[total - 2] == '\r' && buf[total - 1] == '\n')
            break;
    }
    if (total < 2) return total;

    if (buf[0] == '$') {
        int len = atoi(buf + 1);
        if (len >= 0) {
            int want = total + len + 2;
            if (want > buflen - 1) return -1;
            while (total < want) {
                int n = recv(sock, buf + total, want - total, 0);
                if (n <= 0) return -1;
                total += n;
            }
        }
    }
    buf[total] = '\0';
    return total;
}

static int build_hset(char *buf, const char *key, const char *val, int ttl) {
    if (ttl > 0) {
        char t[16];
        sprintf(t, "%d", ttl);
        return sprintf(buf,
            "*5\r\n$4\r\nHSET\r\n$%zu\r\n%s\r\n$%zu\r\n%s\r\n$2\r\nEX\r\n$%zu\r\n%s\r\n",
            strlen(key), key, strlen(val), val, strlen(t), t);
    } else {
        return sprintf(buf,
            "*3\r\n$4\r\nHSET\r\n$%zu\r\n%s\r\n$%zu\r\n%s\r\n",
            strlen(key), key, strlen(val), val);
    }
}

static int build_hget(char *buf, const char *key) {
    return sprintf(buf,
        "*2\r\n$4\r\nHGET\r\n$%zu\r\n%s\r\n",
        strlen(key), key);
}

static int hget_exists(int sock, const char *key) {
    char sb[256];
    char rb[4096];
    int sl = build_hget(sb, key);
    if (send_all(sock, sb, sl) < 0) return -1;
    int rl = recv_resp(sock, rb, sizeof(rb));
    if (rl <= 0) return -1;
    if (rb[0] == '-') return -1;
    if (rb[0] == '$') {
        if (strncmp(rb, "$-1", 3) == 0) return 0;
        return 1;
    }
    return -1;
}

/* 每条记录：HSET + 立即 HGET 验证存在
 * QPS 按 HSET+HGET 两条一起算
 * immediate_fail 出参：立即验证不存在的条数 */
static double bench_hset_with_verify(int sock, const char *prefix,
                                     long total, int ttl_sec,
                                     long *immediate_fail) {
    char sb[512];
    char rb[512];
    char key[64];
    char val[64];

    long fail = 0;
    double t0 = now_sec();

    for (long i = 0; i < total; i++) {
        sprintf(key, "%s_%08ld", prefix, i);
        sprintf(val, "v_%08ld", i);

        /* HSET */
        int sl = build_hset(sb, key, val, ttl_sec);
        if (send_all(sock, sb, sl) < 0) {
            fprintf(stderr, "[FATAL] HSET send failed at i=%ld\n", i);
            return -1;
        }
        int rl = recv_resp(sock, rb, sizeof(rb));
        if (rl <= 0) {
            fprintf(stderr, "[FATAL] HSET recv failed at i=%ld\n", i);
            return -1;
        }

        /* 立即 HGET 验证存在 */
        int e = hget_exists(sock, key);
        if (e < 0) {
            fprintf(stderr, "[FATAL] HGET err at i=%ld\n", i);
            return -1;
        }
        if (e == 0) {
            if (fail < 5)
                printf("  [MISS] just-inserted key not found: %s\n", key);
            fail++;
        }

        if (i > 0 && i % 100000 == 0) {
            fprintf(stderr, "  [%s] progress %ld/%ld (%.2fs)\n",
                    prefix, i, total, now_sec() - t0);
        }
    }

    double dt = now_sec() - t0;
    double total_ops = (double)total * 2.0;
    double qps = total_ops / dt;

    printf("  [%s] Records=%ld (HSET+HGET=%ld ops), elapsed=%.3fs\n",
           prefix, total, total * 2, dt);
    printf("  [%s] QPS = %.0f ops/s (%.0f records/s)\n",
           prefix, qps, (double)total / dt);
    printf("  [%s] Immediate-verify fail = %ld\n", prefix, fail);

    if (immediate_fail) *immediate_fail = fail;
    return qps;
}

/* 全量验证：全部应过期。不计入 QPS，仅做正确性检查 */
static int verify_all_expired(int sock, const char *prefix, long total) {
    int fail = 0;
    char key[64];
    for (long i = 0; i < total; i++) {
        sprintf(key, "%s_%08ld", prefix, i);
        int e = hget_exists(sock, key);
        if (e < 0) { fprintf(stderr, "[FATAL] HGET err i=%ld\n", i); return -1; }
        if (e == 1) {
            if (fail < 5) printf("  [LEAK] expected EXPIRED: %s\n", key);
            fail++;
        }
    }
    if (fail == 0) printf("  Verify OK: all %ld keys EXPIRED\n", total);
    else           printf("  Verify FAIL: %d / %ld still exist\n", fail, total);
    return fail;
}

int main(int argc, char *argv[]) {
    setvbuf(stdout, NULL, _IONBF, 0);

    long total    = 100000;
    int  ttl_sec  = 30;
    int  wait_sec = 35;

    if (argc >= 2) total    = atol(argv[1]);
    if (argc >= 3) ttl_sec  = atoi(argv[2]);
    if (argc >= 4) wait_sec = atoi(argv[3]);

    printf("====================================================\n");
    printf("  HSET + immediate HGET verify QPS benchmark\n");
    printf("  Records        : %ld\n", total);
    printf("  Engine         : Hash (HSET / HGET)\n");
    printf("  Per record     : HSET + HGET (verify exists)\n");
    printf("  TTL for round2 : %d s\n", ttl_sec);
    printf("  Wait then check: %d s\n", wait_sec);
    printf("  Server         : %s:%d\n", SERVER_IP, SERVER_PORT);
    printf("====================================================\n");

    /* Round 1: 无 TTL */
    printf("\n>>> Round 1: HSET without TTL (with per-record HGET)\n");
    int s1 = connect_server();
    if (s1 < 0) { fprintf(stderr, "[FATAL] connect failed\n"); return 1; }

    long fail1 = 0;
    double qps1 = bench_hset_with_verify(s1, "nottl", total, 0, &fail1);
    close(s1);
    if (qps1 < 0) return 1;
    if (fail1 > 0) {
        printf("\n[FAIL] Round 1: %ld keys missing right after HSET\n", fail1);
        return 1;
    }

    /* Round 2: 带 TTL */
    printf("\n>>> Round 2: HSET with TTL=%d s (with per-record HGET)\n", ttl_sec);
    int s2 = connect_server();
    if (s2 < 0) { fprintf(stderr, "[FATAL] connect failed\n"); return 1; }

    long fail2 = 0;
    double qps2 = bench_hset_with_verify(s2, "ttl", total, ttl_sec, &fail2);
    if (qps2 < 0) { close(s2); return 1; }
    if (fail2 > 0) {
        printf("\n[FAIL] Round 2: %ld keys missing right after HSET\n", fail2);
        close(s2);
        return 1;
    }

    /* sleep 到超时，全量验证已删（不计入 QPS） */
    printf("  Sleeping %d s for TTL expiration...\n", wait_sec);
    sleep(wait_sec);

    printf("  Verifying all keys EXPIRED (not counted in QPS)...\n");
    int f2 = verify_all_expired(s2, "ttl", total);
    close(s2);
    if (f2 < 0) return 1;
    if (f2 > 0) { printf("\n[FAIL] Round 2 expire verify failed\n"); return 1; }

    /* Summary */
    printf("\n====================================================\n");
    printf("  Summary (HSET+HGET counted together)\n");
    printf("----------------------------------------------------\n");
    printf("  Without TTL : %.0f ops/s\n", qps1);
    printf("  With TTL    : %.0f ops/s\n", qps2);
    if (qps1 > 0 && qps2 > 0) {
        double ratio = qps2 / qps1;
        printf("  Ratio       : %.4f  (%.2f%% of baseline)\n",
               ratio, ratio * 100);
        printf("  TTL overhead: %.1f%%\n", (1.0 - ratio) * 100.0);
    }
    printf("====================================================\n");

    return 0;
}