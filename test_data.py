#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
KVStore 全命令压测客户端
用法：
  python3 bench_kvstore.py --mode all --count 10000
  python3 bench_kvstore.py --mode getctx --count 100000 --concurrency 10
  python3 bench_kvstore.py --mode keep --count 100
"""

import socket
import time
import argparse
import statistics
import threading


# ============ RESP 编解码 ============

def encode_command(*args):
    parts = [f"*{len(args)}\r\n".encode()]
    for arg in args:
        if isinstance(arg, str):
            arg = arg.encode('utf-8')
        parts.append(f"${len(arg)}\r\n".encode())
        parts.append(arg)
        parts.append(b"\r\n")
    return b"".join(parts)


def read_response(sock):
    buf = b""
    while not buf.endswith(b"\r\n"):
        chunk = sock.recv(1)
        if not chunk:
            return None
        buf += chunk

    line = buf.strip()
    if not line:
        return None

    prefix = line[:1]

    if prefix in (b"+", b"-"):
        return line[1:]

    if prefix == b"$":
        try:
            length = int(line[1:])
        except ValueError:
            return None
        if length < 0:
            return None
        data = b""
        while len(data) < length + 2:
            chunk = sock.recv(length + 2 - len(data))
            if not chunk:
                return None
            data += chunk
        return data[:length]

    if prefix == b"*":
        try:
            count = int(line[1:])
        except ValueError:
            return None
        for _ in range(count):
            read_response(sock)
        return line

    return line


def send_and_wait(sock, *args):
    cmd = encode_command(*args)
    sock.sendall(cmd)
    return read_response(sock)


def percentile(data, p):
    if not data:
        return 0
    s = sorted(data)
    k = (len(s) - 1) * p / 100
    f = int(k)
    c = min(f + 1, len(s) - 1)
    return s[f] + (s[c] - s[f]) * (k - f)


# ============ 命令生成器 ============

def make_command(mode, worker_id, i):
    uid = f"bench_{mode}_{worker_id}_{i}"

    if mode == "keep":
        return ("KEEP", f"问题_{worker_id}_{i}", f"答案_{worker_id}_{i}")
    if mode == "match":
        return ("MATCH", f"问题_{worker_id}_{i}")
    if mode == "setctx":
        return ("SETCTX", uid, f'{{"id":"{uid}","data":"bench"}}', "1800")
    if mode == "getctx":
        return ("GETCTX", f"bench_setctx_0_{i % 1000}")
    if mode == "setrec":
        return ("SETREC", uid, f'{{"id":"{uid}","data":"bench"}}')
    if mode == "getrec":
        return ("GETREC", f"bench_setrec_0_{i % 1000}")
    if mode == "setidx":
        return ("SETIDX", "bench_keyword", uid)
    if mode == "getidx":
        return ("GETIDX", "bench_keyword")
    if mode == "zadd":
        return ("ZADD", str(int(time.time() * 1000) + i), uid)
    if mode == "zrange":
        return ("ZRANGE", "0", "9")
    raise ValueError(f"未知模式: {mode}")


# ============ 数据准备（读命令前置写入） ============

def prepare_data(host, port, mode, prep_count=1000):
    if mode not in ("getctx", "getrec", "getidx", "zrange"):
        return

    if mode == "getctx":
        prep_mode = "setctx"
    elif mode == "getrec":
        prep_mode = "setrec"
    elif mode == "getidx":
        prep_mode = "setidx"
    elif mode == "zrange":
        prep_mode = "zadd"
    else:
        return

    print(f"  准备数据中（{prep_mode} x {prep_count}）...")

    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.connect((host, port))

    for i in range(prep_count):
        cmd = make_command(prep_mode, 0, i)
        send_and_wait(sock, *cmd)

    sock.close()
    print(f"  准备完成\n")


# ============ 工作线程 ============

def worker(host, port, mode, count, results, worker_id):
    latencies = []

    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.connect((host, port))

    for i in range(count):
        cmd = make_command(mode, worker_id, i)

        t0 = time.perf_counter()
        send_and_wait(sock, *cmd)
        t1 = time.perf_counter()

        latencies.append((t1 - t0) * 1000)

    sock.close()
    results[worker_id] = latencies


# ============ 主测试 ============

def run_bench(host, port, mode, count, concurrency):
    print(f"\n{'='*60}")
    print(f"压测模式: {mode.upper()}")
    print(f"总次数: {count}，并发: {concurrency}")
    print(f"目标: {host}:{port}")
    print(f"{'='*60}\n")

    prepare_data(host, port, mode, prep_count=min(count, 1000))

    per_thread = count // concurrency
    remainder = count % concurrency

    results = {}
    threads = []

    start = time.perf_counter()

    for tid in range(concurrency):
        n = per_thread + (1 if tid < remainder else 0)
        t = threading.Thread(
            target=worker,
            args=(host, port, mode, n, results, tid)
        )
        threads.append(t)
        t.start()

    for t in threads:
        t.join()

    end = time.perf_counter()

    all_latencies = []
    for tid in results:
        all_latencies.extend(results[tid])

    total_time = end - start
    qps = len(all_latencies) / total_time

    print(f"\n{'='*60}")
    print(f"压测结果 ({mode.upper()})")
    print(f"{'='*60}")
    print(f"总请求数:     {len(all_latencies)}")
    print(f"总耗时:       {total_time:.2f} 秒")
    print(f"QPS:          {qps:.2f}")
    print(f"平均延迟:     {statistics.mean(all_latencies):.2f} ms")
    print(f"P50 延迟:     {percentile(all_latencies, 50):.2f} ms")
    print(f"P95 延迟:     {percentile(all_latencies, 95):.2f} ms")
    print(f"P99 延迟:     {percentile(all_latencies, 99):.2f} ms")
    print(f"最大延迟:     {max(all_latencies):.2f} ms")
    print(f"{'='*60}\n")

    return qps


# ============ 入口 ============

# ★ all 不再包含 keep 和 match
ALL_MODES = [
    "setctx", "getctx",
    "setrec", "getrec",
    "setidx", "getidx",
    "zadd", "zrange",
]

WRITE_MODES = ["setctx", "setrec", "setidx", "zadd"]
READ_MODES = ["getctx", "getrec", "getidx", "zrange"]

# 单独支持的模式（用于 --mode keep / --mode match）
SINGLE_MODES = ["keep", "match"]


def main():
    parser = argparse.ArgumentParser(description="KVStore 全命令压测")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=2000)
    parser.add_argument("--mode", default="all",
                        help="模式：all/write/read/keep/match/setctx/getctx/setrec/getrec/setidx/getidx/zadd/zrange")
    parser.add_argument("--count", type=int, default=1000,
                        help="每个模式的总请求数")
    parser.add_argument("--concurrency", type=int, default=1)
    args = parser.parse_args()

    if args.mode == "all":
        modes = ALL_MODES
    elif args.mode == "write":
        modes = WRITE_MODES
    elif args.mode == "read":
        modes = READ_MODES
    elif args.mode in SINGLE_MODES:
        # 单独测 keep / match
        modes = [args.mode]
    else:
        if args.mode not in (ALL_MODES + WRITE_MODES + READ_MODES + SINGLE_MODES):
            print(f"未知模式: {args.mode}")
            print(f"支持: all, write, read, keep, match, {'/'.join(ALL_MODES)}")
            return
        modes = [args.mode]

    summary = {}

    for mode in modes:
        qps = run_bench(args.host, args.port, mode,
                        args.count, args.concurrency)
        summary[mode] = qps

    print(f"\n{'='*60}")
    print(f"汇总（每个模式 {args.count} 次，并发 {args.concurrency}）")
    print(f"{'='*60}")
    print(f"{'模式':<12} {'QPS':>12}")
    print(f"{'-'*60}")
    for mode, qps in summary.items():
        print(f"{mode:<12} {qps:>12.2f}")
    print(f"{'='*60}\n")


if __name__ == "__main__":
    main()