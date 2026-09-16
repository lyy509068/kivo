#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
KVStore 压测客户端
用法：
  python3 bench_kvstore.py --mode keep --count 1000
  python3 bench_kvstore.py --mode match --count 1000
  python3 bench_kvstore.py --mode both --count 1000
"""

import socket
import time
import argparse
import statistics
import threading
from collections import defaultdict


def encode_command(*args):
    """把命令编码成 RESP 格式"""
    parts = [f"*{len(args)}\r\n".encode()]
    for arg in args:
        if isinstance(arg, str):
            arg = arg.encode('utf-8')
        parts.append(f"${len(arg)}\r\n".encode())
        parts.append(arg)
        parts.append(b"\r\n")
    return b"".join(parts)


def read_response(sock):
    """读取一个 RESP 响应，返回原始字节"""
    buf = b""
    # 读第一行
    while not buf.endswith(b"\r\n"):
        chunk = sock.recv(1)
        if not chunk:
            return None
        buf += chunk

    line = buf.strip()
    if not line:
        return None

    prefix = line[:1]

    # 简单字符串 +OK / 错误 -ERR
    if prefix in (b"+", b"-"):
        return line[1:]

    # Bulk string $<len>
    if prefix == b"$":
        try:
            length = int(line[1:])
        except ValueError:
            return None
        if length < 0:
            return None
        data = b""
        while len(data) < length + 2:  # 加 \r\n
            chunk = sock.recv(length + 2 - len(data))
            if not chunk:
                return None
            data += chunk
        return data[:length]

    # 数组 *
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
    """发送命令并等待响应"""
    cmd = encode_command(*args)
    sock.sendall(cmd)
    return read_response(sock)


def percentile(data, p):
    """计算百分位"""
    if not data:
        return 0
    data_sorted = sorted(data)
    k = (len(data_sorted) - 1) * p / 100
    f = int(k)
    c = min(f + 1, len(data_sorted) - 1)
    return data_sorted[f] + (data_sorted[c] - data_sorted[f]) * (k - f)


def worker(host, port, mode, count, results, worker_id):
    """单个线程跑 N 次命令"""
    latencies = []

    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.connect((host, port))

    for i in range(count):
        # 构造命令
        question = f"压测问题_{worker_id}_{i}"
        answer = f"压测答案_{worker_id}_{i}"

        t0 = time.perf_counter()

        if mode == "keep":
            send_and_wait(sock, "KEEP", question, answer)
        elif mode == "match":
            send_and_wait(sock, "MATCH", question)

        t1 = time.perf_counter()
        latencies.append((t1 - t0) * 1000)  # ms

        # 每 5000 条打印进度
        if (i + 1) % 5000 == 0:
            print(f"  [线程 {worker_id}] 已完成 {i+1}/{count}")

    sock.close()
    results[worker_id] = latencies


def run_bench(host, port, mode, count, concurrency):
    print(f"\n{'='*60}")
    print(f"压测模式: {mode.upper()}")
    print(f"总次数: {count}，并发: {concurrency}")
    print(f"目标: {host}:{port}")
    print(f"{'='*60}\n")

    # 先预热
    print("预热中...")
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.connect((host, port))
    for i in range(10):
        if mode == "keep":
            send_and_wait(sock, "KEEP", "预热问题", "预热答案")
        else:
            send_and_wait(sock, "MATCH", "预热问题")
    sock.close()
    print("预热完成\n")

    # 分配任务
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

    # 汇总
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
    print(f"P90 延迟:     {percentile(all_latencies, 90):.2f} ms")
    print(f"P95 延迟:     {percentile(all_latencies, 95):.2f} ms")
    print(f"P99 延迟:     {percentile(all_latencies, 99):.2f} ms")
    print(f"最小延迟:     {min(all_latencies):.2f} ms")
    print(f"最大延迟:     {max(all_latencies):.2f} ms")
    print(f"{'='*60}\n")

    return qps


def main():
    parser = argparse.ArgumentParser(description="KVStore 压测客户端")
    parser.add_argument("--host", default="127.0.0.1", help="KVStore 地址")
    parser.add_argument("--port", type=int, default=2000, help="KVStore 端口")
    parser.add_argument("--mode", default="both",
                        choices=["keep", "match", "both"],
                        help="压测模式")
    parser.add_argument("--count", type=int, default=1000,
                        help="每个模式的总请求数")
    parser.add_argument("--concurrency", type=int, default=1,
                        help="并发线程数")
    args = parser.parse_args()

    if args.mode in ("keep", "both"):
        run_bench(args.host, args.port, "keep",
                  args.count, args.concurrency)

    if args.mode in ("match", "both"):
        run_bench(args.host, args.port, "match",
                  args.count, args.concurrency)


if __name__ == "__main__":
    main()