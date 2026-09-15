# =========================
# 编译阶段
# =========================
FROM quay.io/0voice/golang:1.22 AS builder

WORKDIR /src

# 一次性安装 KVstore 全部编译依赖
RUN apt-get update && \
    apt-get install -y \
    gcc \
    g++ \
    make \
    clang \
    llvm \
    linux-libc-dev \
    libbpf-dev \
    libelf-dev \
    zlib1g-dev \
    libibverbs-dev \
    librdmacm-dev \
    liburing-dev \
    libcjson-dev \
    libcurl4-openssl-dev \
    pkg-config \
    bpftool \
    iproute2 \
    && rm -rf /var/lib/apt/lists/*

# 复制 KVstore 源码
COPY . .

# 编译
RUN make clean && make


# =========================
# 运行阶段
# =========================
FROM quay.io/0voice/debian:stable-slim

WORKDIR /app

# 安装运行时依赖
RUN apt-get update && \
    apt-get install -y \
    libcjson1 \
    libcurl4 \
    libibverbs1 \
    librdmacm1 \
    liburing2 \
    libbpf1 \
    libelf1 \
    zlib1g \
    iproute2 \
    bpftool \
    && rm -rf /var/lib/apt/lists/*

# 复制 KVstore
COPY --from=builder /src/server ./server

# eBPF relay
COPY --from=builder /src/ebpf_relay ./ebpf_relay

# eBPF 内核程序
COPY --from=builder /src/sync_filter.bpf.o ./sync_filter.bpf.o

# 配置文件
COPY ./config.conf ./config.conf

EXPOSE 2000

ENTRYPOINT ["./server"]

CMD ["config.conf"]