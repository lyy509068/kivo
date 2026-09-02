#!/bin/bash

# ================= 配置 =================
SERVER_PORT=2000
FLAME_GRAPH_DIR="../FlameGraph"

BENCH_CMD=(
    redis-benchmark
    -p "$SERVER_PORT"
    -t SSET
    -n 1000000
    -r 100000000
    -P 40
    -c 1
    -q
    SSET key:__rand_int__ value:__rand_int__
)

# ========================================

echo "======================================================================"
echo "KVstore CPU 火焰图测试"
echo "流程：启动 perf -> 启动 benchmark -> benchmark 结束 -> 停止 perf"
echo "======================================================================"

# 1. 清理
echo "[1/6] 清理旧性能采样文件..."

sudo rm -f perf.data perf.data.old out.perf out.folded kvstore.svg

# 2. 获取 server PID
echo "[2/6] 获取真正的 KVstore server PID..."

REAL_PID=$(pgrep -n -x server)

if [ -z "$REAL_PID" ]; then
    echo "❌ 没有找到 server"
    echo "请先启动：sudo ./server config.conf"
    exit 1
fi

echo "✅ server PID: $REAL_PID"

sudo ps -fp "$REAL_PID"

CMDLINE=$(tr '\0' ' ' < /proc/"$REAL_PID"/cmdline)
echo "CMDLINE: $CMDLINE"

if [[ "$CMDLINE" != *"./server config.conf"* ]]; then
    echo "❌ PID 验证失败"
    exit 1
fi

echo "✅ PID 验证通过"

# 3. 启动 perf
echo "[3/6] 开始 perf 采样..."

sudo perf record \
    -F 99 \
    -p "$REAL_PID" \
    -g \
    -o perf.data &

PERF_PID=$!

sleep 1

if ! sudo kill -0 "$REAL_PID" 2>/dev/null; then
    echo "❌ server 已经退出"
    exit 1
fi

echo "✅ perf 已启动"
echo "   perf PID: $PERF_PID"

# 4. 启动 benchmark
echo "[4/6] 开始 redis-benchmark..."

"${BENCH_CMD[@]}" &
BENCH_PID=$!

echo "   benchmark PID: $BENCH_PID"
echo "   正在压测，等待 benchmark 完成..."

# 5. 等 benchmark 完成
echo "[5/6] 等待 benchmark 结束..."

wait "$BENCH_PID"
BENCH_EXIT=$?

echo ""
echo "✅ benchmark 已结束"
echo "   benchmark exit code: $BENCH_EXIT"

# 给 perf 一点时间处理最后的数据
sleep 1

# 停止 perf
echo "▶ 停止 perf 采样..."

sudo kill -INT "$PERF_PID" 2>/dev/null || true

# 等待 perf 正常退出
wait "$PERF_PID" 2>/dev/null || true

echo "✅ perf 采样结束"

# 检查 perf.data
echo ""
echo "perf.data:"
ls -lh perf.data

# 6. 生成火焰图
echo "[6/6] 生成火焰图..."

echo "▶ 导出 out.perf..."

sudo perf script -i perf.data > out.perf

if [ ! -s out.perf ]; then
    echo "❌ out.perf 为空"
    echo "请检查 perf.data:"
    ls -lh perf.data
    exit 1
fi

echo "▶ 折叠调用栈..."

"$FLAME_GRAPH_DIR/stackcollapse-perf.pl" \
    out.perf > out.folded

echo "▶ 生成 SVG..."

"$FLAME_GRAPH_DIR/flamegraph.pl" \
    out.folded > kvstore.svg

if [ -s kvstore.svg ]; then
    echo ""
    echo "=========================================="
    echo "✅ 火焰图生成成功！"
    echo "=========================================="

    ls -lh perf.data out.perf out.folded kvstore.svg

    echo ""
    echo "火焰图："
    echo "    kvstore.svg"
else
    echo "❌ 火焰图生成失败"
    exit 1
fi