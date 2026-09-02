#!/usr/bin/env bash

# ==========================================
# Redis ZADD vs KVstore SSET 跳表引擎基准测试
# ==========================================

REDIS_PORT=6379
KV_PORT=2000
TEST_N=1000000
PIPELINES=(1 10 20 40 80 160)
KV_AOF_FILE="kvstore.aof"
declare -A RESULTS
SERVER_PID=""

parse_qps() {
    local cmd="$1"
    local output
    output=$(eval "$cmd" 2>&1)
    local qps
    qps=$(echo "$output" | grep -oP '\d+(\.\d+)?(?=\s+requests per second)' | head -n 1 | awk '{print int($1)}')
    if [[ -z "$qps" || "$qps" -eq 0 ]]; then
        echo "FAIL"
    else
        echo "$qps"
    fi
}

# ==========================================
# 彻底杀掉 KVstore 进程并释放端口
# ==========================================
kill_server() {
    echo "正在彻底清理 KVstore 进程和端口..."
    
    # 1. 杀掉记录的 PID
    if [[ -n "$SERVER_PID" ]] && kill -0 "$SERVER_PID" 2>/dev/null; then
        kill -9 "$SERVER_PID" 2>/dev/null
    fi

    # 2. 杀掉所有名为 server 的进程（避免残留）
    sudo pkill -9 -x server 2>/dev/null

    # 3. 释放被占用的端口（如果有）
    sudo fuser -k -9 ${KV_PORT}/tcp >/dev/null 2>&1

    # 4. 等待进程完全退出
    sleep 3

    # 5. 再次确认没有残留
    if pgrep -x server > /dev/null; then
        echo "警告：仍有 server 进程残留，强制再杀..."
        sudo pkill -9 -x server 2>/dev/null
        sleep 3
    fi

    # 6. 确认端口已释放
    if nc -z 127.0.0.1 $KV_PORT 2>/dev/null; then
        echo "警告：端口 $KV_PORT 仍被占用！"
        sleep 3
    fi
}

# ==========================================
# 启动 KVstore（确保干净环境）
# ==========================================
start_kvstore() {
    kill_server

    # 删除旧日志文件，确保引擎从空状态开始
    echo "删除旧 AOF 文件..."
    sudo rm -f "$KV_AOF_FILE"

    echo "正在启动 KVstore 服务器..."
    ./server config.conf > server.log 2>&1 &
    SERVER_PID=$!

    # 等待服务器监听端口
    sleep 3

    # 确认进程还活着
    if ! kill -0 "$SERVER_PID" 2>/dev/null; then
        echo "错误：KVstore 服务器启动失败，请查看 server.log"
        cat server.log
        exit 1
    fi
}

# ==========================================
# 重启 Redis 并清空数据
# ==========================================
restart_redis() {
    echo "正在重启 Redis 服务并清空数据..."
    sudo systemctl restart redis-server >/dev/null 2>&1 || sudo service redis-server restart >/dev/null 2>&1
    sleep 2
    
    # 清空所有数据
    redis-cli -p $REDIS_PORT FLUSHALL >/dev/null 2>&1
    
    # 关闭 AOF
    redis-cli -p $REDIS_PORT CONFIG SET appendonly no >/dev/null 2>&1
    sleep 1
    
    if ! redis-cli -p $REDIS_PORT PING 2>/dev/null | grep -q "PONG"; then
        echo "错误: Redis 未正常启动！"
        exit 1
    fi
}

trap kill_server EXIT

echo "=================================================="
echo "  开始测试 Redis ZADD vs KVstore SSET ($TEST_N 条)"
echo "=================================================="

for p in "${PIPELINES[@]}"; do
    echo "--------------------------------------------------"
    echo ">>> 正在测试 Pipeline -P $p ..."

    # Redis ZADD（使用不同的 zset key 避免数据累积）
    restart_redis
    echo -n "  [1/2] Redis ZADD (skiplist) ... "
    qps_redis_zadd=$(parse_qps "redis-benchmark -p $REDIS_PORT -n $TEST_N -r 100000000 -c 1 -P $p -q ZADD test_zset_$p __rand_int__ key:__rand_int__")
    RESULTS["REDIS_ZADD_$p"]=$qps_redis_zadd
    echo "$qps_redis_zadd QPS"
    
    # 显示当前 zset 大小
    zset_size=$(redis-cli -p $REDIS_PORT ZCARD test_zset_$p 2>/dev/null)
    echo "       Redis ZSet 大小: $zset_size"

    # KVstore SSET（跳表）
    echo -n "  [2/2] KVstore SSET (skiplist) ... "
    sudo sh -c 'echo 3 > /proc/sys/vm/drop_caches'
    start_kvstore
    qps_kv_sset=$(parse_qps "redis-benchmark -p $KV_PORT -n $TEST_N -r 100000000 -c 1 -P $p -q SSET key:__rand_int__ value:__rand_int__")
    RESULTS["KV_SSET_$p"]=$qps_kv_sset
    echo "$qps_kv_sset QPS"
    kill_server
    
    echo ""
done

# 输出结果表格
OUTPUT_FILE="skiplist_benchmark_result.txt"

{
    echo ""
    echo "======================================================================================"
    echo "          Redis ZADD vs KVstore SSET 跳表引擎测试 ($TEST_N 条)                        "
    echo "======================================================================================"
    printf "%-10s | %-20s | %-20s | %-15s\n" \
        "Pipeline" "Redis ZADD (QPS)" "KVstore SSET (QPS)" "性能比(KV/Redis)"
    echo "--------------------------------------------------------------------------------------"
    for p in "${PIPELINES[@]}"; do
        redis_qps="${RESULTS["REDIS_ZADD_$p"]}"
        kv_qps="${RESULTS["KV_SSET_$p"]}"
        
        # 计算性能比（如果都是数字）
        if [[ "$redis_qps" =~ ^[0-9]+$ ]] && [[ "$kv_qps" =~ ^[0-9]+$ ]]; then
            ratio=$(echo "scale=2; $kv_qps / $redis_qps * 100" | bc)
            ratio_str="${ratio}%"
        else
            ratio_str="N/A"
        fi
        
        printf -- "-P %-7d | %-20s | %-20s | %-15s\n" \
            "$p" \
            "$redis_qps" \
            "$kv_qps" \
            "$ratio_str"
    done
    echo "======================================================================================"
    echo ""
    echo "备注："
    echo "- Redis ZADD 使用 ZSet（skiplist + dict）"
    echo "- KVstore SSET 使用纯跳表"
    echo "- 每次测试前都会清空数据，确保从空状态开始"
    echo "- 测试数据量：$TEST_N 条"
} | tee "$OUTPUT_FILE"

echo ""
echo "测试完成！结果已保存至: $OUTPUT_FILE"