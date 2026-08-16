#!/usr/bin/env bash

# ==========================================
# Redis vs KVstore 三大引擎独立基准测试脚本
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

restart_redis() {
    echo "正在重启 Redis 服务并关闭 AOF..."
    sudo systemctl restart redis-server >/dev/null 2>&1 || sudo service redis-server restart >/dev/null 2>&1
    sleep 2
    redis-cli -p $REDIS_PORT CONFIG SET appendonly no >/dev/null 2>&1
    sleep 1
    if ! redis-cli -p $REDIS_PORT PING 2>/dev/null | grep -q "PONG"; then
        echo "错误: Redis 未正常启动！"
        exit 1
    fi
}

trap kill_server EXIT

echo "=================================================="
echo "    开始测试 Redis vs KVstore ($TEST_N 条)        "
echo "=================================================="

for p in "${PIPELINES[@]}"; do
    echo "--------------------------------------------------"
    echo ">>> 正在测试 Pipeline -P $p ..."

    # Redis PING
    restart_redis
    echo -n "  [1/6] Redis PING ... "
    qps_redis_ping=$(parse_qps "redis-benchmark -p $REDIS_PORT -n $TEST_N -c 1 -P $p -q PING")
    RESULTS["REDIS_PING_$p"]=$qps_redis_ping
    echo "$qps_redis_ping QPS"

    # KVstore PING
    echo -n "  [2/6] KVstore PING ... "
    sudo sh -c 'echo 3 > /proc/sys/vm/drop_caches'
    start_kvstore
    qps_kv_ping=$(parse_qps "redis-benchmark -p $KV_PORT -n $TEST_N -c 1 -P $p -q PING")
    RESULTS["KV_PING_$p"]=$qps_kv_ping
    echo "$qps_kv_ping QPS"
    kill_server

    # Redis SET
    restart_redis
    echo -n "  [3/6] Redis SET ... "
    qps_redis_set=$(parse_qps "redis-benchmark -p $REDIS_PORT -n $TEST_N -r 100000000 -c 1 -P $p -q SET key:__rand_int__ value:__rand_int__")
    RESULTS["REDIS_SET_$p"]=$qps_redis_set
    echo "$qps_redis_set QPS"

    # KVstore RBTree
    echo -n "  [4/6] KVstore RBTree (RSET) ... "
    sudo sh -c 'echo 3 > /proc/sys/vm/drop_caches'
    start_kvstore
    qps_rbt=$(parse_qps "redis-benchmark -p $KV_PORT -n $TEST_N -r 100000000 -c 1 -P $p -q RSET key:__rand_int__ value:__rand_int__")
    RESULTS["KV_RBT_$p"]=$qps_rbt
    echo "$qps_rbt QPS"
    kill_server

    # KVstore Hash
    echo -n "  [5/6] KVstore Hash (HSET) ... "
    sudo sh -c 'echo 3 > /proc/sys/vm/drop_caches'
    start_kvstore
    qps_hsh=$(parse_qps "redis-benchmark -p $KV_PORT -n $TEST_N -r 100000000 -c 1 -P $p -q HSET key:__rand_int__ value:__rand_int__")
    RESULTS["KV_HSH_$p"]=$qps_hsh
    echo "$qps_hsh QPS"
    kill_server

    # KVstore SkipList
    echo -n "  [6/6] KVstore SkipList (SSET) ... "
    sudo sh -c 'echo 3 > /proc/sys/vm/drop_caches'
    start_kvstore
    qps_skl=$(parse_qps "redis-benchmark -p $KV_PORT -n $TEST_N -r 100000000 -c 1 -P $p -q SSET key:__rand_int__ value:__rand_int__")
    RESULTS["KV_SKL_$p"]=$qps_skl
    echo "$qps_skl QPS"
    kill_server
done

OUTPUT_FILE="batchcommand_result.txt"

{
    echo ""
    echo "======================================================================================================================================"
    echo "                                Redis vs KVstore 三大引擎独立测试 ($TEST_N 条)                                                         "
    echo "======================================================================================================================================"
    printf "%-10s | %-14s | %-14s | %-14s | %-15s | %-14s | %-16s\n" \
        "Pipeline" "Redis (PING)" "KV (PING)" "Redis (SET)" "KV (RBTree)" "KV (Hash)" "KV (SkipList)"
    echo "--------------------------------------------------------------------------------------------------------------------------------------"
    for p in "${PIPELINES[@]}"; do
        printf -- "-P %-7d | %-14s | %-14s | %-14s | %-15s | %-14s | %-16s\n" \
            "$p" \
            "${RESULTS["REDIS_PING_$p"]}" \
            "${RESULTS["KV_PING_$p"]}" \
            "${RESULTS["REDIS_SET_$p"]}" \
            "${RESULTS["KV_RBT_$p"]}" \
            "${RESULTS["KV_HSH_$p"]}" \
            "${RESULTS["KV_SKL_$p"]}"
    done
    echo "======================================================================================================================================"
} | tee "$OUTPUT_FILE"

echo ""
echo "测试完成！结果已保存至: $OUTPUT_FILE"