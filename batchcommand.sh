#!/usr/bin/env bash

# ==========================================
# Redis vs KVstore 三大引擎独立基准测试脚本
# ==========================================

REDIS_PORT=6379
KV_PORT=2000
# 提示：若虚拟机内存小于 4G，建议先将 TEST_N 设为 100000 (10w) 进行测试，防止触发 OOM
TEST_N=1000000  

# Pipeline 梯度数组
PIPELINES=(1 10 20 40 80 160)
declare -A RESULTS
SERVER_PID=""

# 格式化提取 QPS 值的辅助函数
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
# 辅助函数：精准杀掉 KVstore 服务器
# ==========================================
kill_server() {
    if [[ -n "$SERVER_PID" ]] && kill -0 "$SERVER_PID" 2>/dev/null; then
        kill -9 "$SERVER_PID" 2>/dev/null
    fi
    # 依靠端口精准清理残留进程，避免模糊 pkill 误伤
    sudo fuser -k -9 ${KV_PORT}/tcp >/dev/null 2>&1
    sleep 1
}

# ==========================================
# 辅助函数：重启 Redis 并关闭 AOF
# ==========================================
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

# ==========================================
# 辅助函数：启动 KVstore 服务器（带输出隔离与就绪等待）
# ==========================================
start_kvstore() {
    kill_server
    echo "正在启动 KVstore 服务器..."
    # 重定向 stdout 和 stderr 到日志文件，防止污染终端控制码
    ./server config.conf > server.log 2>&1 &
    SERVER_PID=$!
    # 给服务器 1 秒时间完成 socket 监听绑定
    sleep 1
}

# 脚本退出时自动清理后台服务器
trap kill_server EXIT

# ==========================================
# 2. 开始测试
# ==========================================
echo "=================================================="
echo "    开始测试 Redis vs KVstore ($TEST_N 条)        "
echo "=================================================="

for p in "${PIPELINES[@]}"; do
    echo "--------------------------------------------------"
    echo ">>> 正在测试 Pipeline -P $p ..."

    # 2.1 Redis PING
    restart_redis
    echo -n "  [1/6] Redis PING ... "
    qps_redis_ping=$(parse_qps "redis-benchmark -p $REDIS_PORT -n $TEST_N -c 1 -P $p -q PING")
    RESULTS["REDIS_PING_$p"]=$qps_redis_ping
    echo "$qps_redis_ping QPS"

    # 2.2 KVstore PING
    echo -n "  [2/6] KVstore PING ... "
    sudo sh -c 'echo 3 > /proc/sys/vm/drop_caches'
    start_kvstore
    qps_kv_ping=$(parse_qps "redis-benchmark -p $KV_PORT -n $TEST_N -c 1 -P $p -q PING")
    RESULTS["KV_PING_$p"]=$qps_kv_ping
    echo "$qps_kv_ping QPS"
    kill_server

    # 2.3 Redis SET
    restart_redis
    echo -n "  [3/6] Redis SET ... "
    qps_redis_set=$(parse_qps "redis-benchmark -p $REDIS_PORT -n $TEST_N -r 100000000 -c 1 -P $p -q SET key:__rand_int__ value:__rand_int__")
    RESULTS["REDIS_SET_$p"]=$qps_redis_set
    echo "$qps_redis_set QPS"

    # 2.4 KVstore RBTree (RSET)
    echo -n "  [4/6] KVstore RBTree (RSET) ... "
    sudo sh -c 'echo 3 > /proc/sys/vm/drop_caches'
    start_kvstore
    qps_rbt=$(parse_qps "redis-benchmark -p $KV_PORT -n $TEST_N -r 100000000 -c 1 -P $p -q RSET key:__rand_int__ value:__rand_int__")
    RESULTS["KV_RBT_$p"]=$qps_rbt
    echo "$qps_rbt QPS"
    kill_server

    # 2.5 KVstore Hash (HSET)
    echo -n "  [5/6] KVstore Hash (HSET) ... "
    sudo sh -c 'echo 3 > /proc/sys/vm/drop_caches'
    start_kvstore
    qps_hsh=$(parse_qps "redis-benchmark -p $KV_PORT -n $TEST_N -r 100000000 -c 1 -P $p -q HSET key:__rand_int__ value:__rand_int__")
    RESULTS["KV_HSH_$p"]=$qps_hsh
    echo "$qps_hsh QPS"
    kill_server

    # 2.6 KVstore SkipList (SSET)
    echo -n "  [6/6] KVstore SkipList (SSET) ... "
    sudo sh -c 'echo 3 > /proc/sys/vm/drop_caches'
    start_kvstore
    qps_skl=$(parse_qps "redis-benchmark -p $KV_PORT -n $TEST_N -r 100000000 -c 1 -P $p -q SSET key:__rand_int__ value:__rand_int__")
    RESULTS["KV_SKL_$p"]=$qps_skl
    echo "$qps_skl QPS"
    kill_server

done

# ==========================================
# 3. 打印最终对比表格
# ==========================================
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