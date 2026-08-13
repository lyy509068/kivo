#!/usr/bin/env bash

# ==========================================================
# Redis vs KVstore 日志开关性能对比测试脚本（自动重启隔离版）
# ==========================================================

REDIS_PORT=6379
KV_PORT=2000
TOTAL_REQUESTS=1000000
RAND_RANGE=100000000
KV_CONFIG_FILE="config.conf"
KV_SERVER_BIN="./server"
KV_AOF_FILE="kvstore.aof"

declare -A LOG_OFF_RESULTS
declare -A LOG_ON_RESULTS

parse_qps() {
    local output
    output=$(eval "$1" 2>&1)
    local qps
    qps=$(echo "$output" | grep -oP '\d+(\.\d+)?(?=\s+requests per second)' | head -n 1 | awk '{print int($1)}')
    [[ -z "$qps" ]] && echo "N/A" || echo "$qps"
}

# ---------- KVstore 控制 ----------
stop_kvstore() {
    echo "停止 KVstore 服务器..."
    if pgrep -x server > /dev/null; then
        sudo pkill -x server
        sleep 1
    fi
}

set_kvstore_persistence() {
    local value="$1"
    echo "设置 KVstore persistence = $value"
    if [ ! -f "$KV_CONFIG_FILE" ]; then
        {
            echo "port 2000"
            echo "persistence OFF"
            echo "snapshot OFF"
            echo "expire OFF"
            echo "mempool OFF"
            echo "replication OFF"
            echo "transport OFF"
        } > "$KV_CONFIG_FILE"
    fi
    if grep -q "^persistence" "$KV_CONFIG_FILE"; then
        sed -i "s/^persistence.*/persistence $value/" "$KV_CONFIG_FILE"
    else
        echo "persistence $value" >> "$KV_CONFIG_FILE"
    fi
}

start_kvstore() {
    local mode="$1"
    stop_kvstore
    set_kvstore_persistence "$mode"
    echo "启动 KVstore 服务器 (persistence=$mode)..."
    rm -f "$KV_AOF_FILE"

    # 启动服务器，输出重定向到日志，防止污染终端
    "$KV_SERVER_BIN" "$KV_CONFIG_FILE" > server.log 2>&1 &
    SERVER_PID=$!

    # 等待端口就绪
    local waited=0
    while ! nc -z 127.0.0.1 $KV_PORT 2>/dev/null; do
        sleep 0.2
        waited=$((waited+1))
        if [ $waited -ge 25 ]; then
            echo "错误：KVstore 启动超时！"
            exit 1
        fi
    done

    echo "KVstore 已启动，PID=$SERVER_PID"
}

# ---------- Redis 控制 ----------
restart_redis() {
    echo "重启 Redis 服务器..."
    sudo systemctl stop redis-server
    sleep 1
    sudo rm -f /var/lib/redis/appendonly.aof
    sudo rm -f /var/lib/redis/dump.rdb
    sudo systemctl start redis-server
    sleep 2
    for i in {1..10}; do
        redis-cli -p $REDIS_PORT ping > /dev/null 2>&1 && break
        sleep 1
    done
}

set_redis_appendonly() {
    local value="$1"
    echo "设置 Redis appendonly = $value"
    redis-cli -p $REDIS_PORT CONFIG SET appendonly "$value"
    [ "$value" == "yes" ] && redis-cli -p $REDIS_PORT CONFIG SET appendfsync everysec
}

# ---------- 引擎测试函数（每个引擎独立启动/停止 KVstore） ----------
run_kv_engine_test() {
    local test_name="$1"       # 用于显示
    local resp_cmd="$2"        # redis-benchmark 的测试命令（不含 -p/-n/-r 等公共参数）
    local mode="$3"            # 当前持久化模式 OFF/ON
    local result_key="$4"      # 存储结果的键名
    local request_count="$5"   # 请求数

    echo -n "  $test_name ... "
    start_kvstore "$mode"
    local qps
    # ★ 修改点：添加 -c 1 强制单连接
    qps=$(parse_qps "redis-benchmark -p $KV_PORT -n $request_count -r $RAND_RANGE -c 1 -q $resp_cmd")
    eval "${result_key}=\$qps"
    echo "$qps QPS"
    stop_kvstore
}

# ---------- 完整压测流程（给定持久化模式） ----------
run_benchmarks_for_mode() {
    local mode="$1"      # OFF 或 ON
    local prefix="$2"    # LOG_OFF 或 LOG_ON

    echo "--------------------------------------------------"
    echo ">>> 开始执行 [日志 $mode] 模式下的压测..."
    echo "--------------------------------------------------"

    # 1. Redis PING
    echo -n "  [1/7] Redis PING ... "
    # ★ 修改点：添加 -c 1
    qps=$(parse_qps "redis-benchmark -p $REDIS_PORT -n $TOTAL_REQUESTS -r $RAND_RANGE -c 1 -q PING")
    eval "${prefix}_RESULTS['REDIS_PING']=\$qps"
    echo "$qps QPS"

    # 2. Redis SET
    echo -n "  [2/7] Redis SET ... "
    # ★ 修改点：添加 -c 1
    qps=$(parse_qps "redis-benchmark -p $REDIS_PORT -n $TOTAL_REQUESTS -r $RAND_RANGE -c 1 -q SET key:__rand_int__ value:__rand_int__")
    eval "${prefix}_RESULTS['REDIS_SET']=\$qps"
    echo "$qps QPS"

    # 3. KVstore PING（不需要重启，仅检查服务）
    echo -n "  [3/7] KVstore PING ... "
    start_kvstore "$mode"
    # ★ 修改点：添加 -c 1
    qps=$(parse_qps "redis-benchmark -p $KV_PORT -n $TOTAL_REQUESTS -r $RAND_RANGE -c 1 -q PING")
    eval "${prefix}_RESULTS['KV_PING']=\$qps"
    echo "$qps QPS"

    # 4. KVstore SET (Array)
    run_kv_engine_test "[4/7] KVstore SET (数组)" "SET key:__rand_int__ value:__rand_int__" "$mode" "${prefix}_RESULTS['KV_SET']" 10000

    # 5. KVstore RSET (红黑树)
    run_kv_engine_test "[5/7] KVstore RSET (红黑树)" "RSET key:__rand_int__ value:__rand_int__" "$mode" "${prefix}_RESULTS['KV_RSET']" $TOTAL_REQUESTS

    # 6. KVstore HSET (哈希表)
    run_kv_engine_test "[6/7] KVstore HSET (哈希表)" "HSET key:__rand_int__ value:__rand_int__" "$mode" "${prefix}_RESULTS['KV_HSET']" $TOTAL_REQUESTS

    # 7. KVstore SSET (跳表)
    run_kv_engine_test "[7/7] KVstore SSET (跳表)" "SSET key:__rand_int__ value:__rand_int__" "$mode" "${prefix}_RESULTS['KV_SSET']" $TOTAL_REQUESTS
}

# ---------- 主流程 ----------
echo "=================================================="
echo "      自动化日志开关性能对比测试（引擎隔离版）"
echo "=================================================="

# 第一轮：关闭日志
echo ""
echo ">>> 第一轮：关闭日志 (AOF OFF)"
echo "=================================================="
restart_redis
set_redis_appendonly "no"
run_benchmarks_for_mode "OFF" "LOG_OFF"

# 第二轮：开启日志
echo ""
echo ">>> 第二轮：开启日志 (AOF ON)"
echo "=================================================="
restart_redis
set_redis_appendonly "yes"
run_benchmarks_for_mode "ON" "LOG_ON"

# 清理可能残留的 KVstore 进程
stop_kvstore

# 汇总结果
OUTPUT_FILE="incrementpersistence_result.txt"
{
    echo ""
    echo "========================================================================="
    echo "                     日志开关性能影响对比测试汇总                       "
    echo "========================================================================="
    printf "%-35s | %-15s | %-15s\n" "测试命令 / 数据结构" "关闭日志 QPS" "打开日志 QPS"
    echo "------------------------------------------------------------------------- "
    printf "%-35s | %-15s | %-15s\n" "Redis PING"             "${LOG_OFF_RESULTS['REDIS_PING']}" "${LOG_ON_RESULTS['REDIS_PING']}"
    printf "%-35s | %-15s | %-15s\n" "Redis SET"              "${LOG_OFF_RESULTS['REDIS_SET']}"  "${LOG_ON_RESULTS['REDIS_SET']}"
    printf "%-35s | %-15s | %-15s\n" "KVstore PING"           "${LOG_OFF_RESULTS['KV_PING']}"    "${LOG_ON_RESULTS['KV_PING']}"
    printf "%-35s | %-15s | %-15s\n" "KVstore SET (Array)"    "${LOG_OFF_RESULTS['KV_SET']}"     "${LOG_ON_RESULTS['KV_SET']}"
    printf "%-35s | %-15s | %-15s\n" "KVstore RSET (Red-Black)" "${LOG_OFF_RESULTS['KV_RSET']}"    "${LOG_ON_RESULTS['KV_RSET']}"
    printf "%-35s | %-15s | %-15s\n" "KVstore HSET (Hash)"     "${LOG_OFF_RESULTS['KV_HSET']}"    "${LOG_ON_RESULTS['KV_HSET']}"
    printf "%-35s | %-15s | %-15s\n" "KVstore SSET (SkipList)" "${LOG_OFF_RESULTS['KV_SSET']}"    "${LOG_ON_RESULTS['KV_SSET']}"
    echo "========================================================================="
} | tee "$OUTPUT_FILE"

echo ""
echo "测试完成！结果已成功导出保存至: $OUTPUT_FILE"