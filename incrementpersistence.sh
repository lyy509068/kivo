#!/usr/bin/env bash

# ==========================================================
# Redis vs KVstore 日志开关性能对比测试脚本（逐行对比 + 严格隔离版）
# 测试顺序：逐个数据结构测试，先测 OFF，清理日志/重启，再测 ON
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
    if pgrep -x server > /dev/null; then
        sudo pkill -x server
        sleep 5
    fi
}

set_kvstore_persistence() {
    local value="$1"   # ON 或 OFF
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
    local mode="$1"   # ON 或 OFF
    stop_kvstore
    set_kvstore_persistence "$mode"
    
    # 核心：启动前务必清理历史 AOF 日志，保证环境纯净
    rm -f "$KV_AOF_FILE"

    "$KV_SERVER_BIN" "$KV_CONFIG_FILE" > server.log 2>&1 &
    SERVER_PID=$!

    local waited=0
    while ! nc -z 127.0.0.1 $KV_PORT 2>/dev/null; do
        sleep 0.2
        waited=$((waited+1))
        if [ $waited -ge 25 ]; then
            echo "错误：KVstore 启动超时！"
            exit 1
        fi
    done
}

# ---------- Redis 控制 ----------
restart_redis() {
    local mode="$1"  # yes 或 no
    
    sudo systemctl stop redis-server
    sleep 2
    
    # 核心：启动前彻底清理 Redis 的 AOF 和 RDB 日志文件
    sudo rm -f /var/lib/redis/appendonly.aof
    sudo rm -f /var/lib/redis/dump.rdb
    sudo rm -rf /var/lib/redis/appendonlydir/ 2>/dev/null # 兼容 Redis 7+ 的 appendonlydir 目录
    
    sudo systemctl start redis-server
    sleep 2
    for i in {1..10}; do
        redis-cli -p $REDIS_PORT ping > /dev/null 2>&1 && break
        sleep 1
    done

    # 动态设置 Redis 对应的日志模式
    redis-cli -p $REDIS_PORT CONFIG SET appendonly "$mode" > /dev/null 2>&1
    if [ "$mode" == "yes" ]; then
        redis-cli -p $REDIS_PORT CONFIG SET appendfsync everysec > /dev/null 2>&1
    fi
}

# ---------- 执行单行测试 (Redis) ----------
run_redis_row() {
    local test_index="$1"
    local test_name="$2"
    local resp_cmd="$3"
    local result_key="$4"

    echo ">>> [$test_index/7] 测试项目: $test_name"
    
    # 测试 OFF
    echo -n "  ├─ [关闭日志 OFF] 删除日志并重启... "
    restart_redis "no"
    echo -n "压测中... "
    local qps_off
    qps_off=$(parse_qps "redis-benchmark -p $REDIS_PORT -n $TOTAL_REQUESTS -r $RAND_RANGE -c 1 -q $resp_cmd")
    LOG_OFF_RESULTS["$result_key"]=$qps_off
    echo "${qps_off} QPS"

    # 测试 ON
    echo -n "  └─ [开启日志 ON]  删除日志并重启... "
    restart_redis "yes"
    echo -n "压测中... "
    local qps_on
    qps_on=$(parse_qps "redis-benchmark -p $REDIS_PORT -n $TOTAL_REQUESTS -r $RAND_RANGE -c 1 -q $resp_cmd")
    LOG_ON_RESULTS["$result_key"]=$qps_on
    echo "${qps_on} QPS"
    echo ""
}

# ---------- 执行单行测试 (KVstore) ----------
run_kv_row() {
    local test_index="$1"
    local test_name="$2"
    local resp_cmd="$3"
    local result_key="$4"
    local request_count="${5:-$TOTAL_REQUESTS}"

    echo ">>> [$test_index/7] 测试项目: $test_name"
    
    # 测试 OFF
    echo -n "  ├─ [关闭日志 OFF] 删除日志并重启... "
    start_kvstore "OFF"
    echo -n "压测中... "
    local qps_off
    qps_off=$(parse_qps "redis-benchmark -p $KV_PORT -n $request_count -r $RAND_RANGE -c 1 -q $resp_cmd")
    LOG_OFF_RESULTS["$result_key"]=$qps_off
    echo "${qps_off} QPS"

    # 测试 ON
    echo -n "  └─ [开启日志 ON]  删除日志并重启... "
    start_kvstore "ON"
    echo -n "压测中... "
    local qps_on
    qps_on=$(parse_qps "redis-benchmark -p $KV_PORT -n $request_count -r $RAND_RANGE -c 1 -q $resp_cmd")
    LOG_ON_RESULTS["$result_key"]=$qps_on
    echo "${qps_on} QPS"
    
    stop_kvstore
    echo ""
}

# ---------- 主流程 ----------
echo "=================================================="
echo "    自动化日志开关性能对比测试       "
echo "=================================================="
echo ""

# 1~2: Redis 引擎测试
run_redis_row "1" "Redis PING" "PING" "REDIS_PING"
run_redis_row "2" "Redis SET" "SET key:__rand_int__ value:__rand_int__" "REDIS_SET"

# 3~7: KVstore 引擎测试
run_kv_row "3" "KVstore PING" "PING" "KV_PING"
run_kv_row "4" "KVstore SET (Array)" "SET key:__rand_int__ value:__rand_int__" "KV_SET" 10000
run_kv_row "5" "KVstore RSET (Red-Black)" "RSET key:__rand_int__ value:__rand_int__" "KV_RSET"
run_kv_row "6" "KVstore HSET (Hash)" "HSET key:__rand_int__ value:__rand_int__" "KV_HSET"
run_kv_row "7" "KVstore SSET (SkipList)" "SSET key:__rand_int__ value:__rand_int__" "KV_SSET"


# 汇总结果
OUTPUT_FILE="incrementpersistence_result.txt"
{
    echo "========================================================================="
    echo "                     日志开关性能影响对比测试汇总                        "
    echo "========================================================================="
    printf "%-35s | %-15s | %-15s\n" "测试命令 / 数据结构" "关闭日志 QPS" "开启日志 QPS"
    echo "------------------------------------------------------------------------- "
    printf "%-35s | %-15s | %-15s\n" "Redis PING"             "${LOG_OFF_RESULTS['REDIS_PING']}" "${LOG_ON_RESULTS['REDIS_PING']}"
    printf "%-35s | %-15s | %-15s\n" "Redis SET"              "${LOG_OFF_RESULTS['REDIS_SET']}"  "${LOG_ON_RESULTS['REDIS_SET']}"
    printf "%-35s | %-15s | %-15s\n" "KVstore PING"           "${LOG_OFF_RESULTS['KV_PING']}"    "${LOG_ON_RESULTS['KV_PING']}"
    printf "%-35s | %-15s | %-15s\n" "KVstore SET (Array)"    "${LOG_OFF_RESULTS['KV_SET']}"     "${LOG_ON_RESULTS['KV_SET']}"
    printf "%-35s | %-15s | %-15s\n" "KVstore RSET (Red-Black)" "${LOG_OFF_RESULTS['KV_RSET']}"    "${LOG_ON_RESULTS['KV_RSET']}"
    printf "%-35s | %-15s | %-15s\n" "KVstore HSET (Hash)"      "${LOG_OFF_RESULTS['KV_HSET']}"    "${LOG_ON_RESULTS['KV_HSET']}"
    printf "%-35s | %-15s | %-15s\n" "KVstore SSET (SkipList)" "${LOG_OFF_RESULTS['KV_SSET']}"    "${LOG_ON_RESULTS['KV_SSET']}"
    echo "========================================================================="
} | tee "$OUTPUT_FILE"

echo ""
echo "测试完成！结果已成功导出保存至: $OUTPUT_FILE"