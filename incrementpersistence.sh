#!/usr/bin/env bash

# ==========================================================
# Redis vs KVstore 日志开关性能对比测试脚本
# ==========================================================

REDIS_PORT=6379
KV_PORT=2000
TOTAL_REQUESTS=10000
RAND_RANGE=100000000

declare -A LOG_OFF_RESULTS
declare -A LOG_ON_RESULTS

# 提取 QPS 的辅助函数
parse_qps() {
    local cmd="$1"
    local output
    output=$(eval "$cmd" 2>&1)
    
    local qps
    qps=$(echo "$output" | grep -oP '\d+(\.\d+)?(?=\s+requests per second)' | head -n 1 | awk '{print int($1)}')
    
    if [[ -z "$qps" ]]; then
        echo "N/A"
    else
        echo "$qps"
    fi
}

run_benchmarks() {
    local mode_name="$1"  # "OFF" 或 "ON"
    echo "--------------------------------------------------"
    echo ">>> 开始执行 [日志 $mode_name] 模式下的压测..."
    echo "--------------------------------------------------"

    # 1. Redis PING
    echo -n "  [1/7] Redis PING ... "
    qps=$(parse_qps "redis-benchmark -p $REDIS_PORT -n $TOTAL_REQUESTS -r $RAND_RANGE -q PING")
    eval "${mode_name}_RESULTS['REDIS_PING']=\$qps"
    echo "$qps QPS"

    # 2. Redis SET
    echo -n "  [2/7] Redis SET ... "
    qps=$(parse_qps "redis-benchmark -p $REDIS_PORT -n $TOTAL_REQUESTS -r $RAND_RANGE -q SET key:__rand_int__ value:__rand_int__")
    eval "${mode_name}_RESULTS['REDIS_SET']=\$qps"
    echo "$qps QPS"

    # 3. KVstore PING
    echo -n "  [3/7] KVstore PING ... "
    qps=$(parse_qps "redis-benchmark -p $KV_PORT -n $TOTAL_REQUESTS -r $RAND_RANGE -q PING")
    eval "${mode_name}_RESULTS['KV_PING']=\$qps"
    echo "$qps QPS"

    # 4. KVstore SET (Array)
    echo -n "  [4/7] KVstore SET (数组) ... "
    qps=$(parse_qps "redis-benchmark -p $KV_PORT -n $TOTAL_REQUESTS -r $RAND_RANGE -q SET key:__rand_int__ value:__rand_int__")
    eval "${mode_name}_RESULTS['KV_SET']=\$qps"
    echo "$qps QPS"

    # 5. KVstore RSET (Red-Black Tree)
    echo -n "  [5/7] KVstore RSET (红黑树) ... "
    qps=$(parse_qps "redis-benchmark -p $KV_PORT -n $TOTAL_REQUESTS -r $RAND_RANGE -q RSET key:__rand_int__ value:__rand_int__")
    eval "${mode_name}_RESULTS['KV_RSET']=\$qps"
    echo "$qps QPS"

    # 6. KVstore HSET (Hash)
    echo -n "  [6/7] KVstore HSET (哈希表) ... "
    qps=$(parse_qps "redis-benchmark -p $KV_PORT -n $TOTAL_REQUESTS -r $RAND_RANGE -q HSET key:__rand_int__ value:__rand_int__")
    eval "${mode_name}_RESULTS['KV_HSET']=\$qps"
    echo "$qps QPS"

    # 7. KVstore SSET (SkipList)
    echo -n "  [7/7] KVstore SSET (跳表) ... "
    qps=$(parse_qps "redis-benchmark -p $KV_PORT -n $TOTAL_REQUESTS -r $RAND_RANGE -q SSET key:__rand_int__ value:__rand_int__")
    eval "${mode_name}_RESULTS['KV_SSET']=\$qps"
    echo "$qps QPS"
}

# ==================== 第一阶段：关闭日志测试 ====================
echo "=================================================="
echo "      步骤 1/2: 请确保当前服务端处于 [关闭日志] 状态"
echo "=================================================="
read -p "准备好后按 [Enter] 键开始测试关闭日志 QPS..."

run_benchmarks "LOG_OFF"

echo ""
# ==================== 第二阶段：打开日志测试 ====================
echo "=================================================="
echo "      步骤 2/2: 请手动去修改配置/重新编译以 [打开日志]"
echo "=================================================="
echo "提示: 请确保服务端(Redis & KVstore)已经开启日志记录并正常运行。"
read -p "修改完成并重启服务后，按 [Enter] 键开始测试打开日志 QPS..."

run_benchmarks "LOG_ON"

# ==================== 汇总对比输出 ====================
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