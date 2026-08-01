#!/usr/bin/env bash

# ==========================================
# Redis vs KVstore 四大引擎独立基准测试脚本
# ==========================================

REDIS_PORT=6379
KV_PORT=2000
TEST_N=1000000  

# Pipeline 梯度数组
PIPELINES=(1 10 20 40 80 160)
declare -A RESULTS

# 格式化提取 QPS 值的辅助函数
parse_qps() {
    local cmd="$1"
    local output
    output=$(eval "$cmd" 2>&1)
    
    # 直接提取数字，防止被 redis-benchmark 的 CONFIG 警告误伤
    local qps
    qps=$(echo "$output" | grep -oP '\d+(\.\d+)?(?=\s+requests per second)' | head -n 1 | awk '{print int($1)}')
    
    if [[ -z "$qps" || "$qps" -eq 0 ]]; then
        echo "FAIL"
    else
        echo "$qps"
    fi
}

echo "=================================================="
echo "    开始测试 Redis vs KVstore (各引擎独立 1W条)    "
echo "=================================================="

for p in "${PIPELINES[@]}"; do
    echo "--------------------------------------------------"
    echo ">>> 正在测试 Pipeline -P $p ..."

    # 1. 测试 Redis SET (1W条)
    echo -n "  [1/5] Redis SET ... "
    qps_redis=$(parse_qps "redis-benchmark -p $REDIS_PORT -n $TEST_N -r 100000000 -P $p -q SET key:__rand_int__ value:__rand_int__")
    RESULTS["REDIS_$p"]=$qps_redis
    echo "$qps_redis QPS"

    # 2. 测试 KVstore Array (1W条)
    echo -n "  [2/5] KVstore Array (SET) ... "
    qps_arr=$(parse_qps "redis-benchmark -p $KV_PORT -n 10000 -r 100000000 -P $p -q SET key:__rand_int__ value:__rand_int__")
    RESULTS["KV_ARR_$p"]=$qps_arr
    echo "$qps_arr QPS"

    # 3. 测试 KVstore RBTree (1W条)
    echo -n "  [3/5] KVstore RBTree (RSET) ... "
    qps_rbt=$(parse_qps "redis-benchmark -p $KV_PORT -n $TEST_N -r 100000000 -P $p -q RSET key:__rand_int__ value:__rand_int__")
    RESULTS["KV_RBT_$p"]=$qps_rbt
    echo "$qps_rbt QPS"

    # 4. 测试 KVstore Hash (1W条)
    echo -n "  [4/5] KVstore Hash (HSET) ... "
    qps_hsh=$(parse_qps "redis-benchmark -p $KV_PORT -n $TEST_N -r 100000000 -P $p -q HSET key:__rand_int__ value:__rand_int__")
    RESULTS["KV_HSH_$p"]=$qps_hsh
    echo "$qps_hsh QPS"

    # 5. 测试 KVstore SkipList (1W条)
    echo -n "  [5/5] KVstore SkipList (SSET) ... "
    qps_skl=$(parse_qps "redis-benchmark -p $KV_PORT -n $TEST_N -r 100000000 -P $p -q SSET key:__rand_int__ value:__rand_int__")
    RESULTS["KV_SKL_$p"]=$qps_skl
    echo "$qps_skl QPS"
done

# ==========================================
# 打印最终对比表格
# ==========================================
OUTPUT_FILE="batchcommand_result.txt"

{
    echo ""
    echo "========================================================================================================"
    echo "                              Redis vs KVstore 四大引擎独立测试 (1W条)                                  "
    echo "========================================================================================================"
    printf "%-10s | %-14s | %-14s | %-15s | %-14s | %-16s\n" \
        "Pipeline" "Redis (SET)" "KV (Array)" "KV (RBTree)" "KV (Hash)" "KV (SkipList)"
    echo "--------------------------------------------------------------------------------------------------------"
    for p in "${PIPELINES[@]}"; do
        printf -- "-P %-7d | %-14s | %-14s | %-15s | %-14s | %-16s\n" \
            "$p" \
            "${RESULTS["REDIS_$p"]}" \
            "${RESULTS["KV_ARR_$p"]}" \
            "${RESULTS["KV_RBT_$p"]}" \
            "${RESULTS["KV_HSH_$p"]}" \
            "${RESULTS["KV_SKL_$p"]}"
    done
    echo "========================================================================================================"
} | tee "$OUTPUT_FILE"

echo ""
echo "测试完成！结果已保存至: $OUTPUT_FILE"