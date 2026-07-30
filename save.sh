#!/bin/bash

# ============================================
# SAVE 持久化性能自动化测试脚本
# 执行要求: sudo bash save.sh 或通过 make save 调用
# ============================================

set -e

SERVER_PORT=2000
SNAPSHOT_FILE="kvstore.snap"
SERVER_BIN="./server"
SERVER_CONF="config.conf"
CLIENT_BIN="./bench_save"
RESULT_FILE="save_test_results.txt"

# 检查依赖程序是否存在
if [ ! -f "$SERVER_BIN" ] || [ ! -f "$CLIENT_BIN" ]; then
    echo "❌ 错误: 未找到 $SERVER_BIN 或 $CLIENT_BIN，请先执行 make 编译！"
    exit 1
fi

# ANSI 颜色定义
CLR_RESET="\033[0m"
CLR_BOLD="\033[1m"
CLR_CYAN="\033[36m"
CLR_GREEN="\033[32m"
CLR_YELLOW="\033[33m"
CLR_BLUE="\033[34m"

# 精确关闭服务器 (绝不误杀 VS Code)
stop_server() {
    if command -v fuser >/dev/null 2>&1; then
        fuser -k -9 "${SERVER_PORT}/tcp" >/dev/null 2>&1 || true
    fi
    pkill -9 -x "server" >/dev/null 2>&1 || true
    sleep 0.3
}

# 启动服务器
start_server() {
    [ -f "$SNAPSHOT_FILE" ] && rm -f "$SNAPSHOT_FILE"
    "$SERVER_BIN" "$SERVER_CONF" > /dev/null 2>&1 &
    
    local waited=0
    while ! nc -z 127.0.0.1 $SERVER_PORT 2>/dev/null; do
        sleep 0.1
        waited=$((waited+1))
        if [ $waited -ge 30 ]; then
            echo "❌ 服务器启动超时，请检查配置！"
            exit 1
        fi
    done
}

# 写文件表头
if [ ! -f "$RESULT_FILE" ]; then
    echo "测试时间            间隔命令条数    SAVE次数   时间(s)    QPS        最后一次SAVE耗时" > "$RESULT_FILE"
    echo "------------------ -------------- ---------- --------- ---------- ----------------" >> "$RESULT_FILE"
fi

# 终端界面表头
echo -e "${CLR_BOLD}===============================================================================${CLR_RESET}"
echo -e "${CLR_CYAN}${CLR_BOLD}                    KVStore SAVE 持久化性能自动化测试                          ${CLR_RESET}"
echo -e "${CLR_BOLD}===============================================================================${CLR_RESET}"
printf "${CLR_BOLD}%-16s %-12s %-10s %-12s %-14s${CLR_RESET}\n" "触发间隔(条)" "QPS (条/秒)" "总耗时(s)" "SAVE次数" "末次SAVE耗时"
echo -e "-------------------------------------------------------------------------------"

intervals=(1000000 100000 10000 1000)

for interval in "${intervals[@]}"; do
    stop_server
    start_server
    
    # 执行压测
    output=$("$CLIENT_BIN" "$interval")
    
    # 提取指标
    save_count=$(echo "$output" | grep -oP 'Total SAVEs:\s*\K\d+')
    total_time=$(echo "$output" | grep -oP 'Total time:\s*\K[0-9.]+')
    qps=$(echo "$output" | grep -oP 'QPS:\s*\K[0-9]+')
    last_ms=$(echo "$output" | grep -oP 'Last SAVE:\s*\K[0-9.]+')
    now=$(date "+%Y-%m-%d %H:%M:%S")

    # 格式化终端输出
    printf "${CLR_GREEN}%-16s${CLR_RESET} ${CLR_YELLOW}%-12s${CLR_RESET} %-10s %-12s %s ms\n" \
        "每 $interval 条" \
        "${qps:-N/A}" \
        "${total_time:-N/A}" \
        "${save_count:-N/A}" \
        "${last_ms:-N/A}"
    
    # 追加保存至本地文件
    printf "%-18s %-14s %-10s %-10s %-10s %s\n" \
        "$now" "$interval" "${save_count:-N/A}" "${total_time:-N/A}" "${qps:-N/A}" "${last_ms:-N/A}ms" >> "$RESULT_FILE"
done

stop_server

echo -e "-------------------------------------------------------------------------------"
echo -e "${CLR_BLUE}[✓] 测试完成！详细数据已追加保存至: ${CLR_BOLD}$RESULT_FILE${CLR_RESET}"
echo -e "${CLR_BOLD}===============================================================================${CLR_RESET}"