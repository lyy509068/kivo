#!/bin/bash
# 一键主从同步性能测试脚本（含全量+增量）
# 使用方式: ./auto_test.sh

set -e

# ==================== 配置 ====================
MASTER_IP="192.168.37.128"
SLAVE_IP="192.168.37.129"
CLIENT_IP="192.168.37.130"
MASTER_PORT=2000

MASTER_PROJ_DIR="/home/lyy/course/project/KVstore2/9.1-kvstore"
SLAVE_PROJ_DIR="/home/c2/project/KVstore6/9.1-kvstore"
CLIENT_PROJ_DIR="/home/c1/project"

MASTER_USER="lyy"
SLAVE_USER="c2"
CLIENT_USER="c1"

SSH_OPTS="-n -o StrictHostKeyChecking=no"

RESULT_DIR="./results_$(date +%Y%m%d_%H%M%S)"
mkdir -p "$RESULT_DIR"
RESULT_FILE="$RESULT_DIR/all_in_one_results.txt"

# ==================== 工具函数 ====================
remote_exec() {
    local user="$1" ip="$2" cmd="$3"
    ssh $SSH_OPTS "${user}@${ip}" "$cmd"
}

clean_aof() {
    echo "[清理] 正在清理集群 AOF 文件..."
    remote_exec "$MASTER_USER" "$MASTER_IP" "rm -f $MASTER_PROJ_DIR/kvstore.aof" || true
    remote_exec "$SLAVE_USER" "$SLAVE_IP" "rm -f $SLAVE_PROJ_DIR/kvstore.aof" || true
}

clean_logs() {
    echo "[清理] 正在清理服务器日志..."
    remote_exec "$MASTER_USER" "$MASTER_IP" "sudo rm -f /tmp/server_MASTER.log /tmp/server_SLAVE.log /tmp/ebpf_relay.log" || true
    remote_exec "$SLAVE_USER" "$SLAVE_IP" "sudo rm -f /tmp/server_MASTER.log /tmp/server_SLAVE.log /tmp/ebpf_relay.log" || true
}

write_config() {
    local user="$1" ip="$2" proj_dir="$3" port="$4" persistence="$5" snapshot="$6" expire="$7" mempool="$8" replication="$9" transport="${10}"
    
    remote_exec "$user" "$ip" "cat > $proj_dir/config.conf << EOF
port $port
persistence $persistence
snapshot $snapshot
expire $expire
mempool $mempool
replication $replication
transport $transport
EOF"
    
    echo "[配置] ${user}@${ip}: port=$port replication=$replication transport=$transport"
}

start_server() {
    local user="$1" ip="$2" role="$3" proj_dir="$4"
    remote_exec "$user" "$ip" "sudo pkill -9 server || true"
    sleep 1
    
    echo "[启动] 正在启动 $role 服务器 (${user}@${ip})..."
    remote_exec "$user" "$ip" "cd $proj_dir && sudo -b nohup ./server config.conf </dev/null >/tmp/server_${role}.log 2>&1"
    
    local retries=10
    local started=false
    while [ $retries -gt 0 ]; do
        if remote_exec "$user" "$ip" "ss -tlnp 2>/dev/null | grep -q ':$MASTER_PORT'"; then
            started=true
            break
        fi
        sleep 1
        retries=$((retries - 1))
    done

    if [ "$started" = true ]; then
        echo "[成功] $role 服务器已正常运行！"
    else
        echo "❌ [致命错误] $role 服务器启动失败！"
        echo "------ 最新报错日志片段 ------"
        remote_exec "$user" "$ip" "tail -n 15 /tmp/server_${role}.log 2>/dev/null || echo '(日志为空)'"
        exit 1
    fi
}

stop_server() {
    local user="$1" ip="$2"
    remote_exec "$user" "$ip" "sudo pkill -9 server || true"
    echo "[停止] 已停止 ${user}@${ip} 上的 server"
}

# eBPF 相关
load_ebpf() {
    remote_exec "$MASTER_USER" "$MASTER_IP" "cd $MASTER_PROJ_DIR && sudo make unload_bpf > /dev/null 2>&1 || true"
    remote_exec "$MASTER_USER" "$MASTER_IP" "cd $MASTER_PROJ_DIR && sudo make load_bpf > /dev/null 2>&1"
    echo "[eBPF] 已加载 eBPF 转发程序"
}

unload_ebpf() {
    remote_exec "$MASTER_USER" "$MASTER_IP" "cd $MASTER_PROJ_DIR && sudo make unload_bpf > /dev/null 2>&1 || true"
    echo "[eBPF] 已卸载 eBPF 转发程序"
}

start_ebpf_relay() {
    remote_exec "$MASTER_USER" "$MASTER_IP" "sudo pkill -9 ebpf_relay || true"
    sleep 1
    remote_exec "$MASTER_USER" "$MASTER_IP" "cd $MASTER_PROJ_DIR && sudo -b nohup ./ebpf_relay </dev/null >/tmp/ebpf_relay.log 2>&1"
    sleep 2
    echo "[eBPF] 已启动 eBPF Relay 进程"
}

stop_ebpf_relay() {
    remote_exec "$MASTER_USER" "$MASTER_IP" "sudo pkill -9 ebpf_relay || true"
    echo "[eBPF] 已停止 eBPF Relay 进程"
}

# ★ 修复：分别从主端和从端收集全量同步性能日志
collect_full_sync_stats() {
    echo "--- 主端全量同步性能 ---" >> "$RESULT_FILE"
    remote_exec "$MASTER_USER" "$MASTER_IP" "grep -E 'Perf TCP|Synchronized EXACT|Successfully sent|RDMA Master|bytes in' /tmp/server_MASTER.log 2>/dev/null" >> "$RESULT_FILE" || echo "(无数据)" >> "$RESULT_FILE"
    echo "" >> "$RESULT_FILE"
    
    echo "--- 从端全量同步性能 ---" >> "$RESULT_FILE"
    remote_exec "$SLAVE_USER" "$SLAVE_IP" "grep -E 'Perf TCP|Synchronized EXACT|Successfully sent|RDMA Slave|AOF reload|bytes in' /tmp/server_SLAVE.log 2>/dev/null" >> "$RESULT_FILE" || echo "(无数据)" >> "$RESULT_FILE"
    echo "" >> "$RESULT_FILE"
}

# ★ 检查从机日志是否有全量同步完成标志
wait_for_full_sync() {
    local transport="$1"  # "RDMA" 或 "TCP"
    local timeout=120
    local start=$(date +%s)
    
    echo "[等待] 等待 ${transport} 全量同步完成..."
    
    while true; do
        if [ "$transport" == "RDMA" ]; then
            # RDMA 完成标志：从机日志出现 Synchronized EXACT
            if remote_exec "$SLAVE_USER" "$SLAVE_IP" "grep -q 'Synchronized EXACT' /tmp/server_SLAVE.log 2>/dev/null"; then
                echo "[完成] RDMA 全量同步完成"
                return 0
            fi
        elif [ "$transport" == "TCP" ]; then
            # TCP 完成标志：从机日志出现 Perf TCP
            if remote_exec "$SLAVE_USER" "$SLAVE_IP" "grep -q 'Perf TCP' /tmp/server_SLAVE.log 2>/dev/null"; then
                echo "[完成] TCP 全量同步完成"
                return 0
            fi
        fi
        
        local elapsed=$(( $(date +%s) - start ))
        if [ $elapsed -gt $timeout ]; then
            echo "⚠️ 全量同步等待超时 (${timeout}秒)"
            return 1
        fi
        sleep 1
    done
}

# ==================== 三轮核心逻辑 ====================

run_round_1() {
    echo -e "\n=============================================="
    echo " 第 1 轮：基准测试 (无同步，无 eBPF)"
    echo "=============================================="
    echo -e "\n=================== 第一轮：基准测试 ===================" >> "$RESULT_FILE"
    echo "配置: replication=OFF transport=OFF" >> "$RESULT_FILE"
    echo "时间: $(date '+%Y-%m-%d %H:%M:%S')" >> "$RESULT_FILE"
    
    clean_aof
    clean_logs
    
    write_config "$MASTER_USER" "$MASTER_IP" "$MASTER_PROJ_DIR" 2000 "ON" "OFF" "OFF" "OFF" "OFF" "OFF"
    start_server "$MASTER_USER" "$MASTER_IP" "MASTER" "$MASTER_PROJ_DIR"
    
    echo "--- 基准 QPS ---" >> "$RESULT_FILE"
    echo "[执行] 正在从客户端插入数据..."
    remote_exec "$CLIENT_USER" "$CLIENT_IP" "cd $CLIENT_PROJ_DIR && ./test_transport $MASTER_IP $MASTER_PORT" >> "$RESULT_FILE" 2>&1
    echo "" >> "$RESULT_FILE"
    
    stop_server "$MASTER_USER" "$MASTER_IP"
    echo "[完成] 第一轮测试结束"
}

run_round_2() {
    echo -e "\n=============================================="
    echo " 第 2 轮：eBPF 转发 (RDMA全量 + eBPF增量)"
    echo "=============================================="
    echo -e "\n=================== 第二轮：eBPF 转发 ===================" >> "$RESULT_FILE"
    echo "全量同步: RDMA" >> "$RESULT_FILE"
    echo "增量同步: eBPF" >> "$RESULT_FILE"
    echo "时间: $(date '+%Y-%m-%d %H:%M:%S')" >> "$RESULT_FILE"
    
    clean_aof
    clean_logs
    
    # 配置：主从用 RDMA 做全量同步
    write_config "$MASTER_USER" "$MASTER_IP" "$MASTER_PROJ_DIR" 2000 "OFF" "OFF" "OFF" "OFF" "MASTER" "RDMA"
    write_config "$SLAVE_USER" "$SLAVE_IP" "$SLAVE_PROJ_DIR" 2000 "OFF" "OFF" "OFF" "OFF" "SLAVE" "RDMA"
    
    start_server "$MASTER_USER" "$MASTER_IP" "MASTER" "$MASTER_PROJ_DIR"
    start_server "$SLAVE_USER" "$SLAVE_IP" "SLAVE" "$SLAVE_PROJ_DIR"
    
    # ★ 启动 iperf3 服务端
    echo "[带宽] 启动 iperf3 服务端..."
    remote_exec "$SLAVE_USER" "$SLAVE_IP" "pkill -9 iperf3 || true; nohup iperf3 -s > /dev/null 2>&1 &"
    sleep 1
    
    # ★ 后台启动 iperf3 客户端（30秒）
    echo "[带宽] 启动 30 秒带宽测试..."
    echo "--- 全量同步期间带宽 (iperf3 30s) ---" >> "$RESULT_FILE"
    remote_exec "$MASTER_USER" "$MASTER_IP" "iperf3 -c $SLAVE_IP -t 30" >> "$RESULT_FILE" 2>&1 &
    IPERF_PID=$!
    
    # ★ 启动全量同步
    echo "[全量] 从客户端插入 1GB 数据触发 RDMA 全量同步..."
    remote_exec "$CLIENT_USER" "$CLIENT_IP" "cd $CLIENT_PROJ_DIR && ./test_1G $MASTER_IP $MASTER_PORT" > /dev/null 2>&1 &
    FULL_PID=$!
    
    # ★ 等待全量同步完成
    wait_for_full_sync "RDMA"
    
    # 等待后台任务结束
    wait $FULL_PID 2>/dev/null || true
    wait $IPERF_PID 2>/dev/null || true
    remote_exec "$SLAVE_USER" "$SLAVE_IP" "pkill -9 iperf3 || true"
    
    # ★ 收集全量同步性能
    collect_full_sync_stats
    
    # ★ 全量同步完成后加载 eBPF 做增量测试
    echo "[eBPF] 全量同步完成，加载 eBPF..."
    load_ebpf
    start_ebpf_relay
    
    echo "--- eBPF 增量 QPS ---" >> "$RESULT_FILE"
    echo "[执行] 正在从客户端插入增量数据..."
    remote_exec "$CLIENT_USER" "$CLIENT_IP" "cd $CLIENT_PROJ_DIR && ./test_transport $MASTER_IP $MASTER_PORT" >> "$RESULT_FILE" 2>&1
    echo "" >> "$RESULT_FILE"
    
    echo "[等待] 等待 5 秒让增量同步完成..."
    sleep 5
    
    stop_ebpf_relay
    unload_ebpf
    stop_server "$SLAVE_USER" "$SLAVE_IP"
    stop_server "$MASTER_USER" "$MASTER_IP"
    
    echo "[完成] 第二轮测试结束"
}

run_round_3() {
    echo -e "\n=============================================="
    echo " 第 3 轮：TCP 转发 (TCP全量 + TCP增量)"
    echo "=============================================="
    echo -e "\n=================== 第三轮：TCP 网络转发 ===================" >> "$RESULT_FILE"
    echo "全量同步: TCP" >> "$RESULT_FILE"
    echo "增量同步: TCP" >> "$RESULT_FILE"
    echo "时间: $(date '+%Y-%m-%d %H:%M:%S')" >> "$RESULT_FILE"
    
    clean_aof
    clean_logs
    
    write_config "$MASTER_USER" "$MASTER_IP" "$MASTER_PROJ_DIR" 2000 "OFF" "OFF" "OFF" "OFF" "MASTER" "TCP"
    write_config "$SLAVE_USER" "$SLAVE_IP" "$SLAVE_PROJ_DIR" 2000 "OFF" "OFF" "OFF" "OFF" "SLAVE" "TCP"
    
    start_server "$MASTER_USER" "$MASTER_IP" "MASTER" "$MASTER_PROJ_DIR"
    start_server "$SLAVE_USER" "$SLAVE_IP" "SLAVE" "$SLAVE_PROJ_DIR"
    
    # ★ iperf3 带宽测试
    echo "[带宽] 启动 iperf3 服务端..."
    remote_exec "$SLAVE_USER" "$SLAVE_IP" "pkill -9 iperf3 || true; nohup iperf3 -s > /dev/null 2>&1 &"
    sleep 1
    echo "[带宽] 启动 30 秒带宽测试..."
    echo "--- 全量同步期间带宽 (iperf3 30s) ---" >> "$RESULT_FILE"
    remote_exec "$MASTER_USER" "$MASTER_IP" "iperf3 -c $SLAVE_IP -t 30" >> "$RESULT_FILE" 2>&1 &
    IPERF_PID=$!
    
    # ★ 全量同步
    echo "[全量] 从客户端插入 1GB 数据触发 TCP 全量同步..."
    remote_exec "$CLIENT_USER" "$CLIENT_IP" "cd $CLIENT_PROJ_DIR && ./test_1G $MASTER_IP $MASTER_PORT" > /dev/null 2>&1 &
    FULL_PID=$!
    
    # 等待全量同步完成
    wait_for_full_sync "TCP"
    
    wait $FULL_PID 2>/dev/null || true
    wait $IPERF_PID 2>/dev/null || true
    remote_exec "$SLAVE_USER" "$SLAVE_IP" "pkill -9 iperf3 || true"
    
    # 收集性能
    collect_full_sync_stats
    
    # TCP 增量
    echo "--- TCP 增量 QPS ---" >> "$RESULT_FILE"
    echo "[执行] 正在从客户端插入增量数据..."
    remote_exec "$CLIENT_USER" "$CLIENT_IP" "cd $CLIENT_PROJ_DIR && ./test_transport $MASTER_IP $MASTER_PORT" >> "$RESULT_FILE" 2>&1
    echo "" >> "$RESULT_FILE"
    
    echo "[等待] 等待 5 秒让增量同步完成..."
    sleep 5
    
    stop_server "$SLAVE_USER" "$SLAVE_IP"
    stop_server "$MASTER_USER" "$MASTER_IP"
    
    echo "[完成] 第三轮测试结束"
}

# ==================== 主入口 ====================

echo "=============================================="
echo " KVStore 综合性能自动化压测启动"
echo " 报告路径: $RESULT_FILE"
echo "=============================================="
echo "生成时间: $(date '+%Y-%m-%d %H:%M:%S')" > "$RESULT_FILE"

remote_exec "$MASTER_USER" "$MASTER_IP" "mkdir -p $MASTER_PROJ_DIR" || true
remote_exec "$SLAVE_USER" "$SLAVE_IP" "mkdir -p $SLAVE_PROJ_DIR" || true

run_round_1
sleep 3
run_round_2
sleep 3
run_round_3

echo -e "\n=============================================="
echo " 所有测试完成！"
echo " 使用 cat $RESULT_FILE 快速查看结果"
echo "=============================================="

cat "$RESULT_FILE"