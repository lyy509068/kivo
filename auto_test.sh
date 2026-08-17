#!/bin/bash
# 一键主从同步性能测试脚本（终极修复版 - 本地回环插入 + 日志大小检查）

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

global_cleanup() {
    echo "[清理] 正在执行全局环境清理..."
    remote_exec "$MASTER_USER" "$MASTER_IP" "cd $MASTER_PROJ_DIR && sudo make unload_bpf > /dev/null 2>&1 || true"
    remote_exec "$MASTER_USER" "$MASTER_IP" "sudo pkill -9 server || true; sudo pkill -9 ebpf_relay || true; sudo pkill -9 iperf3 || true"
    remote_exec "$SLAVE_USER" "$SLAVE_IP" "sudo pkill -9 server || true; sudo pkill -9 iperf3 || true"
}
trap global_cleanup EXIT

clean_aof() {
    echo "[清理] 正在清理集群 AOF 文件..."
    remote_exec "$MASTER_USER" "$MASTER_IP" "rm -f $MASTER_PROJ_DIR/kvstore.aof" || true
    remote_exec "$SLAVE_USER" "$SLAVE_IP" "rm -f $SLAVE_PROJ_DIR/kvstore.aof" || true
}

clean_logs() {
    remote_exec "$MASTER_USER" "$MASTER_IP" "sudo rm -f /tmp/server_MASTER.log /tmp/server_SLAVE.log /tmp/ebpf_relay.log /tmp/iperf_master.log" || true
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
}

wait_port_free() {
    local user="$1" ip="$2" port="$3"
    while remote_exec "$user" "$ip" "ss -tln | grep -q ':$port'"; do
        echo "  等待 ${ip}:${port} 端口释放中..."
        sleep 2
    done
}

start_server() {
    local user="$1" ip="$2" role="$3" proj_dir="$4"
    remote_exec "$user" "$ip" "sudo pkill -9 server || true"
    wait_port_free "$user" "$ip" "$MASTER_PORT"
    
    echo "[启动] 正在启动 $role 服务器 (${user}@${ip})..."
    remote_exec "$user" "$ip" "cd $proj_dir && sudo sh -c 'nohup ./server config.conf > /tmp/server_${role}.log 2>&1 &'"
    
    sleep 2
    
    if remote_exec "$user" "$ip" "pgrep -x server > /dev/null"; then
        echo "[成功] $role 服务器已正常运行！"
    else
        echo "❌ [致命错误] $role 服务器启动失败！"
        echo "--- 日志片段 ---"
        remote_exec "$user" "$ip" "cat /tmp/server_${role}.log 2>/dev/null || echo '(日志为空)'"
        exit 1
    fi
}

stop_server() {
    local user="$1" ip="$2"
    remote_exec "$user" "$ip" "sudo pkill -9 server || true"
    echo "[停止] 已停止 ${user}@${ip} 上的 server"
}

load_ebpf() {
    remote_exec "$MASTER_USER" "$MASTER_IP" "cd $MASTER_PROJ_DIR && sudo make unload_bpf > /dev/null 2>&1 || true"
    remote_exec "$MASTER_USER" "$MASTER_IP" "cd $MASTER_PROJ_DIR && sudo make load_bpf > /dev/null 2>&1"
    echo "[eBPF] 已加载 eBPF 转发程序"
}

start_ebpf_relay() {
    remote_exec "$MASTER_USER" "$MASTER_IP" "sudo pkill -9 ebpf_relay || true"
    sleep 1
    remote_exec "$MASTER_USER" "$MASTER_IP" "cd $MASTER_PROJ_DIR && sudo sh -c 'nohup ./ebpf_relay > /tmp/ebpf_relay.log 2>&1 &'"
    echo "[eBPF] 已启动 eBPF Relay 进程"
}

stop_ebpf_relay() {
    remote_exec "$MASTER_USER" "$MASTER_IP" "sudo pkill -9 ebpf_relay || true"
}

collect_master_full_sync_perf() {
    local transport="$1"
    echo "--- 主端 $transport 全量同步日志 ---" >> "$RESULT_FILE"
    echo "  [1] RDMA_CONNECT 握手:" >> "$RESULT_FILE"
    remote_exec "$MASTER_USER" "$MASTER_IP" "grep -F 'Recieved RDMA_CONNECT' /tmp/server_MASTER.log 2>/dev/null" >> "$RESULT_FILE" || echo "    (未找到)" >> "$RESULT_FILE"
    echo "  [2] ACK 发送:" >> "$RESULT_FILE"
    remote_exec "$MASTER_USER" "$MASTER_IP" "grep -F 'RDMA_CONNECT_ACK sent' /tmp/server_MASTER.log 2>/dev/null" >> "$RESULT_FILE" || echo "    (未找到)" >> "$RESULT_FILE"
    echo "  [3] 全量同步性能:" >> "$RESULT_FILE"
    remote_exec "$MASTER_USER" "$MASTER_IP" "grep -E '\[(RDMA|TCP)\] (Sent|Received)' /tmp/server_MASTER.log 2>/dev/null" >> "$RESULT_FILE" || echo "    (未找到)" >> "$RESULT_FILE"
    echo "  [4] 错误/失败:" >> "$RESULT_FILE"
    remote_exec "$MASTER_USER" "$MASTER_IP" "grep -E 'Failed|Error|failed|error' /tmp/server_MASTER.log 2>/dev/null" >> "$RESULT_FILE" || true
    echo "" >> "$RESULT_FILE"
}

collect_iperf_stats() {
    echo "--- 全量同步期间网络带宽 (iperf3) ---" >> "$RESULT_FILE"
    local iperf_res=$(remote_exec "$MASTER_USER" "$MASTER_IP" "grep -E 'sender|receiver' /tmp/iperf_master.log 2>/dev/null" || true)
    if [ -n "$iperf_res" ]; then
        echo "$iperf_res" >> "$RESULT_FILE"
    else
        echo "(iperf3 测速无数据或未正常结束)" >> "$RESULT_FILE"
    fi
    echo "" >> "$RESULT_FILE"
}

wait_for_full_sync() {
    local transport="$1"
    local timeout=60
    local start=$(date +%s)
    
    echo "[等待] 等待主端 ${transport} 全量同步完成..."
    while true; do
        if remote_exec "$MASTER_USER" "$MASTER_IP" "grep -qF '[${transport}] Sent' /tmp/server_MASTER.log 2>/dev/null"; then
            echo "[完成] 主端 ${transport} 全量发送完成！"
            return 0
        fi
        
        if remote_exec "$MASTER_USER" "$MASTER_IP" "grep -qF '[${transport}] Empty sync completed' /tmp/server_MASTER.log 2>/dev/null"; then
            echo "⚠️ [警告] 主端报告 Empty sync completed（AOF 文件为空！）"
            return 1
        fi
        
        local elapsed=$(( $(date +%s) - start ))
        if [ $elapsed -gt $timeout ]; then
            echo "❌ [错误] 全量同步等待超时 (${timeout}秒)"
            echo "--- 主端完整日志 ---"
            remote_exec "$MASTER_USER" "$MASTER_IP" "cat /tmp/server_MASTER.log 2>/dev/null"
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
    
    global_cleanup
    clean_aof
    clean_logs
    
    write_config "$MASTER_USER" "$MASTER_IP" "$MASTER_PROJ_DIR" 2000 "ON" "OFF" "OFF" "OFF" "OFF" "OFF"
    start_server "$MASTER_USER" "$MASTER_IP" "MASTER" "$MASTER_PROJ_DIR"
    
    echo "--- 基准 QPS ---" >> "$RESULT_FILE"
    remote_exec "$CLIENT_USER" "$CLIENT_IP" "cd $CLIENT_PROJ_DIR && ./test_transport $MASTER_IP $MASTER_PORT" >> "$RESULT_FILE" 2>&1
    echo "" >> "$RESULT_FILE"
    
    stop_server "$MASTER_USER" "$MASTER_IP"
}

run_full_sync_with_bandwidth() {
    local transport="$1"
    local label="$2"
    
    echo -e "\n=============================================="
    echo " $label"
    echo "=============================================="
    echo -e "\n=================== $label ===================" >> "$RESULT_FILE"
    echo "全量同步: $transport" >> "$RESULT_FILE"
    echo "时间: $(date '+%Y-%m-%d %H:%M:%S')" >> "$RESULT_FILE"
    
    global_cleanup
    clean_aof
    clean_logs
    
    write_config "$MASTER_USER" "$MASTER_IP" "$MASTER_PROJ_DIR" 2000 "ON" "OFF" "OFF" "OFF" "MASTER" "$transport"
    write_config "$SLAVE_USER" "$SLAVE_IP" "$SLAVE_PROJ_DIR" 2000 "OFF" "OFF" "OFF" "OFF" "SLAVE" "$transport"
    
    start_server "$MASTER_USER" "$MASTER_IP" "MASTER" "$MASTER_PROJ_DIR"
    
    echo "[插入] 在主机本地回环插入 1GB 数据..."
    remote_exec "$MASTER_USER" "$MASTER_IP" "cd $MASTER_PROJ_DIR && ./test_1G" > /dev/null 2>&1 &
    INSERT_PID=$!
    wait $INSERT_PID 2>/dev/null || true
    echo "[完成] 1GB 数据插入完成"
    
    echo "[检查] 主机 AOF 文件大小:"
    remote_exec "$MASTER_USER" "$MASTER_IP" "ls -lh $MASTER_PROJ_DIR/kvstore.aof 2>/dev/null || echo 'AOF 文件不存在!'"
    
    # 记录主机 AOF 大小
    echo "--- 主机 AOF 文件大小 ---" >> "$RESULT_FILE"
    remote_exec "$MASTER_USER" "$MASTER_IP" "ls -lh $MASTER_PROJ_DIR/kvstore.aof 2>/dev/null" >> "$RESULT_FILE" || echo "AOF 文件不存在" >> "$RESULT_FILE"
    echo "" >> "$RESULT_FILE"
    
    # ★★★ 关键修复：先启动 iperf3，再启动从机触发全量同步 ★★★
    echo "[带宽] 启动 iperf3 服务端(从机)..."
    remote_exec "$SLAVE_USER" "$SLAVE_IP" "nohup iperf3 -s > /dev/null 2>&1 &"
    sleep 1
    
    echo "[带宽] 后台启动 iperf3 客户端(主机) 20秒..."
    remote_exec "$MASTER_USER" "$MASTER_IP" "nohup iperf3 -c $SLAVE_IP -t 20 > /tmp/iperf_master.log 2>&1 &"
    sleep 1
    
    echo "[启动] 启动从机服务器（触发 ${transport} 全量同步）..."
    start_server "$SLAVE_USER" "$SLAVE_IP" "SLAVE" "$SLAVE_PROJ_DIR"
    
    echo "[等待] 等待 ${transport} 全量同步完成..."
    wait_for_full_sync "$transport" || true
    
    # 等待 iperf3 完成（20秒）
    echo "[带宽] 等待 iperf3 测速完成（20秒）..."
    sleep 20
    
    # 收集结果
    collect_master_full_sync_perf "$transport"
    collect_iperf_stats
    
    remote_exec "$SLAVE_USER" "$SLAVE_IP" "sudo pkill -9 iperf3 || true"
    
    # 记录从机 AOF 大小
    echo "--- 从机 AOF 文件大小 ---" >> "$RESULT_FILE"
    remote_exec "$SLAVE_USER" "$SLAVE_IP" "ls -lh $SLAVE_PROJ_DIR/kvstore.aof 2>/dev/null" >> "$RESULT_FILE" || echo "AOF 文件不存在" >> "$RESULT_FILE"
    echo "" >> "$RESULT_FILE"
    
    stop_server "$SLAVE_USER" "$SLAVE_IP"
    stop_server "$MASTER_USER" "$MASTER_IP"
}


run_round_2() {
    run_full_sync_with_bandwidth "RDMA" "第 2 轮：RDMA 全量 + eBPF 增量转发"
    
    # 重新启动主从做增量测试
    global_cleanup
    clean_aof
    clean_logs
    
    write_config "$MASTER_USER" "$MASTER_IP" "$MASTER_PROJ_DIR" 2000 "OFF" "OFF" "OFF" "OFF" "MASTER" "RDMA"
    write_config "$SLAVE_USER" "$SLAVE_IP" "$SLAVE_PROJ_DIR" 2000 "OFF" "OFF" "OFF" "OFF" "SLAVE" "RDMA"
    
    start_server "$MASTER_USER" "$MASTER_IP" "MASTER" "$MASTER_PROJ_DIR"
    start_server "$SLAVE_USER" "$SLAVE_IP" "SLAVE" "$SLAVE_PROJ_DIR"
    
    echo "[eBPF] 加载 eBPF..."
    load_ebpf
    start_ebpf_relay
    
    echo "--- eBPF 增量 QPS ---" >> "$RESULT_FILE"
    remote_exec "$CLIENT_USER" "$CLIENT_IP" "cd $CLIENT_PROJ_DIR && ./test_transport $MASTER_IP $MASTER_PORT" >> "$RESULT_FILE" 2>&1
    echo "" >> "$RESULT_FILE"
    
    sleep 5
    stop_ebpf_relay
    stop_server "$SLAVE_USER" "$SLAVE_IP"
    stop_server "$MASTER_USER" "$MASTER_IP"
}

run_round_3() {
    run_full_sync_with_bandwidth "TCP" "第 3 轮：TCP 全量 + TCP 增量转发"
    
    # 重新启动主从做增量测试
    global_cleanup
    clean_aof
    clean_logs
    
    write_config "$MASTER_USER" "$MASTER_IP" "$MASTER_PROJ_DIR" 2000 "OFF" "OFF" "OFF" "OFF" "MASTER" "TCP"
    write_config "$SLAVE_USER" "$SLAVE_IP" "$SLAVE_PROJ_DIR" 2000 "OFF" "OFF" "OFF" "OFF" "SLAVE" "TCP"
    
    start_server "$MASTER_USER" "$MASTER_IP" "MASTER" "$MASTER_PROJ_DIR"
    start_server "$SLAVE_USER" "$SLAVE_IP" "SLAVE" "$SLAVE_PROJ_DIR"
    
    echo "--- TCP 增量 QPS ---" >> "$RESULT_FILE"
    remote_exec "$CLIENT_USER" "$CLIENT_IP" "cd $CLIENT_PROJ_DIR && ./test_transport $MASTER_IP $MASTER_PORT" >> "$RESULT_FILE" 2>&1
    echo "" >> "$RESULT_FILE"
    
    sleep 5
    stop_server "$SLAVE_USER" "$SLAVE_IP"
    stop_server "$MASTER_USER" "$MASTER_IP"
}

# ==================== 主入口 ====================

echo "=============================================="
echo " KVStore 综合性能自动化压测启动"
echo "=============================================="
echo "测试启动时间: $(date '+%Y-%m-%d %H:%M:%S')" > "$RESULT_FILE"

run_round_1
sleep 3
run_round_2
sleep 3
run_round_3

trap - EXIT
global_cleanup

echo -e "\n=============================================="
echo " 🎉 所有测试完成！"
echo " 使用 cat $RESULT_FILE 查看完整报告"
echo "=============================================="

cat "$RESULT_FILE"