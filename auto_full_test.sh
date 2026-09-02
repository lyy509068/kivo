#!/bin/bash
# 全量同步性能测试脚本
# 使用方式: ./auto_full_test.sh

set -e

# ==================== 配置 ====================
MASTER_IP="192.168.88.128"
SLAVE_IP="192.168.88.130"
CLIENT_IP="192.168.88.129"
MASTER_PORT=2000

MASTER_PROJ_DIR="/home/lyy/course/project/KVstore/9.1-kvstore"
SLAVE_PROJ_DIR="/home/c2/project/KVstore8/9.1-kvstore"
CLIENT_PROJ_DIR="/home/c1/project"

MASTER_USER="lyy"
SLAVE_USER="c2"
CLIENT_USER="c1"

SSH_OPTS="-n -o StrictHostKeyChecking=no"

RESULT_DIR="./full_sync_results_$(date +%Y%m%d_%H%M%S)"
mkdir -p "$RESULT_DIR"
RESULT_FILE="$RESULT_DIR/full_sync_results.txt"

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
    remote_exec "$MASTER_USER" "$MASTER_IP" "sudo rm -f /tmp/server_MASTER.log /tmp/server_SLAVE.log" || true
    remote_exec "$SLAVE_USER" "$SLAVE_IP" "sudo rm -f /tmp/server_MASTER.log /tmp/server_SLAVE.log" || true
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

start_server() {
    local user="$1" ip="$2" role="$3" proj_dir="$4"
    remote_exec "$user" "$ip" "sudo pkill -9 server || true"
    sleep 1
    
    echo "[启动] 正在启动 $role 服务器 (${user}@${ip})..."
    remote_exec "$user" "$ip" "cd $proj_dir && sudo sh -c 'nohup ./server config.conf > /tmp/server_${role}.log 2>&1 &'"
    
    sleep 2
    
    if remote_exec "$user" "$ip" "pgrep -x server > /dev/null"; then
        echo "[成功] $role 服务器已正常运行！"
    else
        echo "❌ [致命错误] $role 服务器启动失败！"
        remote_exec "$user" "$ip" "cat /tmp/server_${role}.log 2>/dev/null || echo '(日志为空)'"
        exit 1
    fi
}

stop_server() {
    local user="$1" ip="$2"
    remote_exec "$user" "$ip" "sudo pkill -9 server || true"
    echo "[停止] 已停止 ${user}@${ip} 上的 server"
}

wait_for_full_sync() {
    local transport="$1"
    local timeout=120
    local start=$(date +%s)
    
    echo "[等待] 等待主端 ${transport} 全量同步完成..."
    
    while true; do
        if remote_exec "$MASTER_USER" "$MASTER_IP" "grep -qE '\[(RDMA|TCP)\] (Sent|Received)' /tmp/server_MASTER.log 2>/dev/null"; then
            echo "[完成] 主端 ${transport} 全量发送完成！"
            return 0
        fi
        
        if remote_exec "$MASTER_USER" "$MASTER_IP" "grep -qE '\[(RDMA|TCP)\] Empty sync completed' /tmp/server_MASTER.log 2>/dev/null"; then
            echo "⚠️ 主端报告 Empty sync completed（AOF 文件为空！）"
            return 1
        fi
        
        local elapsed=$(( $(date +%s) - start ))
        if [ $elapsed -gt $timeout ]; then
            echo "❌ 全量同步等待超时 (${timeout}秒)"
            remote_exec "$MASTER_USER" "$MASTER_IP" "cat /tmp/server_MASTER.log 2>/dev/null"
            return 1
        fi
        sleep 1
    done
}

# ==================== 第一轮：基准带宽 ====================
run_round_1() {
    echo -e "\n=============================================="
    echo " 第 1 轮：基准带宽测试 (无服务器，无文件传输)"
    echo "=============================================="
    echo -e "\n=================== 第一轮：基准带宽 ===================" >> "$RESULT_FILE"
    echo "时间: $(date '+%Y-%m-%d %H:%M:%S')" >> "$RESULT_FILE"
    
    # 清理环境
    remote_exec "$MASTER_USER" "$MASTER_IP" "sudo pkill -9 server || true; sudo pkill -9 iperf3 || true"
    remote_exec "$SLAVE_USER" "$SLAVE_IP" "sudo pkill -9 server || true; sudo pkill -9 iperf3 || true"
    
    # 启动 iperf3 服务端
    echo "[带宽] 启动 iperf3 服务端(从机)..."
    remote_exec "$SLAVE_USER" "$SLAVE_IP" "nohup iperf3 -s > /dev/null 2>&1 &"
    sleep 2
    
    # 启动 iperf3 客户端
    echo "[带宽] 启动 iperf3 客户端(主机) 20秒..."
    echo "--- 基准网络带宽 (iperf3 20s) ---" >> "$RESULT_FILE"
    remote_exec "$MASTER_USER" "$MASTER_IP" "iperf3 -c $SLAVE_IP -t 20" >> "$RESULT_FILE" 2>&1
    echo "" >> "$RESULT_FILE"
    
    remote_exec "$SLAVE_USER" "$SLAVE_IP" "sudo pkill -9 iperf3 || true"
    echo "[完成] 第一轮测试结束"
}

# ==================== 第二轮：RDMA 全量同步 ====================
run_round_2() {
    echo -e "\n=============================================="
    echo " 第 2 轮：RDMA 全量同步"
    echo "=============================================="
    echo -e "\n=================== 第二轮：RDMA 全量同步 ===================" >> "$RESULT_FILE"
    echo "时间: $(date '+%Y-%m-%d %H:%M:%S')" >> "$RESULT_FILE"
    
    clean_aof
    clean_logs
    
    # 清理进程
    remote_exec "$MASTER_USER" "$MASTER_IP" "sudo pkill -9 server || true; sudo pkill -9 iperf3 || true"
    remote_exec "$SLAVE_USER" "$SLAVE_IP" "sudo pkill -9 server || true; sudo pkill -9 iperf3 || true"
    
    # 配置：主机 MASTER + RDMA，从机 SLAVE + RDMA
    write_config "$MASTER_USER" "$MASTER_IP" "$MASTER_PROJ_DIR" 2000 "ON" "OFF" "OFF" "OFF" "MASTER" "RDMA"
    write_config "$SLAVE_USER" "$SLAVE_IP" "$SLAVE_PROJ_DIR" 2000 "OFF" "OFF" "OFF" "OFF" "SLAVE" "RDMA"
    
    # 启动主机
    echo "[启动] 启动主机服务器..."
    start_server "$MASTER_USER" "$MASTER_IP" "MASTER" "$MASTER_PROJ_DIR"
    
    # 插入 1GB 数据
    echo "[插入] 在主机本地回环插入 1GB 数据..."
    remote_exec "$MASTER_USER" "$MASTER_IP" "cd $MASTER_PROJ_DIR && ./test_1G" > /dev/null 2>&1 &
    INSERT_PID=$!
    wait $INSERT_PID 2>/dev/null || true
    echo "[完成] 1GB 数据插入完成"
    
    # 检查 AOF 大小
    echo "[检查] 主机 AOF 文件大小:"
    remote_exec "$MASTER_USER" "$MASTER_IP" "ls -lh $MASTER_PROJ_DIR/kvstore.aof 2>/dev/null || echo 'AOF 文件不存在!'"
    
    # 启动从机触发全量同步
    echo "[启动] 启动从机服务器，触发 RDMA 全量同步..."
    start_server "$SLAVE_USER" "$SLAVE_IP" "SLAVE" "$SLAVE_PROJ_DIR"
    
    # 等待全量同步完成
    wait_for_full_sync "RDMA" || true
    
    # 抓取主机日志
    echo "--- 主机 RDMA 全量同步输出 ---" >> "$RESULT_FILE"
    remote_exec "$MASTER_USER" "$MASTER_IP" "grep -E '\[RDMA\] (Sent|Received|Empty)' /tmp/server_MASTER.log 2>/dev/null" >> "$RESULT_FILE" || echo "(未找到)" >> "$RESULT_FILE"
    echo "" >> "$RESULT_FILE"
    
    # 检查从机 AOF 大小
    echo "--- 从机 AOF 文件大小 ---" >> "$RESULT_FILE"
    remote_exec "$SLAVE_USER" "$SLAVE_IP" "ls -lh $SLAVE_PROJ_DIR/kvstore.aof 2>/dev/null" >> "$RESULT_FILE" || echo "AOF 文件不存在" >> "$RESULT_FILE"
    echo "" >> "$RESULT_FILE"
    
    stop_server "$SLAVE_USER" "$SLAVE_IP"
    stop_server "$MASTER_USER" "$MASTER_IP"
    echo "[完成] 第二轮测试结束"
}

# ==================== 第三轮：TCP 全量同步 ====================
run_round_3() {
    echo -e "\n=============================================="
    echo " 第 3 轮：TCP 全量同步"
    echo "=============================================="
    echo -e "\n=================== 第三轮：TCP 全量同步 ===================" >> "$RESULT_FILE"
    echo "时间: $(date '+%Y-%m-%d %H:%M:%S')" >> "$RESULT_FILE"
    
    clean_aof
    clean_logs
    
    # 清理进程
    remote_exec "$MASTER_USER" "$MASTER_IP" "sudo pkill -9 server || true; sudo pkill -9 iperf3 || true"
    remote_exec "$SLAVE_USER" "$SLAVE_IP" "sudo pkill -9 server || true; sudo pkill -9 iperf3 || true"
    
    # 配置：主机 MASTER + TCP，从机 SLAVE + TCP
    write_config "$MASTER_USER" "$MASTER_IP" "$MASTER_PROJ_DIR" 2000 "ON" "OFF" "OFF" "OFF" "MASTER" "TCP"
    write_config "$SLAVE_USER" "$SLAVE_IP" "$SLAVE_PROJ_DIR" 2000 "OFF" "OFF" "OFF" "OFF" "SLAVE" "TCP"
    
    # 启动主机
    echo "[启动] 启动主机服务器..."
    start_server "$MASTER_USER" "$MASTER_IP" "MASTER" "$MASTER_PROJ_DIR"
    
    # 插入 1GB 数据
    echo "[插入] 在主机本地回环插入 1GB 数据..."
    remote_exec "$MASTER_USER" "$MASTER_IP" "cd $MASTER_PROJ_DIR && ./test_1G" > /dev/null 2>&1 &
    INSERT_PID=$!
    wait $INSERT_PID 2>/dev/null || true
    echo "[完成] 1GB 数据插入完成"
    
    # 检查 AOF 大小
    echo "[检查] 主机 AOF 文件大小:"
    remote_exec "$MASTER_USER" "$MASTER_IP" "ls -lh $MASTER_PROJ_DIR/kvstore.aof 2>/dev/null || echo 'AOF 文件不存在!'"
    
    # 启动从机触发全量同步
    echo "[启动] 启动从机服务器，触发 TCP 全量同步..."
    start_server "$SLAVE_USER" "$SLAVE_IP" "SLAVE" "$SLAVE_PROJ_DIR"
    
    # 等待全量同步完成
    wait_for_full_sync "TCP" || true
    
    # 抓取主机日志
    echo "--- 主机 TCP 全量同步输出 ---" >> "$RESULT_FILE"
    remote_exec "$MASTER_USER" "$MASTER_IP" "grep -E '\[TCP\] (Sent|Received|Empty)' /tmp/server_MASTER.log 2>/dev/null" >> "$RESULT_FILE" || echo "(未找到)" >> "$RESULT_FILE"
    echo "" >> "$RESULT_FILE"
    
    # 检查从机 AOF 大小
    echo "--- 从机 AOF 文件大小 ---" >> "$RESULT_FILE"
    remote_exec "$SLAVE_USER" "$SLAVE_IP" "ls -lh $SLAVE_PROJ_DIR/kvstore.aof 2>/dev/null" >> "$RESULT_FILE" || echo "AOF 文件不存在" >> "$RESULT_FILE"
    echo "" >> "$RESULT_FILE"
    
    stop_server "$SLAVE_USER" "$SLAVE_IP"
    stop_server "$MASTER_USER" "$MASTER_IP"
    echo "[完成] 第三轮测试结束"
}

# ==================== 主入口 ====================

echo "=============================================="
echo " 全量同步性能测试"
echo " 报告路径: $RESULT_FILE"
echo "=============================================="
echo "测试启动时间: $(date '+%Y-%m-%d %H:%M:%S')" > "$RESULT_FILE"

run_round_1
sleep 3
run_round_2
sleep 3
run_round_3

echo -e "\n=============================================="
echo " 🎉 所有测试完成！"
echo " 使用 cat $RESULT_FILE 查看完整报告"
echo "=============================================="

cat "$RESULT_FILE"