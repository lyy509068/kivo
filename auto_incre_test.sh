#!/bin/bash
# 一键主从同步性能测试脚本
# 使用方式: ./auto_incre_test.sh

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

# ==================== 三轮核心逻辑 ====================

run_round_1() {
    echo -e "\n=============================================="
    echo " 第 1 轮：基准测试 (无同步，无 eBPF)"
    echo "=============================================="
    echo -e "\n=================== 第一轮：基准测试 ===================" >> "$RESULT_FILE"
    
    clean_aof
    clean_logs
    
    write_config "$MASTER_USER" "$MASTER_IP" "$MASTER_PROJ_DIR" 2000 "ON" "OFF" "OFF" "OFF" "OFF" "OFF"
    start_server "$MASTER_USER" "$MASTER_IP" "MASTER" "$MASTER_PROJ_DIR"
    
    echo "[执行] 正在从客户端插入数据..."
    remote_exec "$CLIENT_USER" "$CLIENT_IP" "cd $CLIENT_PROJ_DIR && ./test_transport $MASTER_IP $MASTER_PORT" >> "$RESULT_FILE" 2>&1
    echo "" >> "$RESULT_FILE"
    
    stop_server "$MASTER_USER" "$MASTER_IP"
    echo "[完成] 第一轮测试结束"
}

run_round_2() {
    echo -e "\n=============================================="
    echo " 第 2 轮：eBPF 转发 (主从同步)"
    echo "=============================================="
    echo -e "\n=================== 第二轮：eBPF 转发 ===================" >> "$RESULT_FILE"
    
    clean_aof
    clean_logs
    
    write_config "$MASTER_USER" "$MASTER_IP" "$MASTER_PROJ_DIR" 2000 "OFF" "OFF" "OFF" "OFF" "MASTER" "RDMA"
    write_config "$SLAVE_USER" "$SLAVE_IP" "$SLAVE_PROJ_DIR" 2000 "OFF" "OFF" "OFF" "OFF" "SLAVE" "RDMA"
    
    start_server "$MASTER_USER" "$MASTER_IP" "MASTER" "$MASTER_PROJ_DIR"
    start_server "$SLAVE_USER" "$SLAVE_IP" "SLAVE" "$SLAVE_PROJ_DIR"

    # 等待全量同步完成（传空文件，3秒足够）
    echo "[等待] 等待全量同步完成 (3秒)..."
    sleep 3
    
    # 加载 eBPF
    load_ebpf
    start_ebpf_relay
    
    # 插入增量数据并记录QPS
    echo "[执行] 正在从客户端插入增量数据..."
    remote_exec "$CLIENT_USER" "$CLIENT_IP" "cd $CLIENT_PROJ_DIR && ./test_transport $MASTER_IP $MASTER_PORT" >> "$RESULT_FILE" 2>&1
    echo "" >> "$RESULT_FILE"
    
    # ★ 客户端插入完成后，等待5秒让增量同步完成
    echo "[等待] 客户端插入完成，等待 5 秒让增量同步完成..."
    sleep 5
    
    # 停止 eBPF
    stop_ebpf_relay
    unload_ebpf
    
    stop_server "$SLAVE_USER" "$SLAVE_IP"
    stop_server "$MASTER_USER" "$MASTER_IP"
    
    echo "[完成] 第二轮测试结束"
}

run_round_3() {
    echo -e "\n=============================================="
    echo " 第 3 轮：TCP 网络转发 (主从同步)"
    echo "=============================================="
    echo -e "\n=================== 第三轮：TCP 网络转发 ===================" >> "$RESULT_FILE"
    
    clean_aof
    clean_logs
    
    write_config "$MASTER_USER" "$MASTER_IP" "$MASTER_PROJ_DIR" 2000 "OFF" "OFF" "OFF" "OFF" "MASTER" "TCP"
    write_config "$SLAVE_USER" "$SLAVE_IP" "$SLAVE_PROJ_DIR" 2000 "OFF" "OFF" "OFF" "OFF" "SLAVE" "TCP"
    
    start_server "$MASTER_USER" "$MASTER_IP" "MASTER" "$MASTER_PROJ_DIR"
    start_server "$SLAVE_USER" "$SLAVE_IP" "SLAVE" "$SLAVE_PROJ_DIR"
    
    # 等待全量同步完成（传空文件，3秒足够）
    echo "[等待] 等待全量同步完成 (3秒)..."
    sleep 3
    
    # 插入增量数据并记录QPS
    echo "[执行] 正在从客户端插入增量数据..."
    remote_exec "$CLIENT_USER" "$CLIENT_IP" "cd $CLIENT_PROJ_DIR && ./test_transport $MASTER_IP $MASTER_PORT" >> "$RESULT_FILE" 2>&1
    echo "" >> "$RESULT_FILE"
    
    # ★ 客户端插入完成后，等待5秒让增量同步完成
    echo "[等待] 客户端插入完成，等待 5 秒让增量同步完成..."
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