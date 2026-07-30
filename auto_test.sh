#!/bin/bash
# 一键主从同步性能测试脚本
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
SCP_OPTS="-o StrictHostKeyChecking=no"

RESULT_DIR="./results_$(date +%Y%m%d_%H%M%S)"
mkdir -p "$RESULT_DIR"
# 所有结果只输出到这一个文件中
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

set_config() {
    local user="$1" ip="$2" proj_dir="$3" replication="$4" transport="$5"
    remote_exec "$user" "$ip" "sed -i 's/^replication.*/replication $replication/' $proj_dir/config.conf"
    remote_exec "$user" "$ip" "sed -i 's/^transport.*/transport $transport/' $proj_dir/config.conf"
    echo "[配置] ${user}@${ip}: replication=$replication transport=$transport"
}

start_server() {
    local user="$1" ip="$2" role="$3" proj_dir="$4"
    remote_exec "$user" "$ip" "sudo pkill -9 server || true"
    sleep 1
    
    echo "[启动] 正在启动 $role 服务器 (${user}@${ip})..."
    remote_exec "$user" "$ip" "cd $proj_dir && sudo -b nohup ./server config.conf </dev/null >/tmp/server_${role}.log 2>&1"
    
    # 【健康检查】循环检测端口是否存活，防止因OOM直接Killed却盲目往下执行
    local retries=5
    local started=false
    while [ $retries -gt 0 ]; do
        if remote_exec "$user" "$ip" "nc -z -w 1 127.0.0.1 $MASTER_PORT 2>/dev/null"; then
            started=true
            break
        fi
        sleep 1
        retries=$((retries - 1))
    done

    if [ "$started" = true ]; then
        echo "[成功] $role 服务器已正常运行！"
    else
        echo "❌ [致命错误] $role 服务器启动失败或被Killed！"
        echo "------ 最新报错日志片段 ------"
        remote_exec "$user" "$ip" "tail -n 15 /tmp/server_${role}.log"
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
    remote_exec "$MASTER_USER" "$MASTER_IP" "cd $MASTER_PROJ_DIR && sudo make load_bpf > /dev/null 2>&1"
    echo "[eBPF] 已加载 eBPF 转发程序"
}
unload_ebpf() {
    remote_exec "$MASTER_USER" "$MASTER_IP" "cd $MASTER_PROJ_DIR && sudo make unload_bpf > /dev/null 2>&1 || true"
    echo "[eBPF] 已卸载 eBPF 转发程序"
}
start_ebpf_relay() {
    remote_exec "$MASTER_USER" "$MASTER_IP" "sudo pkill -9 ebpf_relay || true"
    remote_exec "$MASTER_USER" "$MASTER_IP" "cd $MASTER_PROJ_DIR && sudo -b nohup ./ebpf_relay </dev/null >/tmp/ebpf_relay.log 2>&1"
    sleep 1
    echo "[eBPF] 已启动 eBPF Relay 进程"
}
stop_ebpf_relay() {
    remote_exec "$MASTER_USER" "$MASTER_IP" "sudo pkill -9 ebpf_relay || true"
}

# 动作函数
insert_full_data() {
    echo "[执行] 正在插入第一轮全量数据 (Localhost)..."
    remote_exec "$MASTER_USER" "$MASTER_IP" "cd $MASTER_PROJ_DIR && ./test_1G > /dev/null 2>&1"
    echo "[完成] 全量数据插入完毕"
}

run_iperf3_client() {
    echo "[执行] 正在运行 iperf3 测速 (持续10秒)..."
    remote_exec "$MASTER_USER" "$MASTER_IP" "iperf3 -c $SLAVE_IP -t 10" >> "$RESULT_FILE"
}

wait_for_sync_done() {
    local timeout=120 start_time=$(date +%s)
    echo "[等待] 正在等待全量同步完成..."
    while true; do
        if remote_exec "$SLAVE_USER" "$SLAVE_IP" "grep -q 'AOF reload' /tmp/server_SLAVE.log 2>/dev/null"; then
            echo "[完成] 从端 AOF 加载完成，全量同步结束"
            break
        fi
        if remote_exec "$MASTER_USER" "$MASTER_IP" "grep -q 'SYNC_DONE' /tmp/server_MASTER.log 2>/dev/null"; then
            echo "[完成] 主端收到 SYNC_DONE，全量同步结束"
            break
        fi
        [ $(( $(date +%s) - start_time )) -gt $timeout ] && echo "⚠️ 全量同步等待超时！" && break
        sleep 2
    done
}


# ==================== 三轮核心逻辑 ====================

run_round_1() {
    echo -e "\n=============================================="
    echo " 第 1 轮：基准测试 (空闲带宽 + 基准QPS)"
    echo "=============================================="
    echo -e "\n=================== 第一轮：基准测试 ===================" >> "$RESULT_FILE"
    clean_aof
    
    # 1. 打开主机
    set_config "$MASTER_USER" "$MASTER_IP" "$MASTER_PROJ_DIR" "MASTER" "RDMA"
    start_server "$MASTER_USER" "$MASTER_IP" "MASTER" "$MASTER_PROJ_DIR"
    
    # 2. 回环地址插入第一轮数据
    insert_full_data

    # 3. 测试空闲时带宽 (从机尚未启动KVStore)
    remote_exec "$SLAVE_USER" "$SLAVE_IP" "pkill -9 iperf3 || true"
    remote_exec "$SLAVE_USER" "$SLAVE_IP" "nohup iperf3 -s </dev/null >/dev/null 2>&1 &"
    sleep 1
    echo "--- 1. 空闲网络带宽 ---" >> "$RESULT_FILE"
    run_iperf3_client
    remote_exec "$SLAVE_USER" "$SLAVE_IP" "pkill -9 iperf3 || true"

    # 4. 130插入第二波数据 (记录QPS)
    echo "--- 2. 基准增量 QPS (无从机负担) ---" >> "$RESULT_FILE"
    echo "[执行] 正在客户端(130)插入第二波增量数据..."
    remote_exec "$CLIENT_USER" "$CLIENT_IP" "cd $CLIENT_PROJ_DIR && ./test_transport $MASTER_IP $MASTER_PORT" >> "$RESULT_FILE"
    
    stop_server "$MASTER_USER" "$MASTER_IP"
}

run_round_2() {
    echo -e "\n=============================================="
    echo " 第 2 轮：RDMA 环境 (全量性能 + 带宽 + eBPF增量)"
    echo "=============================================="
    echo -e "\n=================== 第二轮：RDMA + eBPF ===================" >> "$RESULT_FILE"
    clean_aof
    
    # 1. 打开主机，回环插入数据
    set_config "$MASTER_USER" "$MASTER_IP" "$MASTER_PROJ_DIR" "MASTER" "RDMA"
    start_server "$MASTER_USER" "$MASTER_IP" "MASTER" "$MASTER_PROJ_DIR"
    insert_full_data

    # 2. 准备 iperf3 服务器和从机配置
    remote_exec "$SLAVE_USER" "$SLAVE_IP" "pkill -9 iperf3 || true"
    remote_exec "$SLAVE_USER" "$SLAVE_IP" "nohup iperf3 -s </dev/null >/dev/null 2>&1 &"
    set_config "$SLAVE_USER" "$SLAVE_IP" "$SLAVE_PROJ_DIR" "SLAVE" "RDMA"
    
    # 3. 打开从机 (触发 RDMA 全量同步) 并立刻测带宽
    start_server "$SLAVE_USER" "$SLAVE_IP" "SLAVE" "$SLAVE_PROJ_DIR"
    echo "--- 1. RDMA全量同步期间带宽 ---" >> "$RESULT_FILE"
    run_iperf3_client
    remote_exec "$SLAVE_USER" "$SLAVE_IP" "pkill -9 iperf3 || true"

    # 4. 等待全量同步完成，记录性能
    wait_for_sync_done
    echo "--- 2. RDMA全量同步性能数据 ---" >> "$RESULT_FILE"
    remote_exec "$SLAVE_USER" "$SLAVE_IP" "grep 'Perf RDMA' /tmp/server_SLAVE.log 2>/dev/null" >> "$RESULT_FILE" || true

    # 5. 加载 eBPF 准备转发增量
    load_ebpf
    start_ebpf_relay

    # 6. 130 插入第二波数据 (记录QPS)
    echo "--- 3. eBPF 增量转发 QPS ---" >> "$RESULT_FILE"
    echo "[执行] 正在客户端(130)插入第二波增量数据..."
    remote_exec "$CLIENT_USER" "$CLIENT_IP" "cd $CLIENT_PROJ_DIR && ./test_transport $MASTER_IP $MASTER_PORT" >> "$RESULT_FILE"

    stop_ebpf_relay
    unload_ebpf
    stop_server "$SLAVE_USER" "$SLAVE_IP"
    stop_server "$MASTER_USER" "$MASTER_IP"
}

run_round_3() {
    echo -e "\n=============================================="
    echo " 第 3 轮：TCP 环境 (全量性能 + 带宽 + TCP增量)"
    echo "=============================================="
    echo -e "\n=================== 第三轮：TCP ===================" >> "$RESULT_FILE"
    clean_aof
    
    # 1. 打开主机，回环插入数据
    set_config "$MASTER_USER" "$MASTER_IP" "$MASTER_PROJ_DIR" "MASTER" "TCP"
    start_server "$MASTER_USER" "$MASTER_IP" "MASTER" "$MASTER_PROJ_DIR"
    insert_full_data

    # 2. 准备 iperf3 服务器和从机配置
    remote_exec "$SLAVE_USER" "$SLAVE_IP" "pkill -9 iperf3 || true"
    remote_exec "$SLAVE_USER" "$SLAVE_IP" "nohup iperf3 -s </dev/null >/dev/null 2>&1 &"
    set_config "$SLAVE_USER" "$SLAVE_IP" "$SLAVE_PROJ_DIR" "SLAVE" "TCP"
    
    # 3. 打开从机 (触发 TCP 全量同步) 并立刻测带宽
    start_server "$SLAVE_USER" "$SLAVE_IP" "SLAVE" "$SLAVE_PROJ_DIR"
    echo "--- 1. TCP全量同步期间带宽 ---" >> "$RESULT_FILE"
    run_iperf3_client
    remote_exec "$SLAVE_USER" "$SLAVE_IP" "pkill -9 iperf3 || true"

    # 4. 等待全量同步完成，记录性能
    wait_for_sync_done
    echo "--- 2. TCP全量同步性能数据 ---" >> "$RESULT_FILE"
    remote_exec "$SLAVE_USER" "$SLAVE_IP" "grep 'Perf TCP' /tmp/server_SLAVE.log 2>/dev/null" >> "$RESULT_FILE" || true

    # 5. TCP模式下服务器自动处理转发，直接让 130 插入第二波数据
    echo "--- 3. TCP 增量转发 QPS ---" >> "$RESULT_FILE"
    echo "[执行] 正在客户端(130)插入第二波增量数据..."
    remote_exec "$CLIENT_USER" "$CLIENT_IP" "cd $CLIENT_PROJ_DIR && ./test_transport $MASTER_IP $MASTER_PORT" >> "$RESULT_FILE"

    stop_server "$SLAVE_USER" "$SLAVE_IP"
    stop_server "$MASTER_USER" "$MASTER_IP"
    clean_aof
}

# ==================== 主入口 ====================

echo "=============================================="
echo " KVStore 综合性能自动化压测启动"
echo " 报告路径: $RESULT_FILE"
echo "=============================================="
echo "生成时间: $(date '+%Y-%m-%d %H:%M:%S')" > "$RESULT_FILE"

# 依次执行三轮测试
run_round_1
sleep 3
run_round_2
sleep 3
run_round_3

echo -e "\n=============================================="
echo " 所有测试完成！"
echo " 使用 cat $RESULT_FILE 快速查看结果"
echo "=============================================="