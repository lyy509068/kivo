#!/usr/bin/env bash
# =============================================================================
# 内存池性能对比自动化测试脚本 (全自动静默版)
# =============================================================================

# ========================== 配置参数 ==========================
SERVER_BIN="./server"
SERVER_CONF="config.conf"
CLIENT_BIN="./test_mempool"
JEMALLOC_LIB="/usr/lib/x86_64-linux-gnu/libjemalloc.so.2"

RUNS=3          # <--- 修改这里：每种分配器测试次数
OPS=1000000
RESULT_FILE="mempool_results.txt"
LOG_FILE="mempool_bench.log"

# ========================== 日志函数 ==========================
log_info()  { echo "[$(date '+%H:%M:%S')] [INFO] $1" >> "$LOG_FILE"; }
log_warn()  { echo "[$(date '+%H:%M:%S')] [WARN] $1" >> "$LOG_FILE"; }
log_error() { echo "[$(date '+%H:%M:%S')] [ERROR] $1" >> "$LOG_FILE"; }

# ========================== 工具函数 ==========================

check_jemalloc() {
    if [ ! -f "$JEMALLOC_LIB" ]; then
        log_warn "Jemalloc 库未找到，尝试安装..."
        sudo apt-get update -qq 2>> "$LOG_FILE"
        sudo apt-get install -y -qq libjemalloc2 2>> "$LOG_FILE"
        if [ ! -f "$JEMALLOC_LIB" ]; then
            log_error "Jemalloc 安装失败，跳过测试"
            return 1
        fi
        log_info "Jemalloc 安装成功"
    fi
    return 0
}

wait_for_server() {
    local max_wait=10
    local waited=0
    while [ $waited -lt $max_wait ]; do
        if pgrep -x server > /dev/null 2>&1; then
            sleep 1
            return 0
        fi
        sleep 1
        ((waited++))
    done
    return 1
}

stop_server() {
    if pgrep -x server > /dev/null 2>&1; then
        sudo pkill -x server 2>/dev/null || true
        sleep 2
        if pgrep -x server > /dev/null 2>&1; then
            sudo pkill -9 -x server 2>/dev/null || true
            sleep 1
        fi
    fi
}

# ========================== 关键修改：适配你的配置格式 ==========================

set_mempool_config() {
    local val=$1  # ON 或 OFF
    
    if grep -q "mempool " "$SERVER_CONF" 2>/dev/null; then
        # 格式: mempool ON 或 mempool OFF
        sed -i "s/mempool ON/mempool $val/" "$SERVER_CONF" 2>> "$LOG_FILE"
        sed -i "s/mempool OFF/mempool $val/" "$SERVER_CONF" 2>> "$LOG_FILE"
    elif grep -q "mempool=" "$SERVER_CONF" 2>/dev/null; then
        # 格式: mempool=1 或 mempool=0
        local num_val=0
        [ "$val" = "ON" ] && num_val=1
        sed -i "s/mempool=[01]/mempool=$num_val/" "$SERVER_CONF" 2>> "$LOG_FILE"
    fi
    log_info "设置 mempool=$val"
}

# ========================== 其他函数保持不变 ==========================

clean_data() {
    rm -f kvstore.snap kvstore.snap.tmp kvstore.aof 2>/dev/null
    log_info "清理旧数据文件"
}

build_project() {
    log_info "开始编译项目..."
    make clean >> "$LOG_FILE" 2>&1 || true
    if ! make >> "$LOG_FILE" 2>&1; then
        log_error "编译失败！"
        return 1
    fi
    
    if [ ! -f "$SERVER_BIN" ] || [ ! -f "$CLIENT_BIN" ]; then
        log_error "编译失败：可执行文件不存在！"
        return 1
    fi
    
    log_info "编译完成"
    return 0
}

create_default_config() {
    if [ ! -f "$SERVER_CONF" ]; then
        log_warn "配置文件 $SERVER_CONF 不存在，创建默认配置..."
        cat > "$SERVER_CONF" << 'EOF'
port 2000
persistence OFF
snapshot OFF
expire OFF
mempool OFF
replication OFF 
transport TCP
EOF
        log_info "默认配置文件已创建"
    fi
}

# ========================== 单轮测试 ==========================
run_single_round() {
    local strategy=$1
    local strategy_name=$2
    local mempool_val=$3    # ON 或 OFF
    local use_jemalloc=$4
    local run_id=$5

    log_info ""
    log_info "--- $strategy_name (第 $run_id/$RUNS 次) ---"

    # 重要：每次测试前强制停止旧服务器，清理数据，重新配置，然后启动全新服务器
    stop_server
    clean_data
    set_mempool_config "$mempool_val"

    log_info "启动服务器..."
    if [ "$use_jemalloc" = true ]; then
        sudo LD_PRELOAD="$JEMALLOC_LIB" "$SERVER_BIN" "$SERVER_CONF" >> "$LOG_FILE" 2>&1 &
    else
        sudo "$SERVER_BIN" "$SERVER_CONF" >> "$LOG_FILE" 2>&1 &
    fi

    if ! wait_for_server; then
        log_error "服务器启动超时！"
        return 1
    fi
    log_info "服务器就绪"

    log_info "运行客户端测试 ($OPS 次操作)..."
    "$CLIENT_BIN" "$strategy" "$OPS" >> "$LOG_FILE" 2>&1
    local client_ret=$?

    stop_server

    if [ $client_ret -ne 0 ]; then
        log_error "客户端测试失败 (返回码: $client_ret)"
        return 1
    fi

    log_info "完成: $strategy_name (第 $run_id/$RUNS 次)"
    return 0
}

# ========================== 完整测试组 ==========================
run_test_group() {
    local strategy=$1
    local strategy_name=$2
    local mempool_val=$3
    local use_jemalloc=$4

    log_info ""
    log_info "============================================================"
    log_info "  测试组: $strategy_name"
    log_info "============================================================"

    local success=0
    local fail=0

    for i in $(seq 1 $RUNS); do
        if run_single_round "$strategy" "$strategy_name" "$mempool_val" "$use_jemalloc" "$i"; then
            ((success++))
        else
            ((fail++))
            log_error "$strategy_name 第 $i 次失败"
        fi
        sleep 1
    done

    log_info "测试组完成: $strategy_name (成功=$success, 失败=$fail)"
}

# ========================== 主流程 ==========================
main() {
    > "$LOG_FILE"
    > "$RESULT_FILE"

    log_info "============================================================"
    log_info "  内存池性能对比自动化测试"
    log_info "  开始时间: $(date)"
    log_info "  测试次数: $RUNS 次/组"
    log_info "  每次操作数: $OPS"
    log_info "============================================================"

    echo "============================================"
    echo "  内存池性能对比测试"
    echo "  日志: $LOG_FILE"
    echo "  结果: $RESULT_FILE"
    echo "============================================"

    create_default_config

    echo -n "编译... "
    if ! build_project; then
        echo "失败！查看日志: $LOG_FILE"
        exit 1
    fi
    echo "完成"

    local jemalloc_available=false
    if check_jemalloc; then
        jemalloc_available=true
    fi

    # Glibc_Malloc: mempool=OFF
    echo -n "Glibc_Malloc... "
    run_test_group 0 "Glibc_Malloc" "OFF" false
    echo "完成"

    # Jemalloc: mempool=OFF, LD_PRELOAD
    if [ "$jemalloc_available" = true ]; then
        echo -n "Jemalloc... "
        run_test_group 1 "Jemalloc" "OFF" true
        echo "完成"
    else
        echo "Jemalloc: 跳过"
    fi

    # Custom_Mempool: mempool=ON
    echo -n "Custom_Mempool... "
    run_test_group 2 "Custom_Mempool" "ON" false
    echo "完成"

    stop_server
    clean_data

    echo ""
    echo "============================================"
    echo "  测试完成！"
    echo "  日志: $LOG_FILE"
    echo "  结果: $RESULT_FILE"
    echo "============================================"
}

main "$@"