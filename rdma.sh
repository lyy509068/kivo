#!/bin/bash
# restore_rdma.sh
# 一键恢复 RDMA 环境：卸载旧设备 → 加载模块 → 设置 MTU → 创建 rxe0 → 验证
# 使用方法: sudo ./restore_rdma.sh

set -e

echo "=============================================="
echo " RDMA 环境一键恢复脚本"
echo "=============================================="

# 1. 加载内核模块
echo "[1/5] 加载 rdma_rxe 内核模块..."
sudo modprobe rdma_rxe || true
echo "     done"

# 2. 找到物理网卡名
echo "[2/5] 检测物理网卡..."
NETDEV=$(ip route get 1.1.1.1 2>/dev/null | grep -oP 'dev \K\S+' || echo "ens33")
echo "     使用网卡: $NETDEV"

# 3. 设置巨型帧 MTU 9000
echo "[3/5] 设置网卡 MTU 为 9000..."
sudo ip link set "$NETDEV" mtu 9000
echo "     done"

# 4. 删除旧 RDMA 设备并重新创建
echo "[4/5] 重建 RDMA 设备..."
# 删除所有 rxe 设备
for dev in $(rdma link show 2>/dev/null | grep -oP 'rxe\d+'); do
    echo "     删除旧设备: $dev"
    sudo rdma link del "$dev" 2>/dev/null || true
done
# 删除 siw 设备（如果存在）
for dev in $(rdma link show 2>/dev/null | grep -oP 'siw\d+'); do
    echo "     删除 siw 设备: $dev"
    sudo rdma link del "$dev" 2>/dev/null || true
done

# 创建新的 rxe0
echo "     创建 rxe0 绑定到 $NETDEV"
sudo rdma link add rxe0 type rxe netdev "$NETDEV"
echo "     done"

# 5. 验证
echo "[5/5] 验证 RDMA 环境..."
echo ""
echo "=== RDMA 设备列表 ==="
ibv_devices
echo ""
echo "=== 关键信息 ==="
ibv_devinfo -v | grep -E 'hca_id|state|max_mtu|active_mtu|link_layer'
echo ""
echo "=== IPv4 GID ==="
ibv_devinfo -v | grep -E 'GID.*RoCE v2'
echo ""

echo "=============================================="
echo " RDMA 环境恢复完成！"
echo " 如需测试连通性，可运行:"
echo "   ib_send_bw (需安装 perftest)"
echo "   或在 server 中执行 RDMA 同步测试"
echo "=============================================="