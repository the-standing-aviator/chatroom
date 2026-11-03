#!/usr/bin/env bash
# 一键起 N 个 gateway + 1 个 LB，共用 docker 里的 PostgreSQL/NATS/Redis，
# 供 V3.4 水平扩展压测用（bench 连 LB 地址）。
#
# 用法：
#   scripts/run-cluster.sh          # 默认 2 个节点
#   scripts/run-cluster.sh 1        # 1 个节点（做对比基线）
#   scripts/run-cluster.sh 3        # 3 个节点
#
# 前置：先 docker compose up -d 把 postgres/nats/redis 起好。
# 停止：Ctrl-C（脚本会清理所有 gateway/lb 进程）。
# 兜底：若脚本被强杀没走清理，手动执行 pkill -x chat-gateway; pkill -x chat-lb。
#       二进制名刻意 ≤15 字符，因为 Linux 进程名（comm）上限 15 字符，超长 pkill -x 匹配不到。
set -euo pipefail

cd "$(dirname "$0")/.." # 切到仓库根目录，后面相对路径都基于这里

NODES="${1:-2}"
GO=/usr/local/go/bin/go

BASE_PORT=8888    # 第 i 个 gateway 监听 127.0.0.1:$((BASE_PORT+i))
METRICS_BASE=9091 # 第 i 个 gateway 的 /metrics 端口
LB_LISTEN=127.0.0.1:9000

# ---- 前置检查：外部依赖（postgres/nats/redis）是否就绪，连不上就快速失败 ----
wait_port() {
  local host=$1 port=$2
  for _ in $(seq 1 30); do
    if (exec 3<>"/dev/tcp/$host/$port") 2>/dev/null; then
      return 0
    fi
    sleep 1
  done
  return 1
}
echo "==> 检查外部依赖（postgres:5432 / nats:4222 / redis:6379）"
for hp in "127.0.0.1 5432" "127.0.0.1 4222" "127.0.0.1 6379"; do
  set -- $hp
  if ! wait_port "$1" "$2"; then
    echo "错误：$1:$2 连不上，先运行 docker compose up -d" >&2
    exit 1
  fi
done

# ---- 构建 ----
echo "==> 构建 gateway / lb"
$GO build -o /tmp/chat-gateway ./cmd/gateway
$GO build -o /tmp/chat-lb ./cmd/lb

# ---- 启动 ----
PIDS=()
cleanup() {
  echo
  echo "==> 停止所有 gateway 与 lb"
  kill "${PIDS[@]}" 2>/dev/null || true
  wait "${PIDS[@]}" 2>/dev/null || true
}
# 清理只挂在 EXIT 上；INT/TERM 转成 exit，由 EXIT trap 统一走清理，避免清理跑两遍。
trap cleanup EXIT
trap 'exit 0' INT TERM

# 多节点部署要求各节点共享状态，统一挂分布式后端：postgres + redis + nats。
backends=""
for ((i = 0; i < NODES; i++)); do
  addr="127.0.0.1:$((BASE_PORT + i))"
  node="node-$i"
  metrics="127.0.0.1:$((METRICS_BASE + i))"
  echo "==> 起 gateway $node（$addr，metrics $metrics）"
  /tmp/chat-gateway \
    -addr "$addr" -node "$node" -metrics "$metrics" \
    -store postgres -presence redis -bus nats &
  PIDS+=($!)
  backends+="${addr},"
done
backends="${backends%,}"

echo "==> 起 lb（$LB_LISTEN -> $backends）"
/tmp/chat-lb -listen "$LB_LISTEN" -backends "$backends" &
PIDS+=($!)

echo
echo "集群就绪：$NODES 个 gateway + 1 个 lb"
echo "  bench 目标地址（连 LB）：$LB_LISTEN"
echo "  gateway metrics：127.0.0.1:$METRICS_BASE ~ 127.0.0.1:$((METRICS_BASE + NODES - 1))"
echo "Ctrl-C 停止"
wait
