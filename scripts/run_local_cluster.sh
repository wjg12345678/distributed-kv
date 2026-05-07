#!/usr/bin/env bash

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BUILD_DIR="${BUILD_DIR:-$ROOT_DIR/build}"
BINARY="$BUILD_DIR/distributed_kv_network_node"
RUNTIME_DIR="${RUNTIME_DIR:-/tmp/distributed-kv-local-cluster}"
DATA_DIR="$RUNTIME_DIR/data"
LOG_DIR="$RUNTIME_DIR/logs"
PID_DIR="$RUNTIME_DIR/pids"

usage() {
  cat <<'EOF'
用法：scripts/run_local_cluster.sh <start|stop|restart|status>

可覆盖的环境变量：
  BUILD_DIR    包含 distributed_kv_network_node 的构建目录
  RUNTIME_DIR  节点数据、日志和 pid 文件的运行目录
EOF
}

node_raft_port() {
  local node_id="$1"
  printf '%d' "$((9100 + node_id))"
}

node_http_port() {
  local node_id="$1"
  printf '%d' "$((9200 + node_id))"
}

node_data_dir() {
  local node_id="$1"
  printf '%s/node%d' "$DATA_DIR" "$node_id"
}

node_log_file() {
  local node_id="$1"
  printf '%s/node%d.log' "$LOG_DIR" "$node_id"
}

node_pid_file() {
  local node_id="$1"
  printf '%s/node%d.pid' "$PID_DIR" "$node_id"
}

require_binary() {
  if [[ ! -x "$BINARY" ]]; then
    echo "缺少可执行文件：$BINARY" >&2
    echo "请先执行：cmake -S . -B build && cmake --build build -j" >&2
    exit 1
  fi
}

pid_is_running() {
  local pid_file="$1"
  [[ -f "$pid_file" ]] || return 1
  local pid
  pid="$(<"$pid_file")"
  [[ -n "$pid" ]] || return 1
  kill -0 "$pid" 2>/dev/null
}

stop_node() {
  local node_id="$1"
  local pid_file
  pid_file="$(node_pid_file "$node_id")"
  if ! pid_is_running "$pid_file"; then
    rm -f "$pid_file"
    return 0
  fi

  local pid
  pid="$(<"$pid_file")"
  kill "$pid" 2>/dev/null || true
  for _ in {1..20}; do
    if ! kill -0 "$pid" 2>/dev/null; then
      break
    fi
    sleep 0.1
  done
  rm -f "$pid_file"
}

start_node() {
  local node_id="$1"
  local pid_file
  pid_file="$(node_pid_file "$node_id")"
  if pid_is_running "$pid_file"; then
    echo "节点 $node_id 已经在运行" >&2
    return 1
  fi

  local data_dir
  data_dir="$(node_data_dir "$node_id")"
  rm -rf "$data_dir"
  mkdir -p "$data_dir"

  local args=(
    --node-id "$node_id"
    --raft-port "$(node_raft_port "$node_id")"
    --http-port "$(node_http_port "$node_id")"
    --data-dir "$data_dir"
  )

  for peer_id in 1 2 3; do
    if [[ "$peer_id" -eq "$node_id" ]]; then
      continue
    fi
    args+=(
      --peer
      "$peer_id:127.0.0.1:$(node_raft_port "$peer_id"):$(node_http_port "$peer_id")"
    )
  done

  nohup "$BINARY" "${args[@]}" >"$(node_log_file "$node_id")" 2>&1 &
  echo "$!" >"$pid_file"
}

start_cluster() {
  require_binary
  mkdir -p "$DATA_DIR" "$LOG_DIR" "$PID_DIR"
  for node_id in 1 2 3; do
    if pid_is_running "$(node_pid_file "$node_id")"; then
      echo "集群似乎已经在运行" >&2
      status_cluster
      exit 1
    fi
  done

  rm -rf "$DATA_DIR"
  mkdir -p "$DATA_DIR" "$LOG_DIR" "$PID_DIR"
  for node_id in 1 2 3; do
    start_node "$node_id"
  done

  sleep 1
  status_cluster
  echo "日志目录：$LOG_DIR"
}

stop_cluster() {
  for node_id in 1 2 3; do
    stop_node "$node_id"
  done
}

status_cluster() {
  for node_id in 1 2 3; do
    local pid_file
    pid_file="$(node_pid_file "$node_id")"
    local state="已停止"
    local pid="-"
    if pid_is_running "$pid_file"; then
      pid="$(<"$pid_file")"
      state="运行中"
    elif [[ -f "$pid_file" ]]; then
      pid="$(<"$pid_file")"
      rm -f "$pid_file"
    fi

    printf '节点=%d pid=%s raft_port=%s http_port=%s 状态=%s\n' \
      "$node_id" \
      "$pid" \
      "$(node_raft_port "$node_id")" \
      "$(node_http_port "$node_id")" \
      "$state"
  done
}

main() {
  if [[ $# -ne 1 ]]; then
    usage
    exit 1
  fi

  case "$1" in
    start)
      start_cluster
      ;;
    stop)
      stop_cluster
      ;;
    restart)
      stop_cluster
      start_cluster
      ;;
    status)
      status_cluster
      ;;
    *)
      usage
      exit 1
      ;;
  esac
}

main "$@"
