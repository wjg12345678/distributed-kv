#!/usr/bin/env bash

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
CLUSTER_SCRIPT="$ROOT_DIR/scripts/run_local_cluster.sh"
OUTPUT_DIR="${OUTPUT_DIR:-/tmp/distributed-kv-local-cluster/bench}"
KEEP_CLUSTER_RUNNING="${KEEP_CLUSTER_RUNNING:-0}"

cleanup() {
  if [[ "$KEEP_CLUSTER_RUNNING" != "1" ]]; then
    "$CLUSTER_SCRIPT" stop >/dev/null 2>&1 || true
  fi
}

wait_for_leader_port() {
  for _ in {1..30}; do
    for port in 9201 9202 9203; do
      local status
      status="$(curl -fsS --max-time 1 "http://127.0.0.1:${port}/status" 2>/dev/null || true)"
      if grep -q '^role=leader$' <<<"$status"; then
        printf '%s' "$port"
        return 0
      fi
    done
    sleep 0.2
  done

  echo "在超时时间内未能探测到主节点" >&2
  return 1
}

start_cluster() {
  "$CLUSTER_SCRIPT" restart >/dev/null
  wait_for_leader_port
}

stop_cluster() {
  if [[ "$KEEP_CLUSTER_RUNNING" != "1" ]]; then
    "$CLUSTER_SCRIPT" stop >/dev/null
  fi
}

run_benchmark() {
  local mode="$1"
  trap cleanup EXIT
  mkdir -p "$OUTPUT_DIR"

  local leader_port
  leader_port="$(start_cluster)"
  curl -fsS -X PUT "http://127.0.0.1:${leader_port}/kv/bench" -d 'benchmark-value' >/dev/null

  if [[ "$mode" == "get" ]]; then
    local get_output="$OUTPUT_DIR/get.txt"
    echo "正在对主节点端口 ${leader_port} 执行 GET 压测"
    wrk -t4 -c16 -d15s --latency -s "$ROOT_DIR/bench/wrk_get.lua" "http://127.0.0.1:${leader_port}" | tee "$get_output"
  else
    local put_output="$OUTPUT_DIR/put.txt"
    echo "正在对主节点端口 ${leader_port} 执行 PUT 压测"
    wrk -t4 -c8 -d15s --latency -s "$ROOT_DIR/bench/wrk_put.lua" "http://127.0.0.1:${leader_port}" | tee "$put_output"
  fi

  stop_cluster
}

main() {
  local mode="${1:-}"
  case "$mode" in
    get|put)
      run_benchmark "$mode"
      echo
      echo "压测输出已保存到：$OUTPUT_DIR"
      ;;
    *)
      echo "用法：bench/run_network_bench.sh <get|put>" >&2
      exit 1
      ;;
  esac
}

main "$@"
