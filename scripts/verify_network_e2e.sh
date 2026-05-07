#!/usr/bin/env bash

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BUILD_DIR="${BUILD_DIR:-$ROOT_DIR/build}"
BINARY="$BUILD_DIR/distributed_kv_network_node"
CLUSTER_SCRIPT="$ROOT_DIR/scripts/run_local_cluster.sh"
RUNTIME_DIR="${RUNTIME_DIR:-/tmp/distributed-kv-e2e}"
DATA_DIR="$RUNTIME_DIR/data"
LOG_DIR="$RUNTIME_DIR/logs"
PID_DIR="$RUNTIME_DIR/pids"
KEEP_CLUSTER_RUNNING="${KEEP_CLUSTER_RUNNING:-0}"

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
  for _ in {1..50}; do
    if ! kill -0 "$pid" 2>/dev/null; then
      break
    fi
    sleep 0.1
  done
  rm -f "$pid_file"
}

start_node() {
  local node_id="$1"
  shift
  local peers=("$@")

  require_binary
  mkdir -p "$DATA_DIR" "$LOG_DIR" "$PID_DIR"

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

  local peer_id
  for peer_id in "${peers[@]}"; do
    args+=(
      --peer
      "$peer_id:127.0.0.1:$(node_raft_port "$peer_id"):$(node_http_port "$peer_id")"
    )
  done

  nohup "$BINARY" "${args[@]}" >"$(node_log_file "$node_id")" 2>&1 </dev/null &
  local pid
  pid="$!"
  disown "$pid" 2>/dev/null || true
  echo "$pid" >"$pid_file"
}

cleanup() {
  if [[ "$KEEP_CLUSTER_RUNNING" == "1" ]]; then
    return 0
  fi
  stop_node 4 || true
  RUNTIME_DIR="$RUNTIME_DIR" "$CLUSTER_SCRIPT" stop >/dev/null 2>&1 || true
}

trap cleanup EXIT

extract_field() {
  local body="$1"
  local key="$2"
  grep -E "^${key}=" <<<"$body" | head -n1 | cut -d= -f2-
}

curl_expect_code() {
  local expected="$1"
  shift

  local output_file
  output_file="$(mktemp)"
  local status
  status="$(curl -sS --max-time 3 -o "$output_file" -w '%{http_code}' "$@")"
  local body
  body="$(<"$output_file")"
  rm -f "$output_file"

  if [[ "$status" != "$expected" ]]; then
    echo "HTTP 状态不符合预期：expected=$expected actual=$status body=$body" >&2
    return 1
  fi

  printf '%s' "$body"
}

wait_for_http() {
  local port="$1"
  for _ in {1..60}; do
    if curl -fsS --max-time 1 "http://127.0.0.1:${port}/status" >/dev/null 2>&1; then
      return 0
    fi
    sleep 0.2
  done

  echo "HTTP 端口未就绪：$port" >&2
  return 1
}

wait_for_leader_port() {
  local ports=("$@")
  local port
  for _ in {1..80}; do
    for port in "${ports[@]}"; do
      local status
      status="$(curl -fsS --max-time 1 "http://127.0.0.1:${port}/status" 2>/dev/null || true)"
      if grep -q '^role=leader$' <<<"$status"; then
        printf '%s' "$port"
        return 0
      fi
    done
    sleep 0.2
  done

  echo "在超时时间内未能探测到 leader" >&2
  return 1
}

wait_for_commit_index() {
  local port="$1"
  local target="$2"
  for _ in {1..80}; do
    local status
    status="$(curl -fsS --max-time 1 "http://127.0.0.1:${port}/status" 2>/dev/null || true)"
    local commit_index
    commit_index="$(extract_field "$status" commit_index || true)"
    if [[ -n "$commit_index" && "$commit_index" =~ ^[0-9]+$ && "$commit_index" -ge "$target" ]]; then
      return 0
    fi
    sleep 0.2
  done

  echo "节点 ${port} 未在超时时间内追平到 commit_index=${target}" >&2
  return 1
}

restart_cluster() {
  stop_node 4 || true
  RUNTIME_DIR="$RUNTIME_DIR" "$CLUSTER_SCRIPT" restart >/dev/null
}

assert_equals() {
  local actual="$1"
  local expected="$2"
  local message="$3"
  if [[ "$actual" != "$expected" ]]; then
    echo "$message: expected=$expected actual=$actual" >&2
    return 1
  fi
}

pick_follower_port() {
  local leader_port="$1"
  local port
  for port in 9201 9202 9203; do
    if [[ "$port" != "$leader_port" ]]; then
      printf '%s' "$port"
      return 0
    fi
  done
  return 1
}

run_lock_and_mvcc_scenario() {
  echo "[e2e] lock + mvcc"
  restart_cluster

  local leader_port
  leader_port="$(wait_for_leader_port 9201 9202 9203)"
  local follower_port
  follower_port="$(pick_follower_port "$leader_port")"

  local body
  body="$(curl_expect_code 200 -X PUT "http://127.0.0.1:${leader_port}/kv/service" -d 'distributed-kv')"
  assert_equals "$body" "stored" "KV 写入返回值异常"

  body="$(curl_expect_code 200 -L "http://127.0.0.1:${follower_port}/kv/service")"
  assert_equals "$body" "distributed-kv" "Follower 重定向后的 KV 读取异常"

  body="$(curl_expect_code 200 -L -X POST "http://127.0.0.1:${leader_port}/lock/acquire" -d 'name=deploy&owner=worker-a')"
  assert_equals "$body" "locked" "首次加锁失败"

  body="$(curl_expect_code 409 -L -X POST "http://127.0.0.1:${leader_port}/lock/acquire" -d 'name=deploy&owner=worker-b')"
  assert_equals "$body" "lock busy" "锁冲突返回值异常"

  body="$(curl_expect_code 200 -L "http://127.0.0.1:${follower_port}/lock/deploy")"
  assert_equals "$body" "owner=worker-a" "锁状态读取异常"

  body="$(curl_expect_code 409 -L -X POST "http://127.0.0.1:${leader_port}/lock/release" -d 'name=deploy&owner=worker-b')"
  assert_equals "$body" "lock owner mismatch" "错误 owner 解锁返回值异常"

  body="$(curl_expect_code 200 -L -X POST "http://127.0.0.1:${leader_port}/lock/release" -d 'name=deploy&owner=worker-a')"
  assert_equals "$body" "released" "正确 owner 解锁失败"

  body="$(curl_expect_code 200 -L -X POST "http://127.0.0.1:${leader_port}/lock/acquire" -d 'name=deploy&owner=worker-b')"
  assert_equals "$body" "locked" "二次加锁失败"

  local begin_tx1
  begin_tx1="$(curl_expect_code 200 -L -X POST "http://127.0.0.1:${leader_port}/mvcc/begin")"
  local tx1
  tx1="$(extract_field "$begin_tx1" tx_id)"
  [[ -n "$tx1" ]] || {
    echo "mvcc begin 未返回 tx_id" >&2
    return 1
  }

  body="$(curl_expect_code 200 -L -X PUT "http://127.0.0.1:${leader_port}/mvcc/tx/${tx1}/kv/order" -d 'v1')"
  assert_equals "$body" "staged" "MVCC 首次写入暂存失败"

  local commit_tx1
  commit_tx1="$(curl_expect_code 200 -L -X POST "http://127.0.0.1:${leader_port}/mvcc/commit" -d "tx_id=${tx1}")"
  local commit_ts1
  commit_ts1="$(extract_field "$commit_tx1" commit_ts)"
  [[ -n "$commit_ts1" ]] || {
    echo "mvcc commit 未返回 commit_ts" >&2
    return 1
  }

  local begin_tx2
  begin_tx2="$(curl_expect_code 200 -L -X POST "http://127.0.0.1:${leader_port}/mvcc/begin")"
  local begin_tx3
  begin_tx3="$(curl_expect_code 200 -L -X POST "http://127.0.0.1:${leader_port}/mvcc/begin")"
  local tx2 tx3 snapshot_ts2 snapshot_ts3
  tx2="$(extract_field "$begin_tx2" tx_id)"
  tx3="$(extract_field "$begin_tx3" tx_id)"
  snapshot_ts2="$(extract_field "$begin_tx2" snapshot_ts)"
  snapshot_ts3="$(extract_field "$begin_tx3" snapshot_ts)"
  assert_equals "$snapshot_ts2" "$commit_ts1" "第二个事务 snapshot_ts 不符合预期"
  assert_equals "$snapshot_ts3" "$commit_ts1" "第三个事务 snapshot_ts 不符合预期"

  body="$(curl_expect_code 200 -L -X PUT "http://127.0.0.1:${leader_port}/mvcc/tx/${tx2}/kv/order" -d 'v2')"
  assert_equals "$body" "staged" "MVCC 第二次写入暂存失败"

  body="$(curl_expect_code 200 -L -X POST "http://127.0.0.1:${leader_port}/mvcc/commit" -d "tx_id=${tx2}")"
  [[ -n "$(extract_field "$body" commit_ts)" ]] || {
    echo "第二个 MVCC commit 未返回 commit_ts" >&2
    return 1
  }

  body="$(curl_expect_code 200 -L -X PUT "http://127.0.0.1:${leader_port}/mvcc/tx/${tx3}/kv/order" -d 'v3')"
  assert_equals "$body" "staged" "冲突事务写入暂存失败"

  body="$(curl_expect_code 409 -L -X POST "http://127.0.0.1:${leader_port}/mvcc/commit" -d "tx_id=${tx3}")"
  assert_equals "$body" "mvcc write-write conflict" "MVCC 冲突返回值异常"

  body="$(curl_expect_code 200 -L "http://127.0.0.1:${leader_port}/mvcc/kv/order?snapshot_ts=${commit_ts1}")"
  assert_equals "$body" "v1" "MVCC 历史快照读取异常"

  body="$(curl_expect_code 200 -L "http://127.0.0.1:${leader_port}/mvcc/kv/order")"
  assert_equals "$body" "v2" "MVCC 最新值读取异常"
}

run_failover_scenario() {
  echo "[e2e] failover"
  restart_cluster

  local leader_port
  leader_port="$(wait_for_leader_port 9201 9202 9203)"
  local leader_id
  leader_id="$((leader_port - 9200))"

  local body
  body="$(curl_expect_code 200 -X PUT "http://127.0.0.1:${leader_port}/kv/failover_before" -d 'v1')"
  assert_equals "$body" "stored" "failover 前写入失败"

  stop_node "$leader_id"

  local surviving_ports=()
  local port
  for port in 9201 9202 9203; do
    if [[ "$port" != "$leader_port" ]]; then
      surviving_ports+=("$port")
    fi
  done

  local new_leader_port
  new_leader_port="$(wait_for_leader_port "${surviving_ports[@]}")"
  if [[ "$new_leader_port" == "$leader_port" ]]; then
    echo "leader 故障后未发生切换" >&2
    return 1
  fi

  body="$(curl_expect_code 200 -X PUT "http://127.0.0.1:${new_leader_port}/kv/failover_after" -d 'v2')"
  assert_equals "$body" "stored" "failover 后写入失败"

  body="$(curl_expect_code 200 "http://127.0.0.1:${new_leader_port}/kv/failover_before")"
  assert_equals "$body" "v1" "failover 后历史数据不可读"

  local follower_port
  follower_port="$(pick_follower_port "$new_leader_port")"
  if [[ "$follower_port" == "$leader_port" ]]; then
    for port in 9201 9202 9203; do
      if [[ "$port" != "$new_leader_port" && "$port" != "$leader_port" ]]; then
        follower_port="$port"
        break
      fi
    done
  fi

  body="$(curl_expect_code 200 -L "http://127.0.0.1:${follower_port}/kv/failover_after")"
  assert_equals "$body" "v2" "failover 后 follower 重定向读取异常"
}

run_membership_scenario() {
  echo "[e2e] membership"
  restart_cluster

  local leader_port
  leader_port="$(wait_for_leader_port 9201 9202 9203)"
  local leader_id
  leader_id="$((leader_port - 9200))"

  start_node 4 1 2 3
  wait_for_http 9204

  local body
  body="$(curl_expect_code 200 -X POST "http://127.0.0.1:${leader_port}/admin/add-peer" \
    -d 'id=4&host=127.0.0.1&raft_port=9104&http_port=9204')"
  assert_equals "$body" "peer added" "add-peer 接口失败"

  body="$(curl_expect_code 200 -X PUT "http://127.0.0.1:${leader_port}/kv/membership_seed" -d 'seed')"
  assert_equals "$body" "stored" "add-peer 后基准写入失败"

  local leader_status
  leader_status="$(curl_expect_code 200 "http://127.0.0.1:${leader_port}/status")"
  local leader_commit_index
  leader_commit_index="$(extract_field "$leader_status" commit_index)"
  local leader_peer_count
  leader_peer_count="$(extract_field "$leader_status" peer_count)"
  assert_equals "$leader_peer_count" "3" "leader peer_count 未包含新节点"
  wait_for_commit_index 9204 "$leader_commit_index"

  local stopped_follower_id=0
  local node_id
  for node_id in 1 2 3; do
    if [[ "$node_id" != "$leader_id" ]]; then
      stopped_follower_id="$node_id"
      break
    fi
  done
  stop_node "$stopped_follower_id"
  leader_port="$(wait_for_leader_port 9201 9202 9203)"

  body="$(curl_expect_code 200 -X PUT "http://127.0.0.1:${leader_port}/kv/membership_after_add" -d 'quorum-with-node4')"
  assert_equals "$body" "stored" "新增节点后无法维持 4 节点多数派写入"

  body="$(curl_expect_code 200 -X POST "http://127.0.0.1:${leader_port}/admin/remove-peer" -d 'id=4')"
  assert_equals "$body" "peer removed" "remove-peer 接口失败"

  leader_status="$(curl_expect_code 200 "http://127.0.0.1:${leader_port}/status")"
  leader_peer_count="$(extract_field "$leader_status" peer_count)"
  assert_equals "$leader_peer_count" "2" "leader peer_count 未移除节点 4"

  stop_node 4
  leader_port="$(wait_for_leader_port 9201 9202 9203)"

  body="$(curl_expect_code 200 -X PUT "http://127.0.0.1:${leader_port}/kv/membership_after_remove" -d 'quorum-without-node4')"
  assert_equals "$body" "stored" "移除节点 4 后无法退回 3 节点多数派写入"
}

main() {
  run_lock_and_mvcc_scenario
  run_failover_scenario
  run_membership_scenario

  echo
  echo "多进程 e2e 验证通过：lock、mvcc、failover、membership"
}

main "$@"
