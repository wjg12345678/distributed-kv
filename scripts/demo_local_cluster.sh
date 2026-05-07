#!/usr/bin/env bash

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
CLUSTER_SCRIPT="$ROOT_DIR/scripts/run_local_cluster.sh"
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

  echo "failed to detect leader within timeout" >&2
  return 1
}

main() {
  trap cleanup EXIT

  "$CLUSTER_SCRIPT" restart >/dev/null
  local leader_port
  leader_port="$(wait_for_leader_port)"
  local follower_port=9201
  if [[ "$leader_port" == "9201" ]]; then
    follower_port=9202
  fi

  echo "leader http port: $leader_port"
  curl -fsS -X PUT "http://127.0.0.1:${leader_port}/kv/demo" -d 'hello-from-demo' >/dev/null

  echo
  echo "direct read from leader:"
  curl -fsS "http://127.0.0.1:${leader_port}/kv/demo"
  echo

  echo
  echo "redirect response from follower:"
  curl -i --max-time 2 "http://127.0.0.1:${follower_port}/kv/demo"
  echo

  echo
  echo "follow redirect and read value:"
  curl -fsSL "http://127.0.0.1:${follower_port}/kv/demo"
  echo

  echo
  echo "selected metrics:"
  curl -fsS "http://127.0.0.1:${leader_port}/metrics" | grep -E '^(raft_(append_entries_rpcs_total|linearizable_read_checks_total)|http_requests_total) '
  echo

  if [[ "$KEEP_CLUSTER_RUNNING" == "1" ]]; then
    echo "cluster is still running; stop it with: scripts/run_local_cluster.sh stop"
  fi
}

main "$@"
