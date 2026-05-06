#pragma once

#include <cstdint>
#include <string>
#include <utility>
#include <vector>

struct PeerEndpoint {
  int id = 0;
  std::string host;
  int raft_port = 0;
  int http_port = 0;
};

enum class Role {
  Follower,
  Candidate,
  Leader,
};

enum class CommandType {
  Noop,
  Put,
  AddPeer,
  RemovePeer,
  AcquireLock,
  ReleaseLock,
  MvccCommit,
};

struct KeyValue {
  std::string key;
  std::string value;
};

struct Command {
  CommandType type = CommandType::Noop;
  std::string key;
  std::string value;
  PeerEndpoint peer;
  std::string request_id;
  std::uint64_t mvcc_commit_ts = 0;
  std::vector<KeyValue> writes;
};

struct LogEntry {
  int term = 0;
  Command command;
};

struct RequestVoteRequest {
  int term = 0;
  int candidate_id = 0;
  int last_log_index = 0;
  int last_log_term = 0;
};

struct RequestVoteResponse {
  int term = 0;
  bool vote_granted = false;
};

struct AppendEntriesRequest {
  int term = 0;
  int leader_id = 0;
  int prev_log_index = 0;
  int prev_log_term = 0;
  int leader_commit = 0;
  std::vector<LogEntry> entries;
};

struct AppendEntriesResponse {
  int term = 0;
  bool success = false;
  int match_index = 0;
  int conflict_index = 0;
  int conflict_term = -1;
};

struct InstallSnapshotRequest {
  int term = 0;
  int leader_id = 0;
  int last_included_index = 0;
  int last_included_term = 0;
  std::vector<std::pair<std::string, std::string>> state;
  std::vector<PeerEndpoint> peers;
};

struct InstallSnapshotResponse {
  int term = 0;
  bool success = false;
  int last_included_index = 0;
};

struct CommandResult {
  bool success = false;
  std::uint64_t version = 0;
};

struct MvccTransaction {
  std::string tx_id;
  std::uint64_t snapshot_ts = 0;
};

struct MvccCommitResult {
  bool committed = false;
  bool conflict = false;
  std::uint64_t commit_ts = 0;
};

struct RaftMetricsSnapshot {
  std::uint64_t pre_vote_rpcs = 0;
  std::uint64_t request_vote_rpcs = 0;
  std::uint64_t append_entries_rpcs = 0;
  std::uint64_t install_snapshot_rpcs = 0;
  std::uint64_t linearizable_read_checks = 0;
  std::uint64_t linearizable_read_failures = 0;
  std::uint64_t client_put_proposals = 0;
  std::uint64_t lock_acquire_proposals = 0;
  std::uint64_t lock_release_proposals = 0;
  std::uint64_t mvcc_begin_total = 0;
  std::uint64_t mvcc_commit_attempts = 0;
  std::uint64_t mvcc_commit_conflicts = 0;
  std::uint64_t committed_entries = 0;
  std::uint64_t applied_entries = 0;
};
