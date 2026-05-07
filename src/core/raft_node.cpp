#include "core/raft_node.h"

#include <algorithm>
#include <cstdint>
#include <iomanip>
#include <mutex>
#include <random>
#include <sstream>

namespace {

constexpr int kSnapshotThreshold = 4;
constexpr std::size_t kMaxStoredCommandResults = 256;
constexpr char kLockOwnerPrefix[] = "__lock__/owner/";
constexpr char kMvccIndexPrefix[] = "__mvcc__/index/";
constexpr char kMvccDataPrefix[] = "__mvcc__/data/";
constexpr char kMvccLatestTsKey[] = "__mvcc__/meta/latest_ts";

std::string LockOwnerKey(const std::string& name) {
  return std::string(kLockOwnerPrefix) + name;
}

std::string MvccIndexKey(const std::string& key) {
  return std::string(kMvccIndexPrefix) + key;
}

std::string MvccDataKey(const std::string& key, std::uint64_t commit_ts) {
  std::ostringstream out;
  out << kMvccDataPrefix << key << '/' << std::setw(20) << std::setfill('0') << commit_ts;
  return out.str();
}

std::vector<std::uint64_t> ParseVersionIndex(const std::string& raw) {
  std::vector<std::uint64_t> versions;
  if (raw.empty()) {
    return versions;
  }

  std::size_t start = 0;
  while (start <= raw.size()) {
    const auto comma = raw.find(',', start);
    const auto piece = raw.substr(start, comma == std::string::npos ? std::string::npos : comma - start);
    if (!piece.empty()) {
      versions.push_back(static_cast<std::uint64_t>(std::stoull(piece)));
    }
    if (comma == std::string::npos) {
      break;
    }
    start = comma + 1;
  }
  return versions;
}

std::string SerializeVersionIndex(const std::vector<std::uint64_t>& versions) {
  std::ostringstream out;
  for (std::size_t i = 0; i < versions.size(); ++i) {
    if (i != 0) {
      out << ',';
    }
    out << versions[i];
  }
  return out.str();
}

}

RaftNode::RaftNode(
    int id,
    std::vector<int> peers,
    IRaftTransport* transport,
    int election_timeout_ticks,
    std::unique_ptr<IKeyValueStore> store,
    std::unique_ptr<IRaftPersistence> persistence)
    : id_(id),
      peers_(std::move(peers)),
      base_election_timeout_ticks_(election_timeout_ticks),
      store_(std::move(store)),
      persistence_(std::move(persistence)),
      transport_(transport),
      random_engine_(static_cast<std::mt19937::result_type>(
          std::random_device{}() ^ (static_cast<unsigned int>(id) * 0x9e3779b9U))) {
  if (persistence_) {
    persistence_->LoadSnapshot(snapshot_state_);
  }

  if (snapshot_state_.last_included_index > 0) {
    store_->ReplaceWith(snapshot_state_.data);
    log_base_index_ = snapshot_state_.last_included_index;
    commit_index_ = snapshot_state_.last_included_index;
    last_applied_ = snapshot_state_.last_included_index;
    log_.push_back(LogEntry{snapshot_state_.last_included_term, Command{}});
    for (const auto& peer : transport_->ListPeers()) {
      transport_->RemovePeer(peer.id);
    }
    peers_.clear();
    for (const auto& peer : snapshot_state_.peers) {
      if (peer.id != id_) {
        peers_.push_back(peer.id);
        transport_->UpsertPeer(peer);
      }
    }
  } else {
    log_.push_back(LogEntry{});
  }

  PersistentState state;
  if (persistence_ && persistence_->LoadState(state)) {
    current_term_ = state.current_term;
    voted_for_ = state.voted_for;
    log_base_index_ = state.log_base_index;
    log_ = std::move(state.log_entries);
    if (log_.empty()) {
      log_.push_back(LogEntry{snapshot_state_.last_included_term, Command{}});
    }
    commit_index_ = std::max(commit_index_, state.commit_index);
    last_applied_ = log_base_index_;
    ApplyCommittedEntries();
  }

  ResetElectionTimer();
}

void RaftNode::Tick() {
  std::lock_guard<std::mutex> lock(mutex_);
  ++election_elapsed_;

  if (role_ == Role::Leader) {
    ++heartbeat_elapsed_;
    if (heartbeat_elapsed_ >= heartbeat_interval_ticks_) {
      SendHeartbeats();
      heartbeat_elapsed_ = 0;
    }
    return;
  }

  if (election_elapsed_ >= randomized_election_timeout_ticks_) {
    StartPreVote();
  }
}

bool RaftNode::Propose(const std::string& key, const std::string& value) {
  Command command;
  command.type = CommandType::Put;
  command.key = key;
  command.value = value;
  return ReplicateCommand(std::move(command));
}

bool RaftNode::ConfirmLeaderForRead() {
  std::lock_guard<std::mutex> lock(mutex_);
  return ConfirmLeaderForReadLocked();
}

std::optional<std::string> RaftNode::GetValue(const std::string& key) const {
  std::lock_guard<std::mutex> lock(mutex_);
  return store_->Get(key);
}

bool RaftNode::AddPeer(const PeerEndpoint& peer) {
  Command command;
  command.type = CommandType::AddPeer;
  command.peer = peer;
  return ReplicateCommand(std::move(command));
}

bool RaftNode::RemovePeer(int peer_id) {
  Command command;
  command.type = CommandType::RemovePeer;
  command.peer.id = peer_id;
  return ReplicateCommand(std::move(command));
}

bool RaftNode::AcquireLock(const std::string& name, const std::string& owner) {
  Command command;
  command.type = CommandType::AcquireLock;
  command.key = name;
  command.value = owner;

  CommandResult result;
  return ReplicateCommand(std::move(command), nullptr, &result) && result.success;
}

bool RaftNode::ReleaseLock(const std::string& name, const std::string& owner) {
  Command command;
  command.type = CommandType::ReleaseLock;
  command.key = name;
  command.value = owner;

  CommandResult result;
  return ReplicateCommand(std::move(command), nullptr, &result) && result.success;
}

std::optional<std::string> RaftNode::LockOwner(const std::string& name) const {
  std::lock_guard<std::mutex> lock(mutex_);
  return LockOwnerUnlocked(name);
}

std::optional<MvccTransaction> RaftNode::BeginMvccTransaction() {
  std::lock_guard<std::mutex> lock(mutex_);
  if (!ConfirmLeaderForReadLocked()) {
    return std::nullopt;
  }

  const std::string tx_id = NextRequestId("mvcc-tx");
  pending_mvcc_transactions_[tx_id] = PendingMvccTransactionState{LatestMvccTimestampUnlocked(), {}};
  ++metrics_.mvcc_begin_total;
  return MvccTransaction{tx_id, pending_mvcc_transactions_[tx_id].snapshot_ts};
}

bool RaftNode::StageMvccWrite(const std::string& tx_id, const std::string& key, const std::string& value) {
  std::lock_guard<std::mutex> lock(mutex_);
  if (role_ != Role::Leader) {
    return false;
  }

  auto it = pending_mvcc_transactions_.find(tx_id);
  if (it == pending_mvcc_transactions_.end()) {
    return false;
  }

  auto& writes = it->second.writes;
  auto existing = std::find_if(writes.begin(), writes.end(), [&key](const KeyValue& item) {
    return item.key == key;
  });
  if (existing == writes.end()) {
    writes.push_back(KeyValue{key, value});
  } else {
    existing->value = value;
  }
  return true;
}

MvccCommitResult RaftNode::CommitMvccTransaction(const std::string& tx_id) {
  std::lock_guard<std::mutex> lock(mutex_);
  if (role_ != Role::Leader) {
    return {};
  }

  auto it = pending_mvcc_transactions_.find(tx_id);
  if (it == pending_mvcc_transactions_.end()) {
    return {};
  }

  ++metrics_.mvcc_commit_attempts;
  if (it->second.writes.empty()) {
    const auto snapshot_ts = it->second.snapshot_ts;
    pending_mvcc_transactions_.erase(it);
    return MvccCommitResult{true, false, snapshot_ts};
  }

  for (const auto& write : it->second.writes) {
    if (LatestMvccTimestampForKeyUnlocked(write.key) > it->second.snapshot_ts) {
      ++metrics_.mvcc_commit_conflicts;
      pending_mvcc_transactions_.erase(it);
      return MvccCommitResult{false, true, 0};
    }
  }

  Command command;
  command.type = CommandType::MvccCommit;
  command.mvcc_commit_ts = LatestMvccTimestampUnlocked() + 1;
  command.writes = it->second.writes;

  CommandResult result;
  const bool replicated = ReplicateCommandLocked(std::move(command), nullptr, &result);
  pending_mvcc_transactions_.erase(tx_id);
  return MvccCommitResult{replicated && result.success, false, result.version};
}

std::optional<std::string> RaftNode::ReadMvcc(const std::string& key, std::uint64_t snapshot_ts) const {
  std::lock_guard<std::mutex> lock(mutex_);
  return ReadMvccUnlocked(key, snapshot_ts);
}

RequestVoteResponse RaftNode::HandleRequestVote(const RequestVoteRequest& request) {
  std::lock_guard<std::mutex> lock(mutex_);
  ++metrics_.request_vote_rpcs;
  if (request.term < current_term_) {
    return RequestVoteResponse{current_term_, false};
  }

  if (request.term > current_term_) {
    BecomeFollower(request.term);
  }

  const bool can_vote = voted_for_ == -1 || voted_for_ == request.candidate_id;
  const bool up_to_date = IsCandidateLogUpToDate(request.last_log_index, request.last_log_term);

  if (can_vote && up_to_date) {
    voted_for_ = request.candidate_id;
    PersistState();
    ResetElectionTimer();
    return RequestVoteResponse{current_term_, true};
  }

  return RequestVoteResponse{current_term_, false};
}

RequestVoteResponse RaftNode::HandlePreVote(const RequestVoteRequest& request) {
  std::lock_guard<std::mutex> lock(mutex_);
  ++metrics_.pre_vote_rpcs;
  if (request.term <= current_term_) {
    return RequestVoteResponse{current_term_, false};
  }

  if (HasRecentLeaderContactLocked()) {
    return RequestVoteResponse{current_term_, false};
  }

  return RequestVoteResponse{
      current_term_,
      IsCandidateLogUpToDate(request.last_log_index, request.last_log_term),
  };
}

AppendEntriesResponse RaftNode::HandleAppendEntries(const AppendEntriesRequest& request) {
  std::lock_guard<std::mutex> lock(mutex_);
  ++metrics_.append_entries_rpcs;
  if (request.term < current_term_) {
    return AppendEntriesResponse{current_term_, false, LastLogIndex(), LastLogIndex() + 1, -1};
  }

  if (request.term > current_term_ || role_ != Role::Follower) {
    BecomeFollower(request.term, request.leader_id);
  } else {
    leader_id_ = request.leader_id;
    ResetElectionTimer();
  }

  if (request.prev_log_index < log_base_index_) {
    return AppendEntriesResponse{
        current_term_,
        false,
        std::max(log_base_index_ - 1, 0),
        log_base_index_,
        snapshot_state_.last_included_term,
    };
  }

  if (!HasLogIndex(request.prev_log_index)) {
    return AppendEntriesResponse{
        current_term_,
        false,
        LastLogIndex(),
        LastLogIndex() + 1,
        -1,
    };
  }

  if (TermAt(request.prev_log_index) != request.prev_log_term) {
    const int conflict_term = TermAt(request.prev_log_index);
    const int conflict_index = FindFirstIndexOfTerm(conflict_term, request.prev_log_index);
    return AppendEntriesResponse{
        current_term_,
        false,
        conflict_index - 1,
        conflict_index,
        conflict_term,
    };
  }

  int insert_index = request.prev_log_index + 1;
  bool changed = false;
  for (std::size_t i = 0; i < request.entries.size(); ++i) {
    const int global_index = insert_index + static_cast<int>(i);
    if (HasLogIndex(global_index)) {
      if (TermAt(global_index) != request.entries[i].term) {
        log_.resize(ToLocalIndex(global_index));
        changed = true;
      }
    }

    if (!HasLogIndex(global_index)) {
      log_.push_back(request.entries[i]);
      changed = true;
    }
  }

  if (changed) {
    PersistState();
  }

  if (request.leader_commit > commit_index_) {
    commit_index_ = std::min(request.leader_commit, LastLogIndex());
    ApplyCommittedEntries();
    MaybeTakeSnapshot();
  }

  return AppendEntriesResponse{
      current_term_,
      true,
      request.prev_log_index + static_cast<int>(request.entries.size()),
      0,
      -1,
  };
}

InstallSnapshotResponse RaftNode::HandleInstallSnapshot(const InstallSnapshotRequest& request) {
  std::lock_guard<std::mutex> lock(mutex_);
  ++metrics_.install_snapshot_rpcs;
  if (request.term < current_term_) {
    return InstallSnapshotResponse{current_term_, false, snapshot_state_.last_included_index};
  }

  if (request.term > current_term_ || role_ != Role::Follower) {
    BecomeFollower(request.term, request.leader_id);
  } else {
    leader_id_ = request.leader_id;
    ResetElectionTimer();
  }

  if (request.last_included_index <= snapshot_state_.last_included_index) {
    return InstallSnapshotResponse{current_term_, true, snapshot_state_.last_included_index};
  }

  snapshot_state_.last_included_index = request.last_included_index;
  snapshot_state_.last_included_term = request.last_included_term;
  snapshot_state_.data = request.state;
  snapshot_state_.peers = request.peers;
  store_->ReplaceWith(snapshot_state_.data);
  for (const auto& peer : transport_->ListPeers()) {
    transport_->RemovePeer(peer.id);
  }
  peers_.clear();
  for (const auto& peer : snapshot_state_.peers) {
    if (peer.id != id_) {
      peers_.push_back(peer.id);
      transport_->UpsertPeer(peer);
    }
  }

  if (LastLogIndex() > request.last_included_index) {
    std::vector<LogEntry> new_log;
    new_log.push_back(LogEntry{request.last_included_term, Command{}});
    for (int index = request.last_included_index + 1; index <= LastLogIndex(); ++index) {
      if (HasLogIndex(index)) {
        new_log.push_back(log_[ToLocalIndex(index)]);
      }
    }
    log_ = std::move(new_log);
  } else {
    log_.clear();
    log_.push_back(LogEntry{request.last_included_term, Command{}});
  }

  log_base_index_ = request.last_included_index;
  commit_index_ = std::max(commit_index_, request.last_included_index);
  last_applied_ = std::max(last_applied_, request.last_included_index);

  if (persistence_) {
    persistence_->SaveSnapshot(snapshot_state_);
  }
  PersistState();

  return InstallSnapshotResponse{current_term_, true, snapshot_state_.last_included_index};
}

int RaftNode::id() const {
  return id_;
}

int RaftNode::current_term() const {
  std::lock_guard<std::mutex> lock(mutex_);
  return current_term_;
}

int RaftNode::leader_id() const {
  std::lock_guard<std::mutex> lock(mutex_);
  return leader_id_;
}

int RaftNode::commit_index() const {
  std::lock_guard<std::mutex> lock(mutex_);
  return commit_index_;
}

Role RaftNode::role() const {
  std::lock_guard<std::mutex> lock(mutex_);
  return role_;
}

const std::vector<LogEntry>& RaftNode::log() const {
  return log_;
}

const IKeyValueStore& RaftNode::store() const {
  return *store_;
}

int RaftNode::log_base_index() const {
  std::lock_guard<std::mutex> lock(mutex_);
  return log_base_index_;
}

RaftMetricsSnapshot RaftNode::metrics() const {
  std::lock_guard<std::mutex> lock(mutex_);
  return metrics_;
}

void RaftNode::ForceElectionTimeout() {
  std::lock_guard<std::mutex> lock(mutex_);
  election_elapsed_ = randomized_election_timeout_ticks_;
}

bool RaftNode::ReplicateCommand(Command command, const std::string* request_id, CommandResult* result) {
  std::lock_guard<std::mutex> lock(mutex_);
  return ReplicateCommandLocked(std::move(command), request_id, result);
}

bool RaftNode::ReplicateCommandLocked(Command command, const std::string* request_id, CommandResult* result) {
  if (role_ != Role::Leader) {
    return false;
  }

  if (result != nullptr && command.request_id.empty()) {
    switch (command.type) {
      case CommandType::AcquireLock:
        command.request_id = NextRequestId("lock-acquire");
        break;
      case CommandType::ReleaseLock:
        command.request_id = NextRequestId("lock-release");
        break;
      case CommandType::MvccCommit:
        command.request_id = NextRequestId("mvcc-commit");
        break;
      default:
        command.request_id = NextRequestId("command");
        break;
    }
  }

  switch (command.type) {
    case CommandType::Put:
      ++metrics_.client_put_proposals;
      break;
    case CommandType::AcquireLock:
      ++metrics_.lock_acquire_proposals;
      break;
    case CommandType::ReleaseLock:
      ++metrics_.lock_release_proposals;
      break;
    default:
      break;
  }

  const std::string effective_request_id = request_id != nullptr ? *request_id : command.request_id;
  const int target_index = LastLogIndex() + 1;
  log_.push_back(LogEntry{current_term_, std::move(command)});
  PersistState();
  match_index_[id_] = LastLogIndex();
  next_index_[id_] = LastLogIndex() + 1;

  const auto peers = peers_;
  for (int peer_id : peers) {
    ReplicateToPeer(peer_id);
  }

  AdvanceCommitIndex();
  SendHeartbeats();
  if (commit_index_ != target_index) {
    return false;
  }

  if (result != nullptr) {
    const auto applied = TakeCommandResult(effective_request_id);
    if (!applied) {
      *result = CommandResult{};
      return false;
    }
    *result = *applied;
  }
  return true;
}

void RaftNode::RecordCommandResult(const std::string& request_id, CommandResult result) {
  if (request_id.empty()) {
    return;
  }
  command_results_[request_id] = result;
  command_result_order_.push_back(request_id);
  while (command_result_order_.size() > kMaxStoredCommandResults) {
    command_results_.erase(command_result_order_.front());
    command_result_order_.pop_front();
  }
}

std::optional<CommandResult> RaftNode::TakeCommandResult(const std::string& request_id) {
  if (request_id.empty()) {
    return std::nullopt;
  }
  auto it = command_results_.find(request_id);
  if (it == command_results_.end()) {
    return std::nullopt;
  }
  auto result = it->second;
  command_results_.erase(it);
  return result;
}

std::string RaftNode::NextRequestId(const std::string& prefix) {
  ++request_sequence_;
  return prefix + "-" + std::to_string(id_) + "-" + std::to_string(current_term_) + "-" + std::to_string(request_sequence_);
}

void RaftNode::StartPreVote() {
  ResetElectionTimer();

  RequestVoteRequest request{
      current_term_ + 1,
      id_,
      LastLogIndex(),
      LastLogTerm(),
  };

  int votes_granted = 1;
  const auto peers = peers_;
  for (int peer_id : peers) {
    auto response = transport_->SendPreVote(peer_id, request);
    if (response.term > current_term_) {
      BecomeFollower(response.term);
      return;
    }
    if (response.vote_granted) {
      ++votes_granted;
    }
  }

  if (votes_granted >= Majority()) {
    StartElection();
  }
}

void RaftNode::StartElection() {
  role_ = Role::Candidate;
  ++current_term_;
  voted_for_ = id_;
  leader_id_ = -1;
  votes_received_ = 1;
  PersistState();
  ResetElectionTimer();

  RequestVoteRequest request{
      current_term_,
      id_,
      LastLogIndex(),
      LastLogTerm(),
  };

  const auto peers = peers_;
  for (int peer_id : peers) {
    auto response = transport_->SendRequestVote(peer_id, request);
    if (response.term > current_term_) {
      BecomeFollower(response.term);
      return;
    }
    if (response.vote_granted) {
      ++votes_received_;
    }
  }

  if (votes_received_ >= Majority()) {
    BecomeLeader();
  }
}

void RaftNode::BecomeFollower(int new_term, int new_leader) {
  role_ = Role::Follower;
  current_term_ = new_term;
  voted_for_ = -1;
  leader_id_ = new_leader;
  votes_received_ = 0;
  heartbeat_elapsed_ = 0;
  PersistState();
  ResetElectionTimer();
}

void RaftNode::BecomeLeader() {
  role_ = Role::Leader;
  leader_id_ = id_;
  heartbeat_elapsed_ = 0;
  log_.push_back(LogEntry{current_term_, Command{CommandType::Noop, "", "", {}}});
  PersistState();

  for (int peer_id : peers_) {
    next_index_[peer_id] = LastLogIndex() + 1;
    match_index_[peer_id] = log_base_index_;
  }
  match_index_[id_] = LastLogIndex();
  next_index_[id_] = LastLogIndex() + 1;

  AdvanceCommitIndex();
  SendHeartbeats();
}

void RaftNode::ResetElectionTimer() {
  election_elapsed_ = 0;
  std::uniform_int_distribution<int> timeout_distribution(
      base_election_timeout_ticks_,
      std::max(base_election_timeout_ticks_, base_election_timeout_ticks_ * 2 - 1));
  randomized_election_timeout_ticks_ = timeout_distribution(random_engine_);
}

void RaftNode::SendHeartbeats() {
  const auto peers = peers_;
  for (int peer_id : peers) {
    ReplicateToPeer(peer_id);
  }
}

bool RaftNode::ReplicateToPeer(int peer_id) {
  if (role_ != Role::Leader) {
    return false;
  }

  if (next_index_[peer_id] <= snapshot_state_.last_included_index) {
    return SendSnapshotToPeer(peer_id);
  }

  const int next_index = next_index_[peer_id];
  const int prev_log_index = next_index - 1;
  const int prev_log_term = TermAt(prev_log_index);

  std::vector<LogEntry> entries;
  for (int index = next_index; index <= LastLogIndex(); ++index) {
    entries.push_back(log_[ToLocalIndex(index)]);
  }

  AppendEntriesRequest request{
      current_term_,
      id_,
      prev_log_index,
      prev_log_term,
      commit_index_,
      entries,
  };

  auto response = transport_->SendAppendEntries(peer_id, request);
  if (response.term > current_term_) {
    BecomeFollower(response.term);
    return false;
  }

  if (response.success) {
    match_index_[peer_id] = response.match_index;
    next_index_[peer_id] = response.match_index + 1;
    AdvanceCommitIndex();
    return true;
  } else {
    if (response.conflict_index <= 0 && response.conflict_term < 0) {
      return false;
    }

    int next_index = response.conflict_index;
    if (response.conflict_term >= 0) {
      const int local_conflict_index = FindLastIndexOfTerm(response.conflict_term);
      next_index = local_conflict_index >= response.conflict_index
          ? local_conflict_index + 1
          : response.conflict_index;
    }
    if (next_index <= 0 || next_index >= next_index_[peer_id]) {
      next_index = std::max(snapshot_state_.last_included_index + 1, next_index_[peer_id] - 1);
    }
    next_index_[peer_id] = next_index;

    if (next_index_[peer_id] <= snapshot_state_.last_included_index) {
      return SendSnapshotToPeer(peer_id);
    }
    return ReplicateToPeer(peer_id);
  }
}

bool RaftNode::SendSnapshotToPeer(int peer_id) {
  InstallSnapshotRequest request{
      current_term_,
      id_,
      snapshot_state_.last_included_index,
      snapshot_state_.last_included_term,
      snapshot_state_.data,
      snapshot_state_.peers,
  };

  auto response = transport_->SendInstallSnapshot(peer_id, request);
  if (response.term > current_term_) {
    BecomeFollower(response.term);
    return false;
  }
  if (response.success) {
    match_index_[peer_id] = response.last_included_index;
    next_index_[peer_id] = response.last_included_index + 1;
    return true;
  }
  return false;
}

void RaftNode::AdvanceCommitIndex() {
  for (int candidate = LastLogIndex(); candidate > commit_index_; --candidate) {
    if (TermAt(candidate) != current_term_) {
      continue;
    }

    int replicated = 1;
    for (int peer_id : peers_) {
      auto it = match_index_.find(peer_id);
      if (it != match_index_.end() && it->second >= candidate) {
        ++replicated;
      }
    }

    if (replicated >= Majority()) {
      metrics_.committed_entries += static_cast<std::uint64_t>(candidate - commit_index_);
      commit_index_ = candidate;
      ApplyCommittedEntries();
      MaybeTakeSnapshot();
      return;
    }
  }
}

void RaftNode::ApplyCommittedEntries() {
  while (last_applied_ < commit_index_) {
    ++last_applied_;
    const auto& entry = log_[ToLocalIndex(last_applied_)];
    ApplyCommand(entry.command);
    ++metrics_.applied_entries;
  }
}

void RaftNode::ApplyCommand(const Command& command) {
  switch (command.type) {
    case CommandType::Noop:
      break;
    case CommandType::Put:
      store_->Put(command.key, command.value);
      break;
    case CommandType::AddPeer: {
      if (command.peer.id == id_) {
        break;
      }
      const bool exists = std::find(peers_.begin(), peers_.end(), command.peer.id) != peers_.end();
      if (!exists) {
        peers_.push_back(command.peer.id);
      }
      transport_->UpsertPeer(command.peer);
      if (role_ == Role::Leader) {
        next_index_[command.peer.id] = LastLogIndex() + 1;
        match_index_[command.peer.id] = snapshot_state_.last_included_index;
      }
      break;
    }
    case CommandType::RemovePeer: {
      peers_.erase(std::remove(peers_.begin(), peers_.end(), command.peer.id), peers_.end());
      transport_->RemovePeer(command.peer.id);
      next_index_.erase(command.peer.id);
      match_index_.erase(command.peer.id);
      break;
    }
    case CommandType::AcquireLock: {
      const auto current_owner = LockOwnerUnlocked(command.key);
      const bool acquired = !current_owner.has_value() || *current_owner == command.value;
      if (acquired) {
        store_->Put(LockOwnerKey(command.key), command.value);
      }
      RecordCommandResult(command.request_id, CommandResult{acquired, 0});
      break;
    }
    case CommandType::ReleaseLock: {
      const auto current_owner = LockOwnerUnlocked(command.key);
      const bool released = current_owner.has_value() && *current_owner == command.value;
      if (released) {
        store_->Put(LockOwnerKey(command.key), "");
      }
      RecordCommandResult(command.request_id, CommandResult{released, 0});
      break;
    }
    case CommandType::MvccCommit: {
      store_->Put(kMvccLatestTsKey, std::to_string(command.mvcc_commit_ts));
      for (const auto& write : command.writes) {
        auto versions = ParseVersionIndex(store_->Get(MvccIndexKey(write.key)).value_or(""));
        versions.push_back(command.mvcc_commit_ts);
        store_->Put(MvccDataKey(write.key, command.mvcc_commit_ts), write.value);
        store_->Put(MvccIndexKey(write.key), SerializeVersionIndex(versions));
      }
      RecordCommandResult(command.request_id, CommandResult{true, command.mvcc_commit_ts});
      break;
    }
  }
}

void RaftNode::MaybeTakeSnapshot() {
  if (commit_index_ - snapshot_state_.last_included_index < kSnapshotThreshold) {
    return;
  }

  snapshot_state_.last_included_index = commit_index_;
  snapshot_state_.last_included_term = TermAt(commit_index_);
  snapshot_state_.data = store_->Entries();
  snapshot_state_.peers = transport_->ListPeers();

  std::vector<LogEntry> new_log;
  new_log.push_back(LogEntry{snapshot_state_.last_included_term, Command{}});
  for (int index = commit_index_ + 1; index <= LastLogIndex(); ++index) {
    new_log.push_back(log_[ToLocalIndex(index)]);
  }

  log_ = std::move(new_log);
  log_base_index_ = snapshot_state_.last_included_index;

  if (persistence_) {
    persistence_->SaveSnapshot(snapshot_state_);
  }
  PersistState();
}

void RaftNode::PersistState() {
  if (!persistence_) {
    return;
  }
  persistence_->SaveState(PersistentState{
      current_term_,
      voted_for_,
      log_base_index_,
      commit_index_,
      log_,
  });
}

bool RaftNode::ConfirmLeaderForReadLocked() {
  ++metrics_.linearizable_read_checks;
  if (role_ != Role::Leader) {
    ++metrics_.linearizable_read_failures;
    return false;
  }

  if (!HasCommittedCurrentTermEntry()) {
    SendHeartbeats();
    if (role_ != Role::Leader || !HasCommittedCurrentTermEntry()) {
      ++metrics_.linearizable_read_failures;
      return false;
    }
  }

  int confirmations = 1;
  const auto peers = peers_;
  for (int peer_id : peers) {
    if (ReplicateToPeer(peer_id)) {
      ++confirmations;
      if (confirmations >= Majority()) {
        break;
      }
    }
    if (role_ != Role::Leader) {
      ++metrics_.linearizable_read_failures;
      return false;
    }
  }

  if (confirmations < Majority()) {
    ++metrics_.linearizable_read_failures;
    return false;
  }

  if (last_applied_ < commit_index_) {
    ApplyCommittedEntries();
  }
  return true;
}

bool RaftNode::HasCommittedCurrentTermEntry() const {
  return commit_index_ > 0 && TermAt(commit_index_) == current_term_;
}

bool RaftNode::HasRecentLeaderContactLocked() const {
  if (role_ == Role::Leader) {
    return true;
  }
  return leader_id_ != -1 && election_elapsed_ < heartbeat_interval_ticks_ * 2;
}

bool RaftNode::IsCandidateLogUpToDate(int last_log_index, int last_log_term) const {
  if (last_log_term != LastLogTerm()) {
    return last_log_term > LastLogTerm();
  }
  return last_log_index >= LastLogIndex();
}

bool RaftNode::HasLogIndex(int global_index) const {
  return global_index >= log_base_index_ && global_index <= LastLogIndex();
}

std::optional<std::string> RaftNode::LockOwnerUnlocked(const std::string& name) const {
  const auto owner = store_->Get(LockOwnerKey(name));
  if (!owner || owner->empty()) {
    return std::nullopt;
  }
  return owner;
}

std::optional<std::string> RaftNode::ReadMvccUnlocked(const std::string& key, std::uint64_t snapshot_ts) const {
  const auto raw_index = store_->Get(MvccIndexKey(key));
  if (!raw_index || raw_index->empty()) {
    return std::nullopt;
  }

  const auto versions = ParseVersionIndex(*raw_index);
  for (auto it = versions.rbegin(); it != versions.rend(); ++it) {
    if (*it <= snapshot_ts) {
      return store_->Get(MvccDataKey(key, *it));
    }
  }
  return std::nullopt;
}

std::uint64_t RaftNode::LatestMvccTimestampUnlocked() const {
  const auto raw = store_->Get(kMvccLatestTsKey);
  if (!raw || raw->empty()) {
    return 0;
  }
  return static_cast<std::uint64_t>(std::stoull(*raw));
}

std::uint64_t RaftNode::LatestMvccTimestampForKeyUnlocked(const std::string& key) const {
  const auto raw_index = store_->Get(MvccIndexKey(key));
  if (!raw_index || raw_index->empty()) {
    return 0;
  }
  const auto versions = ParseVersionIndex(*raw_index);
  return versions.empty() ? 0 : versions.back();
}

int RaftNode::FindFirstIndexOfTerm(int term, int hint_index) const {
  int index = hint_index;
  while (index > log_base_index_ && TermAt(index - 1) == term) {
    --index;
  }
  return index;
}

int RaftNode::FindLastIndexOfTerm(int term) const {
  for (int index = LastLogIndex(); index >= log_base_index_; --index) {
    if (TermAt(index) == term) {
      return index;
    }
  }
  return -1;
}

int RaftNode::TermAt(int global_index) const {
  return log_[ToLocalIndex(global_index)].term;
}

int RaftNode::ToLocalIndex(int global_index) const {
  return global_index - log_base_index_;
}

int RaftNode::LastLogIndex() const {
  return log_base_index_ + static_cast<int>(log_.size()) - 1;
}

int RaftNode::LastLogTerm() const {
  return log_.back().term;
}

int RaftNode::Majority() const {
  return static_cast<int>((peers_.size() + 1) / 2) + 1;
}
