#include "core/raft_node.h"

#include <algorithm>
#include <cstdint>
#include <iomanip>
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

bool ContainsId(const std::vector<int>& ids, int id) {
  return std::find(ids.begin(), ids.end(), id) != ids.end();
}

bool IsConfigCommandType(CommandType type) {
  return type == CommandType::AddPeer ||
      type == CommandType::RemovePeer ||
      type == CommandType::BeginJointConfig ||
      type == CommandType::FinalizeConfig;
}

}  // namespace

RaftNode::RaftNode(
    int id,
    std::vector<int> peers,
    IRaftTransport* transport,
    int election_timeout_ticks,
    std::unique_ptr<IKeyValueStore> store,
    std::unique_ptr<IRaftPersistence> persistence)
    : id_(id),
      base_election_timeout_ticks_(election_timeout_ticks),
      store_(std::move(store)),
      persistence_(std::move(persistence)),
      transport_(transport),
      random_engine_(static_cast<std::mt19937::result_type>(
          std::random_device{}() ^ (static_cast<unsigned int>(id) * 0x9e3779b9U))) {
  peers.push_back(id_);
  initial_voters_ = NormalizeVoters(std::move(peers));
  config_voters_ = initial_voters_;

  if (persistence_) {
    persistence_->LoadSnapshot(snapshot_state_);
  }

  if (snapshot_state_.last_included_index > 0) {
    store_->ReplaceWith(snapshot_state_.data);
    log_base_index_ = snapshot_state_.last_included_index;
    commit_index_ = snapshot_state_.last_included_index;
    last_applied_ = snapshot_state_.last_included_index;
    log_.push_back(LogEntry{snapshot_state_.last_included_term, Command{}});
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
  }

  RebuildMembershipStateLocked();
  last_applied_ = log_base_index_;
  ApplyCommittedEntries();
  MaybeStepDownIfRemovedLocked();
  ResetElectionTimer();
}

RaftNode::~RaftNode() = default;

void RaftNode::Tick() {
  std::unique_lock<std::mutex> lock(mutex_);
  ++election_elapsed_;

  if (role_ == Role::Leader) {
    ++heartbeat_elapsed_;
    if (heartbeat_elapsed_ >= heartbeat_interval_ticks_) {
      heartbeat_elapsed_ = 0;
      lock.unlock();
      SendHeartbeats();
    }
    return;
  }

  if (!IsVotingMemberLocked(id_)) {
    return;
  }

  if (election_elapsed_ >= randomized_election_timeout_ticks_) {
    lock.unlock();
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
  std::unique_lock<std::mutex> lock(mutex_);
  return ConfirmLeaderForReadLocked(lock);
}

std::optional<std::string> RaftNode::GetValue(const std::string& key) const {
  std::lock_guard<std::mutex> lock(mutex_);
  return store_->Get(key);
}

bool RaftNode::AddPeer(const PeerEndpoint& peer) {
  std::unique_lock<std::mutex> lock(mutex_);
  if (role_ != Role::Leader || HasUncommittedConfigChangeLocked() || peer.id == id_) {
    return false;
  }

  const MembershipConfig current_config = ConfigAtIndexLocked(LastLogIndex());
  if (current_config.joint) {
    return false;
  }
  if (IsVoterInConfigLocked(current_config, peer.id)) {
    return true;
  }

  peer_endpoints_[peer.id] = peer;
  transport_->UpsertPeer(peer);
  RefreshReplicationTrackingLocked();

  auto new_voters = current_config.voters;
  new_voters.push_back(peer.id);
  new_voters = NormalizeVoters(std::move(new_voters));

  MembershipConfig joint_config{current_config.voters, new_voters, true};
  MembershipConfig final_config{new_voters, {}, false};

  Command begin_joint;
  begin_joint.type = CommandType::BeginJointConfig;
  begin_joint.config_voters = current_config.voters;
  begin_joint.config_next_voters = new_voters;
  begin_joint.config_peers = PeerEndpointsForConfigLocked(joint_config);

  Command finalize;
  finalize.type = CommandType::FinalizeConfig;
  finalize.config_voters = new_voters;
  finalize.config_peers = PeerEndpointsForConfigLocked(final_config);

  return AppendEntryAndCommitLocked(lock, std::move(begin_joint)) &&
      AppendEntryAndCommitLocked(lock, std::move(finalize));
}

bool RaftNode::RemovePeer(int peer_id) {
  std::unique_lock<std::mutex> lock(mutex_);
  if (role_ != Role::Leader || HasUncommittedConfigChangeLocked()) {
    return false;
  }

  const MembershipConfig current_config = ConfigAtIndexLocked(LastLogIndex());
  if (current_config.joint) {
    return false;
  }
  if (!IsVoterInConfigLocked(current_config, peer_id)) {
    return true;
  }

  auto new_voters = current_config.voters;
  new_voters.erase(std::remove(new_voters.begin(), new_voters.end(), peer_id), new_voters.end());
  new_voters = NormalizeVoters(std::move(new_voters));
  if (new_voters.empty()) {
    return false;
  }

  MembershipConfig joint_config{current_config.voters, new_voters, true};
  MembershipConfig final_config{new_voters, {}, false};

  Command begin_joint;
  begin_joint.type = CommandType::BeginJointConfig;
  begin_joint.config_voters = current_config.voters;
  begin_joint.config_next_voters = new_voters;
  begin_joint.config_peers = PeerEndpointsForConfigLocked(joint_config);

  Command finalize;
  finalize.type = CommandType::FinalizeConfig;
  finalize.config_voters = new_voters;
  finalize.config_peers = PeerEndpointsForConfigLocked(final_config);

  return AppendEntryAndCommitLocked(lock, std::move(begin_joint)) &&
      AppendEntryAndCommitLocked(lock, std::move(finalize));
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
  std::unique_lock<std::mutex> lock(mutex_);
  if (!ConfirmLeaderForReadLocked(lock)) {
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
  std::unique_lock<std::mutex> lock(mutex_);
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
  const bool replicated = ReplicateCommandLocked(lock, std::move(command), nullptr, &result);
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

  if (!IsVotingMemberLocked(id_) || !IsVotingMemberLocked(request.candidate_id)) {
    return RequestVoteResponse{current_term_, false};
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

  if (!IsVotingMemberLocked(id_) || !IsVotingMemberLocked(request.candidate_id)) {
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
    ++leader_contact_sequence_;
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
    if (HasLogIndex(global_index) && TermAt(global_index) != request.entries[i].term) {
      log_.resize(ToLocalIndex(global_index));
      changed = true;
    }

    if (!HasLogIndex(global_index)) {
      log_.push_back(request.entries[i]);
      changed = true;
    }
  }

  if (changed) {
    PersistState();
    RebuildMembershipStateLocked();
  }

  if (request.leader_commit > commit_index_) {
    commit_index_ = std::min(request.leader_commit, LastLogIndex());
    ApplyCommittedEntries();
    MaybeTakeSnapshot();
    MaybeStepDownIfRemovedLocked();
    state_condition_.notify_all();
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
    ++leader_contact_sequence_;
    ResetElectionTimer();
  }

  if (request.last_included_index <= snapshot_state_.last_included_index) {
    return InstallSnapshotResponse{current_term_, true, snapshot_state_.last_included_index};
  }

  snapshot_state_.last_included_index = request.last_included_index;
  snapshot_state_.last_included_term = request.last_included_term;
  snapshot_state_.data = request.state;
  snapshot_state_.voters = NormalizeVoters(request.voters);
  snapshot_state_.next_voters = NormalizeVoters(request.next_voters);
  snapshot_state_.joint = request.joint && !snapshot_state_.next_voters.empty();
  snapshot_state_.peers = request.peers;
  store_->ReplaceWith(snapshot_state_.data);

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
  RebuildMembershipStateLocked();
  MaybeStepDownIfRemovedLocked();
  state_condition_.notify_all();

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

bool RaftNode::IsVotingMember() const {
  std::lock_guard<std::mutex> lock(mutex_);
  return IsVotingMemberLocked(id_);
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
  std::unique_lock<std::mutex> lock(mutex_);
  return ReplicateCommandLocked(lock, std::move(command), request_id, result);
}

bool RaftNode::ReplicateCommandLocked(
    std::unique_lock<std::mutex>& lock,
    Command command,
    const std::string* request_id,
    CommandResult* result) {
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

  return AppendEntryAndCommitLocked(lock, std::move(command), request_id, result);
}

bool RaftNode::AppendEntryAndCommitLocked(
    std::unique_lock<std::mutex>& lock,
    Command command,
    const std::string* request_id,
    CommandResult* result) {
  const std::string effective_request_id = request_id != nullptr ? *request_id : command.request_id;
  const int request_term = current_term_;
  const int target_index = LastLogIndex() + 1;
  log_.push_back(LogEntry{current_term_, std::move(command)});
  PersistState();
  RebuildMembershipStateLocked();
  match_index_[id_] = LastLogIndex();
  next_index_[id_] = LastLogIndex() + 1;
  AdvanceCommitIndex();
  auto peers = ActiveRemotePeerIdsLocked();
  lock.unlock();
  for (int peer_id : peers) {
    ReplicateToPeer(peer_id);
  }
  lock.lock();

  if (commit_index_ < target_index) {
    return false;
  }

  if (role_ == Role::Leader && current_term_ == request_term) {
    peers = ActiveRemotePeerIdsLocked();
    lock.unlock();
    for (int peer_id : peers) {
      ReplicateToPeer(peer_id);
    }
    lock.lock();
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
  RequestVoteRequest request;
  MembershipConfig config;
  std::vector<int> peers;
  int base_term = 0;
  std::uint64_t leader_contact_sequence = 0;
  {
    std::lock_guard<std::mutex> lock(mutex_);
    if (!IsVotingMemberLocked(id_) || HasRecentLeaderContactLocked()) {
      return;
    }
    base_term = current_term_;
    leader_contact_sequence = leader_contact_sequence_;
    ResetElectionTimer();
    request = RequestVoteRequest{
        current_term_ + 1,
        id_,
        LastLogIndex(),
        LastLogTerm(),
    };
    config = ConfigAtIndexLocked(LastLogIndex());
    peers = ActiveRemotePeerIdsLocked();
  }

  std::vector<int> votes_granted{id_};
  for (int peer_id : peers) {
    auto response = transport_->SendPreVote(peer_id, request);
    std::lock_guard<std::mutex> lock(mutex_);
    if (response.term > current_term_) {
      BecomeFollower(response.term);
      return;
    }
    if (current_term_ != base_term || role_ == Role::Leader || leader_contact_sequence_ != leader_contact_sequence) {
      return;
    }
    if (response.vote_granted) {
      votes_granted.push_back(peer_id);
    }
  }

  bool should_start_election = false;
  {
    std::lock_guard<std::mutex> lock(mutex_);
    should_start_election =
        current_term_ == base_term &&
        role_ != Role::Leader &&
        leader_contact_sequence_ == leader_contact_sequence &&
        HasVoteQuorumLocked(votes_granted, config);
  }
  if (should_start_election) {
    StartElection();
  }
}

void RaftNode::StartElection() {
  RequestVoteRequest request;
  MembershipConfig config;
  std::vector<int> peers;
  int election_term = 0;
  {
    std::lock_guard<std::mutex> lock(mutex_);
    if (!IsVotingMemberLocked(id_)) {
      return;
    }
    role_ = Role::Candidate;
    ++current_term_;
    voted_for_ = id_;
    leader_id_ = -1;
    votes_received_ = 1;
    PersistState();
    ResetElectionTimer();
    state_condition_.notify_all();

    election_term = current_term_;
    request = RequestVoteRequest{
        current_term_,
        id_,
        LastLogIndex(),
        LastLogTerm(),
    };
    config = ConfigAtIndexLocked(LastLogIndex());
    peers = ActiveRemotePeerIdsLocked();
  }

  std::vector<int> votes_granted{id_};
  for (int peer_id : peers) {
    auto response = transport_->SendRequestVote(peer_id, request);
    std::lock_guard<std::mutex> lock(mutex_);
    if (response.term > current_term_) {
      BecomeFollower(response.term);
      return;
    }
    if (current_term_ != election_term || role_ != Role::Candidate) {
      return;
    }
    if (response.vote_granted) {
      votes_granted.push_back(peer_id);
      ++votes_received_;
    }
  }

  bool became_leader = false;
  {
    std::lock_guard<std::mutex> lock(mutex_);
    if (current_term_ == election_term &&
        role_ == Role::Candidate &&
        HasVoteQuorumLocked(votes_granted, config)) {
      BecomeLeader();
      became_leader = true;
    }
  }

  if (became_leader) {
    for (int peer_id : peers) {
      ReplicateToPeer(peer_id);
    }
  }
}

void RaftNode::BecomeFollower(int new_term, int new_leader) {
  role_ = Role::Follower;
  current_term_ = new_term;
  voted_for_ = -1;
  leader_id_ = new_leader;
  if (new_leader != -1) {
    ++leader_contact_sequence_;
  }
  votes_received_ = 0;
  heartbeat_elapsed_ = 0;
  PersistState();
  ResetElectionTimer();
  state_condition_.notify_all();
}

void RaftNode::BecomeLeader() {
  if (!IsVotingMemberLocked(id_)) {
    return;
  }

  role_ = Role::Leader;
  leader_id_ = id_;
  heartbeat_elapsed_ = 0;
  log_.push_back(LogEntry{current_term_, Command{CommandType::Noop, "", "", {}}});
  PersistState();
  RebuildMembershipStateLocked();

  match_index_[id_] = LastLogIndex();
  next_index_[id_] = LastLogIndex() + 1;

  AdvanceCommitIndex();
  state_condition_.notify_all();
}

void RaftNode::ResetElectionTimer() {
  election_elapsed_ = 0;
  std::uniform_int_distribution<int> timeout_distribution(
      base_election_timeout_ticks_,
      std::max(base_election_timeout_ticks_, base_election_timeout_ticks_ * 2 - 1));
  randomized_election_timeout_ticks_ = timeout_distribution(random_engine_);
}

void RaftNode::SendHeartbeats() {
  std::vector<int> peers;
  {
    std::lock_guard<std::mutex> lock(mutex_);
    if (role_ != Role::Leader) {
      return;
    }
    peers = ActiveRemotePeerIdsLocked();
  }

  for (int peer_id : peers) {
    ReplicateToPeer(peer_id);
  }
}

bool RaftNode::ReplicateToPeer(int peer_id) {
  while (true) {
    bool send_snapshot = false;
    int request_term = 0;
    int expected_next_index = 0;
    AppendEntriesRequest append_request;
    InstallSnapshotRequest snapshot_request;

    {
      std::lock_guard<std::mutex> lock(mutex_);
      if (role_ != Role::Leader || !ContainsId(ActiveRemotePeerIdsLocked(), peer_id)) {
        return false;
      }

      auto next_it = next_index_.find(peer_id);
      if (next_it == next_index_.end()) {
        next_index_[peer_id] = LastLogIndex() + 1;
        match_index_[peer_id] = 0;
        next_it = next_index_.find(peer_id);
      }

      request_term = current_term_;
      expected_next_index = next_it->second;
      if (snapshot_state_.last_included_index > 0 &&
          next_it->second <= snapshot_state_.last_included_index) {
        send_snapshot = true;
        snapshot_request = InstallSnapshotRequest{
            current_term_,
            id_,
            snapshot_state_.last_included_index,
            snapshot_state_.last_included_term,
            snapshot_state_.data,
            snapshot_state_.voters,
            snapshot_state_.next_voters,
            snapshot_state_.joint,
            snapshot_state_.peers,
        };
      } else {
        const int prev_log_index = next_it->second - 1;
        std::vector<LogEntry> entries;
        for (int index = next_it->second; index <= LastLogIndex(); ++index) {
          entries.push_back(log_[ToLocalIndex(index)]);
        }
        append_request = AppendEntriesRequest{
            current_term_,
            id_,
            prev_log_index,
            TermAt(prev_log_index),
            commit_index_,
            std::move(entries),
        };
      }
    }

    if (send_snapshot) {
      auto response = transport_->SendInstallSnapshot(peer_id, snapshot_request);
      std::lock_guard<std::mutex> lock(mutex_);
      if (response.term > current_term_) {
        BecomeFollower(response.term);
        return false;
      }
      if (role_ != Role::Leader ||
          current_term_ != request_term ||
          !ContainsId(ActiveRemotePeerIdsLocked(), peer_id)) {
        return false;
      }
      if (response.success) {
        match_index_[peer_id] = response.last_included_index;
        next_index_[peer_id] = response.last_included_index + 1;
        AdvanceCommitIndex();
        state_condition_.notify_all();
        return true;
      }
      return false;
    }

    auto response = transport_->SendAppendEntries(peer_id, append_request);
    std::lock_guard<std::mutex> lock(mutex_);
    if (response.term > current_term_) {
      BecomeFollower(response.term);
      return false;
    }
    if (role_ != Role::Leader ||
        current_term_ != request_term ||
        !ContainsId(ActiveRemotePeerIdsLocked(), peer_id)) {
      return false;
    }

    if (response.success) {
      match_index_[peer_id] = std::max(match_index_[peer_id], response.match_index);
      next_index_[peer_id] = std::max(next_index_[peer_id], response.match_index + 1);
      AdvanceCommitIndex();
      state_condition_.notify_all();
      return true;
    }

    if (response.conflict_index <= 0 && response.conflict_term < 0) {
      return false;
    }

    int next_retry_index = response.conflict_index;
    if (response.conflict_term >= 0) {
      const int local_conflict_index = FindLastIndexOfTerm(response.conflict_term);
      next_retry_index = local_conflict_index >= response.conflict_index
          ? local_conflict_index + 1
          : response.conflict_index;
    }
    if (next_retry_index <= 0 || next_retry_index >= expected_next_index) {
      next_retry_index = std::max(log_base_index_ + 1, expected_next_index - 1);
    }
    next_index_[peer_id] = next_retry_index;
  }
}

bool RaftNode::SendSnapshotToPeer(int peer_id) {
  return ReplicateToPeer(peer_id);
}

void RaftNode::AdvanceCommitIndex() {
  for (int candidate = LastLogIndex(); candidate > commit_index_; --candidate) {
    if (TermAt(candidate) != current_term_) {
      continue;
    }

    const MembershipConfig config = ConfigAtIndexLocked(candidate);
    if (HasCommitQuorumLocked(candidate, config)) {
      metrics_.committed_entries += static_cast<std::uint64_t>(candidate - commit_index_);
      commit_index_ = candidate;
      ApplyCommittedEntries();
      MaybeTakeSnapshot();
      MaybeStepDownIfRemovedLocked();
      state_condition_.notify_all();
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
    case CommandType::AddPeer:
    case CommandType::RemovePeer:
    case CommandType::BeginJointConfig:
    case CommandType::FinalizeConfig:
      break;
    case CommandType::Put:
      store_->Put(command.key, command.value);
      break;
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

void RaftNode::RebuildMembershipStateLocked() {
  MembershipConfig config = BaseConfigLocked();
  std::unordered_map<int, PeerEndpoint> endpoints;

  const auto seed_peers = snapshot_state_.last_included_index > 0 ? snapshot_state_.peers : transport_->ListPeers();
  for (const auto& peer : seed_peers) {
    if (peer.id != id_) {
      endpoints[peer.id] = peer;
    }
  }

  for (int index = log_base_index_ + 1; index <= LastLogIndex(); ++index) {
    ApplyConfigCommandLocked(log_[ToLocalIndex(index)].command, config, &endpoints);
  }

  config_voters_ = NormalizeVoters(std::move(config.voters));
  config_next_voters_ = NormalizeVoters(std::move(config.next_voters));
  config_joint_ = config.joint && !config_next_voters_.empty();
  peer_endpoints_ = std::move(endpoints);
  RefreshTransportPeersLocked();
  RefreshReplicationTrackingLocked();
}

void RaftNode::RefreshTransportPeersLocked() {
  const auto active_peers = ActiveRemotePeerIdsLocked();
  for (const auto& peer : transport_->ListPeers()) {
    if (!ContainsId(active_peers, peer.id)) {
      transport_->RemovePeer(peer.id);
    }
  }

  for (int peer_id : active_peers) {
    auto it = peer_endpoints_.find(peer_id);
    if (it == peer_endpoints_.end()) {
      it = peer_endpoints_.emplace(peer_id, PeerEndpoint{peer_id, "127.0.0.1", 0, 0}).first;
    }
    transport_->UpsertPeer(it->second);
  }
}

void RaftNode::RefreshReplicationTrackingLocked() {
  const auto active_peers = ActiveRemotePeerIdsLocked();

  for (auto it = next_index_.begin(); it != next_index_.end();) {
    if (it->first != id_ && !ContainsId(active_peers, it->first)) {
      it = next_index_.erase(it);
    } else {
      ++it;
    }
  }
  for (auto it = match_index_.begin(); it != match_index_.end();) {
    if (it->first != id_ && !ContainsId(active_peers, it->first)) {
      it = match_index_.erase(it);
    } else {
      ++it;
    }
  }

  match_index_[id_] = LastLogIndex();
  next_index_[id_] = LastLogIndex() + 1;

  if (role_ != Role::Leader) {
    return;
  }

  for (int peer_id : active_peers) {
    auto& next = next_index_[peer_id];
    if (next <= 0 || next > LastLogIndex() + 1) {
      next = LastLogIndex() + 1;
    }
    auto& match = match_index_[peer_id];
    if (match < 0) {
      match = 0;
    }
  }
}

void RaftNode::MaybeStepDownIfRemovedLocked() {
  const MembershipConfig committed_config = ConfigAtIndexLocked(commit_index_);
  if (role_ == Role::Leader && !IsVoterInConfigLocked(committed_config, id_)) {
    BecomeFollower(current_term_);
    return;
  }

  if (role_ == Role::Candidate && !IsVotingMemberLocked(id_)) {
    role_ = Role::Follower;
    leader_id_ = -1;
    votes_received_ = 0;
    heartbeat_elapsed_ = 0;
    voted_for_ = -1;
    PersistState();
    ResetElectionTimer();
    state_condition_.notify_all();
  }
}

RaftNode::MembershipConfig RaftNode::BaseConfigLocked() const {
  MembershipConfig config;
  if (!snapshot_state_.voters.empty()) {
    config.voters = snapshot_state_.voters;
    config.next_voters = snapshot_state_.next_voters;
    config.joint = snapshot_state_.joint && !snapshot_state_.next_voters.empty();
    return config;
  }

  config.voters = initial_voters_;
  return config;
}

RaftNode::MembershipConfig RaftNode::ConfigAtIndexLocked(int global_index) const {
  MembershipConfig config = BaseConfigLocked();
  if (global_index < log_base_index_) {
    return config;
  }

  const int capped_index = std::min(global_index, LastLogIndex());
  for (int index = log_base_index_ + 1; index <= capped_index; ++index) {
    ApplyConfigCommandLocked(log_[ToLocalIndex(index)].command, config, nullptr);
  }
  return config;
}

bool RaftNode::HasMajorityLocked(const std::vector<int>& acknowledged_ids, const std::vector<int>& voters) const {
  int matches = 0;
  for (int voter_id : voters) {
    if (ContainsId(acknowledged_ids, voter_id)) {
      ++matches;
    }
  }
  return matches >= static_cast<int>(voters.size() / 2) + 1;
}

bool RaftNode::HasVoteQuorumLocked(const std::vector<int>& granted_ids, const MembershipConfig& config) const {
  if (!config.joint) {
    return HasMajorityLocked(granted_ids, config.voters);
  }
  return HasMajorityLocked(granted_ids, config.voters) &&
      HasMajorityLocked(granted_ids, config.next_voters);
}

bool RaftNode::HasReadQuorumLocked(const std::vector<int>& responder_ids, const MembershipConfig& config) const {
  return HasVoteQuorumLocked(responder_ids, config);
}

bool RaftNode::HasCommitQuorumLocked(int candidate_index, const MembershipConfig& config) const {
  auto all_voters = config.voters;
  if (config.joint) {
    all_voters.insert(all_voters.end(), config.next_voters.begin(), config.next_voters.end());
    all_voters = NormalizeVoters(std::move(all_voters));
  }

  std::vector<int> replicated_ids;
  for (int voter_id : all_voters) {
    auto it = match_index_.find(voter_id);
    if (it != match_index_.end() && it->second >= candidate_index) {
      replicated_ids.push_back(voter_id);
    }
  }
  if (!config.joint) {
    return HasMajorityLocked(replicated_ids, config.voters);
  }
  return HasMajorityLocked(replicated_ids, config.voters) &&
      HasMajorityLocked(replicated_ids, config.next_voters);
}

bool RaftNode::IsVoterInConfigLocked(const MembershipConfig& config, int node_id) const {
  return ContainsId(config.voters, node_id) || (config.joint && ContainsId(config.next_voters, node_id));
}

bool RaftNode::IsVotingMemberLocked(int node_id) const {
  MembershipConfig config;
  config.voters = config_voters_;
  config.next_voters = config_next_voters_;
  config.joint = config_joint_;
  return IsVoterInConfigLocked(config, node_id);
}

bool RaftNode::HasUncommittedConfigChangeLocked() const {
  for (int index = commit_index_ + 1; index <= LastLogIndex(); ++index) {
    if (IsConfigCommandType(log_[ToLocalIndex(index)].command.type)) {
      return true;
    }
  }
  return false;
}

void RaftNode::ApplyConfigCommandLocked(
    const Command& command,
    MembershipConfig& config,
    std::unordered_map<int, PeerEndpoint>* endpoints) const {
  switch (command.type) {
    case CommandType::AddPeer:
      config.voters.push_back(command.peer.id);
      config.voters = NormalizeVoters(std::move(config.voters));
      config.next_voters.clear();
      config.joint = false;
      if (endpoints != nullptr && command.peer.id != id_) {
        (*endpoints)[command.peer.id] = command.peer;
      }
      return;
    case CommandType::RemovePeer:
      config.voters.erase(std::remove(config.voters.begin(), config.voters.end(), command.peer.id), config.voters.end());
      config.voters = NormalizeVoters(std::move(config.voters));
      config.next_voters.clear();
      config.joint = false;
      return;
    case CommandType::BeginJointConfig:
      config.voters = NormalizeVoters(command.config_voters);
      config.next_voters = NormalizeVoters(command.config_next_voters);
      config.joint = !config.next_voters.empty();
      break;
    case CommandType::FinalizeConfig:
      config.voters = NormalizeVoters(command.config_voters);
      config.next_voters.clear();
      config.joint = false;
      break;
    default:
      return;
  }

  if (endpoints == nullptr) {
    return;
  }

  for (const auto& peer : command.config_peers) {
    if (peer.id != id_) {
      (*endpoints)[peer.id] = peer;
    }
  }
}

std::vector<int> RaftNode::NormalizeVoters(std::vector<int> voters) const {
  voters.erase(std::remove_if(voters.begin(), voters.end(), [](int voter_id) {
    return voter_id <= 0;
  }), voters.end());
  std::sort(voters.begin(), voters.end());
  voters.erase(std::unique(voters.begin(), voters.end()), voters.end());
  return voters;
}

std::vector<int> RaftNode::ActiveRemotePeerIdsLocked() const {
  auto peers = config_voters_;
  if (config_joint_) {
    peers.insert(peers.end(), config_next_voters_.begin(), config_next_voters_.end());
    peers = NormalizeVoters(std::move(peers));
  }
  peers.erase(std::remove(peers.begin(), peers.end(), id_), peers.end());
  return peers;
}

std::vector<PeerEndpoint> RaftNode::PeerEndpointsForConfigLocked(const MembershipConfig& config) const {
  auto voters = config.voters;
  if (config.joint) {
    voters.insert(voters.end(), config.next_voters.begin(), config.next_voters.end());
    voters = NormalizeVoters(std::move(voters));
  }

  std::vector<PeerEndpoint> peers;
  peers.reserve(voters.size());
  for (int voter_id : voters) {
    if (voter_id == id_) {
      continue;
    }
    auto it = peer_endpoints_.find(voter_id);
    if (it != peer_endpoints_.end()) {
      peers.push_back(it->second);
    } else {
      peers.push_back(PeerEndpoint{voter_id, "127.0.0.1", 0, 0});
    }
  }
  return peers;
}

void RaftNode::MaybeTakeSnapshot() {
  if (commit_index_ - snapshot_state_.last_included_index < kSnapshotThreshold) {
    return;
  }

  const MembershipConfig committed_config = ConfigAtIndexLocked(commit_index_);
  snapshot_state_.last_included_index = commit_index_;
  snapshot_state_.last_included_term = TermAt(commit_index_);
  snapshot_state_.data = store_->Entries();
  snapshot_state_.voters = committed_config.voters;
  snapshot_state_.next_voters = committed_config.next_voters;
  snapshot_state_.joint = committed_config.joint;
  snapshot_state_.peers = PeerEndpointsForConfigLocked(committed_config);

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

bool RaftNode::ConfirmLeaderForReadLocked(std::unique_lock<std::mutex>& lock) {
  ++metrics_.linearizable_read_checks;
  if (role_ != Role::Leader) {
    ++metrics_.linearizable_read_failures;
    return false;
  }

  int request_term = current_term_;
  if (!HasCommittedCurrentTermEntry()) {
    const auto peers = ActiveRemotePeerIdsLocked();
    lock.unlock();
    for (int peer_id : peers) {
      ReplicateToPeer(peer_id);
    }
    lock.lock();

    if (role_ != Role::Leader || current_term_ != request_term || !HasCommittedCurrentTermEntry()) {
      ++metrics_.linearizable_read_failures;
      return false;
    }
  }

  request_term = current_term_;
  const MembershipConfig config = ConfigAtIndexLocked(LastLogIndex());
  const auto peers = ActiveRemotePeerIdsLocked();
  lock.unlock();

  std::vector<int> responders{id_};
  for (int peer_id : peers) {
    if (ReplicateToPeer(peer_id)) {
      responders.push_back(peer_id);
    }
  }

  lock.lock();
  if (role_ != Role::Leader || current_term_ != request_term || !HasReadQuorumLocked(responders, config)) {
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
