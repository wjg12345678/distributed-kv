#pragma once

#include "storage/key_value_store.h"
#include "core/raft_persistence.h"
#include "core/raft_transport.h"
#include "core/raft_types.h"

#include <condition_variable>
#include <cstdint>
#include <deque>
#include <memory>
#include <mutex>
#include <optional>
#include <random>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

class RaftNode {
public:
  RaftNode(
      int id,
      std::vector<int> peers,
      IRaftTransport* transport,
      int election_timeout_ticks,
      std::unique_ptr<IKeyValueStore> store,
      std::unique_ptr<IRaftPersistence> persistence);
  ~RaftNode();

  void Tick();
  bool Propose(const std::string& key, const std::string& value);
  bool AddPeer(const PeerEndpoint& peer);
  bool RemovePeer(int peer_id);
  bool ConfirmLeaderForRead();
  std::optional<std::string> GetValue(const std::string& key) const;
  bool AcquireLock(const std::string& name, const std::string& owner);
  bool ReleaseLock(const std::string& name, const std::string& owner);
  std::optional<std::string> LockOwner(const std::string& name) const;
  std::optional<MvccTransaction> BeginMvccTransaction();
  bool StageMvccWrite(const std::string& tx_id, const std::string& key, const std::string& value);
  MvccCommitResult CommitMvccTransaction(const std::string& tx_id);
  std::optional<std::string> ReadMvcc(const std::string& key, std::uint64_t snapshot_ts) const;

  RequestVoteResponse HandlePreVote(const RequestVoteRequest& request);
  RequestVoteResponse HandleRequestVote(const RequestVoteRequest& request);
  AppendEntriesResponse HandleAppendEntries(const AppendEntriesRequest& request);
  InstallSnapshotResponse HandleInstallSnapshot(const InstallSnapshotRequest& request);

  int id() const;
  int current_term() const;
  int leader_id() const;
  int commit_index() const;
  Role role() const;
  bool IsVotingMember() const;
  const std::vector<LogEntry>& log() const;
  const IKeyValueStore& store() const;
  int log_base_index() const;
  RaftMetricsSnapshot metrics() const;

  void ForceElectionTimeout();

private:
  struct MembershipConfig {
    std::vector<int> voters;
    std::vector<int> next_voters;
    bool joint = false;
  };

  struct PendingMvccTransactionState {
    std::uint64_t snapshot_ts = 0;
    std::vector<KeyValue> writes;
  };

  void StartPreVote();
  void StartElection();
  void BecomeFollower(int new_term, int new_leader = -1);
  void BecomeLeader();
  void ResetElectionTimer();
  void SendHeartbeats();
  bool ReplicateToPeer(int peer_id);
  bool SendSnapshotToPeer(int peer_id);
  void AdvanceCommitIndex();
  void ApplyCommittedEntries();
  void ApplyCommand(const Command& command);
  void RebuildMembershipStateLocked();
  void RefreshTransportPeersLocked();
  void RefreshReplicationTrackingLocked();
  void MaybeStepDownIfRemovedLocked();
  MembershipConfig BaseConfigLocked() const;
  MembershipConfig ConfigAtIndexLocked(int global_index) const;
  bool HasMajorityLocked(const std::vector<int>& acknowledged_ids, const std::vector<int>& voters) const;
  bool HasVoteQuorumLocked(const std::vector<int>& granted_ids, const MembershipConfig& config) const;
  bool HasReadQuorumLocked(const std::vector<int>& responder_ids, const MembershipConfig& config) const;
  bool HasCommitQuorumLocked(int candidate_index, const MembershipConfig& config) const;
  bool IsVoterInConfigLocked(const MembershipConfig& config, int node_id) const;
  bool IsVotingMemberLocked(int node_id) const;
  bool HasUncommittedConfigChangeLocked() const;
  void ApplyConfigCommandLocked(
      const Command& command,
      MembershipConfig& config,
      std::unordered_map<int, PeerEndpoint>* endpoints) const;
  std::vector<int> NormalizeVoters(std::vector<int> voters) const;
  std::vector<int> ActiveRemotePeerIdsLocked() const;
  std::vector<PeerEndpoint> PeerEndpointsForConfigLocked(const MembershipConfig& config) const;
  bool AppendEntryAndCommitLocked(
      std::unique_lock<std::mutex>& lock,
      Command command,
      const std::string* request_id = nullptr,
      CommandResult* result = nullptr);
  void MaybeTakeSnapshot();
  void PersistState();
  bool ConfirmLeaderForReadLocked(std::unique_lock<std::mutex>& lock);
  bool HasCommittedCurrentTermEntry() const;
  bool HasRecentLeaderContactLocked() const;
  bool IsCandidateLogUpToDate(int last_log_index, int last_log_term) const;
  bool HasLogIndex(int global_index) const;
  int FindFirstIndexOfTerm(int term, int hint_index) const;
  int FindLastIndexOfTerm(int term) const;
  bool ReplicateCommand(Command command, const std::string* request_id = nullptr, CommandResult* result = nullptr);
  bool ReplicateCommandLocked(
      std::unique_lock<std::mutex>& lock,
      Command command,
      const std::string* request_id = nullptr,
      CommandResult* result = nullptr);
  void RecordCommandResult(const std::string& request_id, CommandResult result);
  std::optional<CommandResult> TakeCommandResult(const std::string& request_id);
  std::string NextRequestId(const std::string& prefix);
  std::optional<std::string> LockOwnerUnlocked(const std::string& name) const;
  std::optional<std::string> ReadMvccUnlocked(const std::string& key, std::uint64_t snapshot_ts) const;
  std::uint64_t LatestMvccTimestampUnlocked() const;
  std::uint64_t LatestMvccTimestampForKeyUnlocked(const std::string& key) const;
  int TermAt(int global_index) const;
  int ToLocalIndex(int global_index) const;
  int LastLogIndex() const;
  int LastLogTerm() const;

  int id_;
  std::vector<int> initial_voters_;
  std::vector<int> config_voters_;
  std::vector<int> config_next_voters_;
  bool config_joint_ = false;
  std::unordered_map<int, PeerEndpoint> peer_endpoints_;

  Role role_ = Role::Follower;
  int current_term_ = 0;
  int voted_for_ = -1;
  int leader_id_ = -1;

  std::vector<LogEntry> log_;
  int log_base_index_ = 0;
  int commit_index_ = 0;
  int last_applied_ = 0;
  SnapshotState snapshot_state_;

  std::unordered_map<int, int> next_index_;
  std::unordered_map<int, int> match_index_;
  int votes_received_ = 0;

  int election_elapsed_ = 0;
  int base_election_timeout_ticks_ = 0;
  int randomized_election_timeout_ticks_ = 0;
  int heartbeat_elapsed_ = 0;
  int heartbeat_interval_ticks_ = 2;
  std::uint64_t request_sequence_ = 0;
  std::uint64_t leader_contact_sequence_ = 0;
  std::unordered_map<std::string, PendingMvccTransactionState> pending_mvcc_transactions_;
  std::unordered_map<std::string, CommandResult> command_results_;
  std::deque<std::string> command_result_order_;
  RaftMetricsSnapshot metrics_;

  std::unique_ptr<IKeyValueStore> store_;
  std::unique_ptr<IRaftPersistence> persistence_;
  IRaftTransport* transport_ = nullptr;
  std::mt19937 random_engine_;
  std::condition_variable state_condition_;
  mutable std::mutex mutex_;
};
