#include "core/raft_node.h"
#include "core/raft_transport.h"

#include <cassert>
#include <cstdint>
#include <memory>
#include <stdexcept>
#include <unordered_map>
#include <unordered_set>
#include <vector>

class PartitionableCluster : public IRaftTransport {
public:
  void AddNode(const std::shared_ptr<RaftNode>& node) {
    nodes_.push_back(node);
    node_index_[node->id()] = node;
  }

  RequestVoteResponse SendPreVote(int target_id, const RequestVoteRequest& request) override {
    if (IsBlocked(request.candidate_id, target_id)) {
      return RequestVoteResponse{request.term - 1, false};
    }
    return FindNodeOrThrow(target_id)->HandlePreVote(request);
  }

  RequestVoteResponse SendRequestVote(int target_id, const RequestVoteRequest& request) override {
    if (IsBlocked(request.candidate_id, target_id)) {
      return RequestVoteResponse{request.term, false};
    }
    return FindNodeOrThrow(target_id)->HandleRequestVote(request);
  }

  AppendEntriesResponse SendAppendEntries(int target_id, const AppendEntriesRequest& request) override {
    if (IsBlocked(request.leader_id, target_id)) {
      return AppendEntriesResponse{request.term, false, 0, 0, -1};
    }
    return FindNodeOrThrow(target_id)->HandleAppendEntries(request);
  }

  InstallSnapshotResponse SendInstallSnapshot(int target_id, const InstallSnapshotRequest& request) override {
    if (IsBlocked(request.leader_id, target_id)) {
      return InstallSnapshotResponse{request.term, false, 0};
    }
    return FindNodeOrThrow(target_id)->HandleInstallSnapshot(request);
  }

  void UpsertPeer(const PeerEndpoint& peer) override {
    (void)peer;
  }

  void RemovePeer(int peer_id) override {
    (void)peer_id;
  }

  std::vector<PeerEndpoint> ListPeers() const override {
    return {};
  }

  std::shared_ptr<RaftNode> FindNode(int id) const {
    auto it = node_index_.find(id);
    if (it == node_index_.end()) {
      return nullptr;
    }
    return it->second;
  }

  std::optional<int> LeaderId() const {
    for (const auto& node : nodes_) {
      if (node->role() == Role::Leader) {
        return node->id();
      }
    }
    return std::nullopt;
  }

  void TickAll() {
    for (const auto& node : nodes_) {
      node->Tick();
    }
  }

  void DisconnectBidirectional(int left, int right) {
    blocked_links_.insert(LinkKey(left, right));
    blocked_links_.insert(LinkKey(right, left));
  }

  void ConnectBidirectional(int left, int right) {
    blocked_links_.erase(LinkKey(left, right));
    blocked_links_.erase(LinkKey(right, left));
  }

private:
  static std::uint64_t LinkKey(int source_id, int target_id) {
    return (static_cast<std::uint64_t>(static_cast<std::uint32_t>(source_id)) << 32U) |
        static_cast<std::uint32_t>(target_id);
  }

  bool IsBlocked(int source_id, int target_id) const {
    return blocked_links_.find(LinkKey(source_id, target_id)) != blocked_links_.end();
  }

  std::shared_ptr<RaftNode> FindNodeOrThrow(int id) const {
    auto node = FindNode(id);
    if (!node) {
      throw std::runtime_error("node not found: " + std::to_string(id));
    }
    return node;
  }

  std::vector<std::shared_ptr<RaftNode>> nodes_;
  std::unordered_map<int, std::shared_ptr<RaftNode>> node_index_;
  std::unordered_set<std::uint64_t> blocked_links_;
};

namespace {

void RunPreVoteRecoveryTrial() {
  PartitionableCluster cluster;
  cluster.AddNode(std::make_shared<RaftNode>(
      1,
      std::vector<int>{2, 3},
      &cluster,
      4,
      std::make_unique<InMemoryKeyValueStore>(),
      std::make_unique<NoopRaftPersistence>()));
  cluster.AddNode(std::make_shared<RaftNode>(
      2,
      std::vector<int>{1, 3},
      &cluster,
      7,
      std::make_unique<InMemoryKeyValueStore>(),
      std::make_unique<NoopRaftPersistence>()));
  cluster.AddNode(std::make_shared<RaftNode>(
      3,
      std::vector<int>{1, 2},
      &cluster,
      9,
      std::make_unique<InMemoryKeyValueStore>(),
      std::make_unique<NoopRaftPersistence>()));

  for (int tick = 0; tick < 20 && !cluster.LeaderId(); ++tick) {
    cluster.TickAll();
  }

  const auto leader_id = cluster.LeaderId();
  assert(leader_id.has_value());

  const int isolated_id = *leader_id == 3 ? 2 : 3;
  const int connected_id = 6 - *leader_id - isolated_id;
  auto leader = cluster.FindNode(*leader_id);
  auto isolated = cluster.FindNode(isolated_id);
  assert(leader);
  assert(isolated);

  const int stable_term = leader->current_term();
  cluster.DisconnectBidirectional(isolated_id, *leader_id);
  cluster.DisconnectBidirectional(isolated_id, connected_id);

  for (int tick = 0; tick < 40; ++tick) {
    cluster.TickAll();
  }

  assert(cluster.LeaderId().has_value());
  assert(*cluster.LeaderId() == *leader_id);
  assert(leader->current_term() == stable_term);
  assert(isolated->current_term() == stable_term);

  cluster.ConnectBidirectional(isolated_id, *leader_id);
  cluster.ConnectBidirectional(isolated_id, connected_id);

  for (int tick = 0; tick < 6; ++tick) {
    cluster.TickAll();
  }

  assert(cluster.LeaderId().has_value());
  assert(*cluster.LeaderId() == *leader_id);
  assert(leader->current_term() == stable_term);
  assert(isolated->current_term() == stable_term);
}

}  // namespace

int main() {
  for (int trial = 0; trial < 20; ++trial) {
    RunPreVoteRecoveryTrial();
  }

  return 0;
}
