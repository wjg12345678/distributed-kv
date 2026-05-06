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

int main() {
  PartitionableCluster cluster;
  auto node1 = std::make_shared<RaftNode>(
      1,
      std::vector<int>{2, 3},
      &cluster,
      4,
      std::make_unique<InMemoryKeyValueStore>(),
      std::make_unique<NoopRaftPersistence>());
  auto node2 = std::make_shared<RaftNode>(
      2,
      std::vector<int>{1, 3},
      &cluster,
      7,
      std::make_unique<InMemoryKeyValueStore>(),
      std::make_unique<NoopRaftPersistence>());
  auto node3 = std::make_shared<RaftNode>(
      3,
      std::vector<int>{1, 2},
      &cluster,
      9,
      std::make_unique<InMemoryKeyValueStore>(),
      std::make_unique<NoopRaftPersistence>());
  cluster.AddNode(node1);
  cluster.AddNode(node2);
  cluster.AddNode(node3);

  for (int tick = 0; tick < 20; ++tick) {
    cluster.TickAll();
  }

  std::shared_ptr<RaftNode> initial_leader;
  std::shared_ptr<RaftNode> follower_a;
  std::shared_ptr<RaftNode> follower_b;
  if (node1->role() == Role::Leader) {
    initial_leader = node1;
    follower_a = node2;
    follower_b = node3;
  } else if (node2->role() == Role::Leader) {
    initial_leader = node2;
    follower_a = node1;
    follower_b = node3;
  } else {
    initial_leader = node3;
    follower_a = node1;
    follower_b = node2;
  }

  assert(initial_leader->role() == Role::Leader);
  assert(initial_leader->Propose("service", "v1"));
  assert(initial_leader->ConfirmLeaderForRead());
  const auto first_value = initial_leader->GetValue("service");
  assert(first_value.has_value());
  assert(*first_value == "v1");

  const int initial_term = initial_leader->current_term();
  cluster.DisconnectBidirectional(initial_leader->id(), follower_a->id());
  cluster.DisconnectBidirectional(initial_leader->id(), follower_b->id());

  for (int tick = 0; tick < 40; ++tick) {
    cluster.TickAll();
  }

  assert(!initial_leader->ConfirmLeaderForRead());
  assert(initial_leader->current_term() == initial_term);

  std::shared_ptr<RaftNode> new_leader = follower_a->role() == Role::Leader ? follower_a : follower_b;
  assert(new_leader->role() == Role::Leader);
  assert(new_leader->current_term() > initial_term);
  assert(new_leader->ConfirmLeaderForRead());
  assert(new_leader->Propose("service", "v2"));
  const auto second_value = new_leader->GetValue("service");
  assert(second_value.has_value());
  assert(*second_value == "v2");

  return 0;
}
