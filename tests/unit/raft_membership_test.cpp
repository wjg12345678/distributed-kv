#include "core/raft_node.h"
#include "core/raft_persistence.h"
#include "core/raft_transport.h"
#include "storage/key_value_store.h"

#include <cassert>
#include <chrono>
#include <cstdint>
#include <filesystem>
#include <memory>
#include <stdexcept>
#include <string>
#include <thread>
#include <unordered_map>
#include <unordered_set>
#include <vector>

class MembershipTestCluster : public IRaftTransport {
public:
  void AddNode(const std::shared_ptr<RaftNode>& node) {
    nodes_[node->id()] = node;
    peers_[node->id()] = PeerEndpoint{node->id(), "127.0.0.1", 9100 + node->id(), 9200 + node->id()};
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
    peers_[peer.id] = peer;
  }

  void RemovePeer(int peer_id) override {
    peers_.erase(peer_id);
  }

  std::vector<PeerEndpoint> ListPeers() const override {
    std::vector<PeerEndpoint> peers;
    peers.reserve(peers_.size());
    for (const auto& [id, peer] : peers_) {
      (void)id;
      peers.push_back(peer);
    }
    return peers;
  }

  std::shared_ptr<RaftNode> FindNode(int id) const {
    auto it = nodes_.find(id);
    if (it == nodes_.end()) {
      return nullptr;
    }
    return it->second;
  }

  std::optional<int> LeaderId() const {
    for (const auto& [id, node] : nodes_) {
      (void)id;
      if (node->role() == Role::Leader) {
        return node->id();
      }
    }
    return std::nullopt;
  }

  void TickAll() {
    for (const auto& [id, node] : nodes_) {
      (void)id;
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

  std::unordered_map<int, std::shared_ptr<RaftNode>> nodes_;
  std::unordered_map<int, PeerEndpoint> peers_;
  std::unordered_set<std::uint64_t> blocked_links_;
};

namespace {

template <typename Predicate>
bool WaitUntil(Predicate&& predicate, int attempts = 100, int sleep_ms = 10) {
  for (int attempt = 0; attempt < attempts; ++attempt) {
    if (predicate()) {
      return true;
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(sleep_ms));
  }
  return predicate();
}

PeerEndpoint EndpointFor(int id) {
  return PeerEndpoint{id, "127.0.0.1", 9100 + id, 9200 + id};
}

std::shared_ptr<RaftNode> MakeInMemoryNode(
    int id,
    std::vector<int> peers,
    IRaftTransport* transport,
    int election_timeout_ticks) {
  return std::make_shared<RaftNode>(
      id,
      std::move(peers),
      transport,
      election_timeout_ticks,
      std::make_unique<InMemoryKeyValueStore>(),
      std::make_unique<NoopRaftPersistence>());
}

std::shared_ptr<RaftNode> MakePersistentNode(
    int id,
    std::vector<int> peers,
    IRaftTransport* transport,
    int election_timeout_ticks,
    const std::string& root_dir) {
  std::filesystem::create_directories(root_dir);
  return std::make_shared<RaftNode>(
      id,
      std::move(peers),
      transport,
      election_timeout_ticks,
      std::make_unique<RocksDbKeyValueStore>(root_dir + "/store"),
      std::make_unique<RocksDbRaftPersistence>(root_dir + "/meta"));
}

std::shared_ptr<RaftNode> WaitForLeader(MembershipTestCluster& cluster, int ticks = 40) {
  for (int tick = 0; tick < ticks && !cluster.LeaderId().has_value(); ++tick) {
    cluster.TickAll();
  }
  const auto leader_id = cluster.LeaderId();
  assert(leader_id.has_value());
  auto leader = cluster.FindNode(*leader_id);
  assert(leader);
  return leader;
}

void TickFor(MembershipTestCluster& cluster, int ticks) {
  for (int tick = 0; tick < ticks; ++tick) {
    cluster.TickAll();
  }
}

void RunAddAndRemovePeerScenario() {
  MembershipTestCluster cluster;
  auto node1 = MakeInMemoryNode(1, {2, 3}, &cluster, 4);
  auto node2 = MakeInMemoryNode(2, {1, 3}, &cluster, 7);
  auto node3 = MakeInMemoryNode(3, {1, 2}, &cluster, 9);
  auto node4 = MakeInMemoryNode(4, {1, 2, 3}, &cluster, 20);
  cluster.AddNode(node1);
  cluster.AddNode(node2);
  cluster.AddNode(node3);
  cluster.AddNode(node4);

  auto leader = WaitForLeader(cluster);
  assert(leader->AddPeer(EndpointFor(4)));
  assert(node4->IsVotingMember());
  assert(leader->Propose("after_add", "replicated"));
  assert(WaitUntil([&]() {
    const auto added_value = node4->store().Get("after_add");
    return added_value.has_value() && *added_value == "replicated";
  }));

  assert(leader->RemovePeer(4));
  assert(leader->Propose("after_remove", "still_quorum"));
  std::this_thread::sleep_for(std::chrono::milliseconds(100));
  assert(!node4->store().Get("after_remove").has_value());
}

void RunRemoveLeaderScenario() {
  MembershipTestCluster cluster;
  auto node1 = MakeInMemoryNode(1, {2, 3}, &cluster, 4);
  auto node2 = MakeInMemoryNode(2, {1, 3}, &cluster, 7);
  auto node3 = MakeInMemoryNode(3, {1, 2}, &cluster, 9);
  cluster.AddNode(node1);
  cluster.AddNode(node2);
  cluster.AddNode(node3);

  auto leader = WaitForLeader(cluster);
  const int removed_leader_id = leader->id();
  assert(leader->RemovePeer(removed_leader_id));
  assert(!leader->IsVotingMember());
  TickFor(cluster, 40);

  const auto new_leader_id = cluster.LeaderId();
  assert(new_leader_id.has_value());
  assert(*new_leader_id != removed_leader_id);
  auto new_leader = cluster.FindNode(*new_leader_id);
  assert(new_leader);
  assert(new_leader->Propose("after_leader_remove", "ok"));

  for (int id : {1, 2, 3}) {
    auto node = cluster.FindNode(id);
    assert(node);
    if (id == removed_leader_id) {
      std::this_thread::sleep_for(std::chrono::milliseconds(100));
      assert(!node->store().Get("after_leader_remove").has_value());
    } else {
      assert(WaitUntil([&]() {
        const auto value = node->store().Get("after_leader_remove");
        return value.has_value() && *value == "ok";
      }));
    }
  }
}

void RunPartitionedChangeScenario() {
  MembershipTestCluster cluster;
  auto node1 = MakeInMemoryNode(1, {2, 3}, &cluster, 4);
  auto node2 = MakeInMemoryNode(2, {1, 3}, &cluster, 7);
  auto node3 = MakeInMemoryNode(3, {1, 2}, &cluster, 9);
  cluster.AddNode(node1);
  cluster.AddNode(node2);
  cluster.AddNode(node3);

  auto leader = WaitForLeader(cluster);
  const int leader_id = leader->id();
  const int remove_target = leader_id == 2 ? 3 : 2;
  const int required_survivor = 6 - leader_id - remove_target;
  cluster.DisconnectBidirectional(leader_id, required_survivor);

  assert(!leader->RemovePeer(remove_target));
  auto target = cluster.FindNode(remove_target);
  assert(target);
  assert(target->IsVotingMember());
}

void RunRestartRecoveryScenario() {
  const std::string root = "/tmp/distributed-kv-membership-restart";
  std::filesystem::remove_all(root);

  int removed_id = 0;
  {
    MembershipTestCluster cluster;
    auto node1 = MakePersistentNode(1, {2, 3}, &cluster, 4, root + "/node1");
    auto node2 = MakePersistentNode(2, {1, 3}, &cluster, 7, root + "/node2");
    auto node3 = MakePersistentNode(3, {1, 2}, &cluster, 9, root + "/node3");
    cluster.AddNode(node1);
    cluster.AddNode(node2);
    cluster.AddNode(node3);

    auto leader = WaitForLeader(cluster);
    removed_id = leader->id() == 3 ? 2 : 3;
    auto removed = cluster.FindNode(removed_id);
    assert(removed);
    assert(leader->RemovePeer(removed_id));
    assert(leader->Propose("persist_0", "v0"));
    assert(leader->Propose("persist_1", "v1"));
  }

  {
    MembershipTestCluster cluster;
    auto node1 = MakePersistentNode(1, {2, 3}, &cluster, 4, root + "/node1");
    auto node2 = MakePersistentNode(2, {1, 3}, &cluster, 7, root + "/node2");
    auto node3 = MakePersistentNode(3, {1, 2}, &cluster, 9, root + "/node3");
    cluster.AddNode(node1);
    cluster.AddNode(node2);
    cluster.AddNode(node3);

    auto leader = WaitForLeader(cluster);
    assert(leader->id() != removed_id);
    assert(leader->Propose("after_restart", "restored"));

    for (int id : {1, 2, 3}) {
      auto node = cluster.FindNode(id);
      assert(node);
      if (id == removed_id) {
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
        assert(!node->store().Get("after_restart").has_value());
      } else {
        assert(WaitUntil([&]() {
          const auto value = node->store().Get("after_restart");
          return value.has_value() && *value == "restored";
        }));
      }
    }
  }

  std::filesystem::remove_all(root);
}

}  // namespace

int main() {
  RunAddAndRemovePeerScenario();
  RunRemoveLeaderScenario();
  RunPartitionedChangeScenario();
  RunRestartRecoveryScenario();
  return 0;
}
