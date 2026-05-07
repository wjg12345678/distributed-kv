#include "core/raft_node.h"
#include "core/raft_transport.h"

#include <cassert>
#include <chrono>
#include <condition_variable>
#include <future>
#include <memory>
#include <mutex>
#include <stdexcept>
#include <thread>
#include <unordered_map>
#include <vector>

class BlockingCluster : public IRaftTransport {
public:
  BlockingCluster(int blocked_leader_id, int blocked_target_id)
      : blocked_leader_id_(blocked_leader_id), blocked_target_id_(blocked_target_id) {}

  void AddNode(const std::shared_ptr<RaftNode>& node) {
    nodes_[node->id()] = node;
  }

  RequestVoteResponse SendPreVote(int target_id, const RequestVoteRequest& request) override {
    return FindNodeOrThrow(target_id)->HandlePreVote(request);
  }

  RequestVoteResponse SendRequestVote(int target_id, const RequestVoteRequest& request) override {
    return FindNodeOrThrow(target_id)->HandleRequestVote(request);
  }

  AppendEntriesResponse SendAppendEntries(int target_id, const AppendEntriesRequest& request) override {
    if (blocking_enabled_ && request.leader_id == blocked_leader_id_ && target_id == blocked_target_id_) {
      std::unique_lock<std::mutex> lock(block_mutex_);
      blocked_entered_ = true;
      blocked_condition_.notify_all();
      blocked_condition_.wait(lock, [this]() {
        return blocked_released_;
      });
    }
    return FindNodeOrThrow(target_id)->HandleAppendEntries(request);
  }

  InstallSnapshotResponse SendInstallSnapshot(int target_id, const InstallSnapshotRequest& request) override {
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

  void EnableBlocking() {
    std::lock_guard<std::mutex> lock(block_mutex_);
    blocking_enabled_ = true;
    blocked_entered_ = false;
    blocked_released_ = false;
  }

  bool WaitUntilBlockedFor(std::chrono::milliseconds timeout) {
    std::unique_lock<std::mutex> lock(block_mutex_);
    return blocked_condition_.wait_for(lock, timeout, [this]() {
      return blocked_entered_;
    });
  }

  void ReleaseBlocked() {
    {
      std::lock_guard<std::mutex> lock(block_mutex_);
      blocked_released_ = true;
    }
    blocked_condition_.notify_all();
  }

private:
  std::shared_ptr<RaftNode> FindNodeOrThrow(int id) const {
    auto node = FindNode(id);
    if (!node) {
      throw std::runtime_error("node not found: " + std::to_string(id));
    }
    return node;
  }

  int blocked_leader_id_ = 0;
  int blocked_target_id_ = 0;
  std::unordered_map<int, std::shared_ptr<RaftNode>> nodes_;
  std::unordered_map<int, PeerEndpoint> peers_;
  std::mutex block_mutex_;
  std::condition_variable blocked_condition_;
  bool blocking_enabled_ = false;
  bool blocked_entered_ = false;
  bool blocked_released_ = false;
};

int main() {
  BlockingCluster cluster(1, 2);
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

  node1->ForceElectionTimeout();
  for (int tick = 0; tick < 10 && !cluster.LeaderId(); ++tick) {
    cluster.TickAll();
  }

  const auto leader_id = cluster.LeaderId();
  assert(leader_id.has_value());
  assert(*leader_id == 1);
  cluster.EnableBlocking();

  std::promise<bool> proposal_done;
  auto proposal_result = proposal_done.get_future();
  std::thread writer([&]() {
    proposal_done.set_value(node1->Propose("async", "ok"));
  });

  assert(cluster.WaitUntilBlockedFor(std::chrono::milliseconds(500)));

  auto term_read = std::async(std::launch::async, [&]() {
    return node1->current_term();
  });
  assert(term_read.wait_for(std::chrono::milliseconds(50)) == std::future_status::ready);
  assert(term_read.get() >= 1);

  cluster.ReleaseBlocked();

  assert(proposal_result.wait_for(std::chrono::milliseconds(500)) == std::future_status::ready);
  writer.join();
  assert(proposal_result.get());
  for (int attempt = 0; attempt < 100; ++attempt) {
    const auto follower_value = node3->store().Get("async");
    if (follower_value.has_value() && *follower_value == "ok") {
      return 0;
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
  }
  assert(false);
  return 0;
}
