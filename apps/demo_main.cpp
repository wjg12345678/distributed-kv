#include "core/cluster.h"

#include <iostream>
#include <memory>
#include <stdexcept>
#include <vector>

namespace {

std::shared_ptr<RaftNode> RequireNode(const Cluster& cluster, int id) {
  auto node = cluster.FindNode(id);
  if (!node) {
    throw std::runtime_error("node not found");
  }
  return node;
}

const char* RoleName(Role role) {
  switch (role) {
    case Role::Follower:
      return "Follower";
    case Role::Candidate:
      return "Candidate";
    case Role::Leader:
      return "Leader";
  }
  return "Unknown";
}

void PrintClusterState(const Cluster& cluster, const std::vector<int>& ids) {
  for (int id : ids) {
    auto node = RequireNode(cluster, id);
    std::cout << "node=" << node->id()
              << " role=" << RoleName(node->role())
              << " term=" << node->current_term()
              << " commit=" << node->commit_index()
              << " log_size=" << node->log().size() - 1
              << '\n';
  }
}

void PrintValueOnEachNode(const Cluster& cluster, const std::vector<int>& ids, const std::string& key) {
  for (int id : ids) {
    auto node = RequireNode(cluster, id);
    auto value = node->store().Get(key);
    std::cout << "node=" << id << " key=" << key << " value=" << (value ? *value : "<nil>") << '\n';
  }
}

}  // namespace

int main() {
  Cluster cluster;
  std::vector<int> ids{1, 2, 3};

  cluster.AddNode(std::make_shared<RaftNode>(
      1,
      std::vector<int>{2, 3},
      &cluster,
      5,
      std::make_unique<InMemoryKeyValueStore>(),
      std::make_unique<NoopRaftPersistence>()));
  cluster.AddNode(std::make_shared<RaftNode>(
      2,
      std::vector<int>{1, 3},
      &cluster,
      8,
      std::make_unique<InMemoryKeyValueStore>(),
      std::make_unique<NoopRaftPersistence>()));
  cluster.AddNode(std::make_shared<RaftNode>(
      3,
      std::vector<int>{1, 2},
      &cluster,
      10,
      std::make_unique<InMemoryKeyValueStore>(),
      std::make_unique<NoopRaftPersistence>()));

  for (int tick = 0; tick < 20 && !cluster.LeaderId(); ++tick) {
    cluster.TickAll();
  }

  auto leader_id = cluster.LeaderId();
  if (!leader_id) {
    std::cerr << "failed to elect leader\n";
    return 1;
  }

  auto leader = RequireNode(cluster, *leader_id);
  std::cout << "leader elected: node " << leader->id() << "\n\n";

  if (!leader->Propose("service", "distributed-kv")) {
    std::cerr << "proposal failed\n";
    return 1;
  }
  if (!leader->Propose("language", "cpp17")) {
    std::cerr << "proposal failed\n";
    return 1;
  }

  cluster.TickAll();

  PrintClusterState(cluster, ids);
  std::cout << '\n';
  PrintValueOnEachNode(cluster, ids, "service");
  PrintValueOnEachNode(cluster, ids, "language");

  return 0;
}
