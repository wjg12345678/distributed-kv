#include "core/cluster.h"

#include <cassert>
#include <memory>

int main() {
  Cluster cluster;
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
  auto node4 = std::make_shared<RaftNode>(
      4,
      std::vector<int>{1, 2, 3},
      &cluster,
      20,
      std::make_unique<InMemoryKeyValueStore>(),
      std::make_unique<NoopRaftPersistence>());

  cluster.AddNode(node1);
  cluster.AddNode(node2);
  cluster.AddNode(node3);
  cluster.AddNode(node4);

  for (int tick = 0; tick < 20 && !cluster.LeaderId(); ++tick) {
    cluster.TickAll();
  }

  auto leader = cluster.FindNode(*cluster.LeaderId());
  assert(leader);
  assert(leader->AddPeer(PeerEndpoint{4, "127.0.0.1", 0, 0}));
  assert(leader->Propose("after_add", "replicated"));

  auto value = node4->store().Get("after_add");
  assert(value.has_value());
  assert(*value == "replicated");
  assert(node4->commit_index() >= 3);

  return 0;
}
