#include "core/cluster.h"

#include <cassert>
#include <memory>
#include <string>

int main() {
  Cluster cluster;
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

  auto leader = cluster.FindNode(*cluster.LeaderId());
  assert(leader);

  for (int i = 0; i < 6; ++i) {
    assert(leader->Propose("k" + std::to_string(i), "v" + std::to_string(i)));
  }

  for (int id : {1, 2, 3}) {
    auto node = cluster.FindNode(id);
    assert(node);
    assert(node->log_base_index() > 0);
    for (int i = 0; i < 6; ++i) {
      auto value = node->store().Get("k" + std::to_string(i));
      assert(value.has_value());
      assert(*value == "v" + std::to_string(i));
    }
  }

  return 0;
}
