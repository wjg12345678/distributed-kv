#include "core/cluster.h"

#include <cassert>
#include <memory>

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

  auto leader_id = cluster.LeaderId();
  assert(leader_id.has_value());

  auto leader = cluster.FindNode(*leader_id);
  assert(leader);
  assert(leader->role() == Role::Leader);
  assert(leader->commit_index() >= 1);

  const bool replicated = leader->Propose("k", "v");
  assert(replicated);

  for (int id : {1, 2, 3}) {
    auto node = cluster.FindNode(id);
    assert(node);
    auto value = node->store().Get("k");
    assert(value.has_value());
    assert(*value == "v");
    assert(node->commit_index() == 2);
  }

  return 0;
}
