#include "core/cluster.h"

#include <cassert>
#include <memory>

int main() {
  Cluster cluster;
  auto follower = std::make_shared<RaftNode>(
      2,
      std::vector<int>{1, 3},
      &cluster,
      7,
      std::make_unique<InMemoryKeyValueStore>(),
      std::make_unique<NoopRaftPersistence>());
  cluster.AddNode(std::make_shared<RaftNode>(
      1,
      std::vector<int>{2, 3},
      &cluster,
      4,
      std::make_unique<InMemoryKeyValueStore>(),
      std::make_unique<NoopRaftPersistence>()));
  cluster.AddNode(follower);
  cluster.AddNode(std::make_shared<RaftNode>(
      3,
      std::vector<int>{1, 2},
      &cluster,
      9,
      std::make_unique<InMemoryKeyValueStore>(),
      std::make_unique<NoopRaftPersistence>()));

  AppendEntriesRequest seed{
      1,
      1,
      0,
      0,
      0,
      {LogEntry{1, Command{CommandType::Put, "stale", "v1", {}}},
       LogEntry{1, Command{CommandType::Put, "stale2", "v2", {}}}},
  };
  auto seeded = follower->HandleAppendEntries(seed);
  assert(seeded.success);

  AppendEntriesRequest conflict{
      2,
      1,
      0,
      0,
      2,
      {LogEntry{2, Command{CommandType::Put, "fresh", "ok", {}}},
       LogEntry{2, Command{CommandType::Put, "fresh2", "ok2", {}}}},
  };
  auto replaced = follower->HandleAppendEntries(conflict);
  assert(replaced.success);
  assert(follower->log().size() == 3);
  assert(follower->log()[1].command.key == "fresh");
  assert(follower->log()[2].command.key == "fresh2");
  assert(follower->store().Get("fresh").has_value());
  assert(follower->store().Get("stale").has_value() == false);

  return 0;
}
