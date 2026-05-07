#include "core/cluster.h"

#include <cassert>
#include <chrono>
#include <memory>
#include <thread>

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

}  // namespace

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
  assert(leader->AcquireLock("deploy", "worker-a"));
  assert(!leader->AcquireLock("deploy", "worker-b"));
  assert(!leader->ReleaseLock("deploy", "worker-b"));
  assert(leader->ReleaseLock("deploy", "worker-a"));
  assert(leader->AcquireLock("deploy", "worker-b"));

  for (int id : {1, 2, 3}) {
    auto node = cluster.FindNode(id);
    assert(node);
    assert(WaitUntil([&]() {
      const auto owner = node->LockOwner("deploy");
      return owner.has_value() && *owner == "worker-b";
    }));
  }

  return 0;
}
