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

  const auto tx1 = leader->BeginMvccTransaction();
  assert(tx1.has_value());
  assert(leader->StageMvccWrite(tx1->tx_id, "order", "v1"));
  const auto commit1 = leader->CommitMvccTransaction(tx1->tx_id);
  assert(commit1.committed);
  assert(commit1.commit_ts >= 1);

  const auto tx2 = leader->BeginMvccTransaction();
  const auto tx3 = leader->BeginMvccTransaction();
  assert(tx2.has_value());
  assert(tx3.has_value());
  assert(tx2->snapshot_ts == commit1.commit_ts);
  assert(tx3->snapshot_ts == commit1.commit_ts);

  assert(leader->StageMvccWrite(tx2->tx_id, "order", "v2"));
  const auto commit2 = leader->CommitMvccTransaction(tx2->tx_id);
  assert(commit2.committed);
  assert(commit2.commit_ts > commit1.commit_ts);

  assert(leader->StageMvccWrite(tx3->tx_id, "order", "v3"));
  const auto commit3 = leader->CommitMvccTransaction(tx3->tx_id);
  assert(!commit3.committed);
  assert(commit3.conflict);

  for (int id : {1, 2, 3}) {
    auto node = cluster.FindNode(id);
    assert(node);
    assert(WaitUntil([&]() {
      const auto snapshot_v1 = node->ReadMvcc("order", commit1.commit_ts);
      const auto snapshot_v2 = node->ReadMvcc("order", commit2.commit_ts);
      return snapshot_v1.has_value() &&
          snapshot_v2.has_value() &&
          *snapshot_v1 == "v1" &&
          *snapshot_v2 == "v2";
    }));
  }

  return 0;
}
