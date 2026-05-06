#include "core/cluster.h"

#include <cassert>
#include <filesystem>
#include <memory>

int main() {
  const std::string data_path = "/tmp/distributed-kv-persistence-data";
  const std::string meta_path = "/tmp/distributed-kv-persistence-meta";
  std::filesystem::remove_all(data_path);
  std::filesystem::remove_all(meta_path);

  {
    Cluster cluster;
    auto node = std::make_shared<RaftNode>(
        1,
        std::vector<int>{},
        &cluster,
        4,
        std::make_unique<RocksDbKeyValueStore>(data_path),
        std::make_unique<RocksDbRaftPersistence>(meta_path));
    cluster.AddNode(node);

    node->ForceElectionTimeout();
    node->Tick();
    assert(node->role() == Role::Leader);
    assert(node->Propose("k1", "v1"));
    assert(node->Propose("k2", "v2"));
    assert(node->current_term() == 1);
  }

  {
    Cluster cluster;
    auto restored = std::make_shared<RaftNode>(
        1,
        std::vector<int>{},
        &cluster,
        4,
        std::make_unique<RocksDbKeyValueStore>(data_path),
        std::make_unique<RocksDbRaftPersistence>(meta_path));
    cluster.AddNode(restored);

    assert(restored->current_term() == 1);
    assert(restored->store().Get("k1").has_value());
    assert(restored->store().Get("k2").has_value());
    assert(restored->log_base_index() + static_cast<int>(restored->log().size()) - 1 >= 2);
  }

  std::filesystem::remove_all(data_path);
  std::filesystem::remove_all(meta_path);
  return 0;
}
