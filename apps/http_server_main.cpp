#include "core/cluster.h"
#include "network/http_server.h"

#include <filesystem>
#include <iostream>
#include <memory>
#include <string>
#include <vector>

namespace {

void RemoveOldDb(const std::string& path) {
  std::error_code error;
  std::filesystem::remove_all(path, error);
}

}  // namespace

int main() {
  Cluster cluster;
  const std::string base = "/tmp/distributed-kv-rocksdb";
  const int port = 9006;

  RemoveOldDb(base + "-node1");
  RemoveOldDb(base + "-node2");
  RemoveOldDb(base + "-node3");
  RemoveOldDb(base + "-node1-meta");
  RemoveOldDb(base + "-node2-meta");
  RemoveOldDb(base + "-node3-meta");

  cluster.AddNode(std::make_shared<RaftNode>(
      1,
      std::vector<int>{2, 3},
      &cluster,
      5,
      std::make_unique<RocksDbKeyValueStore>(base + "-node1"),
      std::make_unique<RocksDbRaftPersistence>(base + "-node1-meta")));
  cluster.AddNode(std::make_shared<RaftNode>(
      2,
      std::vector<int>{1, 3},
      &cluster,
      8,
      std::make_unique<RocksDbKeyValueStore>(base + "-node2"),
      std::make_unique<RocksDbRaftPersistence>(base + "-node2-meta")));
  cluster.AddNode(std::make_shared<RaftNode>(
      3,
      std::vector<int>{1, 2},
      &cluster,
      10,
      std::make_unique<RocksDbKeyValueStore>(base + "-node3"),
      std::make_unique<RocksDbRaftPersistence>(base + "-node3-meta")));

  for (int tick = 0; tick < 20 && !cluster.LeaderId(); ++tick) {
    cluster.TickAll();
  }

  auto leader_id = cluster.LeaderId();
  if (!leader_id) {
    std::cerr << "failed to elect leader\n";
    return 1;
  }

  std::cout << "leader elected: node " << *leader_id << '\n';
  std::cout << "HTTP server listening on http://127.0.0.1:" << port << '\n';
  std::cout << "PUT /kv/<key> with raw request body\n";
  std::cout << "GET /kv/<key>\n";

  HttpServer server(&cluster, port);
  server.Run();
  return 0;
}
