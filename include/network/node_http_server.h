#pragma once

#include "network/network_codec.h"
#include "network/task_executor.h"
#include "core/raft_node.h"

#include <cstddef>
#include <atomic>
#include <chrono>
#include <memory>
#include <mutex>
#include <optional>
#include <vector>

class NodeHttpServer {
public:
  NodeHttpServer(
      std::shared_ptr<RaftNode> node,
      std::vector<PeerEndpoint> peers,
      int port,
      std::size_t worker_count = 0);
  void Run();

private:
  void HandleClient(int client_fd);
  std::string HandleRequest(const std::string& request);
  std::string HandleGet(const std::string& key);
  std::string HandlePut(const std::string& key, const std::string& value);
  std::string HandleLockGet(const std::string& name) const;
  std::string HandleLockAcquire(const std::string& body);
  std::string HandleLockRelease(const std::string& body);
  std::string HandleMvccBegin();
  std::string HandleMvccWrite(const std::string& tx_id, const std::string& key, const std::string& value);
  std::string HandleMvccCommit(const std::string& body);
  std::string HandleMvccRead(const std::string& key, const std::optional<std::string>& snapshot_ts) const;
  std::string HandleAddPeer(const std::string& body);
  std::string HandleRemovePeer(const std::string& body);
  std::string HandleStatus() const;
  std::string HandleMetrics() const;
  std::string RedirectToLeader(const std::string& path) const;
  std::string Response(int status_code, const std::string& status_text, const std::string& body) const;
  std::optional<PeerEndpoint> FindPeer(int id) const;

  std::shared_ptr<RaftNode> node_;
  std::vector<PeerEndpoint> peers_;
  int port_ = 0;
  TaskExecutor executor_;
  mutable std::mutex peers_mutex_;
  std::chrono::steady_clock::time_point started_at_ = std::chrono::steady_clock::now();
  mutable std::atomic<std::uint64_t> http_requests_total_{0};
  mutable std::atomic<std::uint64_t> kv_get_total_{0};
  mutable std::atomic<std::uint64_t> kv_put_total_{0};
  mutable std::atomic<std::uint64_t> lock_acquire_total_{0};
  mutable std::atomic<std::uint64_t> lock_release_total_{0};
  mutable std::atomic<std::uint64_t> mvcc_begin_total_{0};
  mutable std::atomic<std::uint64_t> mvcc_write_total_{0};
  mutable std::atomic<std::uint64_t> mvcc_commit_total_{0};
  mutable std::atomic<std::uint64_t> mvcc_read_total_{0};
};
