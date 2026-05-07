#pragma once

#include "core/raft_node.h"
#include "network/http_message.h"
#include "network/network_codec.h"

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
  HttpResponse HandleRequest(const HttpRequest& request);
  HttpResponse HandleGet(const std::string& key);
  HttpResponse HandlePut(const std::string& key, const std::string& value);
  HttpResponse HandleLockGet(const std::string& name) const;
  HttpResponse HandleLockAcquire(const std::string& body);
  HttpResponse HandleLockRelease(const std::string& body);
  HttpResponse HandleMvccBegin();
  HttpResponse HandleMvccWrite(const std::string& tx_id, const std::string& key, const std::string& value);
  HttpResponse HandleMvccCommit(const std::string& body);
  HttpResponse HandleMvccRead(const std::string& key, const std::optional<std::string>& snapshot_ts) const;
  HttpResponse HandleAddPeer(const std::string& body);
  HttpResponse HandleRemovePeer(const std::string& body);
  std::string HandleStatus() const;
  std::string HandleMetrics() const;
  HttpResponse RedirectToLeader(const std::string& path) const;
  HttpResponse Response(int status_code, const std::string& status_text, const std::string& body) const;
  std::optional<PeerEndpoint> FindPeer(int id) const;

  std::shared_ptr<RaftNode> node_;
  std::vector<PeerEndpoint> peers_;
  int port_ = 0;
  std::size_t worker_count_ = 0;
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
