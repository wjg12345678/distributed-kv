#include "network/network_transport.h"

#include <uv.h>

#include <arpa/inet.h>
#include <netdb.h>

#include <condition_variable>
#include <cstring>
#include <deque>
#include <functional>
#include <memory>
#include <mutex>
#include <queue>
#include <stdexcept>
#include <string>
#include <thread>
#include <unordered_map>
#include <utility>

namespace {

std::string EncodeFrame(const std::string& payload) {
  std::string frame(sizeof(std::uint32_t), '\0');
  const std::uint32_t length = htonl(static_cast<std::uint32_t>(payload.size()));
  std::memcpy(frame.data(), &length, sizeof(length));
  frame.append(payload);
  return frame;
}

struct PendingRpc {
  int peer_id = 0;
  std::string payload;
  std::string response;
  bool done = false;
  bool success = false;
  std::mutex mutex;
  std::condition_variable condition;
};

struct PeerConnection {
  PeerConnection(void* owner_ptr, const PeerEndpoint& endpoint_value)
      : owner(owner_ptr), peer_id(endpoint_value.id), endpoint(endpoint_value), port(std::to_string(endpoint_value.raft_port)) {}

  void* owner = nullptr;
  int peer_id = 0;
  PeerEndpoint endpoint;
  std::string port;
  uv_tcp_t handle{};
  uv_connect_t connect_request{};
  uv_getaddrinfo_t resolve_request{};
  std::deque<std::shared_ptr<PendingRpc>> queue;
  std::shared_ptr<PendingRpc> inflight;
  std::string read_buffer;
  bool resolving = false;
  bool connecting = false;
  bool connected = false;
  bool write_in_progress = false;
  bool closing = false;
};

struct WriteRequest {
  uv_write_t request{};
  std::shared_ptr<PeerConnection> connection;
  std::shared_ptr<std::string> payload;
};

void CompletePending(const std::shared_ptr<PendingRpc>& pending, bool success, std::string response = {}) {
  if (!pending) {
    return;
  }

  {
    std::lock_guard<std::mutex> lock(pending->mutex);
    if (pending->done) {
      return;
    }
    pending->done = true;
    pending->success = success;
    pending->response = std::move(response);
  }
  pending->condition.notify_all();
}

}  // namespace

struct NetworkTransport::Impl {
  explicit Impl() {
    int status = uv_loop_init(&loop);
    if (status != 0) {
      throw std::runtime_error(std::string("failed to initialize libuv loop: ") + uv_strerror(status));
    }

    status = uv_async_init(&loop, &async, &Impl::OnAsync);
    if (status != 0) {
      uv_loop_close(&loop);
      throw std::runtime_error(std::string("failed to initialize libuv async: ") + uv_strerror(status));
    }
    async.data = this;

    loop_thread = std::thread([this]() {
      uv_run(&loop, UV_RUN_DEFAULT);
      uv_loop_close(&loop);
    });
  }

  ~Impl() {
    Shutdown();
  }

  void Enqueue(std::function<void()> task) {
    {
      std::lock_guard<std::mutex> lock(tasks_mutex);
      if (stopping) {
        return;
      }
      tasks.push(std::move(task));
    }
    uv_async_send(&async);
  }

  void Shutdown() {
    {
      std::lock_guard<std::mutex> lock(tasks_mutex);
      if (stopping) {
        if (loop_thread.joinable()) {
          loop_thread.join();
        }
        return;
      }
      stopping = true;
      tasks.push([this]() {
        for (auto& [peer_id, connection] : connections) {
          (void)peer_id;
          FailAndClose(connection);
        }
        connections.clear();
        if (!uv_is_closing(reinterpret_cast<uv_handle_t*>(&async))) {
          uv_close(reinterpret_cast<uv_handle_t*>(&async), nullptr);
        }
      });
    }
    uv_async_send(&async);
    if (loop_thread.joinable()) {
      loop_thread.join();
    }
  }

  void Submit(const PeerEndpoint& endpoint, const std::shared_ptr<PendingRpc>& pending) {
    Enqueue([this, endpoint, pending]() {
      auto connection = GetOrCreateConnection(endpoint);
      if (!connection) {
        CompletePending(pending, false);
        return;
      }

      connection->queue.push_back(pending);
      MaybeConnect(connection);
      MaybeSendNext(connection);
    });
  }

  void ResetPeer(int peer_id) {
    Enqueue([this, peer_id]() {
      auto it = connections.find(peer_id);
      if (it == connections.end()) {
        return;
      }
      FailAndClose(it->second);
      connections.erase(it);
    });
  }

  void ReconfigurePeer(const PeerEndpoint& endpoint) {
    Enqueue([this, endpoint]() {
      auto it = connections.find(endpoint.id);
      if (it == connections.end()) {
        return;
      }

      const auto& current = it->second->endpoint;
      if (current.host == endpoint.host && current.raft_port == endpoint.raft_port) {
        return;
      }
      FailAndClose(it->second);
      connections.erase(it);
    });
  }

  void RemovePeer(int peer_id) {
    ResetPeer(peer_id);
  }

  static void OnAsync(uv_async_t* handle) {
    auto* impl = static_cast<Impl*>(handle->data);
    impl->DrainTasks();
  }

  static void AllocateBuffer(uv_handle_t*, std::size_t suggested_size, uv_buf_t* buffer) {
    const std::size_t size = suggested_size == 0 ? 4096 : suggested_size;
    buffer->base = new char[size];
    buffer->len = static_cast<unsigned int>(size);
  }

  static void OnResolved(uv_getaddrinfo_t* request, int status, addrinfo* result) {
    auto* connection = static_cast<PeerConnection*>(request->data);
    auto* impl = static_cast<Impl*>(connection->owner);
    impl->Resolved(connection, status, result);
  }

  static void OnConnected(uv_connect_t* request, int status) {
    auto* connection = static_cast<PeerConnection*>(request->data);
    auto* impl = static_cast<Impl*>(connection->owner);
    impl->Connected(connection, status);
  }

  static void OnRead(uv_stream_t* stream, ssize_t nread, const uv_buf_t* buffer) {
    auto* connection = static_cast<PeerConnection*>(stream->data);
    auto* impl = static_cast<Impl*>(connection->owner);
    impl->Read(connection, nread, buffer);
  }

  static void OnWriteDone(uv_write_t* request, int status) {
    std::unique_ptr<WriteRequest> write_request(static_cast<WriteRequest*>(request->data));
    auto* impl = static_cast<Impl*>(write_request->connection->owner);
    impl->WriteCompleted(write_request->connection, status);
  }

  static void OnConnectionClosed(uv_handle_t* handle) {
    auto* connection = static_cast<PeerConnection*>(handle->data);
    auto* impl = static_cast<Impl*>(connection->owner);
    impl->closing_connections.erase(connection);
  }

  void DrainTasks() {
    std::queue<std::function<void()>> pending;
    {
      std::lock_guard<std::mutex> lock(tasks_mutex);
      pending.swap(tasks);
    }

    while (!pending.empty()) {
      pending.front()();
      pending.pop();
    }
  }

  std::shared_ptr<PeerConnection> GetOrCreateConnection(const PeerEndpoint& endpoint) {
    auto it = connections.find(endpoint.id);
    if (it != connections.end()) {
      const auto& current = it->second->endpoint;
      if (current.host == endpoint.host && current.raft_port == endpoint.raft_port) {
        return it->second;
      }
      FailAndClose(it->second);
      connections.erase(it);
    }

    auto connection = std::make_shared<PeerConnection>(this, endpoint);
    int status = uv_tcp_init(&loop, &connection->handle);
    if (status != 0) {
      return nullptr;
    }
    connection->handle.data = connection.get();
    connections[endpoint.id] = connection;
    return connection;
  }

  void MaybeConnect(const std::shared_ptr<PeerConnection>& connection) {
    if (connection->closing || connection->connected || connection->connecting || connection->resolving) {
      return;
    }

    addrinfo hints{};
    hints.ai_family = AF_INET;
    hints.ai_socktype = SOCK_STREAM;
    connection->resolve_request.data = connection.get();
    connection->resolving = true;
    const int status = uv_getaddrinfo(
        &loop,
        &connection->resolve_request,
        &Impl::OnResolved,
        connection->endpoint.host.c_str(),
        connection->port.c_str(),
        &hints);
    if (status != 0) {
      connection->resolving = false;
      FailAndClose(connection);
      connections.erase(connection->peer_id);
    }
  }

  void Resolved(PeerConnection* raw_connection, int status, addrinfo* result) {
    auto connection = Lookup(raw_connection);
    if (!connection) {
      if (result != nullptr) {
        uv_freeaddrinfo(result);
      }
      return;
    }

    connection->resolving = false;
    if (connection->closing || status != 0 || result == nullptr) {
      if (result != nullptr) {
        uv_freeaddrinfo(result);
      }
      FailAndClose(connection);
      connections.erase(connection->peer_id);
      return;
    }

    uv_tcp_nodelay(&connection->handle, 1);
    uv_tcp_keepalive(&connection->handle, 1, 60);
    connection->connect_request.data = connection.get();
    connection->connecting = true;
    const int connect_status = uv_tcp_connect(
        &connection->connect_request,
        &connection->handle,
        result->ai_addr,
        &Impl::OnConnected);
    uv_freeaddrinfo(result);
    if (connect_status != 0) {
      connection->connecting = false;
      FailAndClose(connection);
      connections.erase(connection->peer_id);
    }
  }

  void Connected(PeerConnection* raw_connection, int status) {
    auto connection = Lookup(raw_connection);
    if (!connection) {
      return;
    }

    connection->connecting = false;
    if (connection->closing || status != 0) {
      FailAndClose(connection);
      connections.erase(connection->peer_id);
      return;
    }

    connection->connected = true;
    uv_read_start(reinterpret_cast<uv_stream_t*>(&connection->handle), &Impl::AllocateBuffer, &Impl::OnRead);
    MaybeSendNext(connection);
  }

  void MaybeSendNext(const std::shared_ptr<PeerConnection>& connection) {
    if (connection->closing || !connection->connected || connection->write_in_progress || connection->inflight || connection->queue.empty()) {
      return;
    }

    connection->inflight = connection->queue.front();
    connection->queue.pop_front();
    auto payload = std::make_shared<std::string>(EncodeFrame(connection->inflight->payload));

    auto* write_request = new WriteRequest{};
    write_request->request.data = write_request;
    write_request->connection = connection;
    write_request->payload = std::move(payload);
    connection->write_in_progress = true;

    uv_buf_t buffer = uv_buf_init(write_request->payload->data(), static_cast<unsigned int>(write_request->payload->size()));
    const int status = uv_write(
        &write_request->request,
        reinterpret_cast<uv_stream_t*>(&connection->handle),
        &buffer,
        1,
        &Impl::OnWriteDone);
    if (status != 0) {
      connection->write_in_progress = false;
      delete write_request;
      FailAndClose(connection);
      connections.erase(connection->peer_id);
    }
  }

  void WriteCompleted(const std::shared_ptr<PeerConnection>& connection, int status) {
    connection->write_in_progress = false;
    if (connection->closing || status != 0) {
      FailAndClose(connection);
      connections.erase(connection->peer_id);
      return;
    }
  }

  void Read(PeerConnection* raw_connection, ssize_t nread, const uv_buf_t* buffer) {
    std::unique_ptr<char[]> guard(buffer->base);
    auto connection = Lookup(raw_connection);
    if (!connection) {
      return;
    }

    if (nread == 0) {
      return;
    }
    if (nread < 0) {
      FailAndClose(connection);
      connections.erase(connection->peer_id);
      return;
    }

    connection->read_buffer.append(buffer->base, static_cast<std::size_t>(nread));
    while (connection->read_buffer.size() >= sizeof(std::uint32_t)) {
      std::uint32_t length_network = 0;
      std::memcpy(&length_network, connection->read_buffer.data(), sizeof(length_network));
      const std::uint32_t length = ntohl(length_network);
      if (connection->read_buffer.size() < sizeof(length_network) + length) {
        return;
      }

      std::string payload = connection->read_buffer.substr(sizeof(length_network), length);
      connection->read_buffer.erase(0, sizeof(length_network) + length);

      if (!connection->inflight) {
        FailAndClose(connection);
        connections.erase(connection->peer_id);
        return;
      }

      auto pending = connection->inflight;
      connection->inflight.reset();
      CompletePending(pending, true, std::move(payload));
      MaybeSendNext(connection);
    }
  }

  void FailAndClose(const std::shared_ptr<PeerConnection>& connection) {
    if (!connection) {
      return;
    }

    if (connection->inflight) {
      CompletePending(connection->inflight, false);
      connection->inflight.reset();
    }
    while (!connection->queue.empty()) {
      CompletePending(connection->queue.front(), false);
      connection->queue.pop_front();
    }

    if (connection->closing) {
      return;
    }

    connection->closing = true;
    connection->connected = false;
    connection->connecting = false;
    connection->resolving = false;
    uv_cancel(reinterpret_cast<uv_req_t*>(&connection->resolve_request));
    uv_read_stop(reinterpret_cast<uv_stream_t*>(&connection->handle));
    closing_connections.emplace(connection.get(), connection);
    uv_close(reinterpret_cast<uv_handle_t*>(&connection->handle), &Impl::OnConnectionClosed);
  }

  std::shared_ptr<PeerConnection> Lookup(PeerConnection* connection) {
    auto it = connections.find(connection->peer_id);
    if (it != connections.end() && it->second.get() == connection) {
      return it->second;
    }
    auto closing_it = closing_connections.find(connection);
    if (closing_it != closing_connections.end()) {
      return closing_it->second;
    }
    return nullptr;
  }

  uv_loop_t loop{};
  uv_async_t async{};
  std::thread loop_thread;
  std::mutex tasks_mutex;
  std::queue<std::function<void()>> tasks;
  std::unordered_map<int, std::shared_ptr<PeerConnection>> connections;
  std::unordered_map<PeerConnection*, std::shared_ptr<PeerConnection>> closing_connections;
  bool stopping = false;
};

NetworkTransport::NetworkTransport(std::vector<PeerEndpoint> peers) : impl_(std::make_unique<Impl>()) {
  for (auto& peer : peers) {
    peers_.emplace(peer.id, std::move(peer));
  }
}

NetworkTransport::~NetworkTransport() = default;

RequestVoteResponse NetworkTransport::SendPreVote(int target_id, const RequestVoteRequest& request) {
  std::string response;
  if (!RoundTrip(target_id, EncodePreVoteRequest(request), response)) {
    return RequestVoteResponse{request.term - 1, false};
  }
  return DecodePreVoteResponse(response);
}

RequestVoteResponse NetworkTransport::SendRequestVote(int target_id, const RequestVoteRequest& request) {
  std::string response;
  if (!RoundTrip(target_id, EncodeRequestVoteRequest(request), response)) {
    return RequestVoteResponse{request.term, false};
  }
  return DecodeRequestVoteResponse(response);
}

AppendEntriesResponse NetworkTransport::SendAppendEntries(int target_id, const AppendEntriesRequest& request) {
  std::string response;
  if (!RoundTrip(target_id, EncodeAppendEntriesRequest(request), response)) {
    return AppendEntriesResponse{request.term, false, 0, 0, -1};
  }
  return DecodeAppendEntriesResponse(response);
}

InstallSnapshotResponse NetworkTransport::SendInstallSnapshot(int target_id, const InstallSnapshotRequest& request) {
  std::string response;
  if (!RoundTrip(target_id, EncodeInstallSnapshotRequest(request), response)) {
    return InstallSnapshotResponse{request.term, false, 0};
  }
  return DecodeInstallSnapshotResponse(response);
}

void NetworkTransport::UpsertPeer(const PeerEndpoint& peer) {
  {
    std::lock_guard<std::mutex> lock(mutex_);
    peers_[peer.id] = peer;
  }
  impl_->ReconfigurePeer(peer);
}

void NetworkTransport::RemovePeer(int peer_id) {
  {
    std::lock_guard<std::mutex> lock(mutex_);
    peers_.erase(peer_id);
  }
  impl_->RemovePeer(peer_id);
}

std::vector<PeerEndpoint> NetworkTransport::ListPeers() const {
  std::lock_guard<std::mutex> lock(mutex_);
  std::vector<PeerEndpoint> peers;
  peers.reserve(peers_.size());
  for (const auto& [id, peer] : peers_) {
    (void)id;
    peers.push_back(peer);
  }
  return peers;
}

bool NetworkTransport::FindPeer(int id, PeerEndpoint& peer) const {
  std::lock_guard<std::mutex> lock(mutex_);
  auto it = peers_.find(id);
  if (it == peers_.end()) {
    return false;
  }
  peer = it->second;
  return true;
}

bool NetworkTransport::RoundTrip(int target_id, const std::string& request, std::string& response, int timeout_ms) {
  PeerEndpoint peer;
  if (!FindPeer(target_id, peer)) {
    return false;
  }

  auto pending = std::make_shared<PendingRpc>();
  pending->peer_id = target_id;
  pending->payload = request;
  impl_->Submit(peer, pending);

  std::unique_lock<std::mutex> lock(pending->mutex);
  const bool completed = pending->condition.wait_for(
      lock,
      std::chrono::milliseconds(timeout_ms),
      [&pending]() {
        return pending->done;
      });
  if (!completed) {
    lock.unlock();
    impl_->ResetPeer(target_id);
    return false;
  }

  if (!pending->success) {
    return false;
  }

  response = std::move(pending->response);
  return true;
}
