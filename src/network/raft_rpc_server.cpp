#include "network/raft_rpc_server.h"

#include "network/network_codec.h"
#include "raft_rpc.pb.h"

#include <uv.h>

#include <arpa/inet.h>

#include <cstring>
#include <deque>
#include <memory>
#include <mutex>
#include <optional>
#include <queue>
#include <stdexcept>
#include <string>
#include <unordered_map>
#include <utility>

namespace {

std::runtime_error UvError(const std::string& action, int status) {
  return std::runtime_error(action + ": " + uv_strerror(status));
}

std::string EncodeFrame(const std::string& payload) {
  std::string frame(sizeof(std::uint32_t), '\0');
  const std::uint32_t length = htonl(static_cast<std::uint32_t>(payload.size()));
  std::memcpy(frame.data(), &length, sizeof(length));
  frame.append(payload);
  return frame;
}

struct RpcConnection {
  explicit RpcConnection(void* owner_ptr) : owner(owner_ptr) {}

  void* owner = nullptr;
  uv_tcp_t handle{};
  std::string read_buffer;
  std::deque<std::string> write_queue;
  bool request_in_flight = false;
  bool write_in_progress = false;
  bool closing = false;
};

struct WriteRequest {
  uv_write_t request{};
  std::shared_ptr<RpcConnection> connection;
  std::shared_ptr<std::string> payload;
};

class RpcServerRuntime {
public:
  RpcServerRuntime(
      int port,
      std::function<std::string(const std::string&)> dispatcher,
      TaskExecutor& executor)
      : port_(port), dispatcher_(std::move(dispatcher)), executor_(executor) {}

  void Run() {
    int status = uv_loop_init(&loop_);
    if (status != 0) {
      throw UvError("failed to initialize libuv loop", status);
    }

    status = uv_async_init(&loop_, &completion_async_, &RpcServerRuntime::OnCompletionAsync);
    if (status != 0) {
      throw UvError("failed to initialize libuv async", status);
    }
    completion_async_.data = this;

    status = uv_tcp_init(&loop_, &server_);
    if (status != 0) {
      throw UvError("failed to initialize tcp server", status);
    }
    server_.data = this;

    sockaddr_in address{};
    status = uv_ip4_addr("0.0.0.0", port_, &address);
    if (status != 0) {
      throw UvError("failed to build raft listen address", status);
    }

    status = uv_tcp_bind(&server_, reinterpret_cast<const sockaddr*>(&address), 0);
    if (status != 0) {
      throw UvError("failed to bind raft rpc server", status);
    }

    status = uv_listen(reinterpret_cast<uv_stream_t*>(&server_), 256, &RpcServerRuntime::OnAccept);
    if (status != 0) {
      throw UvError("failed to listen for raft rpc connections", status);
    }

    uv_run(&loop_, UV_RUN_DEFAULT);
    uv_loop_close(&loop_);
  }

private:
  static void AllocateBuffer(uv_handle_t*, std::size_t suggested_size, uv_buf_t* buffer) {
    const std::size_t size = suggested_size == 0 ? 4096 : suggested_size;
    buffer->base = new char[size];
    buffer->len = static_cast<unsigned int>(size);
  }

  static void OnAccept(uv_stream_t* server, int status) {
    auto* runtime = static_cast<RpcServerRuntime*>(server->data);
    runtime->Accept(status);
  }

  static void OnRead(uv_stream_t* stream, ssize_t nread, const uv_buf_t* buffer) {
    auto* connection = static_cast<RpcConnection*>(stream->data);
    auto* runtime = static_cast<RpcServerRuntime*>(connection->owner);
    runtime->Read(connection, nread, buffer);
  }

  static void OnCompletionAsync(uv_async_t* handle) {
    auto* runtime = static_cast<RpcServerRuntime*>(handle->data);
    runtime->DrainCompletions();
  }

  static void OnWriteDone(uv_write_t* request, int status) {
    std::unique_ptr<WriteRequest> write_request(static_cast<WriteRequest*>(request->data));
    auto* runtime = static_cast<RpcServerRuntime*>(write_request->connection->owner);
    runtime->WriteCompleted(write_request->connection, status);
  }

  static void OnConnectionClosed(uv_handle_t* handle) {
    auto* connection = static_cast<RpcConnection*>(handle->data);
    auto* runtime = static_cast<RpcServerRuntime*>(connection->owner);
    runtime->connections_.erase(connection);
  }

  void Accept(int status) {
    if (status != 0) {
      return;
    }

    auto connection = std::make_shared<RpcConnection>(this);
    status = uv_tcp_init(&loop_, &connection->handle);
    if (status != 0) {
      return;
    }
    connection->handle.data = connection.get();

    if (uv_accept(reinterpret_cast<uv_stream_t*>(&server_), reinterpret_cast<uv_stream_t*>(&connection->handle)) != 0) {
      uv_close(reinterpret_cast<uv_handle_t*>(&connection->handle), &RpcServerRuntime::OnConnectionClosed);
      return;
    }

    uv_tcp_nodelay(&connection->handle, 1);
    uv_tcp_keepalive(&connection->handle, 1, 60);
    connections_.emplace(connection.get(), connection);
    uv_read_start(reinterpret_cast<uv_stream_t*>(&connection->handle), &RpcServerRuntime::AllocateBuffer, &RpcServerRuntime::OnRead);
  }

  void Read(RpcConnection* raw_connection, ssize_t nread, const uv_buf_t* buffer) {
    std::unique_ptr<char[]> guard(buffer->base);
    auto it = connections_.find(raw_connection);
    if (it == connections_.end()) {
      return;
    }
    const auto connection = it->second;

    if (nread == 0) {
      return;
    }
    if (nread < 0) {
      CloseConnection(connection);
      return;
    }

    connection->read_buffer.append(buffer->base, static_cast<std::size_t>(nread));
    ProcessFrames(connection);
  }

  void ProcessFrames(const std::shared_ptr<RpcConnection>& connection) {
    if (connection->closing || connection->request_in_flight) {
      return;
    }

    while (connection->read_buffer.size() >= sizeof(std::uint32_t)) {
      std::uint32_t length_network = 0;
      std::memcpy(&length_network, connection->read_buffer.data(), sizeof(length_network));
      const std::uint32_t length = ntohl(length_network);
      if (connection->read_buffer.size() < sizeof(length_network) + length) {
        return;
      }

      std::string request = connection->read_buffer.substr(sizeof(length_network), length);
      connection->read_buffer.erase(0, sizeof(length_network) + length);
      DispatchRequest(connection, std::move(request));
      return;
    }
  }

  void DispatchRequest(const std::shared_ptr<RpcConnection>& connection, std::string request) {
    connection->request_in_flight = true;
    uv_read_stop(reinterpret_cast<uv_stream_t*>(&connection->handle));

    executor_.Submit([this, connection, request = std::move(request)]() mutable {
      std::optional<std::string> response;
      try {
        response = dispatcher_(request);
      } catch (...) {
      }

      PostCompletion([this, connection, response = std::move(response)]() mutable {
        FinishRequest(connection, std::move(response));
      });
    });
  }

  void FinishRequest(const std::shared_ptr<RpcConnection>& connection, std::optional<std::string> response) {
    if (connection->closing) {
      return;
    }

    connection->request_in_flight = false;
    if (!response) {
      CloseConnection(connection);
      return;
    }

    connection->write_queue.push_back(EncodeFrame(*response));
    StartWrite(connection);
  }

  void StartWrite(const std::shared_ptr<RpcConnection>& connection) {
    if (connection->closing || connection->write_in_progress || connection->write_queue.empty()) {
      return;
    }

    connection->write_in_progress = true;
    auto payload = std::make_shared<std::string>(std::move(connection->write_queue.front()));
    connection->write_queue.pop_front();

    auto* write_request = new WriteRequest{};
    write_request->request.data = write_request;
    write_request->connection = connection;
    write_request->payload = std::move(payload);

    uv_buf_t buffer = uv_buf_init(write_request->payload->data(), static_cast<unsigned int>(write_request->payload->size()));
    const int status = uv_write(
        &write_request->request,
        reinterpret_cast<uv_stream_t*>(&connection->handle),
        &buffer,
        1,
        &RpcServerRuntime::OnWriteDone);
    if (status != 0) {
      delete write_request;
      CloseConnection(connection);
    }
  }

  void WriteCompleted(const std::shared_ptr<RpcConnection>& connection, int status) {
    connection->write_in_progress = false;
    if (status != 0) {
      CloseConnection(connection);
      return;
    }

    if (!connection->write_queue.empty()) {
      StartWrite(connection);
      return;
    }

    ProcessFrames(connection);
    if (!connection->request_in_flight && !connection->closing) {
      uv_read_start(reinterpret_cast<uv_stream_t*>(&connection->handle), &RpcServerRuntime::AllocateBuffer, &RpcServerRuntime::OnRead);
    }
  }

  void CloseConnection(const std::shared_ptr<RpcConnection>& connection) {
    if (connection->closing) {
      return;
    }
    connection->closing = true;
    uv_read_stop(reinterpret_cast<uv_stream_t*>(&connection->handle));
    uv_close(reinterpret_cast<uv_handle_t*>(&connection->handle), &RpcServerRuntime::OnConnectionClosed);
  }

  void PostCompletion(std::function<void()> completion) {
    {
      std::lock_guard<std::mutex> lock(completion_mutex_);
      completion_tasks_.push(std::move(completion));
    }
    uv_async_send(&completion_async_);
  }

  void DrainCompletions() {
    std::queue<std::function<void()>> tasks;
    {
      std::lock_guard<std::mutex> lock(completion_mutex_);
      tasks.swap(completion_tasks_);
    }

    while (!tasks.empty()) {
      tasks.front()();
      tasks.pop();
    }
  }

  int port_ = 0;
  std::function<std::string(const std::string&)> dispatcher_;
  TaskExecutor& executor_;
  uv_loop_t loop_{};
  uv_tcp_t server_{};
  uv_async_t completion_async_{};
  std::mutex completion_mutex_;
  std::queue<std::function<void()>> completion_tasks_;
  std::unordered_map<RpcConnection*, std::shared_ptr<RpcConnection>> connections_;
};

}  // namespace

RaftRpcServer::RaftRpcServer(std::shared_ptr<RaftNode> node, int port, std::size_t worker_count)
    : node_(std::move(node)), port_(port), executor_(worker_count) {}

void RaftRpcServer::Run() {
  RpcServerRuntime runtime(
      port_,
      [this](const std::string& request) {
        return Dispatch(request);
      },
      executor_);
  runtime.Run();
}

std::string RaftRpcServer::Dispatch(const std::string& request) const {
  distributedkv::raft::RpcEnvelope envelope;
  if (!envelope.ParseFromString(request)) {
    throw std::runtime_error("failed to parse rpc envelope");
  }

  switch (envelope.payload_case()) {
    case distributedkv::raft::RpcEnvelope::kPreVoteRequest:
      return EncodePreVoteResponse(node_->HandlePreVote(DecodePreVoteRequest(request)));
    case distributedkv::raft::RpcEnvelope::kRequestVoteRequest:
      return EncodeRequestVoteResponse(node_->HandleRequestVote(DecodeRequestVoteRequest(request)));
    case distributedkv::raft::RpcEnvelope::kAppendEntriesRequest:
      return EncodeAppendEntriesResponse(node_->HandleAppendEntries(DecodeAppendEntriesRequest(request)));
    case distributedkv::raft::RpcEnvelope::kInstallSnapshotRequest:
      return EncodeInstallSnapshotResponse(node_->HandleInstallSnapshot(DecodeInstallSnapshotRequest(request)));
    default:
      break;
  }
  throw std::runtime_error("unknown rpc message");
}
