#include "network/raft_rpc_server.h"

#include "network/network_codec.h"
#include "network/tcp_util.h"
#include "raft_rpc.pb.h"

#include <sys/socket.h>
#include <unistd.h>

#include <stdexcept>
#include <string>

RaftRpcServer::RaftRpcServer(std::shared_ptr<RaftNode> node, int port, std::size_t worker_count)
    : node_(std::move(node)), port_(port), executor_(worker_count) {}

void RaftRpcServer::Run() {
  const int server_fd = CreateListenSocket(port_);
  while (true) {
    const int client_fd = ::accept(server_fd, nullptr, nullptr);
    if (client_fd < 0) {
      continue;
    }

    executor_.Submit([this, client_fd]() {
      HandleClient(client_fd);
    });
  }
}

void RaftRpcServer::HandleClient(int client_fd) const {
  std::string request;
  if (ReadFrame(client_fd, request)) {
    const auto response = Dispatch(request);
    WriteFrame(client_fd, response);
  }
  ::close(client_fd);
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
