#include "network/network_codec.h"

#include "raft_rpc.pb.h"

#include <stdexcept>

namespace {

using ProtoAppendEntriesRequest = distributedkv::raft::AppendEntriesRequest;
using ProtoAppendEntriesResponse = distributedkv::raft::AppendEntriesResponse;
using ProtoCommand = distributedkv::raft::Command;
using ProtoCommandType = distributedkv::raft::CommandType;
using ProtoInstallSnapshotRequest = distributedkv::raft::InstallSnapshotRequest;
using ProtoInstallSnapshotResponse = distributedkv::raft::InstallSnapshotResponse;
using ProtoKeyValue = distributedkv::raft::KeyValue;
using ProtoLogEntry = distributedkv::raft::LogEntry;
using ProtoPeerEndpoint = distributedkv::raft::PeerEndpoint;
using ProtoRequestVoteRequest = distributedkv::raft::RequestVoteRequest;
using ProtoRequestVoteResponse = distributedkv::raft::RequestVoteResponse;
using ProtoRpcEnvelope = distributedkv::raft::RpcEnvelope;

ProtoCommandType ToProtoCommandType(CommandType type) {
  switch (type) {
    case CommandType::Noop:
      return ProtoCommandType::COMMAND_TYPE_NOOP;
    case CommandType::Put:
      return ProtoCommandType::COMMAND_TYPE_PUT;
    case CommandType::AddPeer:
      return ProtoCommandType::COMMAND_TYPE_ADD_PEER;
    case CommandType::RemovePeer:
      return ProtoCommandType::COMMAND_TYPE_REMOVE_PEER;
    case CommandType::AcquireLock:
      return ProtoCommandType::COMMAND_TYPE_ACQUIRE_LOCK;
    case CommandType::ReleaseLock:
      return ProtoCommandType::COMMAND_TYPE_RELEASE_LOCK;
    case CommandType::MvccCommit:
      return ProtoCommandType::COMMAND_TYPE_MVCC_COMMIT;
  }
  return ProtoCommandType::COMMAND_TYPE_NOOP;
}

CommandType FromProtoCommandType(ProtoCommandType type) {
  switch (type) {
    case ProtoCommandType::COMMAND_TYPE_NOOP:
      return CommandType::Noop;
    case ProtoCommandType::COMMAND_TYPE_PUT:
      return CommandType::Put;
    case ProtoCommandType::COMMAND_TYPE_ADD_PEER:
      return CommandType::AddPeer;
    case ProtoCommandType::COMMAND_TYPE_REMOVE_PEER:
      return CommandType::RemovePeer;
    case ProtoCommandType::COMMAND_TYPE_ACQUIRE_LOCK:
      return CommandType::AcquireLock;
    case ProtoCommandType::COMMAND_TYPE_RELEASE_LOCK:
      return CommandType::ReleaseLock;
    case ProtoCommandType::COMMAND_TYPE_MVCC_COMMIT:
      return CommandType::MvccCommit;
    default:
      break;
  }
  return CommandType::Noop;
}

ProtoPeerEndpoint ToProtoPeer(const PeerEndpoint& peer) {
  ProtoPeerEndpoint message;
  message.set_id(peer.id);
  message.set_host(peer.host);
  message.set_raft_port(peer.raft_port);
  message.set_http_port(peer.http_port);
  return message;
}

PeerEndpoint FromProtoPeer(const ProtoPeerEndpoint& peer) {
  return PeerEndpoint{peer.id(), peer.host(), peer.raft_port(), peer.http_port()};
}

ProtoKeyValue ToProtoKeyValue(const KeyValue& value) {
  ProtoKeyValue message;
  message.set_key(value.key);
  message.set_value(value.value);
  return message;
}

KeyValue FromProtoKeyValue(const ProtoKeyValue& value) {
  return KeyValue{value.key(), value.value()};
}

ProtoCommand ToProtoCommand(const Command& command) {
  ProtoCommand message;
  message.set_type(ToProtoCommandType(command.type));
  message.set_key(command.key);
  message.set_value(command.value);
  *message.mutable_peer() = ToProtoPeer(command.peer);
  message.set_request_id(command.request_id);
  message.set_mvcc_commit_ts(command.mvcc_commit_ts);
  for (const auto& write : command.writes) {
    *message.add_writes() = ToProtoKeyValue(write);
  }
  return message;
}

Command FromProtoCommand(const ProtoCommand& command) {
  Command native;
  native.type = FromProtoCommandType(command.type());
  native.key = command.key();
  native.value = command.value();
  native.peer = FromProtoPeer(command.peer());
  native.request_id = command.request_id();
  native.mvcc_commit_ts = command.mvcc_commit_ts();
  for (const auto& write : command.writes()) {
    native.writes.push_back(FromProtoKeyValue(write));
  }
  return native;
}

ProtoLogEntry ToProtoLogEntry(const LogEntry& entry) {
  ProtoLogEntry message;
  message.set_term(entry.term);
  *message.mutable_command() = ToProtoCommand(entry.command);
  return message;
}

LogEntry FromProtoLogEntry(const ProtoLogEntry& entry) {
  return LogEntry{entry.term(), FromProtoCommand(entry.command())};
}

template <typename Message>
std::string SerializeMessage(const Message& message) {
  std::string payload;
  if (!message.SerializeToString(&payload)) {
    throw std::runtime_error("failed to serialize protobuf message");
  }
  return payload;
}

template <typename Message>
Message ParseMessage(const std::string& payload) {
  Message message;
  if (!message.ParseFromString(payload)) {
    throw std::runtime_error("failed to parse protobuf message");
  }
  return message;
}

}  // namespace

std::string EncodePreVoteRequest(const RequestVoteRequest& request) {
  ProtoRequestVoteRequest message;
  message.set_term(request.term);
  message.set_candidate_id(request.candidate_id);
  message.set_last_log_index(request.last_log_index);
  message.set_last_log_term(request.last_log_term);
  ProtoRpcEnvelope envelope;
  *envelope.mutable_pre_vote_request() = message;
  return SerializeMessage(envelope);
}

std::string EncodePreVoteResponse(const RequestVoteResponse& response) {
  ProtoRequestVoteResponse message;
  message.set_term(response.term);
  message.set_vote_granted(response.vote_granted);
  ProtoRpcEnvelope envelope;
  *envelope.mutable_pre_vote_response() = message;
  return SerializeMessage(envelope);
}

std::string EncodeRequestVoteRequest(const RequestVoteRequest& request) {
  ProtoRequestVoteRequest message;
  message.set_term(request.term);
  message.set_candidate_id(request.candidate_id);
  message.set_last_log_index(request.last_log_index);
  message.set_last_log_term(request.last_log_term);
  ProtoRpcEnvelope envelope;
  *envelope.mutable_request_vote_request() = message;
  return SerializeMessage(envelope);
}

std::string EncodeRequestVoteResponse(const RequestVoteResponse& response) {
  ProtoRequestVoteResponse message;
  message.set_term(response.term);
  message.set_vote_granted(response.vote_granted);
  ProtoRpcEnvelope envelope;
  *envelope.mutable_request_vote_response() = message;
  return SerializeMessage(envelope);
}

std::string EncodeAppendEntriesRequest(const AppendEntriesRequest& request) {
  ProtoAppendEntriesRequest message;
  message.set_term(request.term);
  message.set_leader_id(request.leader_id);
  message.set_prev_log_index(request.prev_log_index);
  message.set_prev_log_term(request.prev_log_term);
  message.set_leader_commit(request.leader_commit);
  for (const auto& entry : request.entries) {
    *message.add_entries() = ToProtoLogEntry(entry);
  }
  ProtoRpcEnvelope envelope;
  *envelope.mutable_append_entries_request() = message;
  return SerializeMessage(envelope);
}

std::string EncodeAppendEntriesResponse(const AppendEntriesResponse& response) {
  ProtoAppendEntriesResponse message;
  message.set_term(response.term);
  message.set_success(response.success);
  message.set_match_index(response.match_index);
  message.set_conflict_index(response.conflict_index);
  message.set_conflict_term(response.conflict_term);
  ProtoRpcEnvelope envelope;
  *envelope.mutable_append_entries_response() = message;
  return SerializeMessage(envelope);
}

std::string EncodeInstallSnapshotRequest(const InstallSnapshotRequest& request) {
  ProtoInstallSnapshotRequest message;
  message.set_term(request.term);
  message.set_leader_id(request.leader_id);
  message.set_last_included_index(request.last_included_index);
  message.set_last_included_term(request.last_included_term);
  for (const auto& item : request.state) {
    *message.add_state() = ToProtoKeyValue(KeyValue{item.first, item.second});
  }
  for (const auto& peer : request.peers) {
    *message.add_peers() = ToProtoPeer(peer);
  }
  ProtoRpcEnvelope envelope;
  *envelope.mutable_install_snapshot_request() = message;
  return SerializeMessage(envelope);
}

std::string EncodeInstallSnapshotResponse(const InstallSnapshotResponse& response) {
  ProtoInstallSnapshotResponse message;
  message.set_term(response.term);
  message.set_success(response.success);
  message.set_last_included_index(response.last_included_index);
  ProtoRpcEnvelope envelope;
  *envelope.mutable_install_snapshot_response() = message;
  return SerializeMessage(envelope);
}

RequestVoteRequest DecodePreVoteRequest(const std::string& payload) {
  const auto envelope = ParseMessage<ProtoRpcEnvelope>(payload);
  const auto& message = envelope.pre_vote_request();
  return RequestVoteRequest{message.term(), message.candidate_id(), message.last_log_index(), message.last_log_term()};
}

RequestVoteResponse DecodePreVoteResponse(const std::string& payload) {
  const auto envelope = ParseMessage<ProtoRpcEnvelope>(payload);
  const auto& message = envelope.pre_vote_response();
  return RequestVoteResponse{message.term(), message.vote_granted()};
}

RequestVoteRequest DecodeRequestVoteRequest(const std::string& payload) {
  const auto envelope = ParseMessage<ProtoRpcEnvelope>(payload);
  const auto& message = envelope.request_vote_request();
  return RequestVoteRequest{message.term(), message.candidate_id(), message.last_log_index(), message.last_log_term()};
}

RequestVoteResponse DecodeRequestVoteResponse(const std::string& payload) {
  const auto envelope = ParseMessage<ProtoRpcEnvelope>(payload);
  const auto& message = envelope.request_vote_response();
  return RequestVoteResponse{message.term(), message.vote_granted()};
}

AppendEntriesRequest DecodeAppendEntriesRequest(const std::string& payload) {
  const auto envelope = ParseMessage<ProtoRpcEnvelope>(payload);
  const auto& message = envelope.append_entries_request();
  AppendEntriesRequest request;
  request.term = message.term();
  request.leader_id = message.leader_id();
  request.prev_log_index = message.prev_log_index();
  request.prev_log_term = message.prev_log_term();
  request.leader_commit = message.leader_commit();
  for (const auto& entry : message.entries()) {
    request.entries.push_back(FromProtoLogEntry(entry));
  }
  return request;
}

AppendEntriesResponse DecodeAppendEntriesResponse(const std::string& payload) {
  const auto envelope = ParseMessage<ProtoRpcEnvelope>(payload);
  const auto& message = envelope.append_entries_response();
  return AppendEntriesResponse{
      message.term(),
      message.success(),
      message.match_index(),
      message.conflict_index(),
      message.conflict_term(),
  };
}

InstallSnapshotRequest DecodeInstallSnapshotRequest(const std::string& payload) {
  const auto envelope = ParseMessage<ProtoRpcEnvelope>(payload);
  const auto& message = envelope.install_snapshot_request();
  InstallSnapshotRequest request;
  request.term = message.term();
  request.leader_id = message.leader_id();
  request.last_included_index = message.last_included_index();
  request.last_included_term = message.last_included_term();
  for (const auto& item : message.state()) {
    request.state.push_back({item.key(), item.value()});
  }
  for (const auto& peer : message.peers()) {
    request.peers.push_back(FromProtoPeer(peer));
  }
  return request;
}

InstallSnapshotResponse DecodeInstallSnapshotResponse(const std::string& payload) {
  const auto envelope = ParseMessage<ProtoRpcEnvelope>(payload);
  const auto& message = envelope.install_snapshot_response();
  return InstallSnapshotResponse{message.term(), message.success(), message.last_included_index()};
}
