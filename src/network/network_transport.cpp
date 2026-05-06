#include "network/network_transport.h"

#include "network/tcp_util.h"

#include <mutex>

NetworkTransport::NetworkTransport(std::vector<PeerEndpoint> peers) {
  for (auto& peer : peers) {
    peers_.emplace(peer.id, std::move(peer));
  }
}

RequestVoteResponse NetworkTransport::SendPreVote(int target_id, const RequestVoteRequest& request) {
  PeerEndpoint peer;
  if (!FindPeer(target_id, peer)) {
    return RequestVoteResponse{request.term - 1, false};
  }
  std::string response;
  if (!RoundTripTcp(peer.host, peer.raft_port, EncodePreVoteRequest(request), response)) {
    return RequestVoteResponse{request.term - 1, false};
  }
  return DecodePreVoteResponse(response);
}

RequestVoteResponse NetworkTransport::SendRequestVote(int target_id, const RequestVoteRequest& request) {
  PeerEndpoint peer;
  if (!FindPeer(target_id, peer)) {
    return RequestVoteResponse{request.term, false};
  }
  std::string response;
  if (!RoundTripTcp(peer.host, peer.raft_port, EncodeRequestVoteRequest(request), response)) {
    return RequestVoteResponse{request.term, false};
  }
  return DecodeRequestVoteResponse(response);
}

AppendEntriesResponse NetworkTransport::SendAppendEntries(int target_id, const AppendEntriesRequest& request) {
  PeerEndpoint peer;
  if (!FindPeer(target_id, peer)) {
    return AppendEntriesResponse{request.term, false, 0, 0, -1};
  }
  std::string response;
  if (!RoundTripTcp(peer.host, peer.raft_port, EncodeAppendEntriesRequest(request), response)) {
    return AppendEntriesResponse{request.term, false, 0, 0, -1};
  }
  return DecodeAppendEntriesResponse(response);
}

InstallSnapshotResponse NetworkTransport::SendInstallSnapshot(int target_id, const InstallSnapshotRequest& request) {
  PeerEndpoint peer;
  if (!FindPeer(target_id, peer)) {
    return InstallSnapshotResponse{request.term, false, 0};
  }
  std::string response;
  if (!RoundTripTcp(peer.host, peer.raft_port, EncodeInstallSnapshotRequest(request), response)) {
    return InstallSnapshotResponse{request.term, false, 0};
  }
  return DecodeInstallSnapshotResponse(response);
}

void NetworkTransport::UpsertPeer(const PeerEndpoint& peer) {
  std::lock_guard<std::mutex> lock(mutex_);
  peers_[peer.id] = peer;
}

void NetworkTransport::RemovePeer(int peer_id) {
  std::lock_guard<std::mutex> lock(mutex_);
  peers_.erase(peer_id);
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
