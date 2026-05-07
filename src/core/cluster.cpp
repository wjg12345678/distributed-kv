#include "core/cluster.h"

#include <stdexcept>

void Cluster::AddNode(const std::shared_ptr<RaftNode>& node) {
  nodes_.push_back(node);
  peers_[node->id()] = PeerEndpoint{node->id(), "127.0.0.1", 0, 0};
}

RequestVoteResponse Cluster::SendPreVote(int target_id, const RequestVoteRequest& request) {
  auto node = FindNode(target_id);
  if (!node) {
    throw std::runtime_error("target node not found for PreVote: " + std::to_string(target_id));
  }
  return node->HandlePreVote(request);
}

RequestVoteResponse Cluster::SendRequestVote(int target_id, const RequestVoteRequest& request) {
  auto node = FindNode(target_id);
  if (!node) {
    throw std::runtime_error("target node not found for RequestVote: " + std::to_string(target_id));
  }
  return node->HandleRequestVote(request);
}

AppendEntriesResponse Cluster::SendAppendEntries(int target_id, const AppendEntriesRequest& request) {
  auto node = FindNode(target_id);
  if (!node) {
    throw std::runtime_error("target node not found for AppendEntries: " + std::to_string(target_id));
  }
  return node->HandleAppendEntries(request);
}

InstallSnapshotResponse Cluster::SendInstallSnapshot(int target_id, const InstallSnapshotRequest& request) {
  auto node = FindNode(target_id);
  if (!node) {
    throw std::runtime_error("target node not found for InstallSnapshot: " + std::to_string(target_id));
  }
  return node->HandleInstallSnapshot(request);
}

void Cluster::UpsertPeer(const PeerEndpoint& peer) {
  peers_[peer.id] = peer;
}

void Cluster::RemovePeer(int peer_id) {
  peers_.erase(peer_id);
}

std::vector<PeerEndpoint> Cluster::ListPeers() const {
  std::vector<PeerEndpoint> peers;
  peers.reserve(peers_.size());
  for (const auto& [id, peer] : peers_) {
    (void)id;
    peers.push_back(peer);
  }
  return peers;
}

std::shared_ptr<RaftNode> Cluster::FindNode(int id) const {
  for (const auto& node : nodes_) {
    if (node->id() == id) {
      return node;
    }
  }
  return nullptr;
}

std::optional<int> Cluster::LeaderId() const {
  for (const auto& node : nodes_) {
    if (node->role() == Role::Leader) {
      return node->id();
    }
  }
  return std::nullopt;
}

void Cluster::TickAll() {
  for (const auto& node : nodes_) {
    node->Tick();
  }
}
