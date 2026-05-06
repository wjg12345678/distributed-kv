#include "core/cluster.h"

#include <stdexcept>

void Cluster::AddNode(const std::shared_ptr<RaftNode>& node) {
  nodes_.push_back(node);
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
  (void)peer;
}

void Cluster::RemovePeer(int peer_id) {
  (void)peer_id;
}

std::vector<PeerEndpoint> Cluster::ListPeers() const {
  return {};
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
