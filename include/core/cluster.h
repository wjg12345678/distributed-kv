#pragma once

#include "core/raft_node.h"
#include "core/raft_transport.h"

#include <memory>
#include <optional>
#include <unordered_map>
#include <vector>

class Cluster : public IRaftTransport {
public:
  void AddNode(const std::shared_ptr<RaftNode>& node);

  RequestVoteResponse SendPreVote(int target_id, const RequestVoteRequest& request) override;
  RequestVoteResponse SendRequestVote(int target_id, const RequestVoteRequest& request) override;
  AppendEntriesResponse SendAppendEntries(int target_id, const AppendEntriesRequest& request) override;
  InstallSnapshotResponse SendInstallSnapshot(int target_id, const InstallSnapshotRequest& request) override;
  void UpsertPeer(const PeerEndpoint& peer) override;
  void RemovePeer(int peer_id) override;
  std::vector<PeerEndpoint> ListPeers() const override;

  std::shared_ptr<RaftNode> FindNode(int id) const;
  std::optional<int> LeaderId() const;
  void TickAll();

private:
  std::vector<std::shared_ptr<RaftNode>> nodes_;
  std::unordered_map<int, PeerEndpoint> peers_;
};
