#pragma once

#include "core/raft_transport.h"
#include "network/network_codec.h"

#include <mutex>
#include <unordered_map>
#include <vector>

class NetworkTransport : public IRaftTransport {
public:
  explicit NetworkTransport(std::vector<PeerEndpoint> peers);

  RequestVoteResponse SendPreVote(int target_id, const RequestVoteRequest& request) override;
  RequestVoteResponse SendRequestVote(int target_id, const RequestVoteRequest& request) override;
  AppendEntriesResponse SendAppendEntries(int target_id, const AppendEntriesRequest& request) override;
  InstallSnapshotResponse SendInstallSnapshot(int target_id, const InstallSnapshotRequest& request) override;
  void UpsertPeer(const PeerEndpoint& peer) override;
  void RemovePeer(int peer_id) override;
  std::vector<PeerEndpoint> ListPeers() const override;

  bool FindPeer(int id, PeerEndpoint& peer) const;

private:
  mutable std::mutex mutex_;
  std::unordered_map<int, PeerEndpoint> peers_;
};
