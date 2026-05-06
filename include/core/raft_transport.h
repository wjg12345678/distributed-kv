#pragma once

#include "core/raft_types.h"

class IRaftTransport {
public:
  virtual ~IRaftTransport() = default;

  virtual RequestVoteResponse SendPreVote(int target_id, const RequestVoteRequest& request) = 0;
  virtual RequestVoteResponse SendRequestVote(int target_id, const RequestVoteRequest& request) = 0;
  virtual AppendEntriesResponse SendAppendEntries(int target_id, const AppendEntriesRequest& request) = 0;
  virtual InstallSnapshotResponse SendInstallSnapshot(int target_id, const InstallSnapshotRequest& request) = 0;

  virtual void UpsertPeer(const PeerEndpoint& peer) = 0;
  virtual void RemovePeer(int peer_id) = 0;
  virtual std::vector<PeerEndpoint> ListPeers() const = 0;
};
