#pragma once

#include "core/raft_types.h"

#include <string>

std::string EncodePreVoteRequest(const RequestVoteRequest& request);
std::string EncodePreVoteResponse(const RequestVoteResponse& response);
std::string EncodeRequestVoteRequest(const RequestVoteRequest& request);
std::string EncodeRequestVoteResponse(const RequestVoteResponse& response);
std::string EncodeAppendEntriesRequest(const AppendEntriesRequest& request);
std::string EncodeAppendEntriesResponse(const AppendEntriesResponse& response);
std::string EncodeInstallSnapshotRequest(const InstallSnapshotRequest& request);
std::string EncodeInstallSnapshotResponse(const InstallSnapshotResponse& response);

RequestVoteRequest DecodePreVoteRequest(const std::string& payload);
RequestVoteResponse DecodePreVoteResponse(const std::string& payload);
RequestVoteRequest DecodeRequestVoteRequest(const std::string& payload);
RequestVoteResponse DecodeRequestVoteResponse(const std::string& payload);
AppendEntriesRequest DecodeAppendEntriesRequest(const std::string& payload);
AppendEntriesResponse DecodeAppendEntriesResponse(const std::string& payload);
InstallSnapshotRequest DecodeInstallSnapshotRequest(const std::string& payload);
InstallSnapshotResponse DecodeInstallSnapshotResponse(const std::string& payload);
