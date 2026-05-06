#pragma once

#include "core/raft_types.h"

#include <memory>
#include <string>
#include <utility>
#include <vector>

struct PersistentState {
  int current_term = 0;
  int voted_for = -1;
  int log_base_index = 0;
  int commit_index = 0;
  std::vector<LogEntry> log_entries;
};

struct SnapshotState {
  int last_included_index = 0;
  int last_included_term = 0;
  std::vector<std::pair<std::string, std::string>> data;
  std::vector<PeerEndpoint> peers;
};

class IRaftPersistence {
public:
  virtual ~IRaftPersistence() = default;

  virtual void SaveState(const PersistentState& state) = 0;
  virtual bool LoadState(PersistentState& state) = 0;
  virtual void SaveSnapshot(const SnapshotState& snapshot) = 0;
  virtual bool LoadSnapshot(SnapshotState& snapshot) = 0;
};

class NoopRaftPersistence : public IRaftPersistence {
public:
  void SaveState(const PersistentState& state) override;
  bool LoadState(PersistentState& state) override;
  void SaveSnapshot(const SnapshotState& snapshot) override;
  bool LoadSnapshot(SnapshotState& snapshot) override;
};

class RocksDbRaftPersistence : public IRaftPersistence {
public:
  explicit RocksDbRaftPersistence(std::string db_path);
  ~RocksDbRaftPersistence() override;

  RocksDbRaftPersistence(const RocksDbRaftPersistence&) = delete;
  RocksDbRaftPersistence& operator=(const RocksDbRaftPersistence&) = delete;

  void SaveState(const PersistentState& state) override;
  bool LoadState(PersistentState& state) override;
  void SaveSnapshot(const SnapshotState& snapshot) override;
  bool LoadSnapshot(SnapshotState& snapshot) override;

private:
  class Impl;
  std::unique_ptr<Impl> impl_;
};
