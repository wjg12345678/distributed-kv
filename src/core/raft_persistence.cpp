#include "core/raft_persistence.h"

#include <rocksdb/db.h>
#include <rocksdb/options.h>
#include <rocksdb/write_batch.h>

#include <iomanip>
#include <limits>
#include <memory>
#include <sstream>
#include <stdexcept>
#include <utility>

namespace {

std::string SerializeLogEntries(const std::vector<LogEntry>& entries) {
  std::ostringstream out;
  out << entries.size() << '\n';
  for (const auto& entry : entries) {
    out << entry.term << '\t'
        << static_cast<int>(entry.command.type) << '\t'
        << std::quoted(entry.command.key) << '\t'
        << std::quoted(entry.command.value) << '\t'
        << entry.command.peer.id << '\t'
        << std::quoted(entry.command.peer.host) << '\t'
        << entry.command.peer.raft_port << '\t'
        << entry.command.peer.http_port << '\t'
        << std::quoted(entry.command.request_id) << '\t'
        << entry.command.mvcc_commit_ts << '\t'
        << entry.command.writes.size();
    for (const auto& write : entry.command.writes) {
      out << '\t' << std::quoted(write.key) << '\t' << std::quoted(write.value);
    }
    out << '\n';
  }
  return out.str();
}

std::vector<LogEntry> DeserializeLogEntries(const std::string& payload) {
  std::istringstream in(payload);
  std::size_t size = 0;
  in >> size;
  in.ignore(std::numeric_limits<std::streamsize>::max(), '\n');

  std::vector<LogEntry> entries;
  entries.reserve(size);
  for (std::size_t i = 0; i < size; ++i) {
    LogEntry entry;
    int type = 0;
    in >> entry.term
       >> type
       >> std::quoted(entry.command.key)
       >> std::quoted(entry.command.value)
       >> entry.command.peer.id
       >> std::quoted(entry.command.peer.host)
       >> entry.command.peer.raft_port
       >> entry.command.peer.http_port
       >> std::quoted(entry.command.request_id)
       >> entry.command.mvcc_commit_ts;
    entry.command.type = static_cast<CommandType>(type);
    std::size_t write_count = 0;
    in >> write_count;
    for (std::size_t j = 0; j < write_count; ++j) {
      KeyValue write;
      in >> std::quoted(write.key) >> std::quoted(write.value);
      entry.command.writes.push_back(write);
    }
    in.ignore(std::numeric_limits<std::streamsize>::max(), '\n');
    entries.push_back(entry);
  }
  return entries;
}

std::string SerializeSnapshotEntries(const std::vector<std::pair<std::string, std::string>>& entries) {
  std::ostringstream out;
  out << entries.size() << '\n';
  for (const auto& [key, value] : entries) {
    out << std::quoted(key) << '\t' << std::quoted(value) << '\n';
  }
  return out.str();
}

std::string SerializePeers(const std::vector<PeerEndpoint>& peers) {
  std::ostringstream out;
  out << peers.size() << '\n';
  for (const auto& peer : peers) {
    out << peer.id << '\t'
        << std::quoted(peer.host) << '\t'
        << peer.raft_port << '\t'
        << peer.http_port << '\n';
  }
  return out.str();
}

std::vector<PeerEndpoint> DeserializePeers(const std::string& payload) {
  std::istringstream in(payload);
  std::size_t size = 0;
  in >> size;
  in.ignore(std::numeric_limits<std::streamsize>::max(), '\n');

  std::vector<PeerEndpoint> peers;
  peers.reserve(size);
  for (std::size_t i = 0; i < size; ++i) {
    PeerEndpoint peer;
    in >> peer.id >> std::quoted(peer.host) >> peer.raft_port >> peer.http_port;
    in.ignore(std::numeric_limits<std::streamsize>::max(), '\n');
    peers.push_back(peer);
  }
  return peers;
}

std::vector<std::pair<std::string, std::string>> DeserializeSnapshotEntries(const std::string& payload) {
  std::istringstream in(payload);
  std::size_t size = 0;
  in >> size;
  in.ignore(std::numeric_limits<std::streamsize>::max(), '\n');

  std::vector<std::pair<std::string, std::string>> entries;
  entries.reserve(size);
  for (std::size_t i = 0; i < size; ++i) {
    std::string key;
    std::string value;
    in >> std::quoted(key) >> std::quoted(value);
    in.ignore(std::numeric_limits<std::streamsize>::max(), '\n');
    entries.push_back({key, value});
  }
  return entries;
}

std::string IntToString(int value) {
  return std::to_string(value);
}

int StringToInt(const std::string& value) {
  return std::stoi(value);
}

}  // namespace

class RocksDbRaftPersistence::Impl {
public:
  explicit Impl(std::string db_path) {
    rocksdb::Options options;
    options.create_if_missing = true;

    std::unique_ptr<rocksdb::DB> db;
    auto status = rocksdb::DB::Open(options, db_path, &db);
    if (!status.ok()) {
      throw std::runtime_error("failed to open raft persistence db: " + status.ToString());
    }
    db_ = std::move(db);
  }

  std::unique_ptr<rocksdb::DB> db_;
};

void NoopRaftPersistence::SaveState(const PersistentState& state) {
  (void)state;
}

bool NoopRaftPersistence::LoadState(PersistentState& state) {
  (void)state;
  return false;
}

void NoopRaftPersistence::SaveSnapshot(const SnapshotState& snapshot) {
  (void)snapshot;
}

bool NoopRaftPersistence::LoadSnapshot(SnapshotState& snapshot) {
  (void)snapshot;
  return false;
}

RocksDbRaftPersistence::RocksDbRaftPersistence(std::string db_path)
    : impl_(std::make_unique<Impl>(std::move(db_path))) {}

RocksDbRaftPersistence::~RocksDbRaftPersistence() = default;

void RocksDbRaftPersistence::SaveState(const PersistentState& state) {
  rocksdb::WriteBatch batch;
  batch.Put("raft/current_term", IntToString(state.current_term));
  batch.Put("raft/voted_for", IntToString(state.voted_for));
  batch.Put("raft/log_base_index", IntToString(state.log_base_index));
  batch.Put("raft/commit_index", IntToString(state.commit_index));
  batch.Put("raft/log_entries", SerializeLogEntries(state.log_entries));

  auto status = impl_->db_->Write(rocksdb::WriteOptions(), &batch);
  if (!status.ok()) {
    throw std::runtime_error("failed to save raft state: " + status.ToString());
  }
}

bool RocksDbRaftPersistence::LoadState(PersistentState& state) {
  std::string current_term;
  auto status = impl_->db_->Get(rocksdb::ReadOptions(), "raft/current_term", &current_term);
  if (status.IsNotFound()) {
    return false;
  }
  if (!status.ok()) {
    throw std::runtime_error("failed to load raft state: " + status.ToString());
  }

  std::string voted_for;
  std::string log_base_index;
  std::string commit_index;
  std::string log_entries;
  auto read = [&](const std::string& key, std::string& value) {
    auto s = impl_->db_->Get(rocksdb::ReadOptions(), key, &value);
    if (!s.ok()) {
      throw std::runtime_error("failed to load raft state key " + key + ": " + s.ToString());
    }
  };

  read("raft/voted_for", voted_for);
  read("raft/log_base_index", log_base_index);
  read("raft/commit_index", commit_index);
  read("raft/log_entries", log_entries);

  state.current_term = StringToInt(current_term);
  state.voted_for = StringToInt(voted_for);
  state.log_base_index = StringToInt(log_base_index);
  state.commit_index = StringToInt(commit_index);
  state.log_entries = DeserializeLogEntries(log_entries);
  return true;
}

void RocksDbRaftPersistence::SaveSnapshot(const SnapshotState& snapshot) {
  rocksdb::WriteBatch batch;
  batch.Put("snapshot/last_included_index", IntToString(snapshot.last_included_index));
  batch.Put("snapshot/last_included_term", IntToString(snapshot.last_included_term));
  batch.Put("snapshot/data", SerializeSnapshotEntries(snapshot.data));
  batch.Put("snapshot/peers", SerializePeers(snapshot.peers));

  auto status = impl_->db_->Write(rocksdb::WriteOptions(), &batch);
  if (!status.ok()) {
    throw std::runtime_error("failed to save snapshot: " + status.ToString());
  }
}

bool RocksDbRaftPersistence::LoadSnapshot(SnapshotState& snapshot) {
  std::string index;
  auto status = impl_->db_->Get(rocksdb::ReadOptions(), "snapshot/last_included_index", &index);
  if (status.IsNotFound()) {
    return false;
  }
  if (!status.ok()) {
    throw std::runtime_error("failed to load snapshot: " + status.ToString());
  }

  std::string term;
  std::string data;
  std::string peers;
  auto read = [&](const std::string& key, std::string& value) {
    auto s = impl_->db_->Get(rocksdb::ReadOptions(), key, &value);
    if (!s.ok()) {
      throw std::runtime_error("failed to load snapshot key " + key + ": " + s.ToString());
    }
  };

  read("snapshot/last_included_term", term);
  read("snapshot/data", data);
  read("snapshot/peers", peers);

  snapshot.last_included_index = StringToInt(index);
  snapshot.last_included_term = StringToInt(term);
  snapshot.data = DeserializeSnapshotEntries(data);
  snapshot.peers = DeserializePeers(peers);
  return true;
}
