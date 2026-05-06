#include "storage/key_value_store.h"

#include <rocksdb/cache.h>
#include <rocksdb/db.h>
#include <rocksdb/filter_policy.h>
#include <rocksdb/options.h>
#include <rocksdb/table.h>
#include <rocksdb/write_batch.h>

#include <algorithm>
#include <stdexcept>
#include <utility>

namespace {

rocksdb::CompactionStyle ToCompactionStyle(RocksDbCompactionStyle style) {
  switch (style) {
    case RocksDbCompactionStyle::Level:
      return rocksdb::kCompactionStyleLevel;
    case RocksDbCompactionStyle::Universal:
      return rocksdb::kCompactionStyleUniversal;
    case RocksDbCompactionStyle::Fifo:
      return rocksdb::kCompactionStyleFIFO;
  }
  return rocksdb::kCompactionStyleLevel;
}

}  // namespace

class RocksDbKeyValueStore::Impl {
public:
  Impl(std::string db_path, RocksDbConfig config) : db_path_(std::move(db_path)), config_(config) {
    rocksdb::Options options;
    rocksdb::BlockBasedTableOptions table_options;

    options.create_if_missing = true;
    options.write_buffer_size = config_.write_buffer_size;
    options.max_background_jobs = config_.max_background_jobs;
    options.max_write_buffer_number = config_.max_write_buffer_number;
    options.max_open_files = config_.max_open_files;
    options.max_subcompactions = config_.max_subcompactions;
    options.level0_file_num_compaction_trigger = config_.level0_file_num_compaction_trigger;
    options.target_file_size_base = config_.target_file_size_base;
    options.compression = config_.enable_compression ? rocksdb::kSnappyCompression : rocksdb::kNoCompression;
    options.compaction_style = ToCompactionStyle(config_.compaction_style);
    options.IncreaseParallelism(std::max(1, config_.max_background_jobs));

    if (config_.enable_bloom_filter) {
      table_options.filter_policy.reset(rocksdb::NewBloomFilterPolicy(config_.bloom_bits_per_key, false));
      table_options.cache_index_and_filter_blocks = true;
      table_options.pin_l0_filter_and_index_blocks_in_cache = true;
    }
    options.table_factory.reset(rocksdb::NewBlockBasedTableFactory(table_options));

    std::unique_ptr<rocksdb::DB> db;
    auto status = rocksdb::DB::Open(options, db_path_, &db);
    if (!status.ok()) {
      throw std::runtime_error("failed to open RocksDB at " + db_path_ + ": " + status.ToString());
    }
    db_ = std::move(db);
  }

  std::string db_path_;
  RocksDbConfig config_;
  rocksdb::WriteOptions write_options_;
  std::unique_ptr<rocksdb::DB> db_;
};

void InMemoryKeyValueStore::Put(const std::string& key, const std::string& value) {
  data_[key] = value;
}

std::optional<std::string> InMemoryKeyValueStore::Get(const std::string& key) const {
  auto it = data_.find(key);
  if (it == data_.end()) {
    return std::nullopt;
  }
  return it->second;
}

std::size_t InMemoryKeyValueStore::Size() const {
  return data_.size();
}

std::vector<std::pair<std::string, std::string>> InMemoryKeyValueStore::Entries() const {
  std::vector<std::pair<std::string, std::string>> entries;
  entries.reserve(data_.size());
  for (const auto& [key, value] : data_) {
    entries.push_back({key, value});
  }
  return entries;
}

void InMemoryKeyValueStore::ReplaceWith(const std::vector<std::pair<std::string, std::string>>& entries) {
  data_.clear();
  for (const auto& [key, value] : entries) {
    data_[key] = value;
  }
}

RocksDbKeyValueStore::RocksDbKeyValueStore(std::string db_path, RocksDbConfig config)
    : impl_(std::make_unique<Impl>(std::move(db_path), config)) {
  impl_->write_options_.disableWAL = !impl_->config_.use_wal;
}

RocksDbKeyValueStore::~RocksDbKeyValueStore() = default;

void RocksDbKeyValueStore::Put(const std::string& key, const std::string& value) {
  auto status = impl_->db_->Put(impl_->write_options_, key, value);
  if (!status.ok()) {
    throw std::runtime_error("RocksDB put failed: " + status.ToString());
  }
}

std::optional<std::string> RocksDbKeyValueStore::Get(const std::string& key) const {
  std::string value;
  auto status = impl_->db_->Get(rocksdb::ReadOptions(), key, &value);
  if (status.IsNotFound()) {
    return std::nullopt;
  }
  if (!status.ok()) {
    throw std::runtime_error("RocksDB get failed: " + status.ToString());
  }
  return value;
}

std::size_t RocksDbKeyValueStore::Size() const {
  std::size_t count = 0;
  std::unique_ptr<rocksdb::Iterator> iterator(impl_->db_->NewIterator(rocksdb::ReadOptions()));
  for (iterator->SeekToFirst(); iterator->Valid(); iterator->Next()) {
    ++count;
  }
  if (!iterator->status().ok()) {
    throw std::runtime_error("RocksDB iterate failed: " + iterator->status().ToString());
  }
  return count;
}

std::vector<std::pair<std::string, std::string>> RocksDbKeyValueStore::Entries() const {
  std::vector<std::pair<std::string, std::string>> entries;
  std::unique_ptr<rocksdb::Iterator> iterator(impl_->db_->NewIterator(rocksdb::ReadOptions()));
  for (iterator->SeekToFirst(); iterator->Valid(); iterator->Next()) {
    entries.push_back({iterator->key().ToString(), iterator->value().ToString()});
  }
  if (!iterator->status().ok()) {
    throw std::runtime_error("RocksDB iterate failed: " + iterator->status().ToString());
  }
  return entries;
}

void RocksDbKeyValueStore::ReplaceWith(const std::vector<std::pair<std::string, std::string>>& entries) {
  rocksdb::WriteBatch batch;
  std::unique_ptr<rocksdb::Iterator> iterator(impl_->db_->NewIterator(rocksdb::ReadOptions()));
  for (iterator->SeekToFirst(); iterator->Valid(); iterator->Next()) {
    batch.Delete(iterator->key());
  }
  if (!iterator->status().ok()) {
    throw std::runtime_error("RocksDB iterate failed: " + iterator->status().ToString());
  }

  for (const auto& [key, value] : entries) {
    batch.Put(key, value);
  }

  auto status = impl_->db_->Write(impl_->write_options_, &batch);
  if (!status.ok()) {
    throw std::runtime_error("RocksDB replace failed: " + status.ToString());
  }
}
