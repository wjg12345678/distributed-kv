#pragma once

#include <cstddef>
#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <unordered_map>
#include <vector>

class IKeyValueStore {
public:
  virtual ~IKeyValueStore() = default;

  virtual void Put(const std::string& key, const std::string& value) = 0;
  virtual std::optional<std::string> Get(const std::string& key) const = 0;
  virtual std::size_t Size() const = 0;
  virtual std::vector<std::pair<std::string, std::string>> Entries() const = 0;
  virtual void ReplaceWith(const std::vector<std::pair<std::string, std::string>>& entries) = 0;
};

enum class RocksDbCompactionStyle {
  Level,
  Universal,
  Fifo,
};

struct RocksDbConfig {
  std::size_t write_buffer_size = 64 * 1024 * 1024;
  int max_background_jobs = 2;
  int max_write_buffer_number = 3;
  int max_open_files = -1;
  int max_subcompactions = 2;
  int level0_file_num_compaction_trigger = 4;
  std::uint64_t target_file_size_base = 64ULL * 1024ULL * 1024ULL;
  int bloom_bits_per_key = 10;
  bool enable_compression = true;
  bool enable_bloom_filter = true;
  bool use_wal = true;
  RocksDbCompactionStyle compaction_style = RocksDbCompactionStyle::Level;
};

class InMemoryKeyValueStore : public IKeyValueStore {
public:
  void Put(const std::string& key, const std::string& value) override;
  std::optional<std::string> Get(const std::string& key) const override;
  std::size_t Size() const override;
  std::vector<std::pair<std::string, std::string>> Entries() const override;
  void ReplaceWith(const std::vector<std::pair<std::string, std::string>>& entries) override;

private:
  std::unordered_map<std::string, std::string> data_;
};

class RocksDbKeyValueStore : public IKeyValueStore {
public:
  explicit RocksDbKeyValueStore(std::string db_path, RocksDbConfig config = {});
  ~RocksDbKeyValueStore() override;

  RocksDbKeyValueStore(const RocksDbKeyValueStore&) = delete;
  RocksDbKeyValueStore& operator=(const RocksDbKeyValueStore&) = delete;

  void Put(const std::string& key, const std::string& value) override;
  std::optional<std::string> Get(const std::string& key) const override;
  std::size_t Size() const override;
  std::vector<std::pair<std::string, std::string>> Entries() const override;
  void ReplaceWith(const std::vector<std::pair<std::string, std::string>>& entries) override;

private:
  class Impl;
  std::unique_ptr<Impl> impl_;
};
