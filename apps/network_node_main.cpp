#include "network/network_transport.h"
#include "network/node_http_server.h"
#include "core/raft_node.h"
#include "core/raft_persistence.h"
#include "network/raft_rpc_server.h"

#include <chrono>
#include <filesystem>
#include <iostream>
#include <memory>
#include <string>
#include <thread>
#include <vector>

namespace {

struct NodeConfig {
  int node_id = 0;
  int raft_port = 0;
  int http_port = 0;
  std::string data_dir;
  std::vector<PeerEndpoint> peers;
  RocksDbConfig rocksdb;
};

RocksDbCompactionStyle ParseCompactionStyle(const std::string& value) {
  if (value == "level") {
    return RocksDbCompactionStyle::Level;
  }
  if (value == "universal") {
    return RocksDbCompactionStyle::Universal;
  }
  if (value == "fifo") {
    return RocksDbCompactionStyle::Fifo;
  }
  throw std::runtime_error("invalid --compaction-style, expected level|universal|fifo");
}

const char* ToString(RocksDbCompactionStyle style) {
  switch (style) {
    case RocksDbCompactionStyle::Level:
      return "level";
    case RocksDbCompactionStyle::Universal:
      return "universal";
    case RocksDbCompactionStyle::Fifo:
      return "fifo";
  }
  return "level";
}

PeerEndpoint ParsePeer(const std::string& value) {
  std::vector<std::string> parts;
  std::size_t start = 0;
  while (start <= value.size()) {
    const auto next = value.find(':', start);
    if (next == std::string::npos) {
      parts.push_back(value.substr(start));
      break;
    }
    parts.push_back(value.substr(start, next - start));
    start = next + 1;
  }
  if (parts.size() != 4) {
    throw std::runtime_error("invalid --peer format, expected id:host:raft_port:http_port");
  }
  return PeerEndpoint{std::stoi(parts[0]), parts[1], std::stoi(parts[2]), std::stoi(parts[3])};
}

NodeConfig ParseArgs(int argc, char** argv) {
  NodeConfig config;
  for (int i = 1; i < argc; ++i) {
    const std::string arg = argv[i];
    if (arg == "--node-id" && i + 1 < argc) {
      config.node_id = std::stoi(argv[++i]);
    } else if (arg == "--raft-port" && i + 1 < argc) {
      config.raft_port = std::stoi(argv[++i]);
    } else if (arg == "--http-port" && i + 1 < argc) {
      config.http_port = std::stoi(argv[++i]);
    } else if (arg == "--data-dir" && i + 1 < argc) {
      config.data_dir = argv[++i];
    } else if (arg == "--peer" && i + 1 < argc) {
      config.peers.push_back(ParsePeer(argv[++i]));
    } else if (arg == "--disable-wal") {
      config.rocksdb.use_wal = false;
    } else if (arg == "--disable-compression") {
      config.rocksdb.enable_compression = false;
    } else if (arg == "--disable-bloom-filter") {
      config.rocksdb.enable_bloom_filter = false;
    } else if (arg == "--write-buffer-size" && i + 1 < argc) {
      config.rocksdb.write_buffer_size = static_cast<std::size_t>(std::stoull(argv[++i]));
    } else if (arg == "--max-background-jobs" && i + 1 < argc) {
      config.rocksdb.max_background_jobs = std::stoi(argv[++i]);
    } else if (arg == "--max-write-buffer-number" && i + 1 < argc) {
      config.rocksdb.max_write_buffer_number = std::stoi(argv[++i]);
    } else if (arg == "--max-open-files" && i + 1 < argc) {
      config.rocksdb.max_open_files = std::stoi(argv[++i]);
    } else if (arg == "--max-subcompactions" && i + 1 < argc) {
      config.rocksdb.max_subcompactions = std::stoi(argv[++i]);
    } else if (arg == "--level0-compaction-trigger" && i + 1 < argc) {
      config.rocksdb.level0_file_num_compaction_trigger = std::stoi(argv[++i]);
    } else if (arg == "--target-file-size-base" && i + 1 < argc) {
      config.rocksdb.target_file_size_base = static_cast<std::uint64_t>(std::stoull(argv[++i]));
    } else if (arg == "--bloom-bits-per-key" && i + 1 < argc) {
      config.rocksdb.bloom_bits_per_key = std::stoi(argv[++i]);
    } else if (arg == "--compaction-style" && i + 1 < argc) {
      config.rocksdb.compaction_style = ParseCompactionStyle(argv[++i]);
    } else {
      throw std::runtime_error("unknown or incomplete argument: " + arg);
    }
  }

  if (config.node_id == 0 || config.raft_port == 0 || config.http_port == 0 || config.data_dir.empty()) {
    throw std::runtime_error("missing required args");
  }
  return config;
}

}  // namespace

int main(int argc, char** argv) {
  try {
    const auto config = ParseArgs(argc, argv);
    std::filesystem::create_directories(config.data_dir);

    NetworkTransport transport(config.peers);
    auto node = std::make_shared<RaftNode>(
        config.node_id,
        [&]() {
          std::vector<int> ids;
          for (const auto& peer : config.peers) {
            ids.push_back(peer.id);
          }
          return ids;
        }(),
        &transport,
        4 + config.node_id * 2,
        std::make_unique<RocksDbKeyValueStore>(config.data_dir + "/store", config.rocksdb),
        std::make_unique<RocksDbRaftPersistence>(config.data_dir + "/meta"));

    RaftRpcServer rpc_server(node, config.raft_port);
    NodeHttpServer http_server(node, config.peers, config.http_port);

    std::thread ticker([node]() {
      try {
        while (true) {
          node->Tick();
          std::this_thread::sleep_for(std::chrono::milliseconds(200));
        }
      } catch (const std::exception& ex) {
        std::cerr << "ticker failed: " << ex.what() << '\n';
      }
    });

    std::thread raft_thread([&rpc_server]() {
      try {
        rpc_server.Run();
      } catch (const std::exception& ex) {
        std::cerr << "raft rpc server failed: " << ex.what() << '\n';
      }
    });

    std::cout << "node=" << config.node_id
              << " raft_port=" << config.raft_port
              << " http_port=" << config.http_port
              << " data_dir=" << config.data_dir
              << " wal=" << (config.rocksdb.use_wal ? "on" : "off")
              << " bloom=" << (config.rocksdb.enable_bloom_filter ? "on" : "off")
              << " compaction=" << ToString(config.rocksdb.compaction_style)
              << " bg_jobs=" << config.rocksdb.max_background_jobs
              << '\n';

    try {
      http_server.Run();
    } catch (...) {
      if (raft_thread.joinable()) {
        raft_thread.detach();
      }
      if (ticker.joinable()) {
        ticker.detach();
      }
      throw;
    }
    ticker.join();
    raft_thread.join();
  } catch (const std::exception& ex) {
    std::cerr << ex.what() << '\n';
    return 1;
  }

  return 0;
}
