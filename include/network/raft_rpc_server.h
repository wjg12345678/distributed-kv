#pragma once

#include "core/raft_node.h"
#include "network/task_executor.h"

#include <cstddef>
#include <memory>

class RaftRpcServer {
public:
  RaftRpcServer(std::shared_ptr<RaftNode> node, int port, std::size_t worker_count = 0);
  void Run();

private:
  std::string Dispatch(const std::string& request) const;

  std::shared_ptr<RaftNode> node_;
  int port_ = 0;
  mutable TaskExecutor executor_;
};
