#pragma once

#include "core/cluster.h"
#include "network/task_executor.h"

#include <cstddef>
#include <string>

class HttpServer {
public:
  HttpServer(Cluster* cluster, int port, std::size_t worker_count = 0);
  void Run();

private:
  void HandleClient(int client_fd);
  std::string HandleRequest(const std::string& request);
  std::string HandleGet(const std::string& key);
  std::string HandlePut(const std::string& key, const std::string& value);
  std::string Response(int status_code, const std::string& status_text, const std::string& body) const;

  Cluster* cluster_ = nullptr;
  int port_ = 0;
  TaskExecutor executor_;
};
