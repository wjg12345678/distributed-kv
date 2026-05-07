#pragma once

#include "core/cluster.h"
#include "network/http_message.h"

#include <cstddef>
#include <string>

class HttpServer {
public:
  HttpServer(Cluster* cluster, int port, std::size_t worker_count = 0);
  void Run();

private:
  HttpResponse HandleRequest(const HttpRequest& request);
  HttpResponse HandleGet(const std::string& key);
  HttpResponse HandlePut(const std::string& key, const std::string& value);
  HttpResponse Response(int status_code, const std::string& status_text, const std::string& body) const;

  Cluster* cluster_ = nullptr;
  int port_ = 0;
  std::size_t worker_count_ = 0;
};
