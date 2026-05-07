#pragma once

#include "network/http_message.h"
#include "network/task_executor.h"

#include <cstddef>
#include <functional>

class LibuvHttpServer {
public:
  using RequestHandler = std::function<HttpResponse(const HttpRequest&)>;

  LibuvHttpServer(int port, RequestHandler handler, std::size_t worker_count = 0);
  void Run();

private:
  int port_ = 0;
  RequestHandler handler_;
  TaskExecutor executor_;
};

std::string SerializeHttpResponse(const HttpResponse& response, bool keep_alive);
