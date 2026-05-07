#pragma once

#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

struct HttpRequest {
  std::string method;
  std::string target;
  std::string version;
  std::unordered_map<std::string, std::string> headers;
  std::string body;
  bool keep_alive = true;
};

struct HttpResponse {
  int status_code = 200;
  std::string status_text = "OK";
  std::string content_type = "text/plain";
  std::string body;
  std::vector<std::pair<std::string, std::string>> headers;
  bool close_connection = false;
};
