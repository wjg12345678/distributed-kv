#include "network/http_server.h"

#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

#include <cstring>
#include <cerrno>
#include <optional>
#include <sstream>
#include <stdexcept>
#include <string>

namespace {

std::optional<std::string> ExtractHeaderValue(const std::string& request, const std::string& header_name) {
  const std::string pattern = header_name + ": ";
  auto start = request.find(pattern);
  if (start == std::string::npos) {
    return std::nullopt;
  }
  start += pattern.size();
  auto end = request.find("\r\n", start);
  if (end == std::string::npos) {
    return std::nullopt;
  }
  return request.substr(start, end - start);
}

std::string UrlDecode(std::string value) {
  for (char& ch : value) {
    if (ch == '+') {
      ch = ' ';
    }
  }
  return value;
}

}  // namespace

HttpServer::HttpServer(Cluster* cluster, int port, std::size_t worker_count)
    : cluster_(cluster), port_(port), executor_(worker_count) {}

void HttpServer::Run() {
  const int server_fd = ::socket(AF_INET, SOCK_STREAM, 0);
  if (server_fd < 0) {
    throw std::runtime_error("failed to create socket");
  }

  int opt = 1;
  ::setsockopt(server_fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));

  sockaddr_in address{};
  address.sin_family = AF_INET;
  address.sin_addr.s_addr = htonl(INADDR_ANY);
  address.sin_port = htons(port_);

  if (::bind(server_fd, reinterpret_cast<sockaddr*>(&address), sizeof(address)) < 0) {
    const std::string error = std::strerror(errno);
    ::close(server_fd);
    throw std::runtime_error("failed to bind port: " + error);
  }

  if (::listen(server_fd, 16) < 0) {
    const std::string error = std::strerror(errno);
    ::close(server_fd);
    throw std::runtime_error("failed to listen: " + error);
  }

  while (true) {
    const int client_fd = ::accept(server_fd, nullptr, nullptr);
    if (client_fd < 0) {
      continue;
    }

    executor_.Submit([this, client_fd]() {
      HandleClient(client_fd);
    });
  }
}

void HttpServer::HandleClient(int client_fd) {
  std::string request;
  char buffer[4096];
  ssize_t bytes = 0;
  while ((bytes = ::recv(client_fd, buffer, sizeof(buffer), 0)) > 0) {
    request.append(buffer, static_cast<std::size_t>(bytes));
    auto header_end = request.find("\r\n\r\n");
    if (header_end != std::string::npos) {
      std::size_t expected_body = 0;
      if (auto content_length = ExtractHeaderValue(request, "Content-Length")) {
        expected_body = static_cast<std::size_t>(std::stoul(*content_length));
      }
      if (request.size() >= header_end + 4 + expected_body) {
        break;
      }
    }
  }

  const auto response = HandleRequest(request);
  ::send(client_fd, response.data(), response.size(), 0);
  ::close(client_fd);
}

std::string HttpServer::HandleRequest(const std::string& request) {
  auto line_end = request.find("\r\n");
  if (line_end == std::string::npos) {
    return Response(400, "Bad Request", "malformed request");
  }

  std::istringstream line_stream(request.substr(0, line_end));
  std::string method;
  std::string path;
  std::string version;
  line_stream >> method >> path >> version;

  if (path.rfind("/kv/", 0) != 0) {
    return Response(404, "Not Found", "unknown path");
  }

  const std::string key = UrlDecode(path.substr(4));
  const auto body_start = request.find("\r\n\r\n");
  const std::string body = body_start == std::string::npos ? "" : request.substr(body_start + 4);

  if (method == "GET") {
    return HandleGet(key);
  }
  if (method == "PUT") {
    return HandlePut(key, body);
  }
  return Response(405, "Method Not Allowed", "only GET and PUT are supported");
}

std::string HttpServer::HandleGet(const std::string& key) {
  auto leader_id = cluster_->LeaderId();
  if (!leader_id) {
    return Response(503, "Service Unavailable", "leader not elected");
  }

  auto leader = cluster_->FindNode(*leader_id);
  if (!leader) {
    return Response(500, "Internal Server Error", "leader missing");
  }

  auto value = leader->store().Get(key);
  if (!value) {
    return Response(404, "Not Found", "key not found");
  }
  return Response(200, "OK", *value);
}

std::string HttpServer::HandlePut(const std::string& key, const std::string& value) {
  auto leader_id = cluster_->LeaderId();
  if (!leader_id) {
    return Response(503, "Service Unavailable", "leader not elected");
  }

  auto leader = cluster_->FindNode(*leader_id);
  if (!leader) {
    return Response(500, "Internal Server Error", "leader missing");
  }

  if (!leader->Propose(key, value)) {
    return Response(500, "Internal Server Error", "proposal not committed");
  }

  return Response(200, "OK", "stored");
}

std::string HttpServer::Response(int status_code, const std::string& status_text, const std::string& body) const {
  std::ostringstream out;
  out << "HTTP/1.1 " << status_code << ' ' << status_text << "\r\n";
  out << "Content-Type: text/plain\r\n";
  out << "Content-Length: " << body.size() << "\r\n";
  out << "Connection: close\r\n\r\n";
  out << body;
  return out.str();
}
