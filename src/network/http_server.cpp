#include "network/http_server.h"

#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

#include <charconv>
#include <cstring>
#include <cerrno>
#include <optional>
#include <sstream>
#include <stdexcept>
#include <string>
#include <string_view>

namespace {

class BadRequestError : public std::runtime_error {
public:
  explicit BadRequestError(const std::string& message) : std::runtime_error(message) {}
};

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

int HexValue(char ch) {
  if (ch >= '0' && ch <= '9') {
    return ch - '0';
  }
  if (ch >= 'a' && ch <= 'f') {
    return ch - 'a' + 10;
  }
  if (ch >= 'A' && ch <= 'F') {
    return ch - 'A' + 10;
  }
  return -1;
}

std::string UrlDecode(std::string_view value) {
  std::string decoded;
  decoded.reserve(value.size());
  for (std::size_t i = 0; i < value.size(); ++i) {
    char ch = value[i];
    if (ch == '+') {
      decoded.push_back(' ');
      continue;
    }
    if (ch == '%') {
      if (i + 2 >= value.size()) {
        throw BadRequestError("invalid url encoding");
      }
      const int high = HexValue(value[i + 1]);
      const int low = HexValue(value[i + 2]);
      if (high < 0 || low < 0) {
        throw BadRequestError("invalid url encoding");
      }
      decoded.push_back(static_cast<char>((high << 4) | low));
      i += 2;
      continue;
    }
    decoded.push_back(ch);
  }
  return decoded;
}

template <typename Integer>
Integer ParseIntegerOrThrow(std::string_view raw, const char* field_name) {
  if (raw.empty()) {
    throw BadRequestError(std::string("missing ") + field_name);
  }

  Integer value{};
  const auto* begin = raw.data();
  const auto* end = raw.data() + raw.size();
  const auto result = std::from_chars(begin, end, value);
  if (result.ec != std::errc() || result.ptr != end) {
    throw BadRequestError(std::string("invalid ") + field_name);
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
  std::string response;
  try {
    std::string request;
    char buffer[4096];
    ssize_t bytes = 0;
    while ((bytes = ::recv(client_fd, buffer, sizeof(buffer), 0)) > 0) {
      request.append(buffer, static_cast<std::size_t>(bytes));
      auto header_end = request.find("\r\n\r\n");
      if (header_end != std::string::npos) {
        std::size_t expected_body = 0;
        if (auto content_length = ExtractHeaderValue(request, "Content-Length")) {
          expected_body = ParseIntegerOrThrow<std::size_t>(*content_length, "Content-Length");
        }
        if (request.size() >= header_end + 4 + expected_body) {
          break;
        }
      }
    }
    response = HandleRequest(request);
  } catch (const BadRequestError& ex) {
    response = Response(400, "Bad Request", ex.what());
  } catch (const std::exception&) {
    response = Response(500, "Internal Server Error", "internal server error");
  }

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

  if (!leader->ConfirmLeaderForRead()) {
    return Response(503, "Service Unavailable", "read quorum unavailable");
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
