#include "network/http_server.h"

#include "network/libuv_http_server.h"

#include <charconv>
#include <cerrno>
#include <optional>
#include <stdexcept>
#include <string>
#include <string_view>

namespace {

class BadRequestError : public std::runtime_error {
public:
  explicit BadRequestError(const std::string& message) : std::runtime_error(message) {}
};

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

std::string StripQuery(std::string target) {
  const auto question = target.find('?');
  if (question == std::string::npos) {
    return target;
  }
  return target.substr(0, question);
}

}  // namespace

HttpServer::HttpServer(Cluster* cluster, int port, std::size_t worker_count)
    : cluster_(cluster), port_(port), worker_count_(worker_count) {}

void HttpServer::Run() {
  LibuvHttpServer server(
      port_,
      [this](const HttpRequest& request) {
        return HandleRequest(request);
      },
      worker_count_);
  server.Run();
}

HttpResponse HttpServer::HandleRequest(const HttpRequest& request) {
  try {
    const std::string path = StripQuery(request.target);
    if (path.rfind("/kv/", 0) != 0) {
      return Response(404, "Not Found", "unknown path");
    }

    const std::string key = UrlDecode(path.substr(4));
    if (request.method == "GET") {
      return HandleGet(key);
    }
    if (request.method == "PUT") {
      return HandlePut(key, request.body);
    }
    return Response(405, "Method Not Allowed", "only GET and PUT are supported");
  } catch (const BadRequestError& ex) {
    return Response(400, "Bad Request", ex.what());
  } catch (const std::exception&) {
    return Response(500, "Internal Server Error", "internal server error");
  }
}

HttpResponse HttpServer::HandleGet(const std::string& key) {
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

HttpResponse HttpServer::HandlePut(const std::string& key, const std::string& value) {
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

HttpResponse HttpServer::Response(int status_code, const std::string& status_text, const std::string& body) const {
  HttpResponse response;
  response.status_code = status_code;
  response.status_text = status_text;
  response.body = body;
  return response;
}
