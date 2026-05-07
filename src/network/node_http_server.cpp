#include "network/node_http_server.h"

#include "network/tcp_util.h"

#include <algorithm>
#include <sys/socket.h>
#include <unistd.h>

#include <charconv>
#include <optional>
#include <limits>
#include <sstream>
#include <stdexcept>
#include <string>
#include <string_view>
#include <unordered_map>

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

std::unordered_map<std::string, std::string> ParseKeyValueString(const std::string& body, char delimiter) {
  std::unordered_map<std::string, std::string> values;
  std::size_t start = 0;
  while (start <= body.size()) {
    const auto next = body.find(delimiter, start);
    const auto piece = body.substr(start, next == std::string::npos ? std::string::npos : next - start);
    const auto eq = piece.find('=');
    if (eq != std::string::npos) {
      values[UrlDecode(piece.substr(0, eq))] = UrlDecode(piece.substr(eq + 1));
    }
    if (next == std::string::npos) {
      break;
    }
    start = next + 1;
  }
  return values;
}

std::unordered_map<std::string, std::string> ParseFormBody(const std::string& body) {
  return ParseKeyValueString(body, '&');
}

std::unordered_map<std::string, std::string> ParseQuery(const std::string& query) {
  return ParseKeyValueString(query, '&');
}

std::pair<std::string, std::optional<std::string>> SplitPathAndQuery(const std::string& raw) {
  const auto question = raw.find('?');
  if (question == std::string::npos) {
    return {raw, std::nullopt};
  }
  return {raw.substr(0, question), raw.substr(question + 1)};
}

const char* RoleName(Role role) {
  switch (role) {
    case Role::Leader:
      return "leader";
    case Role::Follower:
      return "follower";
    case Role::Candidate:
      return "candidate";
  }
  return "unknown";
}

}  // namespace

NodeHttpServer::NodeHttpServer(
    std::shared_ptr<RaftNode> node,
    std::vector<PeerEndpoint> peers,
    int port,
    std::size_t worker_count)
    : node_(std::move(node)), peers_(std::move(peers)), port_(port), executor_(worker_count) {}

void NodeHttpServer::Run() {
  const int server_fd = CreateListenSocket(port_);
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

void NodeHttpServer::HandleClient(int client_fd) {
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

std::string NodeHttpServer::HandleRequest(const std::string& request) {
  ++http_requests_total_;

  const auto line_end = request.find("\r\n");
  if (line_end == std::string::npos) {
    return Response(400, "Bad Request", "malformed request");
  }

  std::istringstream line_stream(request.substr(0, line_end));
  std::string method;
  std::string raw_path;
  std::string version;
  line_stream >> method >> raw_path >> version;

  const auto [path, query] = SplitPathAndQuery(raw_path);
  const auto body_start = request.find("\r\n\r\n");
  const std::string body = body_start == std::string::npos ? "" : request.substr(body_start + 4);

  if (path == "/status") {
    return Response(200, "OK", HandleStatus());
  }
  if (path == "/metrics") {
    return Response(200, "OK", HandleMetrics());
  }
  if (path == "/admin/add-peer" && method == "POST") {
    return HandleAddPeer(body);
  }
  if (path == "/admin/remove-peer" && method == "POST") {
    return HandleRemovePeer(body);
  }
  if (path == "/lock/acquire" && method == "POST") {
    return HandleLockAcquire(body);
  }
  if (path == "/lock/release" && method == "POST") {
    return HandleLockRelease(body);
  }
  if (path.rfind("/lock/", 0) == 0 && method == "GET") {
    return HandleLockGet(UrlDecode(path.substr(6)));
  }
  if (path == "/mvcc/begin" && method == "POST") {
    return HandleMvccBegin();
  }
  if (path == "/mvcc/commit" && method == "POST") {
    return HandleMvccCommit(body);
  }
  if (path.rfind("/mvcc/tx/", 0) == 0 && method == "PUT") {
    const std::string prefix = "/mvcc/tx/";
    const auto tx_key_pos = path.find("/kv/", prefix.size());
    if (tx_key_pos == std::string::npos) {
      return Response(400, "Bad Request", "expected /mvcc/tx/<tx_id>/kv/<key>");
    }
    const std::string tx_id = path.substr(prefix.size(), tx_key_pos - prefix.size());
    const std::string key = UrlDecode(path.substr(tx_key_pos + 4));
    return HandleMvccWrite(tx_id, key, body);
  }
  if (path.rfind("/mvcc/kv/", 0) == 0 && method == "GET") {
    const auto query_values = query ? ParseQuery(*query) : std::unordered_map<std::string, std::string>{};
    const auto it = query_values.find("snapshot_ts");
    return HandleMvccRead(
        UrlDecode(path.substr(9)),
        it == query_values.end() ? std::optional<std::string>{} : std::optional<std::string>{it->second});
  }
  if (path.rfind("/kv/", 0) == 0) {
    const std::string key = UrlDecode(path.substr(4));
    if (method == "GET") {
      return HandleGet(key);
    }
    if (method == "PUT") {
      return HandlePut(key, body);
    }
    return Response(405, "Method Not Allowed", "only GET and PUT are supported");
  }
  return Response(404, "Not Found", "unknown path");
}

std::string NodeHttpServer::HandleGet(const std::string& key) {
  ++kv_get_total_;
  if (node_->role() != Role::Leader) {
    auto redirect = RedirectToLeader("/kv/" + key);
    if (!redirect.empty()) {
      return redirect;
    }
    return Response(503, "Service Unavailable", "leader unknown");
  }

  if (!node_->ConfirmLeaderForRead()) {
    return Response(503, "Service Unavailable", "read quorum unavailable");
  }

  auto value = node_->GetValue(key);
  if (!value) {
    return Response(404, "Not Found", "key not found");
  }
  return Response(200, "OK", *value);
}

std::string NodeHttpServer::HandlePut(const std::string& key, const std::string& value) {
  ++kv_put_total_;
  if (node_->role() != Role::Leader) {
    auto redirect = RedirectToLeader("/kv/" + key);
    if (!redirect.empty()) {
      return redirect;
    }
    return Response(503, "Service Unavailable", "leader unknown");
  }

  if (!node_->Propose(key, value)) {
    return Response(500, "Internal Server Error", "proposal not committed");
  }
  return Response(200, "OK", "stored");
}

std::string NodeHttpServer::HandleLockGet(const std::string& name) const {
  if (node_->role() != Role::Leader) {
    auto redirect = RedirectToLeader("/lock/" + name);
    if (!redirect.empty()) {
      return redirect;
    }
    return Response(503, "Service Unavailable", "leader unknown");
  }

  if (!node_->ConfirmLeaderForRead()) {
    return Response(503, "Service Unavailable", "read quorum unavailable");
  }

  const auto owner = node_->LockOwner(name);
  if (!owner) {
    return Response(200, "OK", "unlocked");
  }
  return Response(200, "OK", "owner=" + *owner);
}

std::string NodeHttpServer::HandleLockAcquire(const std::string& body) {
  ++lock_acquire_total_;
  if (node_->role() != Role::Leader) {
    auto redirect = RedirectToLeader("/lock/acquire");
    if (!redirect.empty()) {
      return redirect;
    }
    return Response(503, "Service Unavailable", "leader unknown");
  }

  const auto values = ParseFormBody(body);
  if (!values.count("name") || !values.count("owner")) {
    return Response(400, "Bad Request", "expected name,owner");
  }
  if (!node_->AcquireLock(values.at("name"), values.at("owner"))) {
    return Response(409, "Conflict", "lock busy");
  }
  return Response(200, "OK", "locked");
}

std::string NodeHttpServer::HandleLockRelease(const std::string& body) {
  ++lock_release_total_;
  if (node_->role() != Role::Leader) {
    auto redirect = RedirectToLeader("/lock/release");
    if (!redirect.empty()) {
      return redirect;
    }
    return Response(503, "Service Unavailable", "leader unknown");
  }

  const auto values = ParseFormBody(body);
  if (!values.count("name") || !values.count("owner")) {
    return Response(400, "Bad Request", "expected name,owner");
  }
  if (!node_->ReleaseLock(values.at("name"), values.at("owner"))) {
    return Response(409, "Conflict", "lock owner mismatch");
  }
  return Response(200, "OK", "released");
}

std::string NodeHttpServer::HandleMvccBegin() {
  ++mvcc_begin_total_;
  if (node_->role() != Role::Leader) {
    auto redirect = RedirectToLeader("/mvcc/begin");
    if (!redirect.empty()) {
      return redirect;
    }
    return Response(503, "Service Unavailable", "leader unknown");
  }

  const auto tx = node_->BeginMvccTransaction();
  if (!tx) {
    return Response(503, "Service Unavailable", "read quorum unavailable");
  }

  return Response(
      200,
      "OK",
      "tx_id=" + tx->tx_id + "\n" + "snapshot_ts=" + std::to_string(tx->snapshot_ts));
}

std::string NodeHttpServer::HandleMvccWrite(const std::string& tx_id, const std::string& key, const std::string& value) {
  ++mvcc_write_total_;
  if (node_->role() != Role::Leader) {
    auto redirect = RedirectToLeader("/mvcc/tx/" + tx_id + "/kv/" + key);
    if (!redirect.empty()) {
      return redirect;
    }
    return Response(503, "Service Unavailable", "leader unknown");
  }

  if (!node_->StageMvccWrite(tx_id, key, value)) {
    return Response(404, "Not Found", "mvcc transaction not found");
  }
  return Response(200, "OK", "staged");
}

std::string NodeHttpServer::HandleMvccCommit(const std::string& body) {
  ++mvcc_commit_total_;
  if (node_->role() != Role::Leader) {
    auto redirect = RedirectToLeader("/mvcc/commit");
    if (!redirect.empty()) {
      return redirect;
    }
    return Response(503, "Service Unavailable", "leader unknown");
  }

  const auto values = ParseFormBody(body);
  if (!values.count("tx_id")) {
    return Response(400, "Bad Request", "expected tx_id");
  }

  const auto result = node_->CommitMvccTransaction(values.at("tx_id"));
  if (result.conflict) {
    return Response(409, "Conflict", "mvcc write-write conflict");
  }
  if (!result.committed) {
    return Response(500, "Internal Server Error", "mvcc commit failed");
  }
  return Response(200, "OK", "commit_ts=" + std::to_string(result.commit_ts));
}

std::string NodeHttpServer::HandleMvccRead(const std::string& key, const std::optional<std::string>& snapshot_ts) const {
  ++mvcc_read_total_;
  if (node_->role() != Role::Leader) {
    auto redirect = RedirectToLeader("/mvcc/kv/" + key + (snapshot_ts ? "?snapshot_ts=" + *snapshot_ts : ""));
    if (!redirect.empty()) {
      return redirect;
    }
    return Response(503, "Service Unavailable", "leader unknown");
  }

  if (!node_->ConfirmLeaderForRead()) {
    return Response(503, "Service Unavailable", "read quorum unavailable");
  }

  const std::uint64_t ts = snapshot_ts
      ? ParseIntegerOrThrow<std::uint64_t>(*snapshot_ts, "snapshot_ts")
      : std::numeric_limits<std::uint64_t>::max();
  const auto value = node_->ReadMvcc(key, ts);
  if (!value) {
    return Response(404, "Not Found", "mvcc version not found");
  }
  return Response(200, "OK", *value);
}

std::string NodeHttpServer::HandleAddPeer(const std::string& body) {
  if (node_->role() != Role::Leader) {
    auto redirect = RedirectToLeader("/admin/add-peer");
    if (!redirect.empty()) {
      return redirect;
    }
    return Response(503, "Service Unavailable", "leader unknown");
  }

  const auto values = ParseFormBody(body);
  if (!values.count("id") || !values.count("host") || !values.count("raft_port") || !values.count("http_port")) {
    return Response(400, "Bad Request", "expected id,host,raft_port,http_port");
  }

  PeerEndpoint peer;
  peer.id = ParseIntegerOrThrow<int>(values.at("id"), "id");
  peer.host = values.at("host");
  peer.raft_port = ParseIntegerOrThrow<int>(values.at("raft_port"), "raft_port");
  peer.http_port = ParseIntegerOrThrow<int>(values.at("http_port"), "http_port");
  if (!node_->AddPeer(peer)) {
    return Response(500, "Internal Server Error", "add-peer not committed");
  }
  {
    std::lock_guard<std::mutex> lock(peers_mutex_);
    auto existing = std::find_if(peers_.begin(), peers_.end(), [&peer](const PeerEndpoint& current) {
      return current.id == peer.id;
    });
    if (existing == peers_.end()) {
      peers_.push_back(peer);
    } else {
      *existing = peer;
    }
  }
  return Response(200, "OK", "peer added");
}

std::string NodeHttpServer::HandleRemovePeer(const std::string& body) {
  if (node_->role() != Role::Leader) {
    auto redirect = RedirectToLeader("/admin/remove-peer");
    if (!redirect.empty()) {
      return redirect;
    }
    return Response(503, "Service Unavailable", "leader unknown");
  }

  const auto values = ParseFormBody(body);
  if (!values.count("id")) {
    return Response(400, "Bad Request", "expected id");
  }
  const int peer_id = ParseIntegerOrThrow<int>(values.at("id"), "id");
  if (!node_->RemovePeer(peer_id)) {
    return Response(500, "Internal Server Error", "remove-peer not committed");
  }
  {
    std::lock_guard<std::mutex> lock(peers_mutex_);
    peers_.erase(std::remove_if(peers_.begin(), peers_.end(), [peer_id](const PeerEndpoint& peer) {
      return peer.id == peer_id;
    }), peers_.end());
  }
  return Response(200, "OK", "peer removed");
}

std::string NodeHttpServer::HandleStatus() const {
  std::ostringstream out;
  out << "node_id=" << node_->id() << '\n'
      << "role=" << RoleName(node_->role()) << '\n'
      << "term=" << node_->current_term() << '\n'
      << "leader_id=" << node_->leader_id() << '\n'
      << "commit_index=" << node_->commit_index() << '\n'
      << "log_base_index=" << node_->log_base_index() << '\n'
      << "http_requests_total=" << http_requests_total_.load() << '\n'
      << "peer_count=";
  {
    std::lock_guard<std::mutex> lock(peers_mutex_);
    out << peers_.size();
  }
  out << '\n';
  return out.str();
}

std::string NodeHttpServer::HandleMetrics() const {
  const auto raft_metrics = node_->metrics();
  const auto uptime = std::chrono::duration_cast<std::chrono::duration<double>>(std::chrono::steady_clock::now() - started_at_).count();
  const double avg_qps = uptime > 0.0 ? static_cast<double>(http_requests_total_.load()) / uptime : 0.0;

  std::ostringstream out;
  out << "node_id " << node_->id() << '\n'
      << "raft_current_term " << node_->current_term() << '\n'
      << "raft_commit_index " << node_->commit_index() << '\n'
      << "raft_log_base_index " << node_->log_base_index() << '\n'
      << "raft_role " << RoleName(node_->role()) << '\n'
      << "raft_pre_vote_rpcs_total " << raft_metrics.pre_vote_rpcs << '\n'
      << "raft_request_vote_rpcs_total " << raft_metrics.request_vote_rpcs << '\n'
      << "raft_append_entries_rpcs_total " << raft_metrics.append_entries_rpcs << '\n'
      << "raft_install_snapshot_rpcs_total " << raft_metrics.install_snapshot_rpcs << '\n'
      << "raft_linearizable_read_checks_total " << raft_metrics.linearizable_read_checks << '\n'
      << "raft_linearizable_read_failures_total " << raft_metrics.linearizable_read_failures << '\n'
      << "raft_client_put_proposals_total " << raft_metrics.client_put_proposals << '\n'
      << "raft_lock_acquire_proposals_total " << raft_metrics.lock_acquire_proposals << '\n'
      << "raft_lock_release_proposals_total " << raft_metrics.lock_release_proposals << '\n'
      << "raft_mvcc_begin_total " << raft_metrics.mvcc_begin_total << '\n'
      << "raft_mvcc_commit_attempts_total " << raft_metrics.mvcc_commit_attempts << '\n'
      << "raft_mvcc_commit_conflicts_total " << raft_metrics.mvcc_commit_conflicts << '\n'
      << "raft_committed_entries_total " << raft_metrics.committed_entries << '\n'
      << "raft_applied_entries_total " << raft_metrics.applied_entries << '\n'
      << "http_requests_total " << http_requests_total_.load() << '\n'
      << "http_kv_get_total " << kv_get_total_.load() << '\n'
      << "http_kv_put_total " << kv_put_total_.load() << '\n'
      << "http_lock_acquire_total " << lock_acquire_total_.load() << '\n'
      << "http_lock_release_total " << lock_release_total_.load() << '\n'
      << "http_mvcc_begin_total " << mvcc_begin_total_.load() << '\n'
      << "http_mvcc_write_total " << mvcc_write_total_.load() << '\n'
      << "http_mvcc_commit_total " << mvcc_commit_total_.load() << '\n'
      << "http_mvcc_read_total " << mvcc_read_total_.load() << '\n'
      << "http_avg_qps " << avg_qps << '\n';
  return out.str();
}

std::string NodeHttpServer::RedirectToLeader(const std::string& path) const {
  const int leader_id = node_->leader_id();
  if (leader_id < 0) {
    return {};
  }
  const auto peer = FindPeer(leader_id);
  if (!peer) {
    return {};
  }

  std::ostringstream out;
  const std::string body = "redirecting to leader";
  out << "HTTP/1.1 307 Temporary Redirect\r\n";
  out << "Location: http://" << peer->host << ":" << peer->http_port << path << "\r\n";
  out << "X-Leader-Id: " << peer->id << "\r\n";
  out << "Content-Type: text/plain\r\n";
  out << "Content-Length: " << body.size() << "\r\n";
  out << "Connection: close\r\n\r\n";
  out << body;
  return out.str();
}

std::string NodeHttpServer::Response(int status_code, const std::string& status_text, const std::string& body) const {
  std::ostringstream out;
  out << "HTTP/1.1 " << status_code << ' ' << status_text << "\r\n";
  out << "Content-Type: text/plain\r\n";
  out << "Content-Length: " << body.size() << "\r\n";
  out << "Connection: close\r\n\r\n";
  out << body;
  return out.str();
}

std::optional<PeerEndpoint> NodeHttpServer::FindPeer(int id) const {
  std::lock_guard<std::mutex> lock(peers_mutex_);
  for (const auto& peer : peers_) {
    if (peer.id == id) {
      return peer;
    }
  }
  return std::nullopt;
}
