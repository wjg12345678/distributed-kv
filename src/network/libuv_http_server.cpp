#include "network/libuv_http_server.h"

#include <llhttp.h>
#include <uv.h>

#include <cstring>
#include <deque>
#include <memory>
#include <mutex>
#include <optional>
#include <queue>
#include <sstream>
#include <stdexcept>
#include <string>
#include <unordered_map>
#include <utility>

namespace {

std::runtime_error UvError(const std::string& action, int status) {
  return std::runtime_error(action + ": " + uv_strerror(status));
}

struct HttpConnection {
  enum class HeaderState {
    None,
    Field,
    Value,
  };

  explicit HttpConnection(void* owner_ptr) : owner(owner_ptr) {}

  void* owner = nullptr;
  uv_tcp_t handle{};
  llhttp_t parser{};
  HttpRequest request;
  std::string current_header_field;
  std::string current_header_value;
  std::string buffered_input;
  std::deque<std::string> write_queue;
  HeaderState header_state = HeaderState::None;
  bool message_complete = false;
  bool request_in_flight = false;
  bool write_in_progress = false;
  bool close_after_write = false;
  bool closing = false;
};

struct WriteRequest {
  uv_write_t request{};
  std::shared_ptr<HttpConnection> connection;
  std::shared_ptr<std::string> payload;
};

class HttpServerRuntime {
public:
  HttpServerRuntime(int port, LibuvHttpServer::RequestHandler& handler, TaskExecutor& executor)
      : port_(port), handler_(handler), executor_(executor) {
    llhttp_settings_init(&settings_);
    settings_.on_message_begin = &HttpServerRuntime::OnMessageBegin;
    settings_.on_method = &HttpServerRuntime::OnMethod;
    settings_.on_url = &HttpServerRuntime::OnUrl;
    settings_.on_version = &HttpServerRuntime::OnVersion;
    settings_.on_header_field = &HttpServerRuntime::OnHeaderField;
    settings_.on_header_value = &HttpServerRuntime::OnHeaderValue;
    settings_.on_headers_complete = &HttpServerRuntime::OnHeadersComplete;
    settings_.on_body = &HttpServerRuntime::OnBody;
    settings_.on_message_complete = &HttpServerRuntime::OnMessageComplete;
  }

  void Run() {
    int status = uv_loop_init(&loop_);
    if (status != 0) {
      throw UvError("failed to initialize libuv loop", status);
    }

    status = uv_async_init(&loop_, &completion_async_, &HttpServerRuntime::OnCompletionAsync);
    if (status != 0) {
      throw UvError("failed to initialize libuv async", status);
    }
    completion_async_.data = this;

    status = uv_tcp_init(&loop_, &server_);
    if (status != 0) {
      throw UvError("failed to initialize tcp server", status);
    }
    server_.data = this;

    sockaddr_in address{};
    status = uv_ip4_addr("0.0.0.0", port_, &address);
    if (status != 0) {
      throw UvError("failed to build listen address", status);
    }

    status = uv_tcp_bind(&server_, reinterpret_cast<const sockaddr*>(&address), 0);
    if (status != 0) {
      throw UvError("failed to bind http server", status);
    }

    status = uv_listen(reinterpret_cast<uv_stream_t*>(&server_), 256, &HttpServerRuntime::OnAccept);
    if (status != 0) {
      throw UvError("failed to listen for http connections", status);
    }

    uv_run(&loop_, UV_RUN_DEFAULT);
    uv_loop_close(&loop_);
  }

private:
  static HttpServerRuntime* OwnerOf(uv_handle_t* handle) {
    return static_cast<HttpServerRuntime*>(handle->data);
  }

  static HttpServerRuntime* OwnerOf(llhttp_t* parser) {
    auto* connection = static_cast<HttpConnection*>(parser->data);
    return static_cast<HttpServerRuntime*>(connection->owner);
  }

  static HttpConnection* ConnectionOf(llhttp_t* parser) {
    return static_cast<HttpConnection*>(parser->data);
  }

  static void AllocateBuffer(uv_handle_t*, std::size_t suggested_size, uv_buf_t* buffer) {
    const std::size_t size = suggested_size == 0 ? 4096 : suggested_size;
    buffer->base = new char[size];
    buffer->len = static_cast<unsigned int>(size);
  }

  static void OnAccept(uv_stream_t* server, int status) {
    auto* runtime = static_cast<HttpServerRuntime*>(server->data);
    runtime->Accept(status);
  }

  static void OnRead(uv_stream_t* stream, ssize_t nread, const uv_buf_t* buffer) {
    auto* connection = static_cast<HttpConnection*>(stream->data);
    auto* runtime = static_cast<HttpServerRuntime*>(connection->owner);
    runtime->Read(connection, nread, buffer);
  }

  static void OnWriteDone(uv_write_t* request, int status) {
    std::unique_ptr<WriteRequest> write_request(static_cast<WriteRequest*>(request->data));
    auto* runtime = static_cast<HttpServerRuntime*>(write_request->connection->owner);
    runtime->WriteCompleted(write_request->connection, status);
  }

  static void OnConnectionClosed(uv_handle_t* handle) {
    auto* connection = static_cast<HttpConnection*>(handle->data);
    auto* runtime = static_cast<HttpServerRuntime*>(connection->owner);
    runtime->connections_.erase(connection);
  }

  static void OnCompletionAsync(uv_async_t* handle) {
    auto* runtime = static_cast<HttpServerRuntime*>(handle->data);
    runtime->DrainCompletions();
  }

  static int OnMessageBegin(llhttp_t* parser) {
    auto* connection = ConnectionOf(parser);
    connection->request = HttpRequest{};
    connection->current_header_field.clear();
    connection->current_header_value.clear();
    connection->header_state = HttpConnection::HeaderState::None;
    connection->message_complete = false;
    return 0;
  }

  static int OnMethod(llhttp_t* parser, const char* data, size_t length) {
    ConnectionOf(parser)->request.method.append(data, length);
    return 0;
  }

  static int OnUrl(llhttp_t* parser, const char* data, size_t length) {
    ConnectionOf(parser)->request.target.append(data, length);
    return 0;
  }

  static int OnVersion(llhttp_t* parser, const char* data, size_t length) {
    ConnectionOf(parser)->request.version.append(data, length);
    return 0;
  }

  static int OnHeaderField(llhttp_t* parser, const char* data, size_t length) {
    auto* runtime = OwnerOf(parser);
    auto* connection = ConnectionOf(parser);
    if (connection->header_state == HttpConnection::HeaderState::Value) {
      runtime->CommitHeader(*connection);
      connection->current_header_field.clear();
      connection->current_header_value.clear();
    }
    connection->header_state = HttpConnection::HeaderState::Field;
    connection->current_header_field.append(data, length);
    return 0;
  }

  static int OnHeaderValue(llhttp_t* parser, const char* data, size_t length) {
    auto* connection = ConnectionOf(parser);
    connection->header_state = HttpConnection::HeaderState::Value;
    connection->current_header_value.append(data, length);
    return 0;
  }

  static int OnHeadersComplete(llhttp_t* parser) {
    auto* runtime = OwnerOf(parser);
    auto* connection = ConnectionOf(parser);
    runtime->CommitHeader(*connection);
    if (connection->request.version.empty()) {
      connection->request.version = std::to_string(parser->http_major) + "." + std::to_string(parser->http_minor);
    }
    return 0;
  }

  static int OnBody(llhttp_t* parser, const char* data, size_t length) {
    ConnectionOf(parser)->request.body.append(data, length);
    return 0;
  }

  static int OnMessageComplete(llhttp_t* parser) {
    auto* connection = ConnectionOf(parser);
    connection->request.keep_alive = llhttp_should_keep_alive(parser) != 0;
    connection->message_complete = true;
    llhttp_pause(parser);
    return HPE_PAUSED;
  }

  void Accept(int status) {
    if (status != 0) {
      return;
    }

    auto connection = std::make_shared<HttpConnection>(this);
    status = uv_tcp_init(&loop_, &connection->handle);
    if (status != 0) {
      return;
    }
    connection->handle.data = connection.get();
    llhttp_init(&connection->parser, HTTP_REQUEST, &settings_);
    connection->parser.data = connection.get();

    if (uv_accept(reinterpret_cast<uv_stream_t*>(&server_), reinterpret_cast<uv_stream_t*>(&connection->handle)) != 0) {
      uv_close(reinterpret_cast<uv_handle_t*>(&connection->handle), &HttpServerRuntime::OnConnectionClosed);
      return;
    }

    uv_tcp_nodelay(&connection->handle, 1);
    uv_tcp_keepalive(&connection->handle, 1, 60);
    connections_.emplace(connection.get(), connection);
    uv_read_start(reinterpret_cast<uv_stream_t*>(&connection->handle), &HttpServerRuntime::AllocateBuffer, &HttpServerRuntime::OnRead);
  }

  void Read(HttpConnection* raw_connection, ssize_t nread, const uv_buf_t* buffer) {
    std::unique_ptr<char[]> guard(buffer->base);
    auto it = connections_.find(raw_connection);
    if (it == connections_.end()) {
      return;
    }
    const auto connection = it->second;

    if (nread == 0) {
      return;
    }
    if (nread < 0) {
      CloseConnection(connection);
      return;
    }

    ProcessInput(connection, buffer->base, static_cast<std::size_t>(nread));
  }

  void CommitHeader(HttpConnection& connection) {
    if (connection.current_header_field.empty()) {
      return;
    }
    connection.request.headers[connection.current_header_field] = connection.current_header_value;
    connection.header_state = HttpConnection::HeaderState::None;
  }

  void ProcessInput(const std::shared_ptr<HttpConnection>& connection, const char* data, std::size_t length) {
    if (connection->closing || connection->request_in_flight) {
      return;
    }

    const llhttp_errno_t result = llhttp_execute(&connection->parser, data, length);
    if (result != HPE_OK && result != HPE_PAUSED) {
      HttpResponse response;
      response.status_code = 400;
      response.status_text = "Bad Request";
      response.body = llhttp_errno_name(result);
      QueueResponse(connection, std::move(response), false);
      return;
    }

    if (result == HPE_PAUSED && connection->message_complete) {
      const char* remainder = llhttp_get_error_pos(&connection->parser);
      const char* end = data + length;
      if (remainder != nullptr && remainder < end) {
        connection->buffered_input.append(remainder, static_cast<std::size_t>(end - remainder));
      }
      DispatchRequest(connection);
    }
  }

  void DispatchRequest(const std::shared_ptr<HttpConnection>& connection) {
    connection->request_in_flight = true;
    uv_read_stop(reinterpret_cast<uv_stream_t*>(&connection->handle));
    HttpRequest request = connection->request;

    executor_.Submit([this, connection, request = std::move(request)]() mutable {
      HttpResponse response;
      try {
        response = handler_(request);
      } catch (const std::exception&) {
        response.status_code = 500;
        response.status_text = "Internal Server Error";
        response.body = "internal server error";
        response.close_connection = true;
      }

      PostCompletion([this, connection, response = std::move(response), keep_alive = request.keep_alive]() mutable {
        FinishRequest(connection, std::move(response), keep_alive);
      });
    });
  }

  void PostCompletion(std::function<void()> completion) {
    {
      std::lock_guard<std::mutex> lock(completion_mutex_);
      completion_tasks_.push(std::move(completion));
    }
    uv_async_send(&completion_async_);
  }

  void DrainCompletions() {
    std::queue<std::function<void()>> tasks;
    {
      std::lock_guard<std::mutex> lock(completion_mutex_);
      tasks.swap(completion_tasks_);
    }

    while (!tasks.empty()) {
      tasks.front()();
      tasks.pop();
    }
  }

  void FinishRequest(const std::shared_ptr<HttpConnection>& connection, HttpResponse response, bool keep_alive) {
    if (connection->closing) {
      return;
    }

    connection->request_in_flight = false;
    llhttp_resume(&connection->parser);
    QueueResponse(connection, std::move(response), keep_alive);
  }

  void QueueResponse(const std::shared_ptr<HttpConnection>& connection, HttpResponse response, bool keep_alive) {
    if (connection->closing) {
      return;
    }

    const bool connection_keep_alive = keep_alive && !response.close_connection;
    if (!connection_keep_alive) {
      connection->close_after_write = true;
    }

    connection->write_queue.push_back(SerializeHttpResponse(response, connection_keep_alive));
    StartWrite(connection);
  }

  void StartWrite(const std::shared_ptr<HttpConnection>& connection) {
    if (connection->closing || connection->write_in_progress || connection->write_queue.empty()) {
      return;
    }

    connection->write_in_progress = true;
    auto payload = std::make_shared<std::string>(std::move(connection->write_queue.front()));
    connection->write_queue.pop_front();

    auto* write_request = new WriteRequest{};
    write_request->request.data = write_request;
    write_request->connection = connection;
    write_request->payload = std::move(payload);

    uv_buf_t buffer = uv_buf_init(write_request->payload->data(), static_cast<unsigned int>(write_request->payload->size()));
    const int status = uv_write(
        &write_request->request,
        reinterpret_cast<uv_stream_t*>(&connection->handle),
        &buffer,
        1,
        &HttpServerRuntime::OnWriteDone);
    if (status != 0) {
      delete write_request;
      CloseConnection(connection);
    }
  }

  void WriteCompleted(const std::shared_ptr<HttpConnection>& connection, int status) {
    connection->write_in_progress = false;
    if (status != 0) {
      CloseConnection(connection);
      return;
    }

    if (!connection->write_queue.empty()) {
      StartWrite(connection);
      return;
    }

    if (connection->close_after_write) {
      CloseConnection(connection);
      return;
    }

    if (!connection->buffered_input.empty()) {
      const std::string pending = std::move(connection->buffered_input);
      connection->buffered_input.clear();
      ProcessInput(connection, pending.data(), pending.size());
      return;
    }

    uv_read_start(reinterpret_cast<uv_stream_t*>(&connection->handle), &HttpServerRuntime::AllocateBuffer, &HttpServerRuntime::OnRead);
  }

  void CloseConnection(const std::shared_ptr<HttpConnection>& connection) {
    if (connection->closing) {
      return;
    }
    connection->closing = true;
    uv_read_stop(reinterpret_cast<uv_stream_t*>(&connection->handle));
    uv_close(reinterpret_cast<uv_handle_t*>(&connection->handle), &HttpServerRuntime::OnConnectionClosed);
  }

  int port_ = 0;
  LibuvHttpServer::RequestHandler& handler_;
  TaskExecutor& executor_;
  uv_loop_t loop_{};
  uv_tcp_t server_{};
  uv_async_t completion_async_{};
  llhttp_settings_t settings_{};
  std::mutex completion_mutex_;
  std::queue<std::function<void()>> completion_tasks_;
  std::unordered_map<HttpConnection*, std::shared_ptr<HttpConnection>> connections_;
};

}  // namespace

LibuvHttpServer::LibuvHttpServer(int port, RequestHandler handler, std::size_t worker_count)
    : port_(port), handler_(std::move(handler)), executor_(worker_count) {}

void LibuvHttpServer::Run() {
  HttpServerRuntime runtime(port_, handler_, executor_);
  runtime.Run();
}

std::string SerializeHttpResponse(const HttpResponse& response, bool keep_alive) {
  std::ostringstream out;
  out << "HTTP/1.1 " << response.status_code << ' ' << response.status_text << "\r\n";
  out << "Content-Type: " << response.content_type << "\r\n";
  for (const auto& [name, value] : response.headers) {
    out << name << ": " << value << "\r\n";
  }
  out << "Content-Length: " << response.body.size() << "\r\n";
  out << "Connection: " << (keep_alive ? "keep-alive" : "close") << "\r\n\r\n";
  out << response.body;
  return out.str();
}
