#include "network/tcp_util.h"

#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

#include <cassert>
#include <chrono>
#include <exception>
#include <string>
#include <thread>

int main() {
  int server_fd = -1;
  try {
    server_fd = CreateListenSocket(0);
  } catch (const std::runtime_error& ex) {
    if (std::string(ex.what()).find("Operation not permitted") != std::string::npos) {
      return 0;
    }
    throw;
  }
  assert(server_fd >= 0);

  sockaddr_in address{};
  socklen_t address_length = sizeof(address);
  const int rc = ::getsockname(server_fd, reinterpret_cast<sockaddr*>(&address), &address_length);
  assert(rc == 0);
  const int port = ntohs(address.sin_port);

  std::thread stalled_server([server_fd]() {
    const int client_fd = ::accept(server_fd, nullptr, nullptr);
    assert(client_fd >= 0);
    std::this_thread::sleep_for(std::chrono::milliseconds(250));
    ::close(client_fd);
    ::close(server_fd);
  });

  const auto start = std::chrono::steady_clock::now();
  std::string response;
  const bool success = RoundTripTcp("127.0.0.1", port, "ping", response, 100);
  const auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(
      std::chrono::steady_clock::now() - start);

  assert(!success);
  assert(elapsed.count() < 1000);

  stalled_server.join();
  return 0;
}
