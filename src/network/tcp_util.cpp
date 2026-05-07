#include "network/tcp_util.h"

#include <arpa/inet.h>
#include <fcntl.h>
#include <netdb.h>
#include <netinet/in.h>
#include <poll.h>
#include <sys/socket.h>
#include <sys/time.h>
#include <unistd.h>

#include <cerrno>
#include <cstdint>
#include <cstring>
#include <stdexcept>

namespace {

bool WaitForSocketEvent(int fd, short events, int timeout_ms) {
  pollfd pfd{};
  pfd.fd = fd;
  pfd.events = events;

  while (true) {
    const int rc = ::poll(&pfd, 1, timeout_ms);
    if (rc > 0) {
      return true;
    }
    if (rc == 0) {
      return false;
    }
    if (errno != EINTR) {
      return false;
    }
  }
}

bool SetSocketTimeouts(int fd, int timeout_ms) {
  timeval timeout{};
  timeout.tv_sec = timeout_ms / 1000;
  timeout.tv_usec = (timeout_ms % 1000) * 1000;
  return ::setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &timeout, sizeof(timeout)) == 0 &&
      ::setsockopt(fd, SOL_SOCKET, SO_SNDTIMEO, &timeout, sizeof(timeout)) == 0;
}

bool ConnectWithTimeout(int fd, const sockaddr* address, socklen_t address_length, int timeout_ms) {
  const int original_flags = ::fcntl(fd, F_GETFL, 0);
  if (original_flags < 0) {
    return false;
  }
  if (::fcntl(fd, F_SETFL, original_flags | O_NONBLOCK) != 0) {
    return false;
  }

  const int rc = ::connect(fd, address, address_length);
  if (rc == 0) {
    return ::fcntl(fd, F_SETFL, original_flags) == 0 && SetSocketTimeouts(fd, timeout_ms);
  }
  if (errno != EINPROGRESS) {
    ::fcntl(fd, F_SETFL, original_flags);
    return false;
  }

  if (!WaitForSocketEvent(fd, POLLOUT, timeout_ms)) {
    ::fcntl(fd, F_SETFL, original_flags);
    return false;
  }

  int socket_error = 0;
  socklen_t error_length = sizeof(socket_error);
  if (::getsockopt(fd, SOL_SOCKET, SO_ERROR, &socket_error, &error_length) != 0 || socket_error != 0) {
    ::fcntl(fd, F_SETFL, original_flags);
    return false;
  }

  return ::fcntl(fd, F_SETFL, original_flags) == 0 && SetSocketTimeouts(fd, timeout_ms);
}

bool ReadAll(int fd, void* buffer, std::size_t length) {
  auto* bytes = static_cast<char*>(buffer);
  std::size_t offset = 0;
  while (offset < length) {
    const ssize_t read_size = ::recv(fd, bytes + offset, length - offset, 0);
    if (read_size < 0 && errno == EINTR) {
      continue;
    }
    if (read_size <= 0) {
      return false;
    }
    offset += static_cast<std::size_t>(read_size);
  }
  return true;
}

bool WriteAll(int fd, const void* buffer, std::size_t length) {
  const auto* bytes = static_cast<const char*>(buffer);
  std::size_t offset = 0;
  while (offset < length) {
    const ssize_t wrote = ::send(fd, bytes + offset, length - offset, 0);
    if (wrote < 0 && errno == EINTR) {
      continue;
    }
    if (wrote <= 0) {
      return false;
    }
    offset += static_cast<std::size_t>(wrote);
  }
  return true;
}

}  // namespace

int CreateListenSocket(int port) {
  const int server_fd = ::socket(AF_INET, SOCK_STREAM, 0);
  if (server_fd < 0) {
    throw std::runtime_error("failed to create socket");
  }

  int opt = 1;
  ::setsockopt(server_fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));

  sockaddr_in address{};
  address.sin_family = AF_INET;
  address.sin_addr.s_addr = htonl(INADDR_ANY);
  address.sin_port = htons(port);

  if (::bind(server_fd, reinterpret_cast<sockaddr*>(&address), sizeof(address)) < 0) {
    const std::string error = std::strerror(errno);
    ::close(server_fd);
    throw std::runtime_error("failed to bind port: " + error);
  }

  if (::listen(server_fd, 32) < 0) {
    const std::string error = std::strerror(errno);
    ::close(server_fd);
    throw std::runtime_error("failed to listen: " + error);
  }

  return server_fd;
}

bool ReadFrame(int fd, std::string& payload) {
  std::uint32_t length_network = 0;
  if (!ReadAll(fd, &length_network, sizeof(length_network))) {
    return false;
  }
  const std::uint32_t length = ntohl(length_network);
  payload.assign(length, '\0');
  return ReadAll(fd, payload.data(), length);
}

bool WriteFrame(int fd, const std::string& payload) {
  const std::uint32_t length_network = htonl(static_cast<std::uint32_t>(payload.size()));
  return WriteAll(fd, &length_network, sizeof(length_network)) &&
         WriteAll(fd, payload.data(), payload.size());
}

bool RoundTripTcp(
    const std::string& host,
    int port,
    const std::string& request,
    std::string& response,
    int timeout_ms) {
  addrinfo hints{};
  hints.ai_family = AF_INET;
  hints.ai_socktype = SOCK_STREAM;

  addrinfo* result = nullptr;
  if (::getaddrinfo(host.c_str(), std::to_string(port).c_str(), &hints, &result) != 0) {
    return false;
  }

  int fd = -1;
  bool connected = false;
  for (addrinfo* addr = result; addr != nullptr; addr = addr->ai_next) {
    fd = ::socket(addr->ai_family, addr->ai_socktype, addr->ai_protocol);
    if (fd < 0) {
      continue;
    }
    if (ConnectWithTimeout(fd, addr->ai_addr, addr->ai_addrlen, timeout_ms)) {
      connected = true;
      break;
    }
    ::close(fd);
    fd = -1;
  }
  ::freeaddrinfo(result);

  if (!connected || fd < 0) {
    return false;
  }

  const bool wrote = WriteFrame(fd, request);
  const bool read = wrote && ReadFrame(fd, response);
  ::close(fd);
  return read;
}
