#pragma once

#include <string>

int CreateListenSocket(int port);
bool ReadFrame(int fd, std::string& payload);
bool WriteFrame(int fd, const std::string& payload);
bool RoundTripTcp(const std::string& host, int port, const std::string& request, std::string& response);
