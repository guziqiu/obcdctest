#pragma once

#include <chrono>
#include <string>

#include <time.h>

inline std::string get_time_prefix() {
  auto now = std::chrono::system_clock::now();
  auto now_time_t = std::chrono::system_clock::to_time_t(now);
  struct tm tm_buf;
  localtime_r(&now_time_t, &tm_buf);
  char buf[32];
  strftime(buf, sizeof(buf), "[%Y-%m-%d %H:%M:%S] ", &tm_buf);
  return std::string(buf);
}
