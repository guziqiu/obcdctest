#pragma once

#include <fstream>
#include <mutex>
#include <sstream>
#include <string>

class Logger {
public:
  static Logger &instance();

  bool init(const std::string &file_path, bool console_enabled);
  void log(bool error, const std::string &message);

private:
  Logger() = default;

  std::mutex mutex_;
  std::ofstream file_;
  bool console_enabled_ = true;
};

class LogLine {
public:
  explicit LogLine(bool error);
  ~LogLine();

  template <typename T>
  LogLine &operator<<(const T &value) {
    stream_ << value;
    return *this;
  }

private:
  bool error_;
  std::ostringstream stream_;
};

#define CDC_INFO() LogLine(false)
#define CDC_ERROR() LogLine(true)
