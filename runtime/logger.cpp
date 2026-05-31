#include "runtime/logger.h"

#include <iostream>

Logger &Logger::instance() {
  static Logger logger;
  return logger;
}

bool Logger::init(const std::string &file_path, bool console_enabled) {
  std::lock_guard<std::mutex> lock(mutex_);
  console_enabled_ = console_enabled;
  if (!file_path.empty()) {
    file_.open(file_path, std::ios::app);
    return file_.is_open();
  }
  return true;
}

void Logger::log(bool error, const std::string &message) {
  std::lock_guard<std::mutex> lock(mutex_);
  if (console_enabled_) {
    std::ostream &out = error ? std::cerr : std::cout;
    out << message << std::endl;
  }
  if (file_.is_open()) {
    file_ << message << std::endl;
    file_.flush();
  }
}

LogLine::LogLine(bool error) : error_(error) {}

LogLine::~LogLine() {
  Logger::instance().log(error_, stream_.str());
}
