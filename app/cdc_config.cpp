#include "app/cdc_config.h"

#include <cstdlib>
#include <cstring>
#include <string>

namespace {

bool split_server(const std::string &value, std::string &host, int32_t &port) {
  const size_t pos = value.rfind(':');
  if (pos == std::string::npos || pos == 0 || pos + 1 >= value.size()) {
    return false;
  }
  host = value.substr(0, pos);
  port = static_cast<int32_t>(std::strtol(value.substr(pos + 1).c_str(), nullptr, 10));
  return port > 0;
}

} // namespace

CdcConfig CdcConfig::parse(int argc, char **argv) {
  CdcConfig config;
  for (int i = 1; i < argc; ++i) {
    const std::string arg(argv[i]);
    auto next_value = [&]() -> const char * {
      if (i + 1 >= argc) return nullptr;
      return argv[++i];
    };

    if (arg == "--tenant-id") {
      if (const char *value = next_value()) {
        config.tenant_id = static_cast<uint64_t>(std::strtoull(value, nullptr, 10));
      }
    } else if (arg == "--server") {
      if (const char *value = next_value()) {
        split_server(value, config.server_host, config.server_port);
      }
    } else if (arg == "--ls-id") {
      if (const char *value = next_value()) {
        config.ls_id = std::strtoll(value, nullptr, 10);
      }
    } else if (arg == "--checkpoint-file") {
      if (const char *value = next_value()) {
        config.checkpoint_file = value;
      }
    } else if (arg == "--log-file") {
      if (const char *value = next_value()) {
        config.log_file = value;
      }
    } else if (arg == "--no-console-log") {
      config.log_to_console = false;
    }
  }
  if (config.checkpoint_file.empty()) {
    config.checkpoint_file = "ob_cdc_checkpoint_t" + std::to_string(config.tenant_id)
                           + "_ls" + std::to_string(config.ls_id) + ".txt";
  }
  return config;
}
