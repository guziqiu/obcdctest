#pragma once

#include <cstdint>
#include <string>

struct CdcConfig {
  uint64_t tenant_id = 1006;
  std::string server_host = "192.168.31.205";
  int32_t server_port = 2882;
  int64_t ls_id = 1001;
  std::string checkpoint_file;
  std::string log_file;
  bool log_to_console = true;

  static CdcConfig parse(int argc, char **argv);
};
