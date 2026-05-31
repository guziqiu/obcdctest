#pragma once

#include <cstdint>
#include <string>

struct DmlEvent {
  std::string op;
  uint64_t tablet_id = 0;
  uint64_t table_id = 0;
  int64_t seq = 0;
  int64_t branch = 0;
  int64_t row_bytes = 0;
  int64_t submit_ts = 0;
  uint64_t lsn = 0;
};
