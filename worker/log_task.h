#pragma once

#include <cstdint>
#include <string>

#include "logservice/palf/lsn.h"
#include "share/ob_ls_id.h"

struct LogTask {
  oceanbase::palf::LSN lsn;
  std::string log_data;
  int64_t log_num;
  int64_t progress;
  oceanbase::share::ObLSID ls_id;
};
