#pragma once

#include <string>

#include "logservice/ipalf/ipalf_log_entry.h"
#include "logservice/ob_log_base_type.h"
#include "logservice/palf/lsn.h"

bool parse_log_base_type(
    const oceanbase::ipalf::ILogEntry &log_entry,
    oceanbase::logservice::ObLogBaseType &log_type);

bool is_visible_cdc_log_type(const oceanbase::logservice::ObLogBaseType log_type);

const char *cdc_log_kind(const oceanbase::logservice::ObLogBaseType log_type);

std::string log_base_type_name(const oceanbase::logservice::ObLogBaseType log_type);

void parse_tx_redo_logs(
    const oceanbase::ipalf::ILogEntry &log_entry,
    const oceanbase::palf::LSN &lsn);
