#pragma once

#include <atomic>

#include "rpc/ls_pull_manager.h"
#include "rpc/pull_state.h"
#include "logservice/palf/lsn.h"

void pull_worker_thread(
    LSPullManager &manager,
    PullState &state,
    const std::atomic<bool> &running,
    oceanbase::palf::LSN start_lsn);
