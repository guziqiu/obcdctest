#pragma once

#include <atomic>
#include <cstdint>

#include "rpc/production_log_cb.h"
#include "rpc/pull_state.h"
#include "src/logservice/libobcdc/src/ob_log_rpc.h"
#include "share/ob_ls_id.h"

class LSPullManager {
public:
  LSPullManager(oceanbase::libobcdc::ObLogRpc &rpc,
                uint64_t tenant_id,
                const oceanbase::common::ObAddr &svr,
                oceanbase::share::ObLSID ls_id,
                const std::atomic<bool> &running,
                PullState &state,
                ProductionLogCB &pull_cb);

  void trigger_fetch(const oceanbase::palf::LSN &start_lsn);

private:
  oceanbase::libobcdc::ObLogRpc &rpc_;
  uint64_t tenant_id_;
  oceanbase::common::ObAddr svr_;
  oceanbase::share::ObLSID ls_id_;
  const std::atomic<bool> &running_;
  PullState &state_;
  ProductionLogCB &pull_cb_;
};
