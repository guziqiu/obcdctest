#include "rpc/ls_pull_manager.h"

#include <unistd.h>

#include <cstdint>

#include "lib/ob_errno.h"
#include "logging_utils.h"
#include "runtime/logger.h"
#include "src/logservice/cdcservice/ob_cdc_req.h"

using namespace oceanbase;
using namespace oceanbase::libobcdc;

LSPullManager::LSPullManager(ObLogRpc &rpc,
                             uint64_t tenant_id,
                             const common::ObAddr &svr,
                             share::ObLSID ls_id,
                             const std::atomic<bool> &running,
                             PullState &state,
                             ProductionLogCB &pull_cb)
    : rpc_(rpc),
      tenant_id_(tenant_id),
      svr_(svr),
      ls_id_(ls_id),
      running_(running),
      state_(state),
      pull_cb_(pull_cb) {}

void LSPullManager::trigger_fetch(const palf::LSN &start_lsn) {
  if (!running_.load()) return;

  obrpc::ObCdcLSFetchLogReq req;
  req.set_ls_id(ls_id_);
  req.set_start_lsn(start_lsn);

  // ObCdcLSFetchLogReq requires a positive upper limit timestamp.
  req.set_upper_limit_ts(INT64_MAX);
  req.set_client_pid(static_cast<uint64_t>(getpid()));
  req.set_progress(0);

  int64_t timeout_us = 5000000;
  int ret = rpc_.async_stream_fetch_log(tenant_id_, svr_, req, pull_cb_, timeout_us);
  if (ret != OB_SUCCESS) {
    CDC_ERROR() << get_time_prefix() << "[Pull Manager] async_stream_fetch_log trigger fail, err: " << ret;
    state_.mark_failed();
  }
}
