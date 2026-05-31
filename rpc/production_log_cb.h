#pragma once

#include <atomic>

#include "rpc/pull_state.h"
#include "runtime/safe_queue.h"
#include "src/logservice/cdcservice/ob_cdc_req.h"
#include "src/logservice/libobcdc/src/ob_log_rpc.h"
#include "worker/log_task.h"

class ProductionLogCB : public oceanbase::obrpc::ObCdcProxy::AsyncCB<oceanbase::obrpc::OB_LS_FETCH_LOG2> {
  typedef oceanbase::obrpc::ObCdcProxy::AsyncCB<oceanbase::obrpc::OB_LS_FETCH_LOG2> RpcCBBase;

public:
  ProductionLogCB(PullState &state,
                  SafeQueue<LogTask> &log_queue,
                  const std::atomic<bool> &running,
                  oceanbase::share::ObLSID ls_id);

  void set_args(const oceanbase::obrpc::ObCdcLSFetchLogReq &args) override;
  oceanbase::rpc::frame::ObReqTransport::AsyncCB *clone(
      const oceanbase::rpc::frame::SPAlloc &alloc) const override;
  int process() override;
  void on_timeout() override;
  void on_invalid() override;

private:
  PullState *state_;
  SafeQueue<LogTask> *log_queue_;
  const std::atomic<bool> *running_;
  oceanbase::share::ObLSID ls_id_;
};
