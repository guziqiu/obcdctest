#include "rpc/production_log_cb.h"

#include <new>
#include <utility>

#include "lib/ob_errno.h"
#include "logging_utils.h"
#include "runtime/logger.h"

using namespace oceanbase;

ProductionLogCB::ProductionLogCB(PullState &state,
                                 SafeQueue<LogTask> &log_queue,
                                 const std::atomic<bool> &running,
                                 share::ObLSID ls_id)
    : state_(&state), log_queue_(&log_queue), running_(&running), ls_id_(ls_id) {}

void ProductionLogCB::set_args(const obrpc::ObCdcLSFetchLogReq &args) {
  (void)args;
}

rpc::frame::ObReqTransport::AsyncCB *ProductionLogCB::clone(const rpc::frame::SPAlloc &alloc) const {
  void *buf = nullptr;
  ProductionLogCB *cb = nullptr;
  if (OB_ISNULL(buf = alloc(sizeof(ProductionLogCB)))) {
    CDC_ERROR() << get_time_prefix() << "[RPC CB] clone failed due to memory allocation failure.";
  } else {
    cb = new(buf) ProductionLogCB(*state_, *log_queue_, *running_, ls_id_);
  }
  return cb;
}

int ProductionLogCB::process() {
  obrpc::ObCdcLSFetchLogResp &result = RpcCBBase::result_;
  ObRpcResultCode &rcode = RpcCBBase::rcode_;

  if (rcode.rcode_ == OB_SUCCESS && result.get_err() == OB_SUCCESS) {
    uint64_t current_lsn_val = state_->last_lsn();

    LogTask task;
    task.ls_id = ls_id_;
    task.lsn.val_ = current_lsn_val;
    task.log_num = result.get_log_num();
    task.progress = result.get_progress();
    if (task.log_num > 0 && result.get_log_entry_buf() != nullptr) {
      task.log_data.assign(result.get_log_entry_buf(), result.get_pos());
    }
    log_queue_->push(std::move(task), *running_);

    palf::LSN next_lsn = result.get_next_req_lsn();
    state_->mark_success(next_lsn.is_valid() ? next_lsn.val_ : current_lsn_val);
  } else {
    CDC_ERROR() << get_time_prefix() << "[RPC CB] Error in fetching logs: rcode=" << rcode.rcode_
                << ", biz_err=" << result.get_err();
    state_->mark_failed();
  }

  result.reset();
  return OB_SUCCESS;
}

void ProductionLogCB::on_timeout() {
  CDC_ERROR() << get_time_prefix() << "[RPC CB] Request timeout.";
  state_->mark_failed();
}

void ProductionLogCB::on_invalid() {
  CDC_ERROR() << get_time_prefix() << "[RPC CB] Invalid response packet.";
  state_->mark_failed();
}
