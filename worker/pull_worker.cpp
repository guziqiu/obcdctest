#include "worker/pull_worker.h"

#include <chrono>
#include <thread>

#include "logging_utils.h"
#include "runtime/logger.h"

void pull_worker_thread(
    LSPullManager &manager,
    PullState &state,
    const std::atomic<bool> &running,
    oceanbase::palf::LSN start_lsn) {
  CDC_INFO() << get_time_prefix() << "[Puller] Dedicated RPC pull thread started.";
  oceanbase::palf::LSN current_lsn = start_lsn;

  while (running.load()) {
    state.reset_round();

    static int64_t last_pull_time = 0;
    int64_t now_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::system_clock::now().time_since_epoch()).count();

    if (now_ms - last_pull_time > 60000) {
      CDC_INFO() << get_time_prefix() << "[Puller] Calling RPC async_stream_fetch_log for LSN: " << current_lsn.val_;
      last_pull_time = now_ms;
    }
    manager.trigger_fetch(current_lsn);

    state.wait_round_done(running);

    if (!running.load()) break;

    if (state.success()) {
      current_lsn.val_ = state.next_lsn();
      state.set_last_lsn(current_lsn.val_);
    } else {
      std::this_thread::sleep_for(std::chrono::milliseconds(200));
    }
  }

  CDC_INFO() << get_time_prefix() << "[Puller] Dedicated RPC pull thread safely terminated.";
}
