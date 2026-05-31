#include "rpc/pull_state.h"

void PullState::reset_round() {
  done_ = false;
  success_ = false;
}

void PullState::mark_success(uint64_t next_lsn) {
  next_lsn_ = next_lsn;
  success_ = true;
  mark_done();
}

void PullState::mark_failed() {
  success_ = false;
  mark_done();
}

void PullState::wait_round_done(const std::atomic<bool> &running) {
  std::unique_lock<std::mutex> lock(mutex_);
  cond_.wait(lock, [this, &running]() {
    return done_.load() || !running.load();
  });
}

void PullState::wakeup() {
  mark_done();
}

void PullState::mark_done() {
  {
    std::lock_guard<std::mutex> lock(mutex_);
    done_ = true;
  }
  cond_.notify_all();
}
