#pragma once

#include <atomic>
#include <condition_variable>
#include <cstdint>
#include <mutex>

class PullState {
public:
  void reset_round();
  void mark_success(uint64_t next_lsn);
  void mark_failed();
  void wait_round_done(const std::atomic<bool> &running);
  void wakeup();

  bool success() const { return success_.load(); }
  uint64_t next_lsn() const { return next_lsn_.load(); }
  uint64_t last_lsn() const { return last_lsn_.load(); }
  void set_last_lsn(uint64_t last_lsn) { last_lsn_ = last_lsn; }

private:
  void mark_done();

  std::mutex mutex_;
  std::condition_variable cond_;
  std::atomic<bool> done_{false};
  std::atomic<bool> success_{false};
  std::atomic<uint64_t> next_lsn_{0};
  std::atomic<uint64_t> last_lsn_{0};
};
