#pragma once

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstddef>
#include <mutex>
#include <queue>
#include <utility>

template <typename T>
class SafeQueue {
public:
  explicit SafeQueue(size_t max_size) : max_size_(max_size), closed_(false) {}

  bool push(T &&item, const std::atomic<bool> &running) {
    std::unique_lock<std::mutex> lock(mutex_);
    cond_push_.wait(lock, [this, &running]() {
      return queue_.size() < max_size_ || closed_ || !running.load();
    });
    if (closed_ || !running.load()) return false;
    queue_.push(std::move(item));
    cond_pop_.notify_one();
    return true;
  }

  bool pop(T &item, std::chrono::milliseconds timeout, const std::atomic<bool> &running) {
    std::unique_lock<std::mutex> lock(mutex_);
    if (!cond_pop_.wait_for(lock, timeout, [this, &running]() {
          return !queue_.empty() || closed_ || !running.load();
        })) {
      return false;
    }
    if (queue_.empty()) return false;
    item = std::move(queue_.front());
    queue_.pop();
    cond_push_.notify_one();
    return true;
  }

  void close() {
    std::unique_lock<std::mutex> lock(mutex_);
    closed_ = true;
    cond_pop_.notify_all();
    cond_push_.notify_all();
  }

  bool empty() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return queue_.empty();
  }

  size_t size() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return queue_.size();
  }

private:
  mutable std::mutex mutex_;
  std::queue<T> queue_;
  size_t max_size_;
  std::condition_variable cond_push_;
  std::condition_variable cond_pop_;
  bool closed_;
};
