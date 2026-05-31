#pragma once

#include <atomic>

#include "runtime/checkpoint_store.h"
#include "runtime/safe_queue.h"
#include "worker/log_task.h"

void consumer_worker_thread(
    SafeQueue<LogTask> &log_queue,
    const std::atomic<bool> &running,
    const CheckpointStore *checkpoint_store);
