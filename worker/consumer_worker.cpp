#include "worker/consumer_worker.h"

#include <algorithm>
#include <chrono>

#include "cdc_log_parser.h"
#include "lib/ob_errno.h"
#include "logging_utils.h"
#include "logservice/ipalf/ipalf_log_entry.h"
#include "logservice/ipalf/ipalf_log_group_entry.h"
#include "logservice/ob_log_base_type.h"
#include "runtime/logger.h"

using namespace oceanbase;
using namespace oceanbase::ipalf;
using namespace oceanbase::logservice;

namespace {

int64_t now_millis() {
  return std::chrono::duration_cast<std::chrono::milliseconds>(
      std::chrono::system_clock::now().time_since_epoch()).count();
}

void consume_paxos_heartbeat(const IGroupEntry &group_entry, const char *group_data_buf,
                             int64_t group_data_len, const palf::LSN &current_lsn) {
  static int64_t last_paxos_time = 0;
  int64_t now_ms = now_millis();
  if (now_ms - last_paxos_time > 60000) {
    CDC_INFO() << get_time_prefix() << "[Consumer] [Paxos Alive] LSN: " << current_lsn.val_
               << ", SCN_GTS: " << group_entry.get_scn().get_val_for_gts();
    last_paxos_time = now_ms;
  }

  if (group_data_len > 0 && group_data_buf != nullptr) {
    int64_t entry_pos = 0;
    while (entry_pos < group_data_len) {
      ILogEntry log_entry(true);
      int entry_ret = log_entry.deserialize(current_lsn, group_data_buf, group_data_len, entry_pos);
      if (entry_ret != OB_SUCCESS) break;
    }
  }
}

void consume_visible_entries(const LogTask &task, const IGroupEntry &group_entry,
                             const char *group_data_buf, int64_t group_data_len,
                             const palf::LSN &current_lsn) {
  bool printed_group_header = false;
  if (group_data_len <= 0 || group_data_buf == nullptr) {
    return;
  }

  int64_t entry_pos = 0;
  int entry_idx = 0;
  while (entry_pos < group_data_len) {
    ILogEntry log_entry(true);
    int entry_ret = log_entry.deserialize(current_lsn, group_data_buf, group_data_len, entry_pos);
    if (entry_ret != OB_SUCCESS) {
      CDC_ERROR() << get_time_prefix() << "    [Parser] Failed to deserialize ILogEntry, err=" << entry_ret;
      break;
    }

    ObLogBaseType log_type = INVALID_LOG_BASE_TYPE;
    if (parse_log_base_type(log_entry, log_type) && is_visible_cdc_log_type(log_type)) {
      if (log_type == TRANS_SERVICE_LOG_BASE_TYPE) {
        parse_tx_redo_logs(log_entry, current_lsn);
      } else if (!printed_group_header) {
        CDC_INFO() << get_time_prefix() << "[Consumer] [" << cdc_log_kind(log_type)
                   << "] LSN: " << current_lsn.val_
                   << ", SCN_GTS: " << group_entry.get_scn().get_val_for_gts()
                   << ", log_entries_count: " << task.log_num
                   << ", bytes: " << task.log_data.size()
                   << ", GroupDataLen: " << group_entry.get_data_len();
        printed_group_header = true;

        CDC_INFO() << get_time_prefix() << "    -> [LogEntry " << entry_idx << "] type="
                   << log_base_type_name(log_type)
                   << ", DataLen: " << log_entry.get_data_len()
                   << ", SCN_GTS: " << log_entry.get_scn().get_val_for_gts();
      }
    }
    ++entry_idx;
  }
}

void consume_log_data(const LogTask &task) {
  const char *buf = task.log_data.data();
  const int64_t len = task.log_data.size();
  int64_t pos = 0;
  palf::LSN current_lsn = task.lsn;

  for (int64_t idx = 0; idx < task.log_num; ++idx) {
    if (pos >= len) {
      CDC_ERROR() << get_time_prefix() << "  [Parser] Error: pos exceeds log data len!";
      break;
    }

    IGroupEntry group_entry(true /* enable_logservice */);
    int parse_ret = group_entry.deserialize(current_lsn, buf, len, pos);
    if (parse_ret != OB_SUCCESS) {
      CDC_ERROR() << get_time_prefix() << "  [Parser] Failed to deserialize IGroupEntry, err=" << parse_ret
                  << ", pos=" << pos << "/" << len;
      break;
    }

    const char *group_data_buf = group_entry.get_data_buf();
    int64_t group_data_len = group_entry.get_data_len();

    if (group_data_len == 66) {
      consume_paxos_heartbeat(group_entry, group_data_buf, group_data_len, current_lsn);
    } else {
      consume_visible_entries(task, group_entry, group_data_buf, group_data_len, current_lsn);
    }

    current_lsn.val_ += group_entry.get_group_entry_size(current_lsn);
  }
}

void consume_keepalive(const LogTask &task) {
  static uint64_t last_printed_lsn = 0;
  static int64_t last_printed_time = 0;
  static uint64_t suppressed_keepalive_count = 0;
  int64_t now_ms = now_millis();

  ++suppressed_keepalive_count;
  if (last_printed_time == 0 || now_ms - last_printed_time > 60000) {
    CDC_INFO() << get_time_prefix() << "[Consumer] [Keep-Alive] Watermark advance. LSN: " << task.lsn.val_
               << ", suppressed=" << (suppressed_keepalive_count - 1)
               << ", previous_lsn=" << last_printed_lsn;
    last_printed_lsn = task.lsn.val_;
    last_printed_time = now_ms;
    suppressed_keepalive_count = 0;
  }
}

} // namespace

void consumer_worker_thread(
    SafeQueue<LogTask> &log_queue,
    const std::atomic<bool> &running,
    const CheckpointStore *checkpoint_store) {
  CDC_INFO() << get_time_prefix() << "[Consumer] Consumer worker thread started.";
  LogTask task;

  while (running.load() || !log_queue.empty()) {
    if (log_queue.pop(task, std::chrono::milliseconds(100), running)) {
      if (task.log_num > 0 && !task.log_data.empty()) {
        consume_log_data(task);
      } else {
        consume_keepalive(task);
      }

      if (checkpoint_store != nullptr) {
        checkpoint_store->save(task.lsn);
      }
    }
  }

  CDC_INFO() << get_time_prefix() << "[Consumer] Consumer worker thread safely terminated.";
}
