#include "cdc_log_parser.h"

#include "lib/ob_errno.h"
#include "logging_utils.h"
#include "logservice/ob_log_base_header.h"
#include "parser/dml_event.h"
#include "runtime/logger.h"
#include "storage/memtable/ob_memtable_mutator.h"
#include "storage/tx/ob_tx_log.h"

using namespace oceanbase;
using namespace oceanbase::logservice;

bool parse_log_base_type(const ipalf::ILogEntry &log_entry, ObLogBaseType &log_type) {
  const char *data_buf = log_entry.get_data_buf();
  const int64_t data_len = log_entry.get_data_len();
  if (data_buf == nullptr || data_len <= 0) {
    return false;
  }

  ObLogBaseHeader base_header;
  int64_t pos = 0;
  if (base_header.deserialize(data_buf, data_len, pos) != OB_SUCCESS || !base_header.is_valid()) {
    return false;
  }

  log_type = base_header.get_log_type();
  return true;
}

bool is_visible_cdc_log_type(const ObLogBaseType log_type) {
  return log_type == TRANS_SERVICE_LOG_BASE_TYPE
      || log_type == DDL_LOG_BASE_TYPE
      || log_type == SYS_DDL_SCHEDULER_LOG_BASE_TYPE
      || log_type == DDL_SERVICE_LAUNCHER_LOG_BASE_TYPE;
}

const char *cdc_log_kind(const ObLogBaseType log_type) {
  return log_type == TRANS_SERVICE_LOG_BASE_TYPE ? "DML" : "DDL";
}

std::string log_base_type_name(const ObLogBaseType log_type) {
  char type_name[OB_LOG_BASE_TYPE_STR_MAX_LEN] = {0};
  if (log_base_type_to_string(log_type, type_name, sizeof(type_name)) == OB_SUCCESS) {
    return std::string(type_name);
  }
  return "UNKNOWN";
}

namespace {

const char *tx_log_type_name(const transaction::ObTxLogType log_type) {
  switch (log_type) {
    case transaction::ObTxLogType::TX_REDO_LOG: return "TX_REDO_LOG";
    case transaction::ObTxLogType::TX_ROLLBACK_TO_LOG: return "TX_ROLLBACK_TO_LOG";
    case transaction::ObTxLogType::TX_MULTI_DATA_SOURCE_LOG: return "TX_MULTI_DATA_SOURCE_LOG";
    case transaction::ObTxLogType::TX_DIRECT_LOAD_INC_LOG: return "TX_DIRECT_LOAD_INC_LOG";
    case transaction::ObTxLogType::TX_ACTIVE_INFO_LOG: return "TX_ACTIVE_INFO_LOG";
    case transaction::ObTxLogType::TX_RECORD_LOG: return "TX_RECORD_LOG";
    case transaction::ObTxLogType::TX_COMMIT_INFO_LOG: return "TX_COMMIT_INFO_LOG";
    case transaction::ObTxLogType::TX_PREPARE_LOG: return "TX_PREPARE_LOG";
    case transaction::ObTxLogType::TX_COMMIT_LOG: return "TX_COMMIT_LOG";
    case transaction::ObTxLogType::TX_ABORT_LOG: return "TX_ABORT_LOG";
    case transaction::ObTxLogType::TX_CLEAR_LOG: return "TX_CLEAR_LOG";
    case transaction::ObTxLogType::TX_START_WORKING_LOG: return "TX_START_WORKING_LOG";
    case transaction::ObTxLogType::TX_BIG_SEGMENT_LOG: return "TX_BIG_SEGMENT_LOG";
    case transaction::ObTxLogType::TX_DIRECT_LOAD_INC_MAJOR_LOG: return "TX_DIRECT_LOAD_INC_MAJOR_LOG";
    default: return "UNKNOWN";
  }
}

const char *dml_flag_name(const blocksstable::ObDmlFlag flag) {
  switch (flag) {
    case blocksstable::ObDmlFlag::DF_INSERT: return "INSERT";
    case blocksstable::ObDmlFlag::DF_UPDATE: return "UPDATE";
    case blocksstable::ObDmlFlag::DF_DELETE: return "DELETE";
    default: return "UNKNOWN";
  }
}

bool is_visible_dml_flag(const blocksstable::ObDmlFlag flag) {
  return flag == blocksstable::ObDmlFlag::DF_INSERT
      || flag == blocksstable::ObDmlFlag::DF_UPDATE
      || flag == blocksstable::ObDmlFlag::DF_DELETE;
}

int parse_redo_mutator_rows(
    const char *redo_data,
    const int64_t redo_data_len,
    const palf::LSN &lsn,
    const int64_t submit_ts,
    int64_t &row_count)
{
  int ret = OB_SUCCESS;
  int64_t pos = 0;
  row_count = 0;
  transaction::ObCLogEncryptInfo empty_clog_encrypt_info;
  empty_clog_encrypt_info.init();
  memtable::ObMemtableMutatorIterator iter;
  memtable::ObEncryptRowBuf row_buf;

  if (OB_SUCCESS != (ret = iter.deserialize(redo_data, redo_data_len, pos, empty_clog_encrypt_info))) {
    CDC_ERROR() << get_time_prefix() << "[Redo] Failed to deserialize mutator iterator, err=" << ret
              << ", bytes=" << redo_data_len << ", LSN=" << lsn.val_;
    return ret;
  }

  while (OB_SUCCESS == (ret = iter.iterate_next_row(row_buf, empty_clog_encrypt_info))) {
    const memtable::ObMutatorRowHeader &row_header = iter.get_row_head();
    if (row_header.mutator_type_ == memtable::MutatorType::MUTATOR_ROW
        || row_header.mutator_type_ == memtable::MutatorType::MUTATOR_ROW_EXT_INFO) {
      const memtable::ObMemtableMutatorRow &row = iter.get_mutator_row();
      if (is_visible_dml_flag(row.dml_flag_)) {
        DmlEvent event;
        event.op = dml_flag_name(row.dml_flag_);
        event.tablet_id = row_header.tablet_id_.id();
        event.table_id = row.table_id_;
        event.seq = row.seq_no_.get_seq();
        event.branch = row.seq_no_.get_branch();
        event.submit_ts = submit_ts;
        event.lsn = lsn.val_;
        CDC_INFO() << get_time_prefix() << "    -> [RedoRow " << row_count << "] op="
                  << event.op
                  << ", tablet_id=" << event.tablet_id
                  << ", table_id=" << event.table_id
                  << ", seq=" << event.seq
                  << ", branch=" << event.branch
                  << ", submit_ts=" << event.submit_ts;
        ++row_count;
      }
    }
  }

  if (OB_ITER_END == ret) {
    ret = OB_SUCCESS;
  } else if (OB_SUCCESS != ret) {
    CDC_ERROR() << get_time_prefix() << "[Redo] Failed to iterate mutator rows, err=" << ret
              << ", LSN=" << lsn.val_;
  }

  return ret;
}

} // namespace

void parse_tx_redo_logs(const ipalf::ILogEntry &log_entry, const palf::LSN &lsn) {
  const char *buf = log_entry.get_data_buf();
  const int64_t len = log_entry.get_data_len();
  if (buf == nullptr || len <= 0) {
    return;
  }

  transaction::ObTxLogBlock tx_log_block;
  int ret = tx_log_block.init_for_replay(buf, len);
  if (OB_SUCCESS != ret) {
    CDC_ERROR() << get_time_prefix() << "[Redo] Failed to init tx log block, err=" << ret
              << ", LSN=" << lsn.val_ << ", bytes=" << len;
    return;
  }

  const transaction::ObTxLogBlockHeader &block_header = tx_log_block.get_header();
  const int64_t submit_ts = log_entry.get_scn().get_val_for_logservice();

  while (true) {
    transaction::ObTxLogHeader tx_log_header;
    ret = tx_log_block.get_next_log(tx_log_header);
    if (OB_ITER_END == ret) {
      break;
    } else if (OB_SUCCESS != ret) {
      CDC_ERROR() << get_time_prefix() << "[Redo] Failed to get next tx log, err=" << ret
                << ", LSN=" << lsn.val_;
      break;
    }

    if (tx_log_header.get_tx_log_type() == transaction::ObTxLogType::TX_REDO_LOG) {
      transaction::ObTxRedoLogTempRef tmp_ref;
      transaction::ObTxRedoLog redo_log(tmp_ref);
      if (OB_SUCCESS != (ret = tx_log_block.deserialize_log_body(redo_log))) {
        CDC_ERROR() << get_time_prefix() << "[Redo] Failed to deserialize redo body, err=" << ret
                  << ", LSN=" << lsn.val_;
        break;
      }

      int64_t row_count = 0;
      CDC_INFO() << get_time_prefix() << "[Redo] LSN=" << lsn.val_
                << ", tx_log=" << tx_log_type_name(tx_log_header.get_tx_log_type())
                << ", log_entry_no=" << block_header.get_log_entry_no()
                << ", cluster_version=" << block_header.get_cluster_version()
                << ", mutator_size=" << redo_log.get_mutator_size()
                << ", SCN_GTS=" << log_entry.get_scn().get_val_for_gts();
      if (OB_SUCCESS == parse_redo_mutator_rows(redo_log.get_replay_mutator_buf(),
                                                redo_log.get_mutator_size(),
                                                lsn,
                                                submit_ts,
                                                row_count)) {
        CDC_INFO() << get_time_prefix() << "[Redo] Parsed rows=" << row_count
                  << ", LSN=" << lsn.val_;
      }
    }
  }
}
