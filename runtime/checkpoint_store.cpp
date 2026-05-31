#include "runtime/checkpoint_store.h"

#include <fstream>
#include <utility>

CheckpointStore::CheckpointStore(std::string file_path) : file_path_(std::move(file_path)) {}

bool CheckpointStore::load(oceanbase::palf::LSN &lsn) const {
  std::ifstream fs(file_path_);
  if (fs.is_open()) {
    uint64_t val = 0;
    if (fs >> val) {
      lsn.val_ = val;
      return true;
    }
  }
  return false;
}

void CheckpointStore::save(const oceanbase::palf::LSN &lsn) const {
  std::ofstream fs(file_path_, std::ios::trunc);
  if (fs.is_open()) {
    fs << lsn.val_;
  }
}
