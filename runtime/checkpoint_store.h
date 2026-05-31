#pragma once

#include <string>

#include "logservice/palf/lsn.h"

class CheckpointStore {
public:
  explicit CheckpointStore(std::string file_path);

  bool load(oceanbase::palf::LSN &lsn) const;
  void save(const oceanbase::palf::LSN &lsn) const;

  const std::string &file_path() const { return file_path_; }

private:
  std::string file_path_;
};
