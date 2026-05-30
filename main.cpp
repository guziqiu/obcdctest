#include <iostream>
#include <map>
#include <chrono>
#include <string>
#include <unistd.h>
#include "src/logservice/libobcdc/src/ob_log_rpc.h"
#include "src/logservice/libobcdc/src/ob_log_config.h"
#include "src/logservice/libobcdc/src/ob_log_trace_id.h"
#include "src/logservice/cdcservice/ob_cdc_req.h"
#include "share/ob_ls_id.h"

using namespace oceanbase;
using namespace oceanbase::libobcdc;
using namespace oceanbase::common;

int main()
{
  int ret = OB_SUCCESS;

  std::cout << "Setting self address..." << std::endl;
  get_self_addr().set_ip_addr("127.0.0.1", static_cast<int32_t>(getpid()));

  std::cout << "Initializing ObLogConfig (TCONF)..." << std::endl;
  TCONF.init();
  std::map<std::string, std::string> configs;
  TCONF.load_from_map(configs);

  std::cout << "Initializing ObLogRpc..." << std::endl;
  ObLogRpc rpc;
  int64_t io_thread_num = 1;
  ret = rpc.init(io_thread_num);
  if (OB_SUCCESS != ret) {
    std::cerr << "ObLogRpc init failed, err: " << ret << std::endl;
    return ret;
  }
  std::cout << "ObLogRpc initialized successfully!" << std::endl;

  uint64_t tenant_id = 1002;
  ObAddr svr(ObAddr::IPV4, "127.0.0.1", 10001);

  obrpc::ObCdcReqStartLSNByTsReq req;
  obrpc::ObCdcReqStartLSNByTsReq::LocateParam param;
  param.ls_id_ = share::ObLSID(1);

  // 获取当前系统时间的纳秒级时间戳 (Unix Epoch Nanoseconds)
  auto now = std::chrono::system_clock::now();
  int64_t now_ns = std::chrono::duration_cast<std::chrono::nanoseconds>(now.time_since_epoch()).count();
  param.start_ts_ns_ = now_ns;

  req.append_param(param);

  obrpc::ObCdcReqStartLSNByTsResp resp;
  int64_t timeout = 5000000;

  char svr_buf[128];
  svr.ip_port_to_string(svr_buf, sizeof(svr_buf));
  std::cout << "Sending req_start_lsn_by_tstamp to Observer: " << svr_buf << std::endl;

  ret = rpc.req_start_lsn_by_tstamp(tenant_id, svr, req, resp, timeout);
  if (OB_SUCCESS != ret) {
    std::cerr << "req_start_lsn_by_tstamp failed, err: " << ret << std::endl;
  } else {
    std::cout << "req_start_lsn_by_tstamp succ! response error code: " << resp.get_err() << std::endl;
    const auto &results = resp.get_results();
    for (int64_t i = 0; i < results.count(); ++i) {
      const auto &res = results.at(i);
      std::cout << "Result [" << i << "]: err=" << res.err_ 
                << ", start_lsn=" << res.start_lsn_.val_ 
                << ", start_ts_ns=" << res.start_ts_ns_ << std::endl;
    }
  }

  rpc.destroy();
  std::cout << "ObLogRpc destroyed." << std::endl;
  return 0;
}
