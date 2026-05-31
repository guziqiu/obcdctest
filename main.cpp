#include <map>
#include <chrono>
#include <string>
#include <unistd.h>
#include <thread>
#include <atomic>
#include <csignal>

#include "app/cdc_config.h"
#include "logging_utils.h"
#include "rpc/ls_pull_manager.h"
#include "rpc/production_log_cb.h"
#include "rpc/pull_state.h"
#include "runtime/checkpoint_store.h"
#include "runtime/logger.h"
#include "runtime/safe_queue.h"
#include "src/logservice/libobcdc/src/ob_log_rpc.h"
#include "src/logservice/libobcdc/src/ob_log_config.h"
#include "src/logservice/libobcdc/src/ob_log_trace_id.h"
#include "src/logservice/cdcservice/ob_cdc_req.h"
#include "share/ob_ls_id.h"

#include "worker/consumer_worker.h"
#include "worker/log_task.h"
#include "worker/pull_worker.h"

using namespace oceanbase;
using namespace oceanbase::libobcdc;
using namespace oceanbase::common;

// --- 生产级基础：原子控制与优雅退出信号 ---
std::atomic<bool> g_running(true);

// 优雅关机信号处理函数
void signal_handler(int signum) {
  CDC_INFO() << get_time_prefix() << "\n[System] Received signal (" << signum << "). Initiating graceful shutdown...";
  g_running = false;
}

// 全局日志缓冲队列（背压大小限制为 1000 个 Batch）
SafeQueue<LogTask> g_log_queue(1000);

// --- 生产级设计 2：断点续传（持久化 Checkpoint） ---
const CheckpointStore *g_checkpoint_store = nullptr;

// --- 主程序入口 ---
int main(int argc, char **argv) {
  CdcConfig config = CdcConfig::parse(argc, argv);
  if (!Logger::instance().init(config.log_file, config.log_to_console)) {
    CDC_ERROR() << get_time_prefix() << "[Main] Failed to open log file: " << config.log_file;
    return 1;
  }

  // 1. 注册核心优雅退出信号
  std::signal(SIGINT, signal_handler);
  std::signal(SIGTERM, signal_handler);

  int ret = OB_SUCCESS;

  CDC_INFO() << get_time_prefix() << "[Main] Setting self address...";
  get_self_addr().set_ip_addr("127.0.0.1", static_cast<int32_t>(getpid()));

  CDC_INFO() << get_time_prefix() << "[Main] Initializing ObLogConfig...";
  TCONF.init();
  std::map<std::string, std::string> configs;
  TCONF.load_from_map(configs);

  CDC_INFO() << get_time_prefix() << "[Main] Initializing ObLogRpc...";
  ObLogRpc rpc;
  int64_t io_thread_num = 2; // 生产环境配置为 2 个 or 更多线程
  ret = rpc.init(io_thread_num);
  if (OB_SUCCESS != ret) {
    CDC_ERROR() << get_time_prefix() << "[Main] ObLogRpc init failed, err: " << ret;
    return ret;
  }
  CDC_INFO() << get_time_prefix() << "[Main] ObLogRpc initialized successfully!";

  // 定位所需参数
  uint64_t tenant_id = config.tenant_id;
  ObAddr svr(ObAddr::IPV4, config.server_host.c_str(), config.server_port);
  share::ObLSID ls_id(config.ls_id);
  CheckpointStore checkpoint_store(config.checkpoint_file);
  g_checkpoint_store = &checkpoint_store;
  CDC_INFO() << get_time_prefix() << "[Main] Using checkpoint file: " << checkpoint_store.file_path();

  palf::LSN start_lsn;
  bool has_checkpoint = checkpoint_store.load(start_lsn);

  if (has_checkpoint) {
    // 优先从历史断点恢复
    CDC_INFO() << get_time_prefix() << "[Main] Found checkpoint! Resuming redo log stream from LSN: " << start_lsn.val_;
  } else {
    // 无历史断点，根据当前系统时间定位起始 LSN（含启动重试机制）
    CDC_INFO() << get_time_prefix() << "[Main] No checkpoint found. Locating start LSN by system time...";
    
    obrpc::ObCdcReqStartLSNByTsReq req;
    obrpc::ObCdcReqStartLSNByTsReq::LocateParam param;
    param.ls_id_ = ls_id;
    
    auto now = std::chrono::system_clock::now();
    int64_t now_ns = std::chrono::duration_cast<std::chrono::nanoseconds>(now.time_since_epoch()).count();
    param.start_ts_ns_ = now_ns;
    req.append_param(param);

    obrpc::ObCdcReqStartLSNByTsResp resp;
    int64_t timeout = 5000000;

    // 生产级优势：循环不断重试连接定位，直到 Observer 启动并连通
    while (g_running) {
      ret = rpc.req_start_lsn_by_tstamp(tenant_id, svr, req, resp, timeout);
      if (OB_SUCCESS == ret && resp.get_results().count() > 0) {
        start_lsn = resp.get_results().at(0).start_lsn_;
        CDC_INFO() << get_time_prefix() << "[Main] Successfully located start LSN: " << start_lsn.val_;
        break;
      } else {
        CDC_ERROR() << get_time_prefix() << "[Main] Failed to locate start LSN (err=" << ret << "). Retrying in 3 seconds...";
        std::this_thread::sleep_for(std::chrono::seconds(3));
      }
    }
  }

  // 初始化全局活动变量
  PullState pull_state;
  pull_state.set_last_lsn(start_lsn.val_);
  ProductionLogCB pull_cb(pull_state, g_log_queue, g_running, ls_id);

  // 2. 启动后台消费者线程
  std::thread consumer_thread(consumer_worker_thread, std::ref(g_log_queue),
                              std::cref(g_running), g_checkpoint_store);

  // 3. 实例化拉取管理器
  LSPullManager pull_manager(rpc, tenant_id, svr, ls_id, g_running, pull_state, pull_cb);
  
  // 4. 启动【拉取驱动线程】，开启强健的显式循环 RPC 调用
  std::thread pull_thread(pull_worker_thread, std::ref(pull_manager), std::ref(pull_state),
                          std::cref(g_running), start_lsn);

  // 5. 守护主线程，等待退出信号
  while (g_running) {
    std::this_thread::sleep_for(std::chrono::milliseconds(500));
  }

  // --- 6. 生产级优雅关闭链条 ---
  CDC_INFO() << get_time_prefix() << "[Main] Stopping RPC and clearing queues...";
  g_log_queue.close(); // 唤醒并关闭消费队列，使消费者停止

  if (pull_thread.joinable()) {
    pull_state.wakeup(); // 唤醒阻塞中的 puller 线程使其退出
    pull_thread.join();
  }

  if (consumer_thread.joinable()) {
    consumer_thread.join(); // 等待所有正在消费的数据安全处理并持久化 checkpoint
  }

  rpc.destroy(); // 销毁 RPC 服务，断开与 Observer 的网络连接
  CDC_INFO() << get_time_prefix() << "[Main] ObLogRpc destroyed. System shutdown complete.";

  return 0;
}
