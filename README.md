# obcdc_rpc_test — 轻量级 OceanBase CDC 日志订阅客户端

本项目是一个基于 OceanBase 基础网络库的轻量级、高可用实时 Redo 日志（CLOG）拉取与解码客户端。

该项目剥离了官方 `libobcdc` 重度绑定内核源码的高级上下文管理器，自主实现了**“显式主动同步轮询 RPC”**与**“物理日志原生反序列化解码（Module 2）”**，旨在为您提供一套零 `libobcdc` 内核引擎依赖、低移植门槛、高容错抗灾的 CDC 订阅底座。

---

## 1. 核心架构与技术亮点

* 🔄 **同步驱动异步（Sync-over-Async Pull Thread）**
  设计专属 `pull_thread` 轮询线程，通过条件变量 `g_rpc_cond` 锁与网络回调同步。不论发生丢包、网络假死或 Observer 超时，都会强制、无休止地发起 RPC 请求（调用 `async_stream_fetch_log`），确保接口“永不断流”。
* 🛡️ **全局静态持久化回调（Zero-SIGSEGV Callback）**
  采用全局生命周期对象 `g_pull_cb` 进行 RPC 回调绑定，完全剔除局部栈变量带来的内存销毁隐患，从根源消灭 `SIGSEGV`（段错误/内存损坏）崩溃。
* 📝 **物理日志原生解码（Module 2 CLOG Parser）**
  直接在消费线程端导入 `logservice/ipalf/` 核心结构体，原生地顺序调用 `IGroupEntry::deserialize` 与 `ILogEntry::deserialize` 反序列化二进制字节流，提取 LSN、SCN GTS 时间戳等物理元数据。
* 💾 **毫秒级断点续传（LSN Checkpoint）**
  消费线程消费数据后实时持久化 LSN 到 `ob_cdc_checkpoint.txt`。重启时优先从此断点进行热恢复拉取。
* 🔁 **启动连接重试环（Connection Retry Loop）**
  若 Observer 节点正在启动或网络暂时不可达，定位步骤将在 `main` 循环中每 3 秒无限次重试，直至建立成功连接，杜绝进程一闪而退。
* 🚪 **优雅停机（Graceful Shutdown）**
  完美拦截 `SIGINT` / `SIGTERM` 退出信号，停止拉取线程，等待消费队列中的残余数据消费完成并存盘后，再优雅销毁 ObLogRpc 服务，保障数据“不丢一条”。

---

## 2. 目录结构

```text
.
├── CMakeLists.txt              # CMake 依赖构建与 pthread、libobcdc 动态库链接配置
├── build.sh                    # Centos 8.3 容器内快速编译脚本
├── main.cpp                    # 生产级 CDC 客户端主源码（高可用拉取与原理解码）
├── ob_cdc_checkpoint.txt       # 本地持久化断点文件（运行后自动生成）
├── README.md                   # 本项目快速入门指南文档
├── obcdc_production_solution.md # 深度生产环境设计与架构原理方案手册
└── obcdc_alternative_design.md   # 自研代替 libobcdc 的底层协议全套设计蓝图
```

---

## 3. 快速编译指南

本项目代码挂载在 CentOS 8.3 容器中进行交叉编译。

### 1) 进入编译容器并切换至目录
```bash
cd /usr/local/code/obcdctest
```

### 2) 运行编译脚本
```bash
mkdir -p build && cd build
bash ../build.sh
```
*编译成功后，将在 `build` 目录下生成可执行程序 `./obcdc_rpc_test`。*

---

## 4. 运行与验证说明

### 1) 启动运行
```bash
cd /usr/local/code/obcdctest/build
./obcdc_rpc_test
```

### 2) 正常运行日志预期
当 Observer 连通并开始拉取时，您将看到如下日志输出：
```text
[Main] Successfully located start LSN: 18446744073709551615
[Consumer] Consumer worker thread started.
[Puller] Dedicated RPC pull thread started.
[Puller] Calling RPC async_stream_fetch_log for LSN: 18446744073709551615
[RPC CB] Error in fetching logs: rcode=0, biz_err=-4002
[Puller] Calling RPC async_stream_fetch_log for LSN: 18446744073709551615
```

当有真实 Redo 日志产生时，消费端开始解码并打印组日志及事务日志记录：
```text
[Consumer] [Success] Processing LSN: 18446744073709551615, log_entries_count: 2, bytes: 512
  [Parser] [GroupEntry 0] LSN: 18446744073709551615, SCN_GTS: 1780112123952089, DataLen: 480
    -> [LogEntry 0] DataLen: 220, SCN_GTS: 1780112123952089
    -> [LogEntry 1] DataLen: 220, SCN_GTS: 1780112123952089
```

### 3) 高可用重试验证
如果启动时 Observer 暂时不在线，您将观察到重试日志：
```text
[Main] Failed to locate start LSN (err=-4121). Retrying in 3 seconds...
[Main] Failed to locate start LSN (err=-4121). Retrying in 3 seconds...
```
一旦 Observer 在线，会自动与定位器握手成功并拉起整个拉取流。

### 4) 测试优雅停机
按下 `Ctrl+C` 或发送 `kill -15` 信号，系统会执行收尾关机：
```text
[System] Received signal (15). Initiating graceful shutdown...
[Consumer] Consumer worker thread safely terminated.
[Puller] Dedicated RPC pull thread safely terminated.
[Main] Stopping RPC and clearing queues...
[Main] ObLogRpc destroyed. System shutdown complete.
```
*同时本地会更新 `ob_cdc_checkpoint.txt`，用作下次断点续传。*

---

## 5. 参考方案文档

如果您对系统的底层业务演进和完全自研协议细节感兴趣，建议阅读以下本地设计方案文档：
1. **深度架构原理解析**：[obcdc_production_solution.md](file:///usr/local/code/obcdctest/obcdc_production_solution.md)
2. **完全去 OceanBase 动态库自研代替方案**：[obcdc_alternative_design.md](file:///usr/local/code/obcdctest/obcdc_alternative_design.md)
