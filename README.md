# obcdc_rpc_test — 轻量级 OceanBase CDC 日志订阅客户端

本项目是一个基于 OceanBase 基础网络库的轻量级、高可用实时 Redo 日志（CLOG）拉取与解码客户端。

该项目剥离了官方 `libobcdc` 重度绑定内核源码的高级上下文管理器，自主实现了**“显式主动同步轮询 RPC”**与**“物理日志原生反序列化解码（Module 2）”**，旨在为您提供一套零 `libobcdc` 内核引擎依赖、低移植门槛、高容错抗灾的 CDC 订阅底座。

---

## 1. 核心架构与技术亮点

* 🔄 **同步驱动异步（Sync-over-Async Pull Thread）**
  设计专属 `pull_thread` 轮询线程，通过 `PullState` 中的条件变量与网络回调同步。不论发生丢包、网络假死或 Observer 超时，都会强制、无休止地发起 RPC 请求（调用 `async_stream_fetch_log`），确保接口“永不断流”。
* 🛡️ **持久化回调依赖（Zero-SIGSEGV Callback）**
  `ProductionLogCB` 由主流程创建并持有稳定生命周期，RPC 框架内部 `clone()` 时会复制其 `PullState`、队列和运行状态依赖，避免局部栈变量销毁导致的回调悬挂问题。
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
├── AGENTS.md                               # Codex 项目级指令，包含容器编译方式
├── CMakeLists.txt                          # CMake 依赖构建与 pthread、libobcdc 动态库链接配置
├── README.md                               # 本项目快速入门指南文档
├── app/
│   ├── cdc_config.cpp                      # 命令行参数解析与默认配置生成
│   └── cdc_config.h                        # CDC 启动配置结构体
├── cdc_log_parser.cpp                      # CLOG/事务 redo/DML mutator 解析与日志输出
├── cdc_log_parser.h                        # 日志解析入口声明
├── logging_utils.h                         # 时间前缀工具
├── main.cpp                                # CDC 客户端主流程：RPC 生命周期、拉取线程、消费线程
├── parser/
│   └── dml_event.h                         # DML 行事件结构，预留 schema/列值扩展边界
├── rpc/
│   ├── ls_pull_manager.cpp                 # 构造并发起 OB_LS_FETCH_LOG2 异步 RPC
│   ├── ls_pull_manager.h                   # LS 拉取管理器接口
│   ├── production_log_cb.cpp               # RPC 异步回调：响应转 LogTask、推进 PullState
│   ├── production_log_cb.h                 # RPC 回调接口
│   ├── pull_state.cpp                      # 拉取线程和 RPC 回调之间的同步状态
│   └── pull_state.h                        # PullState 状态接口
├── runtime/
│   ├── checkpoint_store.cpp                # LSN checkpoint 读写
│   ├── checkpoint_store.h                  # checkpoint 存储接口
│   ├── logger.cpp                          # 线程安全日志实现，支持终端和文件输出
│   ├── logger.h                            # CDC_INFO/CDC_ERROR 日志接口
│   └── safe_queue.h                        # 带背压的线程安全阻塞队列
├── worker/
│   ├── consumer_worker.cpp                 # 消费线程：GroupEntry/ILogEntry 分流、心跳限流、checkpoint 保存
│   ├── consumer_worker.h                   # 消费线程入口声明
│   ├── log_task.h                          # RPC 回调和消费线程之间传递的日志任务
│   ├── pull_worker.cpp                     # 生产者线程：循环触发 RPC、等待回调、推进 LSN
│   └── pull_worker.h                       # 生产者线程入口声明
├── obcdc_production_solution.md            # 深度生产环境设计与架构原理方案手册
├── plan.md                                 # 自研 CDC 方案规划草稿
└── docs/superpowers/plans/
    └── 2026-05-31-obcdc-refactor.md        # 本轮重构实施计划
```

运行后可能生成以下文件或目录：

```text
build/                                      # CMake 构建产物目录
ob_cdc_checkpoint_t<tenant>_ls<ls>.txt      # 默认 LSN 断点文件
.codegraph/                                 # 本地代码索引缓存
```

---

## 3. 快速编译指南

本项目代码挂载在 CentOS 8.3 容器中进行交叉编译。

### 1) 使用容器执行编译
```bash
docker exec centos8.3 bash -lc 'cd /usr/local/code/obcdctest/build && rm -rf ./* && cmake ../ && make -j4'
```

*编译成功后，将在 `build` 目录下生成可执行程序 `./obcdc_rpc_test`。*

---

## 4. 运行与验证说明

### 1) 启动运行
```bash
cd /usr/local/code/obcdctest/build
./obcdc_rpc_test
```

支持的启动参数：
```bash
./obcdc_rpc_test \
  --tenant-id 1006 \
  --server 192.168.31.205:2882 \
  --ls-id 1001 \
  --checkpoint-file ./ob_cdc_checkpoint.txt \
  --log-file ./logs/obcdc_rpc_test.log
```

参数说明：

| 参数 | 默认值 | 说明 |
|---|---:|---|
| `--tenant-id` | `1006` | 订阅的租户 ID |
| `--server` | `192.168.31.205:2882` | Observer RPC 地址 |
| `--ls-id` | `1001` | 订阅的 LS ID |
| `--checkpoint-file` | `ob_cdc_checkpoint_t<tenant>_ls<ls>.txt` | LSN 断点文件 |
| `--log-file` | 空 | 配置后同时写入日志文件 |
| `--no-console-log` | 关闭 | 只写日志文件，不输出到终端 |

默认仍输出到终端；指定 `--log-file` 后，同一份日志会同时写入文件。如果使用 `--no-console-log`，建议必须同时设置 `--log-file`，否则启动后不会看到运行日志。

### 2) 正常运行日志预期
当 Observer 连通并开始拉取时，您将看到如下日志输出：
```text
[2026-05-31 12:34:56] [Main] Successfully located start LSN: 18446744073709551615
[2026-05-31 12:34:56] [Consumer] Consumer worker thread started.
[2026-05-31 12:34:56] [Puller] Dedicated RPC pull thread started.
[2026-05-31 12:34:56] [Puller] Calling RPC async_stream_fetch_log for LSN: 18446744073709551615
[2026-05-31 12:34:56] [RPC CB] Error in fetching logs: rcode=0, biz_err=-4002
```

当有真实 Redo 日志产生时，消费端开始解码并打印组日志及事务日志记录：
```text
[2026-05-31 12:34:56] [Redo] LSN=18446744073709551615, tx_log=TX_REDO_LOG, log_entry_no=7, cluster_version=..., mutator_size=384, SCN_GTS=1780112123952089
[2026-05-31 12:34:56]     -> [RedoRow 0] op=INSERT, tablet_id=200001, table_id=500001, seq=1, branch=0, submit_ts=1780112123952089000
[2026-05-31 12:34:56] [Redo] Parsed rows=1, LSN=18446744073709551615
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
