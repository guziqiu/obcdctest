# obcdc Refactor Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [x]`) syntax for tracking.

**Goal:** Refactor the CDC client into clearer modules while preserving the current RPC, checkpoint, and DML log behavior.

**Architecture:** Move reusable runtime concerns into `runtime/`, startup configuration into `app/`, and parser event types into `parser/`. Keep the existing pull/consumer flow in `main.cpp` for this first pass to reduce risk, but route logging and checkpoint storage through dedicated modules.

**Tech Stack:** C++17, CMake, OceanBase internal headers, pthread.

---

### Task 1: Runtime Utilities

**Files:**
- Create: `runtime/logger.h`
- Create: `runtime/logger.cpp`
- Create: `runtime/safe_queue.h`
- Create: `runtime/checkpoint_store.h`
- Create: `runtime/checkpoint_store.cpp`
- Modify: `CMakeLists.txt`

- [x] Add a thread-safe logger that can write to console and optionally append to a file.
- [x] Move `SafeQueue` into a reusable header.
- [x] Move checkpoint load/save into `CheckpointStore`.
- [x] Add the new `.cpp` files to the CMake target.

### Task 2: Configuration

**Files:**
- Create: `app/cdc_config.h`
- Create: `app/cdc_config.cpp`
- Modify: `main.cpp`

- [x] Add command-line parsing for `--tenant-id`, `--server`, `--ls-id`, `--log-file`, `--checkpoint-file`, and `--no-console-log`.
- [x] Initialize logger before the first startup log line.
- [x] Replace hard-coded tenant/server/LS/checkpoint values with `CdcConfig`.

### Task 3: Parser Event Boundary

**Files:**
- Create: `parser/dml_event.h`
- Modify: `cdc_log_parser.cpp`
- Modify: `cdc_log_parser.h`

- [x] Introduce `DmlEvent` as the structured representation of row-level DML metadata.
- [x] Keep the current text log format compatible.
- [x] Keep DML output limited to `INSERT`, `UPDATE`, and `DELETE`.

### Task 4: Documentation And Verification

**Files:**
- Modify: `README.md`

- [x] Document the new module layout.
- [x] Document the new runtime options and log-file behavior.
- [x] Run a build command if the OceanBase dependency tree is available in the current environment.
