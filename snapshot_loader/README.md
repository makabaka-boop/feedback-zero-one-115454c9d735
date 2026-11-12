# 增量快照装载器（Incremental Snapshot Loader）

纯后端实现：一个**分块扫描 + 日志尾追**的装载器，以及一个满足题述全部约束的
**本地 HTTP 模拟源**。仅使用 Python 标准库（`http.server`、`sqlite3`、
`urllib`、`threading`），无第三方依赖。

## 1. 解决的问题

首次装载一个持续被写入的键值目录时，任何一次性全量扫描都可能漏掉扫描期间的
修改，或用旧取样值覆盖新事件。本实现采用**水位标记增量快照**算法：

* 源端为每个块的扫描写两个标记：低标记 **L**（取样之前）与高标记 **H**
  （取样之后），标记本身也占用一个 `pos`；
* 一次扫描对应**取样瞬间源状态的原子快照**，重试永远返回同一份不可变内容；
* 装载器把取样行与日志区间 `(L, H]` 内的写入按源端顺序合并：
  区间内触及的键取**最后一次写入**（删除即移除），其余键保留取样值；
* 所有块完成且日志连续消费到 C 后，原子发布一份恰为"源在 C 的完整副本"。

## 2. 目录结构

```
snapshot_loader/
├── pyproject.toml
├── README.md
├── src/snapshot_loader/
│   ├── protocol.py   # 线协议、uint64 校验、ASCII 键/半开区间纯函数、HTTP 客户端
│   ├── source.py     # 本地 HTTP 模拟源（SQLite、原子取样、两阶段扫描、日志）
│   ├── store.py      # 装载器持久化层 + 纯协调函数 reconcile_chunk
│   ├── loader.py     # 装载引擎：扫描线程、日志消费、(L,H] 协调、原子发布
│   ├── app.py        # HTTP 门面与 CLI 入口（source / loader 两个子命令）
│   ├── __init__.py
│   └── __main__.py
├── tests/
│   ├── harness.py            # 进程内 HTTP 测试支架与等待助手
│   ├── test_unit.py          # 纯逻辑单测（15 个）
│   ├── test_acceptance.py    # 端到端验收（13 个）
│   └── test_crash_recovery.py# 子进程强退恢复验收（4 个）
└── run/
    ├── one.py                # 按点路径快速运行测试
    └── crash_worker.py       # 崩溃恢复测试的子进程工作脚本
```

## 3. 数据与协议约定

* **键**：非空 ASCII 字符串，按 ASCII 字节序比较（与 SQLite TEXT 的 BINARY
  排序一致）。
* **值**：字符串或 SQL `NULL`。删除与"真实空值"严格区分：
  `after = null` 表示删除；真实空值写为 `{"value": null}`，键仍然存在。
* **pos / s / C**：非负 64 位整数，拒绝布尔（`bool` 是 `int` 子类，被显式
  排除）。SQLite 中以 20 位零填充 TEXT 存储，字典序与数值序完全一致，且可
  容纳超过有符号 64 位上限的值。
* **块范围**：升序、互不重叠、覆盖全部键的固定半开区间 `[lo, hi)`，
  端点 `null` 表示无穷。模拟源使用固定分界 `B`：块 0 为 `[null, B)`，
  块 1 为 `[B, null)`。
* **日志**：从 `s` 之后连续提供写入与标记；写入带完整 `after`。装载器的
  连续游标初始即为 `s`，模拟源对早于 `s` 的请求也只返回 `s` 之后的尾部。

### 模拟源端点

| 方法/路径 | 说明 |
| --- | --- |
| `GET /bootstrap` | 返回 `{s, ranges[]}` |
| `POST /scan` `{chunk_id}` | 启动/重取块扫描，写 L，两阶段第一态 |
| `POST /scan/continue` `{chunk_id}` | 推进扫描（取样 → 写 H → 出行） |
| `GET /log?after_pos=&limit=` | 连续日志流 |
| `POST /test/write` / `/test/mark` | 测试用写入/打标记 |
| `POST /test/scan-control` | 手动门控模式下强制推进扫描相位 |
| `GET /test/snapshot?C=` | 独立预言机：从日志重放源在 C 的真实副本 |
| `GET /test/info` | s、tip、各扫描相位 |

`--manual` 模式下 `/scan/continue` 不会自行推进相位，只有控制端能推进，
从而可确定性地把取样卡在 L 与 H 之间插入写入。

### 装载器端点

| 方法/路径 | 说明 |
| --- | --- |
| `GET /query` | 发布前 `409 {status:"not_ready"}`；发布后 `{C, rows[]}`（按键序） |
| `POST /advance` `{C}` | 见下；返回 `published` / `already_published` / `not_ready` / `rejected` / `failed` |
| `GET /status` | 游标、已发布 C、各块相位与 L/H（排障用） |

## 4. 核心算法

### 4.1 单块协调（`store.reconcile_chunk`）

1. 以该块不可变取样行初始化键→值映射；
2. 按源端顺序应用 `(L, H]` 内**且键属于本块范围**的写入；标记为空操作；
3. 删除移除键——因此"L 后写 k、取样看到 k、H 前删 k"最终不会留下 k；
4. 输出按键序排列的协调行。

只处理本块范围的写入是必要的：交错块的 `(L, H]` 可能包含属于其他块的键。

### 4.2 发布到 C（`loader.advance`）

仅当以下条件**同时**满足才发布，否则返回 `not_ready` 并给出原因：

1. 所有块都已协调（无"未完成块"）；
2. 每个块都有 `H <= C`（否则 `C_below_chunk_H`）；
3. 日志已连续消费到 C（否则 `log_not_consumed_through_C`）。

发布副本的构造：先合并各块在其自身 H 处的协调状态，再对 `(min L, C]` 内
每条写入，仅当其位置 **严格大于该键所属块的 H** 时重放（否则该写入已包含
在那块的协调结果里）。副本替换与 C 的发布在**同一个 SQLite 事务**提交。

* `C` 低于已发布位置 → `409 rejected`；
* 等于已发布位置 → `already_published`（幂等）。

### 4.3 日志消费与异常语义

* **缺号等待**：出现 `pos > cursor+1` 时不移动游标，重新轮询；
* **重复 pos、内容相同**：忽略（同批次内与持久化层均做去重）；
* **重复 pos、内容不同**：抛 `DuplicatePosError`，装载器进入永久 `failed`，
  停止后台工作，但**上次成功发布的副本仍可查询**；
* 传输错误/5xx/超时：有界退避重试，扫描响应可延迟、乱序、重复而不影响正确性。

### 4.4 持久化与重启

所有进度均落盘（WAL + `synchronous=FULL`）：bootstrap 参数、日志游标、块
相位与 L/H、不可变取样行、协调行、已消费日志、已发布副本。重启后：

* 已 `done` 的块直接复用源端同一份取样，**不会重新取样**；
* 停在 `scanning` 的块继续驱动既有扫描；
* 日志游标与已发布副本原样恢复。

## 5. 运行方式

无需安装依赖，标准库即可。用模块方式启动（两个独立进程）：

```bash
# 终端 1：模拟源
PYTHONPATH=src python3 -m snapshot_loader.app source \
  --db /tmp/demo/source.db --host 127.0.0.1 --port 8081 \
  --seed '{"A":{"value":"a0"},"B":{"value":null},"C":{"value":"c0"}}'

# 终端 2：装载器
PYTHONPATH=src python3 -m snapshot_loader.app loader \
  --db /tmp/demo/loader.db --source http://127.0.0.1:8081 \
  --host 127.0.0.1 --port 8082
```

操作示例：

```bash
curl -s http://127.0.0.1:8081/bootstrap
curl -s -w ' [%{http_code}]\n' http://127.0.0.1:8082/query        # 409 not_ready
TIP=$(curl -s http://127.0.0.1:8081/test/info | python3 -c 'import sys,json;print(json.load(sys.stdin)["tip"])')
curl -s -X POST http://127.0.0.1:8082/advance -d "{\"C\":$TIP}" -H 'Content-Type: application/json'
curl -s http://127.0.0.1:8082/query
curl -s "http://127.0.0.1:8081/test/snapshot?C=$TIP"             # 独立预言机，应一致
```

`loader` 子命令支持 `--crash-hook {after_scan_started, after_scan_received,
before_publish, in_publish_before_commit, after_publish_commit}`，在指定内部
点以 `os._exit(86)` 硬退出，供恢复验收使用。

## 6. 测试

```bash
# 全部 32 个测试（隔离模式，不依赖 PYTHONPATH/第三方包）
python3 -I -m unittest discover -s tests -t .

# 仅某一类
python3 -I run/one.py tests.test_acceptance.WatermarkReconciliationTests
```

### 验收要点与对应测试

| 题目验收要求 | 测试 |
| --- | --- |
| L 后写 `k=B`、再取样、H 前删 `k`，块最终不留 `k` | `WatermarkReconciliationTests.test_write_after_L_then_sample_then_delete_before_H_leaves_no_key` |
| 取样旧值不得覆盖 H 前的新写入 | `test_stale_sample_is_overwritten_by_write_before_H` |
| 两块交错、H 之后的删除/写入正确重放 | `test_two_chunks_interleaved_with_post_H_writes_and_deletion` |
| 扫描可延迟、乱序、重复，同块一次取样、重试返回原内容 | `ScanRetryTests.test_repeated_calls_return_immutable_sample` |
| 重启续跑、不重新取样 | `test_restart_resumes_without_resampling_and_publishes` |
| 发布前 `not_ready`；C 低于已发布位置被拒绝；幂等 | `BasicFlowTests` / `AdvanceContractTests` |
| 缺号等待；重复 pos 同内容忽略、异内容失败且保留上次发布 | `LogStreamTests` |
| 三写者并发 + 自主扫描，每个发布 C 与源快照一致 | `ConcurrentFuzzTest` |
| **接收扫描后强退**，重启不重新取样且与源一致 | `CrashRecoveryTests.test_crash_after_scan_received_resumes_without_resampling` |
| **发布事务提交前强退**，保留上次发布，重启后一致 | `test_crash_before_publish_commit_keeps_last_publication` |
| **提交后强退**，已提交副本保留 | `test_crash_after_commit_keeps_committed_publication` |
| 多轮发布，每个 C 均与源端快照一致 | `test_replica_matches_source_at_multiple_positions` |

崩溃测试通过真实子进程触发 `os._exit`，模拟源进程在装载器重启期间保持存活，
与生产中"仅装载器死亡"的情形一致。

## 7. 设计取舍

* 模拟源把"延迟/乱序/重复"拆为两阶段扫描加手动门控，使交错时序完全确定、
  可复现；真实源只需在一次 HTTP 调用内阻塞，装载器的重试层天然兼容。
* 位置统一用零填充 TEXT 存储，规避 SQLite 整数的有符号 64 位限制，同时保留
  与数值序一致的索引排序。
* 发布是单事务的全量替换：实现简单且崩溃语义清晰（提交前崩溃 → 旧副本原样
  可用），代价是大副本下的写放大；接口层后续可换为分代/差异发布而不改变
  协调算法。
