# 架构与一致性设计

## 1. 三个独立运行端

```
┌──────────────┐   INSERT events      ┌───────────────┐
│  ingest 端   │ ───────────────────► │               │
│ CLI / HTTP   │   (append-only,      │               │
└──────────────┘    分配单调 seq)      │               │
                                      │   SQLite (WAL) │
┌──────────────┐   读未归属事件        │   单一事实源    │
│  worker 端   │ ├──────────────────► │               │
│ 点面计算/重算 │   写 attributions/   │               │
│ 可中断/恢复   │   conflicts/jobs     │               │
└──────────────┘                      │               │
                                      │               │
┌──────────────┐   只读 attributions  │               │
│ report/web 端│ ◄───────────────────┤               │
│ 聚合/对比/复现│                      └───────────────┘
└──────────────┘
```

* **ingest**：`pointwise/__main__.py ingest` 或 `POST /api/events`。只负责
  校验坐标范围、追加不可变事件、分配 `seq`。
* **worker**：`worker` 子命令或 `POST /api/jobs/<id>/run`。系统中唯一点面
  计算方，ingest 与 report 都不自行判点，保证边线/重叠/无法定位只有一种处理。
* **report/web**：`serve` 子命令。只读归属结果做聚合、新旧对比、报告复现，
  以及上传边界、批准规则、中断作业等管控操作。

三端共享一个 SQLite 文件，WAL + `BEGIN IMMEDIATE` 保证单写多读，可分别用
独立进程（容器）启动。

## 2. 三类特殊记录的一致处理

几何分类（`geometry.classify_point`）是纯函数，候选 region 一律按
`region_id` 排序，输出只有四种状态：

| 状态 | 触发条件 | 规则解析（`worker.resolve_classification`） |
|---|---|---|
| `INSIDE` | 恰好落在一个区域内部 | 直接 `ATTRIBUTED` |
| `ON_BOUNDARY` | 落在任一区域边界线段上（含 EPS 抖动） | `MIN_ID`：确定性取候选最小 id；`MANUAL`：进冲突队列，不自动分配 |
| `OVERLAP` | 同时在两个以上区域内部 | 同上（独立的 overlap 策略） |
| `UNLOCATED` | 不在任何区域内/边上 | **任何策略都不分配区域**，保留无归属状态 |

关键约束：

* 没有任何随机分支；平局只按字典序最小 id 消解，且 `MIN_ID` 与 `MANUAL`
  都是规则版本里显式记录的策略，报告可追溯。
* 冲突解决只能从该事件的候选集合里选区域（`storage.resolve_conflict`
  强制校验），不能凭空指派。
* 上传时区域重叠会被审计进 `validation_notes` 与 `self_intersects`，
  使重叠成为显式数据质量问题而非静默吞并。

## 3. 版本模型与"不混用两版"

```
boundary_sets: DRAFT ──job COMPLETED──► ACTIVE ──新版本激活──► SUPERSEDED
rules:         PENDING ──approve──────► APPROVED
attributions:  PRIMARY KEY (event_id, boundary_version, rule_version)
```

* 同一事件在新旧边界下的归属是**两行不同主键的记录**，原始 lon/lat 在
  `events` 表永久保留，因此任何旧版本都能重算、复现。
* 重算作业创建时把 `snapshot_seq = 当前最大 seq` 固化进 `jobs` 行，作业
  循环只读取 `seq <= snapshot_seq` 的事件。作业运行期间到达的新事件走
  live 归属路径，按**当前 ACTIVE 旧版本**计算，写的是另一组主键，
  物理上不可能混入作业结果。
* DRAFT 在作业未完成（PENDING/RUNNING/INTERRUPTED）时禁止激活
  （`Store.activate_boundary_set` 内校验），杜绝"半成品版本上线"。
* 激活后 worker 对积压新事件做一次 backfill，全部按新版本归属。

## 4. 全量 vs 仅变更区域：影响范围依据

`boundaries.analyze_impact` 产出作业行内永久保存的证据：

1. 按几何内容哈希（`canonical_geometry_json` 的 SHA-256）比对出
   added / removed / changed / unchanged 区域；
2. 取所有变化区域**新旧 bbox 的并集**做候选过滤，落在其中的事件计入
   `affected_event_ids`（只可能过估、不会漏：未变化区域内部点的判定
   必然不变，而 bbox 扩散覆盖了边界挪动的溢出带）；
3. `recommend_scope` 的显式规则：
   * 无历史版本、或全部区域都变 → `FULL`；
   * 受影响事件占比 ≥ 0.8 → `FULL`；
   * 否则 → `CHANGED_REGIONS`，作业只处理候选 id 集合。
4. 两种范围都可由人工显式覆盖；选择与依据（rationale、计数）写入
   `jobs.impact_json`，报告和 UI 直接展示。

## 5. 中断与恢复

* 中断是协作式的：`interrupt_job` 只在 meta 表置位，worker 每处理一条事件
  检查一次，在事务边界（每 100 条批量提交）停止，作业置 `INTERRUPTED`。
* 已处理事件记录在 `job_progress`，恢复时跳过，不重复写、不遗漏。
* 失败走 `FAILED + error`，不产生半版本（归属行按 upsert 幂等）。

## 6. 报告可复现性

每份报告固化四个锚点：边界版本、规则版本、事件 seq 窗口、`params_hash`
（边界几何哈希 + 规则策略 + 窗口的哈希）。`reproduce_report` 用同样锚点
重跑并比对哈希；旧版本数据不可变（SUPERSEDED 行不删除），所以旧报告随时
可按旧版复现，哈希不一致会显式报错。
