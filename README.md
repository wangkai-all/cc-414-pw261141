# Pointwise · 版本化点面归属与边界重算系统

事件按区域边界做**真实点-面计算**并聚合统计；区域边界更新后，同一批事件可在
新旧两版边界下分别归属、对比，且旧报告可按旧版复现。**纯 Python 标准库实现**
（sqlite3 + http.server + 手写射线法），零第三方依赖，不做路线规划。

## 需求对照

| 需求 | 实现位置 |
|---|---|
| 数据接收 / 空间归属 / 统计报告三端独立进程 | `pointwise/__main__.py` 的 `ingest` / `worker` / `serve` 子命令，经 SQLite 协作 |
| 同一批事件按旧/新边界汇总对比 | `attributions` 以 `(event_id, boundary_version, rule_version)` 为主键多版本共存；`reports.compare` |
| 边界相交、点落边线、无法定位的一致处理（不随机归区） | `geometry.classify_point` + `worker.resolve_classification`：候选按 region_id 排序，平局取最小 id 或转人工，UNLOCATED 永不分配 |
| 全量 vs 仅变更区域，有影响范围依据 | `boundaries.analyze_impact` / `recommend_scope`，依据（变化区域、受影响事件、占比、rationale）固化进 `jobs.impact_json` |
| 旧报告按旧版复现 | `reports.reproduce_report`：边界/规则/事件窗口/几何哈希四重锚点 + params_hash 校验 |
| 网页上传虚构边界、查看冲突点、批准规则、中断重算 | `pointwise/web/` 单页控制台 + JSON API |
| 重算期间新事件不混用两版 | 作业创建即固定 `snapshot_seq`，只处理该序号前事件；新事件走 ACTIVE 版本的独立归属主键 |
| 真实点面计算与聚合，从零实现 | `pointwise/geometry.py`：射线法、点-线段、线段相交、bbox、孔洞，全部手写 |
| 坐标系统及转换限制明确记录 | [`docs/COORDINATES.md`](docs/COORDINATES.md) |

## 快速开始

```bash
# 零依赖，Python 3.11+（仅标准库）。处理不可信数据文件时使用 -I 隔离模式，
# scripts/pw 启动器会自行补齐包路径。
python3 -I scripts/pw --db data/demo.db demo      # 灌入确定性演示数据
python3 -I scripts/pw serve --db data/demo.db --port 8080
# 浏览器打开 http://127.0.0.1:8080
```

## 三个独立运行端

```bash
# ① 数据接收端（可多副本，append-only + 单调 seq）
python3 -I scripts/pw --db data/pw.db ingest evt-1 116.40 39.90
python3 -I scripts/pw --db data/pw.db ingest --batch events.jsonl

# ② 空间归属/重算端（唯一点面计算方；可常驻轮询）
python3 -I scripts/pw --db data/pw.db worker --loop          # 常驻
python3 -I scripts/pw --db data/pw.db worker --once          # 单轮
python3 -I scripts/pw --db data/pw.db job run --job 1        # 指定作业

# ③ Web/报告端（只读归属结果 + 管控操作）
python3 -I scripts/pw --db data/pw.db serve --port 8080
python3 -I scripts/pw --db data/pw.db report --boundary 2 --compare 1
```

## 边界更新标准流程

```bash
python3 -I scripts/pw --db data/pw.db boundary upload new-fictional.geojson   # -> DRAFT vN
python3 -I scripts/pw --db data/pw.db job create --to-version N               # 影响分析+推荐范围
python3 -I scripts/pw --db data/pw.db job list
python3 -I scripts/pw --db data/pw.db job interrupt --job 1                   # 任意进程可中断
python3 -I scripts/pw --db data/pw.db job run --job 1                         # 从断点恢复
python3 -I scripts/pw --db data/pw.db activate --version N                    # 仅作业完成后允许
python3 -I scripts/pw --db data/pw.db report --boundary N --compare 1         # 同批事件新旧对比
python3 -I scripts/pw --db data/pw.db report --reproduce 1                    # 旧报告复现校验
```

## 归属规则与冲突

* 种子规则：边线/重叠均 `MIN_ID`（候选按 region_id 字典序取最小，纯确定性）。
* 可在网页或 CLI 提交 `MANUAL` 规则，批准后边线点与重叠点全部进入冲突队列，
  不自动分配；人工解决时只能从该事件的候选区域中选择，无法凭空指派。
* `UNLOCATED`（不在任何区域）在任何规则下都保持无归属，只统计、不乱分。

```bash
python3 -I scripts/pw --db data/pw.db rule propose --on-boundary MANUAL --overlap MANUAL
python3 -I scripts/pw --db data/pw.db rule approve --rule-version 2
python3 -I scripts/pw --db data/pw.db conflict list
python3 -I scripts/pw --db data/pw.db conflict resolve --conflict-id 1 --region-id B
```

## 测试

```bash
python3 -I -m unittest discover -s tests -v   # 27 个用例：几何、版本化、中断、HTTP
```

## 目录

```
pointwise/
  geometry.py    手写平面几何：射线法/线段相交/点线判定/分类/哈希
  storage.py     SQLite schema、版本状态机、事务、作业与报告持久化
  boundaries.py  GeoJSON 校验、重叠审计、影响范围分析、范围推荐
  worker.py      归属引擎、规则解析、可中断/可恢复重算作业
  reports.py     聚合、新旧对比、params_hash 复现
  app.py         用例编排（上传/建作业/激活/批准/中断/接收）
  web/           stdlib HTTP API + 单页控制台（上传/冲突/批准/中断）
  seed_demo.py   确定性演示数据集（含共边、重叠、边界移动、新增区域）
tests/           unittest 套件
docs/            COORDINATES.md（坐标系限制）、ARCHITECTURE.md（一致性设计）
examples/        示例边界与事件文件
```
