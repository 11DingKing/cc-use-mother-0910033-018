# 生产领料预留

本项目维护生产领料预留的领域约定、角色边界与样例数据，供后端服务、接口和自动化验证统一使用。当前契约覆盖采购计划员、供应商、质量工程师、仓储管理员，并明确**多物料原子预留、批次选择策略、数量守恒校验、孤儿预留恢复**等关键约束。

## 目录

- `domain/contract.json`：领域角色、状态、约束和样例。
- `src/domain_contract/`：契约读取与确定性校验。
- `src/reservation/`：生产领料预留服务端（零第三方依赖，仅需 Python ≥3.11）。
  - `models.py`：订单/预留状态机、齐套策略、批次策略、缺料原因码、释放原因。
  - `store.py`：SQLite 存储（WAL、每线程连接、守恒锚点与流水）。
  - `allocation.py`：多物料齐套分配引擎（纯函数：FEFO/FIFO、替代料、抢占、缺料诊断）。
  - `service.py`：领域服务（事务边界、原子确认、缩减、抢占、超时、孤儿恢复、守恒断言）。
  - `server.py`：JSON over HTTP 接口（`http.server.ThreadingHTTPServer`）。
- `tools/check_contract.py`：命令行摘要检查。
- `tests/`：契约完整性与预留领域回归测试。

## 验证

测试命令：`python3 -m unittest discover -s tests -v`

编译命令：`python3 -m compileall -q src tools tests`

命令行检查：`python3 tools/check_contract.py domain/contract.json`

## 运行服务端

```bash
PYTHONPATH=src python3 -m reservation.server --host 0.0.0.0 --port 8080 --db data/reservation.db
```

## 核心设计

### 数量守恒与并发

- 批次 `qty_on_hand`（在库锚点，不可被预留逻辑改写）与 `qty_reserved`（活跃占用合计）；
  数据库约束 `0 <= qty_reserved <= qty_on_hand`。
- 所有写操作在单个 `BEGIN IMMEDIATE` 事务内：**拍快照 → 纯函数分配 → 抢占/释放/写入 →
  守恒断言 → 提交**，任一环节失败整体回滚。多物料预留因此原子生效。
- WAL + `busy_timeout` + 每线程连接：并发确认时第二个写者在锁上等待，拿到锁后读到最新
  `qty_reserved`，不会超卖。`ledger` 记录每次占有/释放流水，可与当前预留量对账。

### 两阶段预留与齐套策略

- `hold`：临时持有（HELD，带 TTL），供计划员锁料后再确认；超时由 `sweep/expired` 释放。
- `confirm`：写入正式预留（CONFIRMED），支持
  - `ALL_OR_NOTHING`：任一物料不齐套则整单回滚（默认）；
  - `PARTIAL`：能备多少备多少，返回每行缺口与建议。

### 批次、保质期与优先级

- 批次策略 `FEFO`（先到期先出，默认）/ `FIFO`；在需求基准日之前到期的批次不参与分配，
  质量冻结批次同样排除，并在缺料原因中给出 `EXPIRED_STOCK` / `BLOCKED_LOT`。
- 高优先级订单确认时可 `preempt=true` 抢占**严格更低优先级**订单的占用（同优先级不抢），
  被抢方预留释放并记录 `preempted_from` 占用来源。
- 替代料按 BOM 行维护比例（1 单位主料消耗 `ratio` 单位替代料）；未批准时不参与分配，
  诊断返回 `APPROVE_SUBSTITUTE` 建议及可覆盖缺口量。

### 订单缩减、超时与恢复

- `reduce` 只减不增（增购需重新确认），按主料单位折算出多余占用后守恒释放，优先保留
  临期早的批次。
- `sweep/expired` 清扫过期 HELD（TTL）与 CONFIRMED（订单截止期）；全部预留超时的订单
  回到草拟。
- `recover/orphans` 清理孤儿预留（订单缺失、原子分组缺失/已释放），释放数量并对账。

## 接口一览

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| POST | `/orders` | 创建订单（`demands` 多物料需求、`priority`、`due_date`） |
| GET  | `/orders/{id}` | 需求、覆盖率、活跃预留、释放/出库历史 |
| POST | `/materials` / `/batches` | 主数据；批次含在库量、入库时间、到期日 |
| POST | `/batches/{id}/block` | 质量冻结/解冻 |
| POST | `/substitutes/approve` | 批准/撤销替代料（`ratio`） |
| POST | `/orders/{id}/plan` | 只读诊断：缺料原因、占用来源、可行调整方案 |
| POST | `/orders/{id}/hold` | 临时持有（`ttl_seconds`、`strategy`、`preempt`） |
| POST | `/orders/{id}/confirm` | 原子确认（`policy`、`strategy`、`preempt`） |
| POST | `/orders/{id}/reduce` | 订单缩减（只减不增，守恒释放） |
| POST | `/orders/{id}/issue` | 领料出库（在库与预留同步扣减） |
| POST | `/orders/{id}/complete` / `/close` | 完工释放尾量 / 关闭 |
| POST | `/sweep/expired` | 超时释放清扫（可由定时器调用） |
| POST | `/recover/orphans` | 孤儿预留恢复任务 |
| GET  | `/conservation` | 数量守恒对账 |
| GET  | `/batches?material=` | 批次库存与当前占用者 |

`plan` / `confirm` 返回的每个需求行包含：

- `shortage_reasons`：`INSUFFICIENT_TOTAL` / `RESERVED_BY_OTHERS` / `EXPIRED_STOCK` /
  `BLOCKED_LOT` / `SUBSTITUTE_NOT_APPROVED`；
- `occupied_by`：占用来源（订单、优先级、状态、批次、数量）；
- `suggestions`：可行调整方案（批准替代料、抢占低优先级、部分确认及可满足数量）。

### 快速体验

```bash
curl -s -XPOST localhost:8080/batches -d '{"batch_id":"B1","material":"M1","qty_on_hand":100,"received_at":"2026-09-01","expiry_date":"2026-11-01"}'
curl -s -XPOST localhost:8080/orders -d '{"order_id":"A","priority":5,"demands":[{"material":"M1","qty_required":80}]}'
curl -s -XPOST localhost:8080/orders/A/plan
curl -s -XPOST localhost:8080/orders/A/confirm
```
