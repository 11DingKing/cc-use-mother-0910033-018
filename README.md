# 生产领料预留

生产订单下达后，计划员需要为一张订单上的多种物料同时预留库存。本项目在领域契约之外提供一个**零外部依赖的 Python 服务端**，解决批次抢占、部分齐套决策、保质/有效期、预留优先级、订单缩减与替代料、超时释放、并发数量守恒以及崩溃后孤儿预留清理等问题。

## 领域约定

- `domain/contract.json`：角色（采购计划员、供应商、质量工程师、仓储管理员）、状态与关键不变量。
- 状态流转：草拟 → 待确认 → 已下达 → 履行中 → 已关闭。
- 关键不变量：**多物料原子预留、批次选择策略、数量守恒校验、孤儿预留恢复**。

## 目录

- `domain/contract.json`：领域角色、状态、约束和样例。
- `src/domain_contract/`：契约读取与确定性校验。
- `src/reservation_service/`：预留服务端
  - `models.py`：需求、需求行、库存批次、预留、缺料与调整方案模型；
  - `store.py`：SQLite 存储（组头 + 明细、幂等键、守恒查询）；
  - `service.py`：核心领域服务（分配引擎、原子确认、缩减、替代、超时、恢复）；
  - `api.py`：标准库 HTTP 接口；`__main__.py` 为启动入口。
- `tools/check_contract.py`：契约命令行检查。
- `tests/`：契约、领域与 HTTP 集成回归测试。

## 核心设计

### 1. 数量守恒

- 数量一律使用最小计量单位的**整数**；
- 批次在库量不被预留直接改写，有效预留量由 `reservations` 中 `tentative/held` 状态求和得到，满足
  `Σ 有效预留(batch) ≤ qty_on_hand(batch)`；
- 每个写事务在提交前执行守恒断言，违例直接回滚，绝不留下半成品；
- 发料出库时在库量与预留量同一事务内一起下降。

### 2. 批次选择与保质/有效期

- FEFO（先到期先出）：同物料按 `expiry_date` 升序分配，无到期日的排最后；
- 质检隔离（`quarantined`）批次不可预留；
- 早于需求 `deadline` 到期的批次视为不可用，缺料原因中明确给出隔离量与临期量。

### 3. 齐套策略

| 策略 | 语义 |
| --- | --- |
| `ALL_OR_NOTHING` | 整单齐套。任一物料不足，**所有物料均不写入**，响应在 `all_shortages_blocked` 给出每种缺料的原因与调整方案。 |
| `PARTIAL` | 部分齐套。能留多少留多少，成功行落库，缺料行标记"缺料"，订单仍下达。 |
| `PARTIAL_PENDING` | 部分齐套但挂起。成功行落库，缺料行等待计划员后续决策（释放/替代/补货），订单保持"待确认"。 |

### 4. 原子多物料预留

- 一次确认生成一个预留组（`reservation_groups`）和多条批次级预留明细；
- 组头先以 `complete=0` 写入，全部明细写完且守恒断言通过后再置 `complete=1`，单事务提交；
- 重新确认会在同一事务内先释放该需求旧的有效组再写新组，避免重复占用。

### 5. 缺料原因、占用来源与调整方案

试算/确认失败响应包含：

- `shortages[].reasons`：硬缺口、被占用、隔离、临期等原因；
- `shortages[].occupied_by`：当前占用相关批次的订单、优先级、批次、数量、是否可被本单抢占（`preemptible`）；
- `adjustments[]`：按收益排序的可行方案：
  - `release_lower_priority`：释放/抢占低优先级订单（标记需要审批）；
  - `substitute`：使用**已批准**的替代料（带转换比）；
  - `reduce_qty`：按 PARTIAL 先预留可行量；
  - `wait_supply`：内部调剂无法覆盖时等待补货。

### 6. 订单缩减、替代料与超时

- `reduce`：新数量不得低于已发料量；多出预留立即释放，释放时优先从最晚到期批次拿回，保证 FEFO 库存留给其余需求；
- `substitutes`：替代料必须显式批准并登记转换比（1 主料 = ratio 替代料），未批准的替代在试算阶段即被拒绝；
- 临时预占（`tentative=true, ttl_seconds`）与带 TTL 的预留到期后由 `sweep-timeouts` 自动释放；批次跨过有效期也会触发释放并把订单打回"待确认"；
- 临时预占可通过 `commit-hold` 转正式，超时后转正式被拒绝。

### 7. 并发预留

- 全部写事务使用 `BEGIN IMMEDIATE` 并在同一把服务锁内串行化，SQLite 文件库额外启用 WAL；
- 确认支持 `Idempotency-Key` 请求头（或参数），同一键的重试返回首次结果，不重复占用；
- 需求带 `lock_version` 乐观版本，带 `expected_version` 的确认在需求已被他人变更时返回 `VERSION_CONFLICT`。

### 8. 孤儿预留恢复

`POST /internal/recover-orphans` 清理：

1. 有明细但组头丢失（`orphan_group_missing`）；
2. 组头 `complete=0` 且超过宽限期（默认 300 秒，`force=true` 可立即清理）；
3. 需求或批次实体已不存在；
4. 外部数据损坏导致的批次超占：先释放临时预占，再按组从年轻到年老裁剪，直至守恒恢复。

## HTTP 接口

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| POST | `/demands` | 创建生产需求（多物料行、优先级、deadline） |
| GET | `/demands` / `/demands/{id}` | 列表 / 详情 |
| POST | `/demands/{id}/plan` | 试算：缺料原因、占用来源、调整方案 |
| POST | `/demands/{id}/confirm` | 原子确认；支持 `policy`、`tentative`、`ttl_seconds`、替代料、`expected_version` 与幂等键 |
| POST | `/demands/{id}/commit-hold` | 临时预占转正式 |
| POST | `/demands/{id}/reduce` | 订单缩减并释放多余预留 |
| POST | `/demands/{id}/substitutes` | 批准替代料（含转换比） |
| POST | `/demands/{id}/issue` | 发料扣减（held → consumed，在库同步下降） |
| POST | `/demands/{id}/release` / `/close` | 释放预留 / 关闭订单 |
| POST/GET | `/batches` | 登记批次（数量、效期、隔离标记） |
| GET | `/occupancy?material_id=` | 批次占用来源视图 |
| POST | `/internal/sweep-timeouts` | 超时与批次过期释放 |
| POST | `/internal/recover-orphans` | 孤儿预留恢复 |

## 快速开始

```bash
# 启动（默认 0.0.0.0:8080，数据文件 data/reservation.db）
PYTHONPATH=src python3 -m reservation_service
# 可选环境变量通过参数调整：run(host, port, db_path)
```

示例：

```bash
curl -s -X POST localhost:8080/batches -H 'Content-Type: application/json' \
  -d '{"batch_id":"B1","material_id":"M1","qty_on_hand":100,"expiry_date":"2026-12-01"}'
curl -s -X POST localhost:8080/demands -H 'Content-Type: application/json' \
  -d '{"demand_id":"D1","product":"P1","priority":5,"deadline":"2026-11-01",
       "lines":[{"material_id":"M1","qty":120}]}'
curl -s -X POST localhost:8080/demands/D1/plan -H 'Content-Type: application/json' \
  -d '{"policy":"ALL_OR_NOTHING"}'
```

## 验证

```bash
python3 -m unittest discover -s tests -v     # 30 项契约/领域/HTTP 测试
python3 -m compileall -q src tools tests     # 编译检查
python3 tools/check_contract.py domain/contract.json
```
