"""生产领料预留核心服务。

职责：
- 管理生产需求、库存批次（含保质/有效期）与预留优先级；
- FEFO 批次选择 + 齐套策略试算，返回缺料原因、占用来源与可行调整方案；
- 确认时在单事务内原子写入多物料预留，并做数量守恒断言；
- 订单缩减、替代料批准、超时/过期释放、并发预留均在串行写事务内完成；
- 恢复任务清理孤儿预留与超占。
"""
from __future__ import annotations

import json
import uuid
from collections import defaultdict
from datetime import date, datetime, timezone
from typing import Any, Callable, Optional

from .errors import ConflictError, DomainError, InvariantViolation, NotFoundError
from .store import ACTIVE_STATES, Store

ACTIVE = tuple(ACTIVE_STATES)


class ReservationService:
    def __init__(self, store: Store, *, clock: Optional[Callable[[], float]] = None,
                 orphan_grace_seconds: float = 300.0) -> None:
        self.store = store
        self._clock = clock or _utc_now
        self.orphan_grace_seconds = orphan_grace_seconds

    # ===================================================================
    # 基础工具
    # ===================================================================

    def now(self) -> float:
        return self._clock()

    def today(self):
        return datetime.fromtimestamp(self.now(), tz=timezone.utc).date()

    def _new_id(self, prefix: str) -> str:
        return f"{prefix}_{uuid.uuid4().hex[:16]}"

    def _txn(self):
        conn = self.store.conn()
        self.store.begin_immediate(conn)
        return conn

    @staticmethod
    def _commit(conn) -> None:
        conn.execute("COMMIT")

    def _assert_conservation(self, conn) -> None:
        """关键不变量：每个批次的有效预留之和不得超过在库量。"""
        on_hand = self.store.on_hand_by_batch(conn)
        for batch_id, used in self.store.active_totals_by_batch(conn).items():
            if used > on_hand.get(batch_id, 0):
                raise InvariantViolation(
                    f"数量守恒被破坏：批次 {batch_id} 预留 {used} > 在库 {on_hand.get(batch_id, 0)}"
                )

    # ===================================================================
    # 需求与批次
    # ===================================================================

    def create_demand(self, payload: dict[str, Any]) -> dict[str, Any]:
        demand_id = payload.get("demand_id") or self._new_id("dmd")
        lines = self._validate_lines(payload.get("lines", []))
        demand = {
            "demand_id": demand_id,
            "product": payload["product"],
            "priority": int(payload.get("priority", 0)),
            "status": "草拟",
            "created_at": _iso(self.now()),
            "deadline": payload.get("deadline"),
            "lock_version": 0,
            "hold_ttl_seconds": payload.get("hold_ttl_seconds"),
            "lines": lines,
        }
        with self.store.lock:
            conn = self._txn()
            try:
                if self.store.get_demand(conn, demand_id) is not None:
                    raise ConflictError("DEMAND_EXISTS", f"需求已存在：{demand_id}")
                self.store.insert_demand(conn, demand)
                self._commit(conn)
            except Exception:
                conn.execute("ROLLBACK")
                raise
        return self.get_demand(demand_id)

    @staticmethod
    def _validate_lines(raw: list[dict[str, Any]]) -> list[dict[str, Any]]:
        if not raw:
            raise DomainError("EMPTY_LINES", "需求至少包含一个物料行")
        lines = []
        seen = set()
        for item in raw:
            mid = item["material_id"]
            qty = int(item["qty"])
            if mid in seen:
                raise DomainError("DUP_LINE", f"物料行重复：{mid}")
            if qty <= 0:
                raise DomainError("BAD_QTY", f"物料 {mid} 数量必须为正整数")
            seen.add(mid)
            subs = item.get("substitutes", {})
            for sub_id, ratio in subs.items():
                if int(ratio) <= 0:
                    raise DomainError("BAD_RATIO", f"替代料 {sub_id} 转换比必须为正整数")
            lines.append({
                "material_id": mid,
                "qty": qty,
                "substitutes": {k: int(v) for k, v in subs.items()},
                "status": "需预留",
                "shortage_reason": None,
                "use_substitute": item.get("use_substitute"),
            })
        return lines

    def get_demand(self, demand_id: str) -> dict[str, Any]:
        with self.store.lock:
            conn = self.store.conn()
            demand = self.store.get_demand(conn, demand_id)
            if demand is None:
                raise NotFoundError("需求", demand_id)
            demand["active_groups"] = self.store.list_groups(conn, demand_id)
            return demand

    def list_demands(self) -> list[dict[str, Any]]:
        with self.store.lock:
            return self.store.list_demands(self.store.conn())

    def register_batch(self, payload: dict[str, Any]) -> dict[str, Any]:
        batch = {
            "batch_id": payload["batch_id"],
            "material_id": payload["material_id"],
            "qty_on_hand": int(payload["qty_on_hand"]),
            "expiry_date": _date(payload.get("expiry_date")),
            "received_date": _date(payload.get("received_date")) or self.today(),
            "shelf_life_days": payload.get("shelf_life_days"),
            "location": payload.get("location", ""),
            "quarantined": bool(payload.get("quarantined", False)),
        }
        if batch["qty_on_hand"] < 0:
            raise DomainError("BAD_QTY", "在库数量不能为负")
        with self.store.lock:
            conn = self._txn()
            try:
                self.store.upsert_batch(conn, batch)
                self._assert_conservation(conn)
                self._commit(conn)
            except Exception:
                conn.execute("ROLLBACK")
                raise
        return self.get_batch(batch["batch_id"])

    def get_batch(self, batch_id: str) -> dict[str, Any]:
        with self.store.lock:
            for b in self.store.get_batches(self.store.conn()):
                if b["batch_id"] == batch_id:
                    return b
        raise NotFoundError("批次", batch_id)

    def list_batches(self, material_id: Optional[str] = None) -> list[dict[str, Any]]:
        with self.store.lock:
            return self.store.get_batches(self.store.conn(), material_id)

    # ===================================================================
    # 库存快照与分配引擎
    # ===================================================================

    def _snapshot(self, conn, *, ignore_demand_id: Optional[str] = None) -> dict[str, Any]:
        batches = self.store.get_batches(conn)
        demands = {d["demand_id"]: d for d in self.store.list_demands(conn)}
        reserved: dict[str, int] = defaultdict(int)
        reservations_by_batch: dict[str, list[dict]] = defaultdict(list)
        for r in self.store.list_reservations(conn, states=ACTIVE):
            if ignore_demand_id and r["demand_id"] == ignore_demand_id:
                continue
            reserved[r["batch_id"]] += r["qty"]
            reservations_by_batch[r["batch_id"]].append(r)
        return {
            "batches": batches,
            "demands": demands,
            "reserved": reserved,
            "reservations_by_batch": reservations_by_batch,
        }

    def _eligible_batches(self, material_id: str, need_date, snap: dict[str, Any]):
        """返回可用于该物料的批次，按 FEFO（最早到期优先）排序。"""
        out = []
        for b in snap["batches"]:
            if b["material_id"] != material_id or b["quarantined"]:
                continue
            if b["expiry_date"] is not None and b["expiry_date"] < need_date:
                continue
            out.append(b)
        out.sort(key=lambda b: (
            b["expiry_date"] is None, b["expiry_date"] or _FAR_FUTURE,
            b["received_date"] or _FAR_FUTURE, b["batch_id"],
        ))
        return out

    def _simulate_line(self, line: dict[str, Any], demand: dict[str, Any],
                       snap: dict[str, Any]) -> dict[str, Any]:
        use_sub = line.get("use_substitute")
        ratio = 1
        if use_sub:
            if use_sub not in line.get("substitutes", {}):
                raise DomainError(
                    "SUBSTITUTE_NOT_APPROVED",
                    f"物料 {line['material_id']} 使用替代料 {use_sub} 尚未批准",
                )
            ratio = line["substitutes"][use_sub]
        material_id = use_sub or line["material_id"]
        required = line["qty"] * ratio
        need_date = _date(demand.get("deadline")) or self.today()

        eligible = self._eligible_batches(material_id, need_date, snap)
        allocated: list[dict[str, Any]] = []
        remaining = required
        blocked_reasons: set[str] = set()

        all_for_material = [b for b in snap["batches"] if b["material_id"] == material_id]
        quarantined = sum(b["qty_on_hand"] for b in all_for_material if b["quarantined"])
        expired = sum(
            b["qty_on_hand"] for b in all_for_material
            if not b["quarantined"] and b["expiry_date"] is not None
            and b["expiry_date"] < need_date
        )
        if not all_for_material:
            blocked_reasons.add("无任何库存批次")
        if quarantined:
            blocked_reasons.add(f"质检隔离 {quarantined}（不可预留）")
        if expired:
            blocked_reasons.add(f"保质期不满足需求截止日 {need_date}：{expired} 已过期/临期")

        for b in eligible:
            avail = b["qty_on_hand"] - snap["reserved"].get(b["batch_id"], 0)
            if avail <= 0:
                continue
            take = min(avail, remaining)
            allocated.append({"batch_id": b["batch_id"], "material_id": material_id, "qty": take})
            remaining -= take
            if remaining == 0:
                break

        available_total = required - remaining
        result = {
            "line_material_id": line["material_id"],
            "material_id": material_id,
            "substitution_of": line["material_id"] if use_sub else None,
            "required": required,
            "allocated": allocated,
            "allocated_qty": available_total,
            "short": remaining,
            "reasons": sorted(blocked_reasons),
        }
        if remaining:
            shortage = self._build_shortage(
                line, material_id, required, available_total, remaining,
                need_date, snap, demand,
            )
            # 合并模拟阶段发现的隔离/临期原因，保证接口缺料原因完整
            shortage["reasons"] = list(dict.fromkeys(
                shortage["reasons"] + sorted(blocked_reasons)))
            result["shortage"] = shortage
        return result

    def _build_shortage(self, line, material_id, required, available, gap,
                        need_date, snap, demand) -> dict[str, Any]:
        # 占用来源：当前在该物料有效批次上的全部有效预留
        occupied_by: list[dict[str, Any]] = []
        lower_priority_gain = 0
        for b in self._eligible_batches(material_id, need_date, snap):
            for r in snap["reservations_by_batch"].get(b["batch_id"], []):
                if r["material_id"] != material_id:
                    continue
                owner = snap["demands"].get(r["demand_id"], {})
                preemptible = owner.get("priority", 1 << 30) < demand["priority"]
                occupied_by.append({
                    "demand_id": r["demand_id"],
                    "owner_priority": owner.get("priority"),
                    "group_id": r["group_id"],
                    "batch_id": b["batch_id"],
                    "batch_expiry": b["expiry_date"].isoformat() if b["expiry_date"] else None,
                    "qty": r["qty"],
                    "state": r["state"],
                    "preemptible": preemptible,
                })
                if preemptible:
                    lower_priority_gain += r["qty"]

        shortage_reasons: list[str] = []
        on_hand_eligible = sum(
            b["qty_on_hand"] for b in self._eligible_batches(material_id, need_date, snap)
        )
        occupied_total = sum(o["qty"] for o in occupied_by)
        if on_hand_eligible - occupied_total < gap and occupied_total:
            shortage_reasons.append(f"有效库存已被其他订单占用 {occupied_total}")
        if on_hand_eligible < required:
            shortage_reasons.append(
                f"满足效期的在库总量 {on_hand_eligible} < 需求 {required}，存在硬缺口 {required - on_hand_eligible}"
            )

        options: list[dict[str, Any]] = []
        # 方案 1：抢占/释放低优先级订单
        lower = [o for o in occupied_by if o["preemptible"]]
        if lower:
            by_owner: dict[str, int] = defaultdict(int)
            for o in lower:
                by_owner[o["demand_id"]] += o["qty"]
            for owner_id, gain in sorted(by_owner.items(), key=lambda kv: -kv[1]):
                options.append({
                    "type": "release_lower_priority",
                    "description": f"释放低优先级订单 {owner_id} 对 {material_id} 的占用",
                    "material_id": material_id,
                    "target_demand_id": owner_id,
                    "gain_qty": min(gain, gap),
                    "requires_approval": True,
                })
        # 方案 2：使用已批准替代料
        for sub_id, ratio in line.get("substitutes", {}).items():
            sub_required = line["qty"] * ratio
            sub_batches = self._eligible_batches(sub_id, need_date, snap)
            sub_avail = sum(
                b["qty_on_hand"] - snap["reserved"].get(b["batch_id"], 0) for b in sub_batches
            )
            if sub_avail > 0:
                options.append({
                    "type": "substitute",
                    "description": f"以已批准替代料 {sub_id} 顶替（转换比 1:{ratio}，需 {sub_required}，可用 {sub_avail}）",
                    "material_id": material_id,
                    "substitute_material_id": sub_id,
                    "gain_qty": min(line["qty"], sub_avail // ratio),
                    "requires_approval": False,
                })
        # 方案 3：部分齐套 / 缩减数量
        if available > 0:
            options.append({
                "type": "reduce_qty",
                "description": f"按 PARTIAL 策略先预留可行量 {available}，缺口 {gap} 待补货",
                "material_id": material_id,
                "gain_qty": 0,
                "requires_approval": False,
            })
        # 方案 4：硬缺口只能等待补货
        recoverable = sum(o["gain_qty"] for o in options if o["type"] != "wait_supply")
        if recoverable < gap:
            options.append({
                "type": "wait_supply",
                "description": f"{material_id} 内部调剂后仍缺 {gap - recoverable}，需采购/补货后重试",
                "material_id": material_id,
                "gain_qty": 0,
                "requires_approval": False,
            })

        return {
            "material_id": material_id,
            "required": required,
            "available": available,
            "gap": gap,
            "reasons": shortage_reasons,
            "occupied_by": sorted(occupied_by, key=lambda o: (-o["qty"], o["demand_id"])),
            "adjustments": options,
        }

    # ===================================================================
    # 试算（不写库）
    # ===================================================================

    def plan(self, demand_id: str, options: Optional[dict[str, Any]] = None) -> dict[str, Any]:
        options = options or {}
        with self.store.lock:
            conn = self.store.conn()
            demand = self.store.get_demand(conn, demand_id)
            if demand is None:
                raise NotFoundError("需求", demand_id)
            lines = self._apply_line_options(demand["lines"], options)
            snap = self._snapshot(conn, ignore_demand_id=demand_id)
            return self._plan_result(demand, lines, options, snap)

    @staticmethod
    def _apply_line_options(lines: list[dict[str, Any]], options: dict[str, Any]) -> list[dict[str, Any]]:
        chosen = options.get("use_substitutes", {})
        out = []
        for line in lines:
            line = dict(line)
            if line["material_id"] in chosen:
                line["use_substitute"] = chosen[line["material_id"]]
            out.append(line)
        return out

    def _plan_result(self, demand, lines, options: dict[str, Any], snap) -> dict[str, Any]:
        policy = options.get("policy", "ALL_OR_NOTHING")
        results = [self._simulate_line(line, demand, snap) for line in lines]
        shortages = [r["shortage"] for r in results if r.get("shortage")]
        all_feasible = not shortages
        blocked = policy == "ALL_OR_NOTHING" and not all_feasible
        line_dtos = []
        for r in results:
            line_dtos.append({
                "line_material_id": r["line_material_id"],
                "material_id": r["material_id"],
                "required": r["required"],
                # AON 缺料时整单不写入，所有行预留量均显示为 0
                "allocated_qty": 0 if blocked else r["allocated_qty"],
                "allocation": [] if blocked else r["allocated"],
                "short": r["short"],
            })
        adjustments = [adj for s in shortages for adj in s["adjustments"]]
        return {
            "demand_id": demand["demand_id"],
            "policy": policy,
            "feasible": all_feasible,
            "will_reserve": not blocked and any(r["allocated_qty"] > 0 for r in results),
            "lines": line_dtos,
            "shortages": [] if blocked else shortages,
            "all_shortages_blocked": shortages if blocked else [],
            "adjustments": _dedup_options(adjustments),
            "planned_at": _iso(self.now()),
        }

    # ===================================================================
    # 确认：原子写入多物料预留
    # ===================================================================

    def confirm(self, demand_id: str, options: Optional[dict[str, Any]] = None,
                *, idem_key: Optional[str] = None) -> dict[str, Any]:
        options = options or {}
        policy = options.get("policy", "ALL_OR_NOTHING")
        if policy not in ("ALL_OR_NOTHING", "PARTIAL", "PARTIAL_PENDING"):
            raise DomainError("BAD_POLICY", f"未知齐套策略：{policy}")
        tentative = bool(options.get("tentative", False))
        ttl = options.get("ttl_seconds")
        with self.store.lock:
            conn = self._txn()
            try:
                if idem_key:
                    cached = self.store.get_idem(conn, f"confirm:{idem_key}")
                    if cached is not None:
                        self._commit(conn)
                        return json.loads(cached)
                demand = self.store.get_demand(conn, demand_id)
                if demand is None:
                    raise NotFoundError("需求", demand_id)
                if demand["status"] == "已关闭":
                    raise ConflictError("DEMAND_CLOSED", "需求已关闭，不能再预留")
                if demand["status"] == "履行中":
                    raise ConflictError(
                        "DEMAND_FULFILLING",
                        "需求已进入履行（存在发料），不能重新确认；如需调整请使用缩减接口",
                    )
                # 重新校验乐观版本，防止并发编辑后用旧决策确认
                expected_version = options.get("expected_version")
                if expected_version is not None and int(expected_version) != demand["lock_version"]:
                    raise ConflictError(
                        "VERSION_CONFLICT",
                        f"需求已被变更（版本 {demand['lock_version']} != {expected_version}），请重新试算",
                    )

                lines = self._apply_line_options(demand["lines"], options)
                for line in lines:
                    if line.get("use_substitute"):
                        sub = line["use_substitute"]
                        if sub not in line.get("substitutes", {}):
                            raise DomainError(
                                "SUBSTITUTE_NOT_APPROVED",
                                f"替代料 {sub} 未批准，不能用于 {line['material_id']}",
                            )
                snap = self._snapshot(conn, ignore_demand_id=demand_id)
                results = [self._simulate_line(line, demand, snap) for line in lines]
                shortages = [r["shortage"] for r in results if r.get("shortage")]

                if policy == "ALL_OR_NOTHING" and shortages:
                    self._mark_shortage_lines(conn, demand_id, results)
                    version = self.store.bump_demand(conn, demand_id, "待确认")
                    response = self._confirm_failure(demand, results, shortages, version)
                    self._store_idem_and_commit(conn, idem_key, response, self.now())
                    return response

                to_write = [r for r in results if r["allocated_qty"] > 0]
                if not to_write:
                    self._mark_shortage_lines(conn, demand_id, results)
                    version = self.store.bump_demand(conn, demand_id, "待确认")
                    response = self._confirm_failure(demand, results, shortages, version)
                    self._store_idem_and_commit(conn, idem_key, response, self.now())
                    return response

                # 替换该需求旧的有效预留组（缩减/重算场景），先释放再写入，全部同事务
                self._release_demand_active(conn, demand_id, reason="replaced_by_reconfirm")

                now = self.now()
                expires_at = now + float(ttl) if ttl else None
                group = {
                    "group_id": self._new_id("grp"),
                    "demand_id": demand_id,
                    "policy": policy,
                    "state": "tentative" if tentative else "held",
                    "created_at": now,
                    "expires_at": expires_at,
                }
                self.store.insert_group(conn, group, complete=False)
                for r in to_write:
                    for part in r["allocated"]:
                        self.store.insert_reservation(conn, {
                            "reservation_id": self._new_id("rsv"),
                            "group_id": group["group_id"],
                            "demand_id": demand_id,
                            "line_material_id": r["line_material_id"],
                            "material_id": part["material_id"],
                            "batch_id": part["batch_id"],
                            "qty": part["qty"],
                            "state": "tentative" if tentative else "held",
                            "tentative": tentative,
                            "created_at": now,
                            "expires_at": expires_at,
                            "substitution_of": r["substitution_of"],
                        })
                # 守恒断言在提交前：失败则整体回滚，绝不留半成品
                self._assert_conservation(conn)
                self.store.set_group_state(
                    conn, group["group_id"],
                    "tentative" if tentative else "held",
                    complete=True, expires_at=expires_at,
                )
                self._sync_line_status(conn, demand_id, results, policy)
                if tentative:
                    new_status = "待确认"
                elif policy == "PARTIAL_PENDING" and shortages:
                    # 部分齐套已落库，缺料行挂起，等待计划员决定释放/补货/替代
                    new_status = "待确认"
                else:
                    new_status = "已下达"
                version = self.store.bump_demand(
                    conn, demand_id, new_status,
                    hold_ttl=ttl if ttl is not None else demand.get("hold_ttl_seconds"),
                )
                self._assert_conservation(conn)
                response = {
                    "demand_id": demand_id,
                    "group_id": group["group_id"],
                    "policy": policy,
                    "state": group["state"],
                    "complete": True,
                    "lock_version": version,
                    "expires_at": expires_at,
                    "reserved": [
                        {
                            "line_material_id": r["line_material_id"],
                            "material_id": r["material_id"],
                            "required": r["required"],
                            "reserved_qty": r["allocated_qty"],
                            "short": r["short"],
                            "batches": r["allocated"],
                        }
                        for r in results
                    ],
                    "shortages": shortages,
                    "adjustments": _dedup_options(
                        [adj for s in shortages for adj in s["adjustments"]]
                    ),
                    "confirmed_at": _iso(now),
                }
                if idem_key:
                    self.store.put_idem(conn, f"confirm:{idem_key}", json.dumps(response, ensure_ascii=False), now)
                self._commit(conn)
                return response
            except Exception:
                conn.execute("ROLLBACK")
                raise

    def _store_idem_and_commit(self, conn, idem_key, response: dict, now: float) -> None:
        if idem_key:
            self.store.put_idem(
                conn, f"confirm:{idem_key}",
                json.dumps(response, ensure_ascii=False), now,
            )
        self._commit(conn)

    def _confirm_failure(self, demand, results, shortages, version: int) -> dict[str, Any]:
        return {
            "demand_id": demand["demand_id"],
            "group_id": None,
            "complete": False,
            "reserved": [],
            "shortages": shortages,
            "all_shortages_blocked": shortages,
            "adjustments": _dedup_options(
                [adj for s in shortages for adj in s["adjustments"]]
            ),
            "lock_version": version,
            "message": "齐套策略 ALL_OR_NOTHING：存在缺料行，整单未写入任何预留",
            "confirmed_at": _iso(self.now()),
        }

    def _mark_shortage_lines(self, conn, demand_id: str, results: list[dict]) -> None:
        for r in results:
            if r.get("shortage"):
                reason = "；".join(r["shortage"]["reasons"] + r["reasons"]) or "可用库存不足"
                self.store.update_line(
                    conn, demand_id, r["line_material_id"],
                    status="缺料", shortage_reason=reason,
                )
            else:
                self.store.update_line(
                    conn, demand_id, r["line_material_id"],
                    status="需预留", shortage_reason=None,
                )

    def _sync_line_status(self, conn, demand_id: str, results: list[dict], policy: str) -> None:
        for r in results:
            if r["short"]:
                reason = "；".join(r["shortage"]["reasons"] + r["reasons"]) or "可用库存不足"
                self.store.update_line(
                    conn, demand_id, r["line_material_id"],
                    status="缺料", shortage_reason=reason,
                )
            elif r["substitution_of"]:
                self.store.update_line(
                    conn, demand_id, r["line_material_id"],
                    status="替代料", shortage_reason=None,
                    use_substitute=r["material_id"],
                )
            else:
                self.store.update_line(
                    conn, demand_id, r["line_material_id"],
                    status="已预留", shortage_reason=None,
                )

    # ===================================================================
    # 临时预占的正式确认
    # ===================================================================

    def commit_hold(self, demand_id: str, group_id: str) -> dict[str, Any]:
        with self.store.lock:
            conn = self._txn()
            try:
                demand = self.store.get_demand(conn, demand_id)
                if demand is None:
                    raise NotFoundError("需求", demand_id)
                group = self.store.get_group(conn, group_id)
                if group is None or group["demand_id"] != demand_id:
                    raise NotFoundError("预留组", group_id)
                rows = self.store.list_reservations(conn, group_id=group_id, states=ACTIVE)
                if not rows or group["state"] != "tentative":
                    raise ConflictError("HOLD_NOT_ACTIVE", "预占不存在、已确认或已释放")
                if group["expires_at"] and self.now() > group["expires_at"]:
                    raise ConflictError("HOLD_EXPIRED", "预占已超时释放，请重新试算")
                now = self.now()
                for rid in [r["reservation_id"] for r in rows]:
                    conn.execute(
                        "UPDATE reservations SET state='held', tentative=0,"
                        " expires_at=NULL WHERE reservation_id=?", (rid,)
                    )
                self.store.set_group_state(conn, group_id, "held", complete=True, expires_at=None)
                version = self.store.bump_demand(conn, demand_id, "已下达", hold_ttl=None)
                self._assert_conservation(conn)
                self._commit(conn)
                return {"demand_id": demand_id, "group_id": group_id, "state": "held",
                        "lock_version": version, "committed_at": _iso(now)}
            except Exception:
                conn.execute("ROLLBACK")
                raise

    # ===================================================================
    # 订单缩减（数量守恒）
    # ===================================================================

    def reduce_demand(self, demand_id: str, reductions: dict[str, int],
                      expected_version: Optional[int] = None) -> dict[str, Any]:
        """缩减订单数量：new_qty 不得低于已发料量；多出的预留立即释放回库存。"""
        with self.store.lock:
            conn = self._txn()
            try:
                demand = self.store.get_demand(conn, demand_id)
                if demand is None:
                    raise NotFoundError("需求", demand_id)
                if expected_version is not None and int(expected_version) != demand["lock_version"]:
                    raise ConflictError("VERSION_CONFLICT", "需求版本不匹配，请刷新后重试")
                released: list[dict[str, Any]] = []
                now = self.now()
                for line in demand["lines"]:
                    if line["material_id"] not in reductions:
                        continue
                    new_qty = int(reductions[line["material_id"]])
                    if new_qty < 0:
                        raise DomainError("BAD_QTY", "数量不能为负")
                    if new_qty > line["qty"]:
                        raise DomainError(
                            "QTY_INCREASE",
                            f"缩减接口只接受不大于原数量的值：{line['material_id']}",
                        )
                    if new_qty < line["issued_qty"]:
                        raise ConflictError(
                            "BELOW_ISSUED",
                            f"物料 {line['material_id']} 已发料 {line['issued_qty']}，"
                            f"不能缩减到 {new_qty}",
                        )
                    delta_units = line["qty"] - new_qty
                    if delta_units == 0:
                        continue
                    ratio = (line["substitutes"].get(line["use_substitute"], 1)
                             if line.get("use_substitute") else 1)
                    release_qty = delta_units * ratio

                    # 从最晚到期的批次开始释放，尽量保留 FEFO 库存给其余需求
                    rows = [r for r in self.store.list_reservations(
                        conn, demand_id=demand_id, states=ACTIVE)
                        if r["line_material_id"] == line["material_id"]]
                    rows.sort(key=lambda r: self._batch_expiry_key(conn, r["batch_id"]), reverse=True)
                    freed = 0
                    for r in rows:
                        if release_qty <= 0:
                            break
                        cut = min(r["qty"], release_qty)
                        if cut == r["qty"]:
                            self.store.release_reservations(
                                conn, [r["reservation_id"]], reason="demand_reduced")
                        else:
                            conn.execute(
                                "UPDATE reservations SET qty=qty-? WHERE reservation_id=?",
                                (cut, r["reservation_id"]),
                            )
                        release_qty -= cut
                        freed += cut
                    released.append({
                        "material_id": line["material_id"],
                        "old_qty": line["qty"],
                        "new_qty": new_qty,
                        "released_qty": freed,
                    })
                    conn.execute(
                        "UPDATE demand_lines SET qty=?, shortage_reason=NULL,"
                        " status=CASE WHEN ?=0 THEN '已关闭' ELSE status END"
                        " WHERE demand_id=? AND material_id=?",
                        (new_qty, new_qty, demand_id, line["material_id"]),
                    )
                # 空组收尾
                for g in self.store.list_groups(conn, demand_id):
                    if not self.store.list_reservations(conn, group_id=g["group_id"], states=ACTIVE):
                        self.store.set_group_state(conn, g["group_id"], "released")
                self._assert_conservation(conn)
                version = self.store.bump_demand(conn, demand_id)
                self._commit(conn)
                return {"demand_id": demand_id, "lock_version": version,
                        "reductions": released, "reduced_at": _iso(now)}
            except Exception:
                conn.execute("ROLLBACK")
                raise

    def _batch_expiry_key(self, conn, batch_id: str):
        for b in self.store.get_batches(conn):
            if b["batch_id"] == batch_id:
                return (b["expiry_date"] is None, b["expiry_date"] or _FAR_FUTURE)
        return (True, _FAR_FUTURE)

    # ===================================================================
    # 替代料批准
    # ===================================================================

    def approve_substitute(self, demand_id: str, material_id: str,
                           substitute_material_id: str, ratio: int = 1) -> dict[str, Any]:
        if int(ratio) <= 0:
            raise DomainError("BAD_RATIO", "转换比必须为正整数")
        with self.store.lock:
            conn = self._txn()
            try:
                demand = self.store.get_demand(conn, demand_id)
                if demand is None:
                    raise NotFoundError("需求", demand_id)
                line = next((l for l in demand["lines"] if l["material_id"] == material_id), None)
                if line is None:
                    raise NotFoundError("物料行", material_id)
                line["substitutes"][substitute_material_id] = int(ratio)
                self.store.update_line(conn, demand_id, material_id,
                                       substitutes=line["substitutes"])
                version = self.store.bump_demand(conn, demand_id)
                self._commit(conn)
                return {"demand_id": demand_id, "material_id": material_id,
                        "substitute_material_id": substitute_material_id,
                        "ratio": int(ratio), "approved": True,
                        "lock_version": version}
            except Exception:
                conn.execute("ROLLBACK")
                raise

    # ===================================================================
    # 发料扣减（held -> consumed，在库同步下降）
    # ===================================================================

    def issue(self, demand_id: str, items: list[dict[str, Any]]) -> dict[str, Any]:
        with self.store.lock:
            conn = self._txn()
            try:
                demand = self.store.get_demand(conn, demand_id)
                if demand is None:
                    raise NotFoundError("需求", demand_id)
                issued: list[dict[str, Any]] = []
                now = self.now()
                for item in items:
                    mid = item["material_id"]
                    qty = int(item["qty"])
                    if qty <= 0:
                        raise DomainError("BAD_QTY", "发料数量必须为正整数")
                    rows = [r for r in self.store.list_reservations(
                        conn, demand_id=demand_id, states=("held",))
                        if r["line_material_id"] == mid and r["state"] == "held"]
                    if item.get("batch_id"):
                        rows = [r for r in rows if r["batch_id"] == item["batch_id"]]
                    rows.sort(key=lambda r: self._batch_expiry_key(conn, r["batch_id"]))
                    remain = qty
                    parts = []
                    for r in rows:
                        if remain <= 0:
                            break
                        cut = min(r["qty"], remain)
                        if cut == r["qty"]:
                            conn.execute(
                                "UPDATE reservations SET state='consumed'"
                                " WHERE reservation_id=?", (r["reservation_id"],))
                        else:
                            conn.execute(
                                "INSERT INTO reservations(reservation_id, group_id, demand_id,"
                                " line_material_id, material_id, batch_id, qty, state, tentative,"
                                " created_at, substitution_of)"
                                " VALUES (?,?,?,?,?,?,?, 'consumed', 0, ?, ?)",
                                (self._new_id("rsv"), r["group_id"], demand_id,
                                 r["line_material_id"], r["material_id"], r["batch_id"], cut,
                                 now, r["substitution_of"]),
                            )
                            conn.execute(
                                "UPDATE reservations SET qty=qty-? WHERE reservation_id=?",
                                (cut, r["reservation_id"]),
                            )
                        # 在库量随实物出库下降，保持 active <= on_hand
                        conn.execute(
                            "UPDATE batches SET qty_on_hand=qty_on_hand-? WHERE batch_id=?",
                            (cut, r["batch_id"]),
                        )
                        parts.append({"batch_id": r["batch_id"], "qty": cut})
                        remain -= cut
                    if remain:
                        raise ConflictError(
                            "INSUFFICIENT_HELD",
                            f"物料 {mid} 已预留量不足以发料 {qty}，还差 {remain}",
                        )
                    self.store.add_issued_qty(conn, demand_id, mid, qty)
                    issued.append({"material_id": mid, "qty": qty, "batches": parts})
                self.store.bump_demand(conn, demand_id, "履行中")
                self._assert_conservation(conn)
                self._commit(conn)
                return {"demand_id": demand_id, "issued": issued, "issued_at": _iso(now)}
            except Exception:
                conn.execute("ROLLBACK")
                raise

    # ===================================================================
    # 手动释放 / 关闭
    # ===================================================================

    def release_demand(self, demand_id: str, reason: str = "manual") -> dict[str, Any]:
        with self.store.lock:
            conn = self._txn()
            try:
                demand = self.store.get_demand(conn, demand_id)
                if demand is None:
                    raise NotFoundError("需求", demand_id)
                if demand["status"] == "已关闭":
                    raise ConflictError("DEMAND_CLOSED", "需求已关闭，不能释放")
                count = self._release_demand_active(conn, demand_id, reason=reason)
                for line in demand["lines"]:
                    if line["issued_qty"] > 0:
                        continue  # 已发料行保持其履行状态，不回退
                    self.store.update_line(conn, demand_id, line["material_id"],
                                           status="需预留", shortage_reason=None)
                # 已有发料时订单仍处于履行中，只释放剩余预留
                new_status = "履行中" if any(l["issued_qty"] > 0 for l in demand["lines"]) else "待确认"
                version = self.store.bump_demand(conn, demand_id, new_status)
                self._assert_conservation(conn)
                self._commit(conn)
                return {"demand_id": demand_id, "released_rows": count,
                        "lock_version": version, "state": new_status}
            except Exception:
                conn.execute("ROLLBACK")
                raise

    def close_demand(self, demand_id: str) -> dict[str, Any]:
        with self.store.lock:
            conn = self._txn()
            try:
                demand = self.store.get_demand(conn, demand_id)
                if demand is None:
                    raise NotFoundError("需求", demand_id)
                self._release_demand_active(conn, demand_id, reason="demand_closed")
                for line in demand["lines"]:
                    self.store.update_line(conn, demand_id, line["material_id"], status="已关闭")
                version = self.store.bump_demand(conn, demand_id, "已关闭")
                self._assert_conservation(conn)
                self._commit(conn)
                return {"demand_id": demand_id, "lock_version": version, "state": "已关闭"}
            except Exception:
                conn.execute("ROLLBACK")
                raise

    def _release_demand_active(self, conn, demand_id: str, *, reason: str) -> int:
        rows = self.store.list_reservations(conn, demand_id=demand_id, states=ACTIVE)
        self.store.release_reservations(conn, [r["reservation_id"] for r in rows], reason=reason)
        for g in self.store.list_groups(conn, demand_id):
            self.store.set_group_state(conn, g["group_id"], "released")
        return len(rows)

    # ===================================================================
    # 超时释放 / 批次过期
    # ===================================================================

    def sweep_timeouts(self) -> dict[str, Any]:
        """释放超过 TTL 的预占/预留，以及落在已过期批次上的预留。"""
        with self.store.lock:
            conn = self._txn()
            try:
                now = self.now()
                today = self.today()
                timed_out = [r for r in self.store.list_reservations(conn, states=ACTIVE)
                             if r["expires_at"] is not None and r["expires_at"] <= now]
                expired_batch_rows = [
                    r for r in self.store.list_reservations(conn, states=ACTIVE)
                    if self._batch_is_expired(conn, r["batch_id"], today)
                ]
                affected_groups: dict[str, str] = {}
                for r in timed_out:
                    affected_groups[r["group_id"]] = "timeout"
                for r in expired_batch_rows:
                    affected_groups[r["group_id"]] = "batch_expired"

                self.store.release_reservations(
                    conn, [r["reservation_id"] for r in timed_out], reason="timeout")
                self.store.release_reservations(
                    conn, [r["reservation_id"] for r in expired_batch_rows],
                    state="expired", reason="batch_expired")
                touched_demands = set()
                for gid, why in affected_groups.items():
                    g = self.store.get_group(conn, gid)
                    if g:
                        self.store.set_group_state(conn, gid, "released")
                        touched_demands.add(g["demand_id"])
                # 受影响需求回到待确认，行重置为缺料/需预留
                for did in touched_demands:
                    demand = self.store.get_demand(conn, did)
                    if not demand or demand["status"] in ("履行中", "已关闭"):
                        continue
                    active_mids = {r["line_material_id"] for r in
                                   self.store.list_reservations(conn, demand_id=did, states=ACTIVE)}
                    for line in demand["lines"]:
                        if line["material_id"] in active_mids:
                            continue
                        self.store.update_line(
                            conn, did, line["material_id"], status="缺料",
                            shortage_reason="预留超时或批次过期被自动释放",
                        )
                    self.store.bump_demand(conn, did, "待确认")
                self._assert_conservation(conn)
                self._commit(conn)
                return {
                    "swept_at": _iso(now),
                    "timeout_released": len(timed_out),
                    "batch_expired": len(expired_batch_rows),
                    "groups_released": len(affected_groups),
                    "demands_reopened": sorted(touched_demands),
                }
            except Exception:
                conn.execute("ROLLBACK")
                raise

    def _batch_is_expired(self, conn, batch_id: str, today) -> bool:
        for b in self.store.get_batches(conn):
            if b["batch_id"] == batch_id:
                return b["expiry_date"] is not None and b["expiry_date"] < today
        return False

    # ===================================================================
    # 恢复任务：清理孤儿预留
    # ===================================================================

    def recover_orphans(self, *, force: bool = False) -> dict[str, Any]:
        """清理各类孤儿预留：

        1. 预留明细存在但组头缺失（orphan_group_missing）；
        2. 组头 complete=0 且超过宽限期（写入中途崩溃，orphan_incomplete_group）；
        3. 需求或批次已不存在（orphan_demand_missing / orphan_batch_missing）；
        4. 批次超占（守恒被外部破坏）：从最年轻组开始释放直至守恒恢复。
        """
        with self.store.lock:
            conn = self._txn()
            try:
                now = self.now()
                actions: list[dict[str, Any]] = []
                active = self.store.list_reservations(conn, states=ACTIVE)
                demand_ids = {d["demand_id"] for d in self.store.list_demands(conn)}
                batch_ids = {b["batch_id"] for b in self.store.get_batches(conn)}
                group_ids = {g["group_id"] for g in
                             self.store.list_groups(conn, states=("tentative", "held", "released"))}

                def _release(rows, reason, state="released"):
                    ids = [r["reservation_id"] for r in rows]
                    self.store.release_reservations(conn, ids, state=state, reason=reason)
                    for r in rows:
                        actions.append({
                            "reservation_id": r["reservation_id"],
                            "group_id": r["group_id"],
                            "demand_id": r["demand_id"],
                            "qty": r["qty"],
                            "reason": reason,
                        })

                by_group: dict[str, list[dict]] = defaultdict(list)
                for r in active:
                    by_group[r["group_id"]].append(r)

                # 1. 组头缺失
                no_header = [r for r in active if r["group_id"] not in group_ids]
                _release(no_header, "orphan_group_missing")

                # 2. 不完整组（超过宽限期，或 force）
                for gid, rows in by_group.items():
                    g = self.store.get_group(conn, gid)
                    if g is None:
                        continue
                    if not g["complete"] and (force or now - g["created_at"] > self.orphan_grace_seconds):
                        _release(rows, "orphan_incomplete_group")
                        self.store.set_group_state(conn, gid, "released")

                # 3. 需求 / 批次缺失
                _release([r for r in active
                          if r["reservation_id"] not in {a["reservation_id"] for a in actions}
                          and r["demand_id"] not in demand_ids],
                         "orphan_demand_missing")
                acted = {a["reservation_id"] for a in actions}
                _release([r for r in active
                          if r["reservation_id"] not in acted
                          and r["batch_id"] not in batch_ids],
                         "orphan_batch_missing")

                # 4. 超占修复
                on_hand = self.store.on_hand_by_batch(conn)
                for batch_id, used in self.store.active_totals_by_batch(conn).items():
                    overflow = used - on_hand.get(batch_id, 0)
                    if overflow <= 0:
                        continue
                    candidates = [r for r in self.store.list_reservations(conn, states=ACTIVE)
                                  if r["batch_id"] == batch_id]
                    # 先释放临时预占，再按组创建时间从年轻到年老释放
                    candidates.sort(key=lambda r: (
                        0 if r["state"] == "tentative" else 1, -r["created_at"]))
                    for r in candidates:
                        if overflow <= 0:
                            break
                        cut = min(r["qty"], overflow)
                        if cut == r["qty"]:
                            _release([r], "orphan_overcommit")
                        else:
                            conn.execute(
                                "UPDATE reservations SET qty=qty-? WHERE reservation_id=?",
                                (cut, r["reservation_id"]),
                            )
                            actions.append({
                                "reservation_id": r["reservation_id"],
                                "group_id": r["group_id"],
                                "demand_id": r["demand_id"],
                                "qty": cut,
                                "reason": "orphan_overcommit_partial",
                            })
                        overflow -= cut

                # 空有效组收尾
                for g in self.store.list_groups(conn, states=ACTIVE):
                    if not self.store.list_reservations(conn, group_id=g["group_id"], states=ACTIVE):
                        self.store.set_group_state(conn, g["group_id"], "released")
                        if not any(a["group_id"] == g["group_id"] for a in actions):
                            actions.append({
                                "reservation_id": None, "group_id": g["group_id"],
                                "demand_id": g["demand_id"], "qty": 0,
                                "reason": "orphan_empty_group_closed",
                            })

                self._assert_conservation(conn)
                self._commit(conn)
                return {"recovered_at": _iso(now), "actions": actions,
                        "cleaned": len(actions)}
            except Exception:
                conn.execute("ROLLBACK")
                raise

    # ===================================================================
    # 查询：占用来源视图
    # ===================================================================

    def occupancy(self, material_id: Optional[str] = None) -> dict[str, Any]:
        with self.store.lock:
            conn = self.store.conn()
            batches = self.store.get_batches(conn, material_id)
            active = self.store.list_reservations(conn, states=ACTIVE)
            out = []
            for b in batches:
                rows = [r for r in active if r["batch_id"] == b["batch_id"]]
                used = sum(r["qty"] for r in rows)
                out.append({
                    "batch_id": b["batch_id"],
                    "material_id": b["material_id"],
                    "qty_on_hand": b["qty_on_hand"],
                    "reserved": used,
                    "available": b["qty_on_hand"] - used,
                    "expiry_date": b["expiry_date"].isoformat() if b["expiry_date"] else None,
                    "quarantined": b["quarantined"],
                    "holders": [
                        {"demand_id": r["demand_id"], "group_id": r["group_id"],
                         "qty": r["qty"], "state": r["state"],
                         "substitution_of": r["substitution_of"]}
                        for r in rows
                    ],
                })
            return {"batches": out, "queried_at": _iso(self.now())}


# ----------------------------------------------------------------------
# 辅助
# ----------------------------------------------------------------------

_FAR_FUTURE = date(9999, 12, 31)


def _utc_now() -> float:
    return datetime.now(tz=timezone.utc).timestamp()


def _iso(ts: float) -> str:
    return datetime.fromtimestamp(ts, tz=timezone.utc).isoformat()


def _date(value):
    if value is None:
        return None
    if hasattr(value, "year"):
        return value
    return datetime.fromisoformat(str(value)).date()


def _dedup_options(options: list[dict[str, Any]]) -> list[dict[str, Any]]:
    seen = set()
    out = []
    for o in options:
        key = (o["type"], o.get("target_demand_id"), o.get("substitute_material_id"), o["material_id"])
        if key in seen:
            continue
        seen.add(key)
        out.append(o)
    rank = {"release_lower_priority": 0, "substitute": 1, "reduce_qty": 2, "wait_supply": 3}
    out.sort(key=lambda o: (rank.get(o["type"], 9), -o["gain_qty"]))
    return out
