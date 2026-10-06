"""领域服务：事务边界、原子预留、数量守恒、抢占、缩减、超时、孤儿恢复。

所有写操作都在单个 BEGIN IMMEDIATE 事务内完成：
- 拍库存快照 → 纯函数分配 → 抢占/写入/释放/改状态 → 守恒断言 → commit
任一步失败整体回滚，因此多物料预留原子生效，批次已预留量始终守恒。
"""
from __future__ import annotations

import sqlite3
import uuid
from datetime import date, datetime, timedelta, timezone
from typing import Any

from . import allocation as alloc
from .models import (
    ACTIVE_STATES,
    BatchStrategy,
    KitPolicy,
    OrderState,
    ReleaseReason,
    ReservationState,
)
from .store import Store


class DomainError(Exception):
    def __init__(self, code: str, message: str, details: dict[str, Any] | None = None,
                 http_status: int = 409) -> None:
        super().__init__(message)
        self.code = code
        self.message = message
        self.details = details or {}
        self.http_status = http_status


def _new_id(prefix: str) -> str:
    return f"{prefix}_{uuid.uuid4().hex[:16]}"


class ReservationService:
    DEFAULT_HOLD_TTL = 300  # 秒

    def __init__(self, store: Store) -> None:
        self.store = store

    # =====================================================================
    # 主数据：订单 / 需求 / 物料 / 替代料 / 批次
    # =====================================================================
    def create_order(self, order_id: str, product: str, demands: list[dict[str, Any]],
                     *, priority: int = 0, due_date: str | None = None,
                     qty_required: float | None = None) -> dict[str, Any]:
        if not demands:
            raise DomainError("EMPTY_DEMANDS", "订单至少包含一条物料需求", http_status=400)
        total = qty_required if qty_required is not None else float(sum(d["qty_required"] for d in demands))
        now = self.store.now_iso()
        tx = self.store.conn
        self.store.begin_immediate()
        try:
            if self.store.get_order_tx(tx, order_id) is not None:
                raise DomainError("ORDER_EXISTS", f"订单 {order_id} 已存在", http_status=400)
            self.store.upsert_order(tx, {
                "order_id": order_id, "product": product, "priority": priority,
                "state": OrderState.DRAFT.value, "qty_required": total,
                "due_date": due_date, "created_at": now, "updated_at": now,
            })
            self.store.replace_demands(tx, order_id, demands)
            self.store.event(tx, "ORDER_CREATED", order_id, {"product": product, "priority": priority})
            self.store.commit()
        except Exception:
            self.store.rollback()
            raise
        return self.get_order(order_id)

    def register_material(self, material: str, name: str = "", unit: str = "") -> None:
        tx = self.store.conn
        self.store.begin_immediate()
        try:
            self.store.upsert_material(tx, material, name, unit)
            self.store.commit()
        except Exception:
            self.store.rollback()
            raise

    def add_batch(self, batch_id: str, material: str, qty_on_hand: float,
                  received_at: str, expiry_date: str | None = None,
                  blocked: bool = False) -> dict[str, Any]:
        if qty_on_hand <= 0:
            raise DomainError("BAD_QTY", "批次在库量必须为正", http_status=400)
        tx = self.store.conn
        self.store.begin_immediate()
        try:
            self.store.upsert_material(tx, material)
            self.store.upsert_batch(tx, {
                "batch_id": batch_id, "material": material, "qty_on_hand": qty_on_hand,
                "received_at": received_at, "expiry_date": expiry_date,
                "blocked": 1 if blocked else 0,
            })
            self.store.event(tx, "BATCH_ADDED", None, {"batch_id": batch_id, "material": material})
            self.store.commit()
        except Exception:
            self.store.rollback()
            raise
        return self.get_batch(batch_id)

    def set_batch_blocked(self, batch_id: str, blocked: bool) -> dict[str, Any]:
        tx = self.store.conn
        self.store.begin_immediate()
        try:
            row = tx.execute("SELECT * FROM batches WHERE batch_id=?", (batch_id,)).fetchone()
            if row is None:
                raise DomainError("BATCH_NOT_FOUND", f"批次 {batch_id} 不存在", http_status=404)
            self.store.set_batch_blocked(tx, batch_id, blocked)
            self.store.event(tx, "BATCH_BLOCKED" if blocked else "BATCH_UNBLOCKED", None,
                             {"batch_id": batch_id})
            violations = self.store.conservation_violations_tx(tx)
            if violations:
                raise DomainError("CONSERVATION_VIOLATION", "数量守恒校验失败", {"violations": violations})
            self.store.commit()
        except Exception:
            self.store.rollback()
            raise
        return self.get_batch(batch_id)

    def approve_substitute(self, material: str, substitute: str, ratio: float = 1.0,
                           approved: bool = True) -> dict[str, Any]:
        if ratio <= 0:
            raise DomainError("BAD_RATIO", "替代比例必须为正", http_status=400)
        tx = self.store.conn
        self.store.begin_immediate()
        try:
            self.store.upsert_material(tx, material)
            self.store.upsert_material(tx, substitute)
            self.store.set_substitute(tx, material, substitute, ratio, approved)
            self.store.event(tx, "SUBSTITUTE_APPROVED" if approved else "SUBSTITUTE_REVOKED",
                             None, {"material": material, "substitute": substitute, "ratio": ratio})
            self.store.commit()
        except Exception:
            self.store.rollback()
            raise
        return {"material": material, "substitute": substitute, "ratio": ratio, "approved": approved}

    # =====================================================================
    # 计划（dry-run，只读诊断：缺料原因 / 占用来源 / 可行调整方案）
    # =====================================================================
    def plan(self, order_id: str, *, strategy: str = BatchStrategy.FEFO.value,
             preempt: bool = False, as_of: str | None = None) -> dict[str, Any]:
        tx = self.store.conn
        order = self.store.get_order(order_id)
        if order is None:
            raise DomainError("ORDER_NOT_FOUND", f"订单 {order_id} 不存在", http_status=404)
        demands = [dict(material=r["material"], qty_required=r["qty_required"])
                   for r in self.store.list_demands(order_id)]
        snapshot = self._snapshot(tx, exclude_order=order_id, as_of=as_of)
        result = alloc.plan_allocation(
            demands, snapshot["batches"], snapshot["substitutes"],
            priority=order["priority"], as_of_date=snapshot["as_of"],
            strategy=strategy, preempt=preempt,
        )
        result["order_id"] = order_id
        result["strategy"] = strategy
        result["as_of"] = snapshot["as_of"]
        return result

    # =====================================================================
    # 持有（临时锁定，带 TTL）与确认（原子写入多物料预留）
    # =====================================================================
    def hold(self, order_id: str, *, ttl_seconds: int | None = None,
             strategy: str = BatchStrategy.FEFO.value, preempt: bool = False,
             as_of: str | None = None) -> dict[str, Any]:
        ttl = ttl_seconds if ttl_seconds is not None else self.DEFAULT_HOLD_TTL
        expires = (datetime.now(timezone.utc) + timedelta(seconds=ttl)).isoformat()
        return self._write_reservation(
            order_id, target_state=ReservationState.HELD, policy=KitPolicy.PARTIAL,
            strategy=strategy, preempt=preempt, as_of=as_of, expires_at=expires,
            order_state_on_success=OrderState.PENDING_CONFIRM,
        )

    def confirm(self, order_id: str, *, policy: str = KitPolicy.ALL_OR_NOTHING.value,
                strategy: str = BatchStrategy.FEFO.value, preempt: bool = False,
                as_of: str | None = None) -> dict[str, Any]:
        order = self.store.get_order(order_id)
        if order is None:
            raise DomainError("ORDER_NOT_FOUND", f"订单 {order_id} 不存在", http_status=404)
        if order["state"] in (OrderState.CLOSED.value, OrderState.FULFILLING.value):
            raise DomainError("ORDER_NOT_OPEN", f"订单状态 {order['state']} 不可确认")
        due = order["due_date"]
        expires = f"{due}T23:59:59+00:00" if due else None
        return self._write_reservation(
            order_id, target_state=ReservationState.CONFIRMED, policy=KitPolicy(policy),
            strategy=strategy, preempt=preempt, as_of=as_of, expires_at=expires,
            order_state_on_success=OrderState.RELEASED,
        )

    def _write_reservation(self, order_id: str, *, target_state: ReservationState,
                           policy: KitPolicy, strategy: str, preempt: bool,
                           as_of: str | None, expires_at: str | None,
                           order_state_on_success: OrderState) -> dict[str, Any]:
        s = self.store
        s.begin_immediate()
        tx = s.conn
        try:
            order = s.get_order_tx(tx, order_id)
            if order is None:
                raise DomainError("ORDER_NOT_FOUND", f"订单 {order_id} 不存在", http_status=404)
            if order["state"] in (OrderState.CLOSED.value,):
                raise DomainError("ORDER_CLOSED", f"订单 {order_id} 已关闭")
            demands = [dict(material=r["material"], qty_required=r["qty_required"])
                       for r in s.list_demands_tx(tx, order_id)]

            snapshot = self._snapshot(tx, exclude_order=order_id, as_of=as_of)
            result = alloc.plan_allocation(
                demands, snapshot["batches"], snapshot["substitutes"],
                priority=order["priority"], as_of_date=snapshot["as_of"],
                strategy=strategy, preempt=preempt,
            )

            if not result["kitted"] and policy == KitPolicy.ALL_OR_NOTHING:
                raise DomainError(
                    "NOT_KITTED",
                    f"订单 {order_id} 未齐套，按 ALL_OR_NOTHING 策略不予预留",
                    details={"plan": self._public_plan(result, order_id, strategy, snapshot["as_of"])},
                )

            now = s.now_iso()
            written: list[dict[str, Any]] = []

            # 1) 抢占低优先级订单的占用
            for p in result["preemptions"]:
                s.reduce_reservation_qty_tx(
                    tx, p["reservation_id"], p["qty"], ReleaseReason.PREEMPTED.value, now,
                    preempted_from=order_id,
                )
                s.event(tx, "RESERVATION_PREEMPTED", p["victim_order"],
                        {"by_order": order_id, "reservation_id": p["reservation_id"],
                         "batch_id": p["batch_id"], "qty": p["qty"]})
                written_victim = self._reservation_view(
                    tx.execute("SELECT * FROM reservations WHERE reservation_id=?",
                               (p["reservation_id"],)).fetchone())
                if written_victim is not None:
                    p["victim_remaining_qty"] = written_victim["qty"] if written_victim["state"] in ACTIVE_STATES else 0.0

            # 2) 释放本订单旧的活跃预留（被本次写入替换）
            old_rows = s.active_for_order_tx(tx, order_id)
            for r in old_rows:
                s.release_reservation_tx(tx, r["reservation_id"],
                                         ReleaseReason.REPLACED_HOLD.value, now)
            if old_rows:
                self._sync_groups_tx(tx, order_id, now)

            # 3) 原子写入新预留（多物料同一事务、同一 group）
            group_id = _new_id("grp")
            s.insert_group(tx, group_id, order_id, target_state.value, now,
                           confirmed_at=now if target_state == ReservationState.CONFIRMED else None)
            for a in result["allocations"]:
                if a["qty"] <= 1e-9:
                    continue
                rid = _new_id("res")
                s.insert_reservation(tx, {
                    "reservation_id": rid, "order_id": order_id,
                    "material": a["material"], "demanded_material": a["demanded_material"],
                    "batch_id": a["batch_id"], "qty": a["qty"],
                    "state": target_state.value, "created_at": now, "expires_at": expires_at,
                })
                s.attach_group_item(tx, group_id, rid)
                a["reservation_id"] = rid
                written.append(a)

            s.set_order_state(tx, order_id, order_state_on_success.value, now)
            s.event(tx, f"RESERVATION_{target_state.value}", order_id,
                    {"group_id": group_id, "kitted": result["kitted"],
                     "item_count": len(written), "policy": policy.value})

            violations = s.conservation_violations_tx(tx)
            if violations:
                raise DomainError("CONSERVATION_VIOLATION", "数量守恒校验失败", {"violations": violations})
            s.commit()
        except DomainError:
            s.rollback()
            raise
        except Exception:
            s.rollback()
            raise

        body = self._public_plan(result, order_id, strategy, snapshot["as_of"])
        body.update({
            "group_id": group_id,
            "reservation_state": target_state.value,
            "expires_at": expires_at,
            "order_state": order_state_on_success.value,
            "written_count": len(written),
        })
        return body

    @staticmethod
    def _public_plan(result: dict[str, Any], order_id: str, strategy: str, as_of: str) -> dict[str, Any]:
        return {
            "order_id": order_id,
            "strategy": strategy,
            "as_of": as_of,
            "kitted": result["kitted"],
            "lines": result["lines"],
            "allocations": result["allocations"],
            "preemptions": result["preemptions"],
        }

    # =====================================================================
    # 订单缩减：只减不增，守恒释放多余预留
    # =====================================================================
    def reduce_order(self, order_id: str, new_demands: list[dict[str, Any]]) -> dict[str, Any]:
        s = self.store
        s.begin_immediate()
        tx = s.conn
        released: list[dict[str, Any]] = []
        try:
            order = s.get_order_tx(tx, order_id)
            if order is None:
                raise DomainError("ORDER_NOT_FOUND", f"订单 {order_id} 不存在", http_status=404)
            old = {r["material"]: r["qty_required"] for r in s.list_demands_tx(tx, order_id)}
            new_map = {d["material"]: float(d["qty_required"]) for d in new_demands}
            for mat, q in new_map.items():
                if mat in old and q > old[mat] + 1e-9:
                    raise DomainError(
                        "REDUCE_ONLY",
                        f"物料 {mat} 新需求 {q} 大于原需求 {old[mat]}，缩减接口不允许增购；请重新确认预留",
                        http_status=400,
                    )
            now = s.now_iso()
            # 需要释放的活跃预留量（含替代料按 demanded_material 归集）
            for mat in set(old) | set(new_map):
                new_qty = new_map.get(mat, 0.0)
                rows = [r for r in s.active_for_order_tx(tx, order_id) if r["demanded_material"] == mat]
                covered = sum(r["qty"] for r in rows)
                # 主料预留按 1:1 计、替代料按各自比例折回主料单位
                covered_primary = 0.0
                for r in rows:
                    ratio = self._sub_ratio_tx(tx, mat, r["material"])
                    covered_primary += r["qty"] / ratio
                excess = round(covered_primary - new_qty, 9)
                if excess <= 1e-9:
                    continue
                # 先释放到期最晚的（保留临期早的批次继续被占用、优先消耗）
                rows.sort(key=lambda r: self._batch_release_key(tx, r["batch_id"]))
                for r in rows:
                    if excess <= 1e-9:
                        break
                    ratio = self._sub_ratio_tx(tx, mat, r["material"])
                    release_primary = min(excess, r["qty"] / ratio)
                    release_qty = round(release_primary * ratio, 9)
                    if release_qty >= r["qty"] - 1e-9:
                        s.release_reservation_tx(tx, r["reservation_id"],
                                                 ReleaseReason.ORDER_REDUCED.value, now)
                    else:
                        s.reduce_reservation_qty_tx(tx, r["reservation_id"], release_qty,
                                                    ReleaseReason.ORDER_REDUCED.value, now, None)
                    released.append({"material": r["material"], "demanded_material": mat,
                                     "batch_id": r["batch_id"], "qty": release_qty})
                    excess = round(excess - release_primary, 9)

            # 删除被裁掉的需求行，其余更新
            s.replace_demands(tx, order_id, [
                {"material": m, "qty_required": q,
                 "seq": i} for i, (m, q) in enumerate(sorted(new_map.items())) if q > 1e-9
            ])
            total = sum(new_map.values())
            tx.execute("UPDATE orders SET qty_required=?, updated_at=? WHERE order_id=?",
                       (total, now, order_id))
            s.event(tx, "ORDER_REDUCED", order_id, {"released": released})
            violations = s.conservation_violations_tx(tx)
            if violations:
                raise DomainError("CONSERVATION_VIOLATION", "数量守恒校验失败", {"violations": violations})
            s.commit()
        except Exception:
            s.rollback()
            raise
        return {"order_id": order_id, "released": released, "order": self.get_order(order_id)}

    @staticmethod
    def _sub_ratio_tx(tx: sqlite3.Connection, demanded: str, actual: str) -> float:
        if demanded == actual:
            return 1.0
        row = tx.execute("SELECT ratio FROM substitutes WHERE material=? AND substitute=?",
                         (demanded, actual)).fetchone()
        return float(row["ratio"]) if row else 1.0

    @staticmethod
    def _batch_release_key(tx: sqlite3.Connection, batch_id: str) -> tuple:
        r = tx.execute("SELECT expiry_date, received_at FROM batches WHERE batch_id=?",
                       (batch_id,)).fetchone()
        expiry = r["expiry_date"]
        # 无期限排最后释放；有期限按到期日降序（晚到期的先释放）
        ordinal = -date.fromisoformat(expiry).toordinal() if expiry else 0
        return (expiry is None, ordinal, r["received_at"])

    # =====================================================================
    # 领料出库 / 完工 / 关闭
    # =====================================================================
    def issue_materials(self, order_id: str, items: list[dict[str, Any]]) -> dict[str, Any]:
        """items: [{"reservation_id":..., "qty":...}] 或 [{"batch_id","material","qty"}]。"""
        s = self.store
        s.begin_immediate()
        tx = s.conn
        consumed: list[dict[str, Any]] = []
        try:
            order = s.get_order_tx(tx, order_id)
            if order is None:
                raise DomainError("ORDER_NOT_FOUND", f"订单 {order_id} 不存在", http_status=404)
            now = s.now_iso()
            active = {r["reservation_id"]: r for r in s.active_for_order_tx(tx, order_id)}
            for item in items:
                rid = item.get("reservation_id")
                qty = float(item["qty"])
                if rid is None:
                    raise DomainError("BAD_ITEM", "领料必须指定 reservation_id", http_status=400)
                if rid not in active:
                    raise DomainError("RESERVATION_NOT_ACTIVE",
                                      f"预留 {rid} 不属于订单 {order_id} 或已失效", http_status=409)
                if qty > active[rid]["qty"] + 1e-9:
                    raise DomainError("QTY_EXCEEDS_RESERVATION",
                                      f"预留 {rid} 剩余 {active[rid]['qty']}，请求出库 {qty}",
                                      http_status=409)
                row = s.consume_reservation_tx(tx, rid, qty, now)
                consumed.append({"reservation_id": rid, "batch_id": row["batch_id"], "qty": qty})
            s.set_order_state(tx, order_id, OrderState.FULFILLING.value, now)
            s.event(tx, "MATERIALS_ISSUED", order_id, {"consumed": consumed})
            violations = s.conservation_violations_tx(tx)
            if violations:
                raise DomainError("CONSERVATION_VIOLATION", "数量守恒校验失败", {"violations": violations})
            s.commit()
        except Exception:
            s.rollback()
            raise
        return {"order_id": order_id, "consumed": consumed, "order": self.get_order(order_id)}

    def complete_order(self, order_id: str) -> dict[str, Any]:
        """完工：释放剩余活跃预留并关单。"""
        s = self.store
        s.begin_immediate()
        try:
            tx = s.conn
            if s.get_order_tx(tx, order_id) is None:
                raise DomainError("ORDER_NOT_FOUND", f"订单 {order_id} 不存在", http_status=404)
            now = s.now_iso()
            for r in s.active_for_order_tx(tx, order_id):
                s.release_reservation_tx(tx, r["reservation_id"],
                                         ReleaseReason.ORDER_COMPLETED.value, now)
            self._sync_groups_tx(tx, order_id, now)
            s.set_order_state(tx, order_id, OrderState.CLOSED.value, now)
            s.event(tx, "ORDER_COMPLETED", order_id, {})
            violations = s.conservation_violations_tx(tx)
            if violations:
                raise DomainError("CONSERVATION_VIOLATION", "数量守恒校验失败", {"violations": violations})
            s.commit()
        except Exception:
            s.rollback()
            raise
        return self.get_order(order_id)

    def close_order(self, order_id: str) -> dict[str, Any]:
        s = self.store
        s.begin_immediate()
        try:
            tx = s.conn
            if s.get_order_tx(tx, order_id) is None:
                raise DomainError("ORDER_NOT_FOUND", f"订单 {order_id} 不存在", http_status=404)
            now = s.now_iso()
            for r in s.active_for_order_tx(tx, order_id):
                s.release_reservation_tx(tx, r["reservation_id"],
                                         ReleaseReason.ORDER_CLOSED.value, now)
            self._sync_groups_tx(tx, order_id, now)
            s.set_order_state(tx, order_id, OrderState.CLOSED.value, now)
            s.event(tx, "ORDER_CLOSED", order_id, {})
            s.commit()
        except Exception:
            s.rollback()
            raise
        return self.get_order(order_id)

    # =====================================================================
    # 超时释放清扫（HELD 的 TTL / CONFIRMED 的订单截止期）
    # =====================================================================
    def sweep_expired(self, *, now: datetime | None = None) -> dict[str, Any]:
        now_dt = now or datetime.now(timezone.utc)
        now_iso = now_dt.astimezone(timezone.utc).isoformat()
        s = self.store
        s.begin_immediate()
        released: list[dict[str, Any]] = []
        try:
            tx = s.conn
            rows = s.expired_active_tx(tx, now_iso)
            affected_orders: set[str] = set()
            for r in rows:
                reason = (ReleaseReason.TEMP_HOLD_TIMEOUT.value if r["state"] == ReservationState.HELD.value
                          else ReleaseReason.RESERVE_TIMEOUT.value)
                s.release_reservation_tx(tx, r["reservation_id"], reason, now_iso)
                released.append({"reservation_id": r["reservation_id"], "order_id": r["order_id"],
                                 "batch_id": r["batch_id"], "qty": r["qty"], "reason": reason})
                affected_orders.add(r["order_id"])
            for oid in affected_orders:
                self._sync_groups_tx(tx, oid, now_iso)
                order = s.get_order_tx(tx, oid)
                if order is None:
                    continue
                remaining = s.active_for_order_tx(tx, oid)
                if not remaining and order["state"] in (
                        OrderState.PENDING_CONFIRM.value, OrderState.RELEASED.value):
                    # 预留全部超时：回到草拟，等待计划员重新安排
                    s.set_order_state(tx, oid, OrderState.DRAFT.value, now_iso)
                    s.event(tx, "ORDER_BACK_TO_DRAFT", oid, {"reason": "TIMEOUT_RELEASE"})
            if released:
                s.event(tx, "SWEEP_EXPIRED", None, {"count": len(released)})
                violations = s.conservation_violations_tx(tx)
                if violations:
                    raise DomainError("CONSERVATION_VIOLATION", "数量守恒校验失败", {"violations": violations})
            s.commit()
        except Exception:
            s.rollback()
            raise
        return {"released_count": len(released), "released": released}

    # =====================================================================
    # 恢复任务：清理孤儿预留（订单缺失 / 分组缺失或已释放 / 数量异常）
    # =====================================================================
    def recover_orphans(self) -> dict[str, Any]:
        s = self.store
        s.begin_immediate()
        cleaned: list[dict[str, Any]] = []
        try:
            tx = s.conn
            now = s.now_iso()
            rows = tx.execute("SELECT * FROM reservations WHERE state IN ('HELD','CONFIRMED')").fetchall()
            grouped = {
                r["reservation_id"] for r in tx.execute(
                    """SELECT gi.reservation_id FROM reservation_group_items gi
                       JOIN reservation_groups g ON g.group_id=gi.group_id
                       WHERE g.state IN ('HELD','CONFIRMED')""")
            }
            order_ids = {r["order_id"] for r in tx.execute("SELECT order_id FROM orders").fetchall()}
            for r in rows:
                orphan_kind: str | None = None
                if r["order_id"] not in order_ids:
                    orphan_kind = "ORDER_MISSING"
                elif r["reservation_id"] not in grouped:
                    orphan_kind = "GROUP_MISSING_OR_RELEASED"
                if orphan_kind is None:
                    continue
                s.release_reservation_tx(tx, r["reservation_id"],
                                         ReleaseReason.ORPHAN_CLEANUP.value, now)
                cleaned.append({"reservation_id": r["reservation_id"], "order_id": r["order_id"],
                                "batch_id": r["batch_id"], "qty": r["qty"], "kind": orphan_kind})
            if cleaned:
                s.event(tx, "ORPHANS_RECOVERED", None,
                        {"count": len(cleaned), "items": cleaned})
            violations = s.conservation_violations_tx(tx)
            s.commit()
        except Exception:
            s.rollback()
            raise
        return {"cleaned_count": len(cleaned), "cleaned": cleaned,
                "conservation_violations": violations}
    def conservation_check(self) -> dict[str, Any]:
        tx = self.store.conn
        return {"violations": self.store.conservation_violations_tx(tx)}

    # =====================================================================
    # 查询
    # =====================================================================
    def get_order(self, order_id: str) -> dict[str, Any]:
        s = self.store
        order = s.get_order(order_id)
        if order is None:
            raise DomainError("ORDER_NOT_FOUND", f"订单 {order_id} 不存在", http_status=404)
        demands = [dict(r) for r in s.list_demands(order_id)]
        active = [self._reservation_view(r) for r in s.active_for_order_tx(s.conn, order_id)]
        released = [self._reservation_view(r) for r in s.conn.execute(
            "SELECT * FROM reservations WHERE order_id=? AND state IN ('RELEASED','CONSUMED') "
            "ORDER BY released_at DESC", (order_id,)).fetchall()]
        by_mat: dict[str, dict[str, float]] = {}
        for r in active:
            d = by_mat.setdefault(r["demanded_material"], {"reserved": 0.0})
            d["reserved"] = round(d["reserved"] + r["qty"], 9)
        return {
            "order_id": order["order_id"], "product": order["product"],
            "priority": order["priority"], "state": order["state"],
            "qty_required": order["qty_required"], "due_date": order["due_date"],
            "version": order["version"], "updated_at": order["updated_at"],
            "demands": demands,
            "coverage": [{"material": m, **v} for m, v in sorted(by_mat.items())],
            "active_reservations": active,
            "history": released,
        }

    def get_batch(self, batch_id: str) -> dict[str, Any]:
        row = self.store.conn.execute("SELECT * FROM batches WHERE batch_id=?", (batch_id,)).fetchone()
        if row is None:
            raise DomainError("BATCH_NOT_FOUND", f"批次 {batch_id} 不存在", http_status=404)
        holders = [self._reservation_view(r) for r in self.store.conn.execute(
            "SELECT * FROM reservations WHERE batch_id=? AND state IN ('HELD','CONFIRMED')",
            (batch_id,)).fetchall()]
        return {
            "batch_id": row["batch_id"], "material": row["material"],
            "qty_on_hand": row["qty_on_hand"], "qty_reserved": row["qty_reserved"],
            "qty_available": round(row["qty_on_hand"] - row["qty_reserved"], 9),
            "received_at": row["received_at"], "expiry_date": row["expiry_date"],
            "blocked": bool(row["blocked"]), "holders": holders,
        }

    def list_batches(self, material: str | None = None) -> list[dict[str, Any]]:
        sql = "SELECT batch_id FROM batches"
        args: tuple = ()
        if material:
            sql += " WHERE material=?"
            args = (material,)
        sql += " ORDER BY material, expiry_date, received_at"
        return [self.get_batch(r["batch_id"]) for r in self.store.conn.execute(sql, args)]

    @staticmethod
    def _reservation_view(row: sqlite3.Row | None) -> dict[str, Any] | None:
        if row is None:
            return None
        keys = ("reservation_id", "order_id", "material", "demanded_material", "batch_id",
                "qty", "state", "created_at", "expires_at", "released_at",
                "release_reason", "preempted_from")
        return {k: row[k] for k in keys if k in row.keys()}

    # =====================================================================
    # 快照：把当前 DB 状态转成分配引擎输入（排除本订单自己的占用）
    # =====================================================================
    def _snapshot(self, tx: sqlite3.Connection, *, exclude_order: str | None,
                  as_of: str | None) -> dict[str, Any]:
        as_of_date = (as_of or date.today().isoformat())[:10]
        batch_rows = tx.execute("SELECT * FROM batches").fetchall()
        views: dict[str, list[alloc.BatchView]] = {}
        by_id: dict[str, alloc.BatchView] = {}
        for b in batch_rows:
            view = alloc.BatchView(
                batch_id=b["batch_id"], material=b["material"],
                qty_on_hand=b["qty_on_hand"], qty_reserved=0,  # 按其他订单占用重算
                received_at=b["received_at"], expiry_date=b["expiry_date"],
                blocked=bool(b["blocked"]), holders=[],
            )
            views.setdefault(b["material"], []).append(view)
            by_id[b["batch_id"]] = view
        for h in tx.execute(
                """SELECT r.*, o.priority AS ord_priority FROM reservations r
                   JOIN orders o ON o.order_id=r.order_id
                   WHERE r.state IN ('HELD','CONFIRMED')"""):
            if exclude_order and h["order_id"] == exclude_order:
                # 本订单自己的占用视为可回收：不计入预留量，也不作为占用者
                continue
            view = by_id.get(h["batch_id"])
            if view is None:
                continue
            view.holders.append(alloc.Holder(
                reservation_id=h["reservation_id"], order_id=h["order_id"], qty=h["qty"],
                priority=h["ord_priority"], state=h["state"], created_at=h["created_at"],
            ))
            view.qty_reserved = round(view.qty_reserved + h["qty"], 9)
        subs: dict[str, list[alloc.SubView]] = {}
        for r in tx.execute("SELECT * FROM substitutes"):
            subs.setdefault(r["material"], []).append(
                alloc.SubView(substitute=r["substitute"], ratio=r["ratio"],
                              approved=bool(r["approved"])))
        return {"as_of": as_of_date, "batches": views, "substitutes": subs}

    def _sync_groups_tx(self, tx: sqlite3.Connection, order_id: str, when: str) -> None:
        groups = tx.execute(
            "SELECT group_id FROM reservation_groups WHERE order_id=? AND state IN ('HELD','CONFIRMED')",
            (order_id,)).fetchall()
        for g in groups:
            active = tx.execute(
                """SELECT COUNT(*) AS c FROM reservation_group_items gi
                   JOIN reservations r ON r.reservation_id=gi.reservation_id
                   WHERE gi.group_id=? AND r.state IN ('HELD','CONFIRMED')""",
                (g["group_id"],)).fetchone()["c"]
            if active == 0:
                tx.execute("UPDATE reservation_groups SET state='RELEASED' WHERE group_id=?",
                           (g["group_id"],))
