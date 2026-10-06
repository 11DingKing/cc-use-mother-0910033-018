"""多物料齐套分配引擎（纯计算，无 IO，便于单测）。

输入调用方在 IMMEDIATE 事务内拍的库存快照，输出：
- allocations：拟写入的批次级占用（含替代料，单位换算后）
- preemptions：需要抢占的低优先级预留
- 每条 BOM 行的缺料原因、占用来源、可行调整建议
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from .models import BatchStrategy, ShortageReason


@dataclass
class Holder:
    """批次上的一条活跃占用（其他订单的 HELD/CONFIRMED 预留）。"""

    reservation_id: str
    order_id: str
    qty: float
    priority: int
    state: str  # HELD / CONFIRMED
    created_at: str


@dataclass
class BatchView:
    batch_id: str
    material: str
    qty_on_hand: float
    qty_reserved: float
    received_at: str
    expiry_date: str | None
    blocked: bool
    holders: list[Holder] = field(default_factory=list)

    @property
    def free(self) -> float:
        return self.qty_on_hand - self.qty_reserved


@dataclass
class SubView:
    substitute: str
    ratio: float       # 1 单位主料消耗 ratio 单位替代料
    approved: bool


def _is_eligible(b: BatchView, as_of_date: str) -> bool:
    if b.blocked:
        return False
    if b.expiry_date is not None and b.expiry_date[:10] < as_of_date:
        return False
    return True


def _eligible(batches: list[BatchView], as_of_date: str) -> list[BatchView]:
    return [b for b in batches if _is_eligible(b, as_of_date)]


def _sort_batches(batches: list[BatchView], strategy: str) -> list[BatchView]:
    if strategy == BatchStrategy.FIFO.value:
        return sorted(batches, key=lambda b: (b.received_at, b.batch_id))
    # FEFO：有期限的按到期日升序，无期限排最后
    return sorted(batches, key=lambda b: (b.expiry_date is None, b.expiry_date or "", b.received_at, b.batch_id))


def _evictable(holder: Holder, my_priority: int) -> bool:
    return holder.priority < my_priority


def _holder_eviction_order(holders: list[Holder]) -> list[Holder]:
    """同批次内先抢 HELD、再抢低优先级、同级按创建时间。"""
    return sorted(holders, key=lambda h: (h.state != "HELD", h.priority, h.created_at, h.reservation_id))


def _take_free(batches: list[BatchView], need: float, in_sub_units: float | None = None,
               allocations: list[dict[str, Any]] | None = None,
               demanded_material: str = "", material: str = "") -> float:
    """从空闲量中取数。in_sub_units 给定时按替代料单位计量（need 为主料单位）。"""
    remaining = need
    conv = in_sub_units if in_sub_units is not None else 1.0
    for b in batches:
        if remaining <= 1e-9 or b.free <= 1e-9:
            continue
        primary_take = min(remaining, b.free / conv)
        actual = round(primary_take * conv, 9)
        if allocations is not None:
            allocations.append({
                "demanded_material": demanded_material or material,
                "material": material,
                "batch_id": b.batch_id,
                "qty": actual,
                "covers_primary": round(primary_take, 9),
            })
        remaining = round(remaining - primary_take, 9)
    return max(remaining, 0.0)


def _take_by_preempt(batches: list[BatchView], need: float, my_priority: int,
                     preemptions: dict[str, dict[str, Any]],
                     allocations: list[dict[str, Any]] | None,
                     demanded_material: str, material: str,
                     conv: float = 1.0) -> float:
    """通过抢占低优先级订单满足需求。返回主料单位剩余缺口。"""
    remaining = need
    for b in batches:
        if remaining <= 1e-9:
            break
        for h in _holder_eviction_order(b.holders):
            if remaining <= 1e-9:
                break
            if not _evictable(h, my_priority):
                continue
            primary_have = h.qty / conv
            primary_take = min(remaining, primary_have)
            actual = round(primary_take * conv, 9)
            entry = preemptions.get(h.reservation_id)
            if entry is None:
                entry = {"reservation_id": h.reservation_id, "victim_order": h.order_id,
                         "batch_id": b.batch_id, "material": material, "qty": 0.0,
                         "victim_priority": h.priority, "victim_state": h.state}
                preemptions[h.reservation_id] = entry
            entry["qty"] = round(entry["qty"] + actual, 9)
            if allocations is not None:
                allocations.append({
                    "demanded_material": demanded_material,
                    "material": material,
                    "batch_id": b.batch_id,
                    "qty": actual,
                    "covers_primary": round(primary_take, 9),
                    "preempted_from": h.order_id,
                })
            remaining = round(remaining - primary_take, 9)
    return max(remaining, 0.0)


def plan_allocation(
    demands: list[dict[str, Any]],
    batches_by_material: dict[str, list[BatchView]],
    substitutes: dict[str, list[SubView]],
    *,
    priority: int,
    as_of_date: str,
    strategy: str = BatchStrategy.FEFO.value,
    preempt: bool = False,
) -> dict[str, Any]:
    """生成多物料分配计划。

    demands: [{"material", "qty_required"}]
    as_of_date: 需求/到期判定基准日（YYYY-MM-DD），当天到期视为可用。
    返回字典可直接 JSON 序列化；每条需求行带 shortage_reasons / occupied_by / suggestions。
    """
    allocations: list[dict[str, Any]] = []
    preemptions: dict[str, dict[str, Any]] = {}
    lines: list[dict[str, Any]] = []
    all_kitted = True

    for d in demands:
        mat = d["material"]
        required = float(d["qty_required"])
        all_primary = list(batches_by_material.get(mat, []))
        primary = _sort_batches(_eligible(all_primary, as_of_date), strategy)
        subs = sorted(substitutes.get(mat, []), key=lambda s: s.substitute)
        approved_subs = [s for s in subs if s.approved]
        unapproved_subs = [s for s in subs if not s.approved]

        line_alloc_start = len(allocations)
        remaining = _take_free(primary, required, allocations=allocations,
                               demanded_material=mat, material=mat)

        sub_usage: list[dict[str, Any]] = []
        for s in approved_subs:
            if remaining <= 1e-9:
                break
            sub_batches = _sort_batches(_eligible(list(batches_by_material.get(s.substitute, [])), as_of_date), strategy)
            before = len(allocations)
            remaining = _take_free(sub_batches, remaining, in_sub_units=s.ratio,
                                   allocations=allocations,
                                   demanded_material=mat, material=s.substitute)
            sub_usage.append({
                "material": s.substitute, "ratio": s.ratio,
                "allocations": [a["batch_id"] for a in allocations[before:]],
            })

        if preempt and remaining > 1e-9:
            remaining = _take_by_preempt(primary, remaining, priority, preemptions,
                                         allocations, mat, mat)
            for s in approved_subs:
                if remaining <= 1e-9:
                    break
                sub_batches = _sort_batches(_eligible(list(batches_by_material.get(s.substitute, [])), as_of_date), strategy)
                before = len(allocations)
                remaining = _take_by_preempt(sub_batches, remaining, priority, preemptions,
                                             allocations, mat, s.substitute, conv=s.ratio)
                if len(allocations) > before:
                    sub_usage.append({
                        "material": s.substitute, "ratio": s.ratio,
                        "allocations": [a["batch_id"] for a in allocations[before:]],
                        "preempted": True,
                    })

        shortage = round(remaining, 9)
        kitted = shortage <= 1e-9
        all_kitted = all_kitted and kitted

        # ---- 缺料诊断 ----------------------------------------------------
        reasons: list[str] = []
        suggestions: list[dict[str, Any]] = []
        eligible_subs = [
            (s, _sort_batches(_eligible(list(batches_by_material.get(s.substitute, [])), as_of_date), strategy))
            for s in approved_subs
        ]
        occupied_by = _occupied_by(primary, eligible_subs)

        if not kitted:
            eligible_on_hand = sum(b.qty_on_hand for b in primary)
            approved_sub_on_hand = 0.0
            for s, sub_batches in eligible_subs:
                approved_sub_on_hand += sum(b.qty_on_hand for b in sub_batches) / s.ratio
            total_eligible = eligible_on_hand + approved_sub_on_hand

            expired_qty = sum(
                b.qty_on_hand for b in all_primary
                if not b.blocked and b.expiry_date is not None and b.expiry_date[:10] < as_of_date
            )
            blocked_qty = sum(b.qty_on_hand for b in all_primary if b.blocked)
            if expired_qty > 1e-9:
                reasons.append(ShortageReason.EXPIRED_STOCK.value)
            if blocked_qty > 1e-9:
                reasons.append(ShortageReason.BLOCKED_LOT.value)

            if total_eligible + 1e-9 < required:
                reasons.append(ShortageReason.INSUFFICIENT_TOTAL.value)
            else:
                reasons.append(ShortageReason.RESERVED_BY_OTHERS.value)

            # 未批准替代料可覆盖缺口 → 批准替代料建议
            for s in unapproved_subs:
                coverable = 0.0
                for b in _sort_batches(_eligible(list(batches_by_material.get(s.substitute, [])), as_of_date), strategy):
                    coverable += b.free / s.ratio
                if coverable > 1e-9:
                    reasons.append(ShortageReason.SUBSTITUTE_NOT_APPROVED.value)
                    suggestions.append({
                        "type": "APPROVE_SUBSTITUTE",
                        "material": mat,
                        "substitute": s.substitute,
                        "ratio": s.ratio,
                        "coverable_primary_qty": round(min(coverable, shortage), 9),
                    })

            # 存在可抢占但本次未启用抢占
            if not preempt:
                preemptable = _preemptable_cover(primary, eligible_subs, priority)
                if preemptable > 1e-9:
                    suggestions.append({
                        "type": "PREEMPT_LOWER_PRIORITY",
                        "coverable_primary_qty": round(min(preemptable, shortage), 9),
                    })

            suggestions.append({
                "type": "PARTIAL_CONFIRM",
                "allocatable_primary_qty": round(required - shortage, 9),
                "shortage_qty": shortage,
            })

        line = {
            "material": mat,
            "qty_required": required,
            "qty_allocated": round(required - shortage, 9),
            "shortage_qty": shortage,
            "kitted": kitted,
            "shortage_reasons": sorted(set(reasons)),
            "occupied_by": occupied_by,
            "substitutes_used": sub_usage,
            "suggestions": suggestions,
            "allocation_batch_ids": [a["batch_id"] for a in allocations[line_alloc_start:]],
        }
        lines.append(line)

    # 合并同批次同物料的多条分配，减少写入行数
    merged = _merge_allocations(allocations)

    return {
        "kitted": all_kitted,
        "lines": lines,
        "allocations": merged,
        "preemptions": list(preemptions.values()),
    }


def _merge_allocations(allocations: list[dict[str, Any]]) -> list[dict[str, Any]]:
    merged: dict[tuple, dict[str, Any]] = {}
    for a in allocations:
        key = (a["demanded_material"], a["material"], a["batch_id"], a.get("preempted_from"))
        if key in merged:
            m = merged[key]
            m["qty"] = round(m["qty"] + a["qty"], 9)
            m["covers_primary"] = round(m["covers_primary"] + a["covers_primary"], 9)
        else:
            merged[key] = dict(a)
    return list(merged.values())


def _occupied_by(primary: list[BatchView],
                 eligible_subs: list[tuple[SubView, list[BatchView]]],
                 top: int = 10) -> list[dict[str, Any]]:
    """占用来源：按订单聚合当前是谁占着主料/已批准替代料的合格批次。"""
    agg: dict[tuple, dict[str, Any]] = {}
    sources: list[list[BatchView]] = [primary]
    for _s, batches in eligible_subs:
        sources.append(batches)
    for batches in sources:
        for b in batches:
            for h in b.holders:
                entry = agg.get((h.order_id, b.material))
                if entry is None:
                    entry = {"order_id": h.order_id, "material": b.material,
                             "priority": h.priority, "state": h.state,
                             "qty": 0.0, "batches": []}
                    agg[(h.order_id, b.material)] = entry
                entry["qty"] = round(entry["qty"] + h.qty, 9)
                if b.batch_id not in entry["batches"]:
                    entry["batches"].append(b.batch_id)
    out = sorted(agg.values(), key=lambda x: (-x["priority"], -x["qty"], x["order_id"]))
    return out[:top]


def _preemptable_cover(primary: list[BatchView],
                       eligible_subs: list[tuple[SubView, list[BatchView]]],
                       priority: int) -> float:
    total = 0.0
    for b in primary:
        total += sum(h.qty for h in b.holders if _evictable(h, priority))
    for _s, batches in eligible_subs:
        for b in batches:
            total += sum(h.qty for h in b.holders if _evictable(h, priority)) / _s.ratio
    return total
