"""端到端领域测试：通过 ReservationService 验证全部关键不变量。"""
from __future__ import annotations

import sys
import threading
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from reservation import DomainError, ReservationService, Store
from reservation.allocation import (
    BatchView, Holder, SubView, plan_allocation,
)
from reservation.models import BatchStrategy, ReleaseReason, ReservationState


def make_service() -> ReservationService:
    svc = ReservationService(Store(":memory:"))
    # 物料 M1/M2/SUB，批次含不同到期日与冻结状态；as_of=2026-10-06
    svc.add_batch("B1", "M1", 100, "2026-09-01", "2026-11-01")
    svc.add_batch("B2", "M1", 100, "2026-09-15", "2026-10-20")   # FEFO 应先用
    svc.add_batch("B3", "M1", 50, "2026-08-01", "2026-09-01")    # 已过期
    svc.add_batch("B4", "M1", 40, "2026-09-20", "2026-12-01", blocked=True)  # 冻结
    svc.add_batch("B5", "M2", 200, "2026-09-10", "2027-01-01")
    svc.add_batch("B6", "SUB", 100, "2026-09-10", "2027-06-01")
    return svc


class AllocationEngineTest(unittest.TestCase):
    def test_fefo_prefers_earlier_expiry(self) -> None:
        batches = {
            "M1": [
                BatchView("B1", "M1", 100, 0, "2026-09-01", "2026-11-01", False),
                BatchView("B2", "M1", 100, 0, "2026-09-15", "2026-10-20", False),
            ]
        }
        plan = plan_allocation([{"material": "M1", "qty_required": 80}],
                               batches, {}, priority=1, as_of_date="2026-10-06")
        self.assertTrue(plan["kitted"])
        self.assertEqual(plan["allocations"][0]["batch_id"], "B2")

    def test_expired_and_blocked_excluded(self) -> None:
        batches = {
            "M1": [
                BatchView("B3", "M1", 50, 0, "2026-08-01", "2026-09-01", False),
                BatchView("B4", "M1", 40, 0, "2026-09-20", "2026-12-01", True),
                BatchView("B1", "M1", 30, 0, "2026-09-01", "2026-11-01", False),
            ]
        }
        plan = plan_allocation([{"material": "M1", "qty_required": 100}],
                               batches, {}, priority=1, as_of_date="2026-10-06")
        self.assertFalse(plan["kitted"])
        reasons = plan["lines"][0]["shortage_reasons"]
        self.assertIn("EXPIRED_STOCK", reasons)
        self.assertIn("BLOCKED_LOT", reasons)
        self.assertIn("INSUFFICIENT_TOTAL", reasons)

    def test_occupied_by_others_diagnosis(self) -> None:
        held = Holder("r1", "ORD-A", 80, priority=5, state="CONFIRMED", created_at="t")
        batches = {"M1": [BatchView("B1", "M1", 100, 80, "t", "2026-12-01", False, [held])]}
        plan = plan_allocation([{"material": "M1", "qty_required": 50}],
                               batches, {}, priority=1, as_of_date="2026-10-06")
        self.assertFalse(plan["kitted"])
        self.assertIn("RESERVED_BY_OTHERS", plan["lines"][0]["shortage_reasons"])
        self.assertEqual(plan["lines"][0]["occupied_by"][0]["order_id"], "ORD-A")
        # 低优先级订单看不到抢占建议以外的途径
        suggestions = {s["type"] for s in plan["lines"][0]["suggestions"]}
        self.assertIn("PARTIAL_CONFIRM", suggestions)

    def test_preemption_requires_higher_priority(self) -> None:
        held = Holder("r1", "ORD-LOW", 60, priority=1, state="CONFIRMED", created_at="t")
        batches = {"M1": [BatchView("B1", "M1", 100, 60, "t", "2026-12-01", False, [held])]}
        plan = plan_allocation([{"material": "M1", "qty_required": 80}],
                               batches, {}, priority=9, as_of_date="2026-10-06",
                               preempt=True)
        self.assertTrue(plan["kitted"])
        self.assertEqual(plan["preemptions"][0]["victim_order"], "ORD-LOW")
        # 空闲 40 + 抢占 40 即满足 80
        self.assertEqual(plan["preemptions"][0]["qty"], 40)

    def test_unapproved_substitute_suggestion(self) -> None:
        batches = {
            "M1": [BatchView("B1", "M1", 10, 0, "t", "2026-12-01", False)],
            "SUB": [BatchView("B6", "SUB", 100, 0, "t", "2027-01-01", False)],
        }
        subs = {"M1": [SubView("SUB", ratio=2.0, approved=False)]}
        plan = plan_allocation([{"material": "M1", "qty_required": 30}],
                               batches, subs, priority=1, as_of_date="2026-10-06")
        self.assertIn("SUBSTITUTE_NOT_APPROVED", plan["lines"][0]["shortage_reasons"])
        sug = next(s for s in plan["lines"][0]["suggestions"] if s["type"] == "APPROVE_SUBSTITUTE")
        self.assertEqual(sug["substitute"], "SUB")
        self.assertEqual(sug["coverable_primary_qty"], 20)  # 100/2=50 但缺口仅 20


class ReservationFlowTest(unittest.TestCase):
    def setUp(self) -> None:
        self.svc = make_service()

    def test_atomic_confirm_all_or_nothing(self) -> None:
        # M1 需 150（可用合格 200），M2 需 300（仅 200）→ 不齐套，整单回滚
        self.svc.create_order("ORD-1", "P", [
            {"material": "M1", "qty_required": 150},
            {"material": "M2", "qty_required": 300},
        ], priority=5)
        with self.assertRaises(DomainError) as ctx:
            self.svc.confirm("ORD-1")
        self.assertEqual(ctx.exception.code, "NOT_KITTED")
        # 回滚后无任何活跃预留，批次预留量为 0
        order = self.svc.get_order("ORD-1")
        self.assertEqual(order["active_reservations"], [])
        self.assertEqual(self.svc.get_batch("B2")["qty_reserved"], 0)
        self.assertEqual(self.svc.conservation_check()["violations"], [])
        # 诊断含缺料原因与可行调整方案
        plan = ctx.exception.details["plan"]
        self.assertFalse(plan["kitted"])
        m2 = next(l for l in plan["lines"] if l["material"] == "M2")
        self.assertIn("INSUFFICIENT_TOTAL", m2["shortage_reasons"])

    def test_partial_policy_and_fefo_allocations(self) -> None:
        self.svc.create_order("ORD-1", "P", [
            {"material": "M1", "qty_required": 250},  # 合格 200，缺 50
            {"material": "M2", "qty_required": 50},
        ], priority=5)
        result = self.svc.confirm("ORD-1", policy="PARTIAL")
        self.assertFalse(result["kitted"])
        self.assertEqual(result["order_state"], "RELEASED")
        order = self.svc.get_order("ORD-1")
        by_batch = {r["batch_id"]: r["qty"] for r in order["active_reservations"]}
        # FEFO：B2(10/20到期) 先用满 100，再 B1 用 100
        self.assertEqual(by_batch.get("B2"), 100)
        self.assertEqual(by_batch.get("B1"), 100)
        self.assertNotIn("B3", by_batch)  # 过期批次不参与
        self.assertNotIn("B4", by_batch)  # 冻结批次不参与
        self.assertEqual(by_batch.get("B5"), 50)
        self.assertEqual(self.svc.conservation_check()["violations"], [])

    def test_hold_ttl_timeout_releases(self) -> None:
        self.svc.create_order("ORD-H", "P", [{"material": "M1", "qty_required": 10}], priority=5)
        self.svc.hold("ORD-H", ttl_seconds=60)
        self.assertEqual(self.svc.get_batch("B2")["qty_reserved"], 10)
        # 未超时不清扫
        res = self.svc.sweep_expired(now=datetime.now(timezone.utc) + timedelta(seconds=30))
        self.assertEqual(res["released_count"], 0)
        # 超时后释放
        res = self.svc.sweep_expired(now=datetime.now(timezone.utc) + timedelta(seconds=61))
        self.assertEqual(res["released_count"], 1)
        self.assertEqual(res["released"][0]["reason"], ReleaseReason.TEMP_HOLD_TIMEOUT.value)
        self.assertEqual(self.svc.get_batch("B2")["qty_reserved"], 0)
        self.assertEqual(self.svc.get_order("ORD-H")["state"], "DRAFT")

    def test_preemption_at_confirm(self) -> None:
        self.svc.create_order("ORD-LOW", "P", [{"material": "M1", "qty_required": 100}], priority=1)
        self.svc.confirm("ORD-LOW")  # 占走 FEFO 的 B2 100
        self.svc.create_order("ORD-HIGH", "P", [{"material": "M1", "qty_required": 200}], priority=9)
        result = self.svc.confirm("ORD-HIGH", preempt=True)
        self.assertTrue(result["kitted"])
        # 低优先级订单 100 件被整单抢占，其预留被释放
        low = self.svc.get_order("ORD-LOW")
        self.assertEqual(low["active_reservations"], [])
        self.assertEqual(low["history"][0]["release_reason"], ReleaseReason.PREEMPTED.value)
        self.assertEqual(low["history"][0]["preempted_from"], "ORD-HIGH")
        # 高优先级拿到 B1 100（空闲）+ B2 100（抢占）
        high = self.svc.get_order("ORD-HIGH")
        by_batch = {r["batch_id"]: r["qty"] for r in high["active_reservations"]}
        self.assertEqual(by_batch["B2"], 100)
        self.assertEqual(by_batch["B1"], 100)
        self.assertEqual(self.svc.conservation_check()["violations"], [])

    def test_higher_priority_protected_from_preempt(self) -> None:
        self.svc.create_order("ORD-HI", "P", [{"material": "M1", "qty_required": 100}], priority=9)
        self.svc.confirm("ORD-HI")
        self.svc.create_order("ORD-LO", "P", [{"material": "M1", "qty_required": 150}], priority=1)
        with self.assertRaises(DomainError) as ctx:
            self.svc.confirm("ORD-LO", preempt=True)
        self.assertEqual(ctx.exception.code, "NOT_KITTED")
        # 高优先级订单预留完好
        self.assertEqual(len(self.svc.get_order("ORD-HI")["active_reservations"]), 1)

    def test_reduce_order_releases_surplus(self) -> None:
        self.svc.create_order("ORD-R", "P", [{"material": "M1", "qty_required": 150}], priority=5)
        self.svc.confirm("ORD-R", policy="PARTIAL")  # B2 100 + B1 50
        # 缩减到 80：应释放 70，优先保留早到期的 B2
        result = self.svc.reduce_order("ORD-R", [{"material": "M1", "qty_required": 80}])
        self.assertEqual(sum(x["qty"] for x in result["released"]), 70)
        order = self.svc.get_order("ORD-R")
        by_batch = {r["batch_id"]: r["qty"] for r in order["active_reservations"]}
        self.assertEqual(by_batch, {"B2": 80})  # 早到期的保留
        # 缩减不允许增加
        with self.assertRaises(DomainError) as ctx:
            self.svc.reduce_order("ORD-R", [{"material": "M1", "qty_required": 90}])
        self.assertEqual(ctx.exception.code, "REDUCE_ONLY")
        self.assertEqual(self.svc.conservation_check()["violations"], [])

    def test_substitute_approval_flow(self) -> None:
        # M1 主料只有 B1=100；SUB 100，比例 2:1，未批准
        svc = ReservationService(Store(":memory:"))
        svc.add_batch("B1", "M1", 100, "2026-09-01", "2026-12-01")
        svc.add_batch("S1", "SUB", 100, "2026-09-01", "2027-01-01")
        svc.create_order("ORD-S", "P", [{"material": "M1", "qty_required": 130}], priority=5)
        with self.assertRaises(DomainError):
            svc.confirm("ORD-S")
        svc.approve_substitute("M1", "SUB", ratio=2.0)
        result = svc.confirm("ORD-S", policy="PARTIAL")
        # 主料 100 + 替代料 30 主料单位 = SUB 60
        self.assertTrue(result["kitted"])
        order = svc.get_order("ORD-S")
        sub_res = [r for r in order["active_reservations"] if r["material"] == "SUB"]
        self.assertEqual(sub_res[0]["qty"], 60)
        self.assertEqual(sub_res[0]["demanded_material"], "M1")
        self.assertEqual(svc.conservation_check()["violations"], [])

    def test_issue_and_complete(self) -> None:
        self.svc.create_order("ORD-I", "P", [{"material": "M2", "qty_required": 50}], priority=5)
        conf = self.svc.confirm("ORD-I")
        rid = conf["allocations"][0]["reservation_id"]
        self.svc.issue_materials("ORD-I", [{"reservation_id": rid, "qty": 30}])
        b5 = self.svc.get_batch("B5")
        self.assertEqual(b5["qty_on_hand"], 170)  # 在库同步扣减
        self.assertEqual(b5["qty_reserved"], 20)
        # 超领被拒
        with self.assertRaises(DomainError) as ctx:
            self.svc.issue_materials("ORD-I", [{"reservation_id": rid, "qty": 999}])
        self.assertEqual(ctx.exception.code, "QTY_EXCEEDS_RESERVATION")
        # 完工释放尾量 20
        self.svc.complete_order("ORD-I")
        b5 = self.svc.get_batch("B5")
        self.assertEqual(b5["qty_reserved"], 0)
        self.assertEqual(self.svc.get_order("ORD-I")["state"], "CLOSED")

    def test_orphan_recovery(self) -> None:
        self.svc.create_order("ORD-O", "P", [{"material": "M1", "qty_required": 20}], priority=5)
        self.svc.confirm("ORD-O")
        self.assertEqual(self.svc.get_batch("B2")["qty_reserved"], 20)
        # 模拟崩溃残留：用关闭外键的连接删掉订单行，但预留仍在（孤儿）
        import sqlite3
        aux = sqlite3.connect("file:reservation_mem_" + self.svc.store._mem_id
                              + "?mode=memory&cache=shared", uri=True)
        aux.execute("PRAGMA foreign_keys=OFF")
        aux.execute("DELETE FROM orders WHERE order_id='ORD-O'")
        aux.execute("DELETE FROM order_demands WHERE order_id='ORD-O'")
        aux.commit()
        aux.close()
        result = self.svc.recover_orphans()
        self.assertEqual(result["cleaned_count"], 1)
        self.assertEqual(result["cleaned"][0]["kind"], "ORDER_MISSING")
        self.assertEqual(self.svc.get_batch("B2")["qty_reserved"], 0)
        self.assertEqual(result["conservation_violations"], [])

    def test_concurrent_conflicting_confirms_no_oversell(self) -> None:
        # 文件库 + WAL 是生产部署形态；两线程同时确认争抢库存（合格总量 200，
        # 各需 150）：BEGIN IMMEDIATE 串行化，一个成功一个缺料回滚，预留量守恒
        import tempfile
        tmp = tempfile.mktemp(suffix=".db")
        self.addCleanup(self._cleanup_db, tmp)
        svc = ReservationService(Store(tmp))
        svc.add_batch("B1", "M1", 100, "2026-09-01", "2026-11-01")
        svc.add_batch("B2", "M1", 100, "2026-09-15", "2026-10-20")
        svc.create_order("ORD-X1", "P", [{"material": "M1", "qty_required": 150}], priority=5)
        svc.create_order("ORD-X2", "P", [{"material": "M1", "qty_required": 150}], priority=5)
        errors: list[str] = []
        barrier = threading.Barrier(2)

        def worker(oid: str) -> None:
            barrier.wait()
            try:
                svc.confirm(oid)
            except DomainError as exc:
                errors.append(exc.code)
            except Exception as exc:  # 串行化等待类错误也记录，不应发生
                errors.append(f"UNEXPECTED:{exc!r}")

        t1 = threading.Thread(target=worker, args=("ORD-X1",))
        t2 = threading.Thread(target=worker, args=("ORD-X2",))
        t1.start(); t2.start(); t1.join(); t2.join()
        self.assertEqual(sorted(errors), ["NOT_KITTED"])
        reserved_total = sum(b["qty_reserved"] for b in svc.list_batches("M1"))
        self.assertEqual(reserved_total, 150)
        self.assertEqual(svc.conservation_check()["violations"], [])

    @staticmethod
    def _cleanup_db(path: str) -> None:
        import os
        for ext in ("", "-wal", "-shm"):
            try:
                os.remove(path + ext)
            except FileNotFoundError:
                pass

    def test_reconfirm_replaces_old_reservations(self) -> None:
        self.svc.create_order("ORD-C", "P", [{"material": "M1", "qty_required": 30}], priority=5)
        self.svc.confirm("ORD-C")  # B2 上 30
        # 重新持有：旧的正式预留应被释放替换，只留新 HELD
        self.svc.hold("ORD-C", ttl_seconds=120)
        order = self.svc.get_order("ORD-C")
        self.assertEqual(len(order["active_reservations"]), 1)
        self.assertEqual(order["active_reservations"][0]["state"], ReservationState.HELD.value)
        self.assertEqual(order["active_reservations"][0]["qty"], 30)
        reasons = {r["release_reason"] for r in order["history"]}
        self.assertIn(ReleaseReason.REPLACED_HOLD.value, reasons)
        self.assertEqual(self.svc.conservation_check()["violations"], [])


if __name__ == "__main__":
    unittest.main()
