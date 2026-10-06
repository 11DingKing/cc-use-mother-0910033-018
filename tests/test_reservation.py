"""生产领料预留服务的领域回归测试。"""
from __future__ import annotations

import sys
import threading
import unittest
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from reservation_service import ConflictError, DomainError, ReservationService, Store
from reservation_service.errors import InvariantViolation

# 固定"今天"为 2026-10-01，使效期相关用例可写确定日期
START_TS = datetime(2026, 10, 1, tzinfo=timezone.utc).timestamp()


class FakeClock:
    def __init__(self, start: float = START_TS) -> None:
        self.t = start

    def __call__(self) -> float:
        return self.t

    def advance(self, seconds: float) -> None:
        self.t += seconds


class ReservationTestBase(unittest.TestCase):
    def setUp(self) -> None:
        self.clock = FakeClock()
        self.svc = ReservationService(Store(":memory:"), clock=self.clock)

    def batch(self, batch_id: str, material_id: str, qty: int, **kw) -> None:
        self.svc.register_batch({"batch_id": batch_id, "material_id": material_id,
                                 "qty_on_hand": qty, **kw})

    def demand(self, demand_id: str, lines, *, priority: int = 0, deadline=None) -> dict:
        normalized: list[dict] = []
        for item in lines:
            if isinstance(item, tuple):
                normalized.append({"material_id": item[0], "qty": item[1]})
            else:
                normalized.append(item)
        return self.svc.create_demand({
            "demand_id": demand_id, "product": "P", "priority": priority,
            "deadline": deadline, "lines": normalized,
        })

    def available(self, material_id: str) -> int:
        return sum(b["available"] for b in self.svc.occupancy(material_id)["batches"])


class FefoAndExpiryTest(ReservationTestBase):
    def test_fefo_picks_earliest_expiry_first(self) -> None:
        self.batch("B_OLD", "M", 50, expiry_date="2026-11-01")
        self.batch("B_NEW", "M", 50, expiry_date="2027-01-01")
        self.demand("D", [("M", 30)], deadline="2026-10-20")
        result = self.svc.confirm("D", {"policy": "ALL_OR_NOTHING"})
        alloc = result["reserved"][0]["batches"]
        self.assertEqual([a["batch_id"] for a in alloc], ["B_OLD"])

    def test_expired_and_quarantined_batches_excluded(self) -> None:
        self.batch("B_EXP", "M", 40, expiry_date="2026-09-01")
        self.batch("B_Q", "M", 40, quarantined=True)
        self.batch("B_OK", "M", 20, expiry_date="2026-12-01")
        self.demand("D", [("M", 25)], deadline="2026-10-20")
        plan = self.svc.plan("D")
        self.assertFalse(plan["feasible"])
        s = plan["all_shortages_blocked"][0]
        self.assertEqual(s["gap"], 5)
        self.assertTrue(any("保质期" in r for r in s["reasons"]))
        self.assertTrue(any("质检隔离" in r for r in s["reasons"]))

    def test_batch_expiry_releases_existing_reservation(self) -> None:
        self.batch("B1", "M", 30, expiry_date="2026-10-10")
        self.demand("D", [("M", 30)], deadline="2026-10-01")
        self.svc.confirm("D", {"policy": "ALL_OR_NOTHING"})
        self.assertEqual(self.available("M"), 0)
        self.clock.advance(86400 * 20)  # 跨过有效期
        swept = self.svc.sweep_timeouts()
        self.assertEqual(swept["batch_expired"], 1)
        self.assertEqual(self.available("M"), 30)
        self.assertEqual(self.svc.get_demand("D")["status"], "待确认")


class KittingPolicyTest(ReservationTestBase):
    def test_all_or_nothing_is_atomic_on_shortage(self) -> None:
        self.batch("B1", "M1", 100)
        self.batch("B2", "M2", 5)
        self.demand("D", [("M1", 100), ("M2", 10)])
        result = self.svc.confirm("D", {"policy": "ALL_OR_NOTHING"})
        self.assertIsNone(result["group_id"])
        # 缺一种 => 多物料全部不写入
        self.assertEqual(self.available("M1"), 100)
        self.assertEqual(self.available("M2"), 5)
        self.assertEqual(len(result["shortages"]), 1)
        self.assertEqual(result["shortages"][0]["gap"], 5)
        self.assertEqual(self.svc.get_demand("D")["lines"][1]["status"], "缺料")

    def test_partial_kit_reserves_feasible_lines(self) -> None:
        self.batch("B1", "M1", 100)
        self.batch("B2", "M2", 5)
        self.demand("D", [("M1", 100), ("M2", 10)])
        result = self.svc.confirm("D", {"policy": "PARTIAL"})
        self.assertIsNotNone(result["group_id"])
        by_mat = {r["line_material_id"]: r for r in result["reserved"]}
        self.assertEqual(by_mat["M1"]["reserved_qty"], 100)
        self.assertEqual(by_mat["M2"]["reserved_qty"], 5)
        self.assertEqual(by_mat["M2"]["short"], 5)
        self.assertEqual(self.available("M1"), 0)
        self.assertEqual(self.available("M2"), 0)

    def test_partial_pending_keeps_demand_pending_for_decision(self) -> None:
        self.batch("B1", "M1", 10)
        self.demand("D", [("M1", 10), ("M2", 5)])
        result = self.svc.confirm("D", {"policy": "PARTIAL_PENDING"})
        self.assertIsNotNone(result["group_id"])
        self.assertEqual(self.svc.get_demand("D")["status"], "待确认")


class OccupancyAndAdjustmentTest(ReservationTestBase):
    def test_shortage_exposes_occupancy_sources_and_preemption(self) -> None:
        self.batch("B1", "M", 100)
        self.demand("LOW", [("M", 100)], priority=1)
        self.svc.confirm("LOW", {"policy": "ALL_OR_NOTHING"})
        self.demand("HIGH", [("M", 20)], priority=9)
        plan = self.svc.plan("HIGH")
        s = plan["all_shortages_blocked"][0]
        self.assertEqual(s["occupied_by"][0]["demand_id"], "LOW")
        self.assertTrue(s["occupied_by"][0]["preemptible"])
        types = [a["type"] for a in plan["adjustments"]]
        self.assertIn("release_lower_priority", types)
        # 低优先级占用足以补齐缺口时，不再建议等待补货
        self.assertNotIn("wait_supply", types)
        rel = next(a for a in plan["adjustments"] if a["type"] == "release_lower_priority")
        self.assertEqual(rel["target_demand_id"], "LOW")
        self.assertEqual(rel["gain_qty"], 20)

    def test_hard_gap_offers_wait_supply(self) -> None:
        self.batch("B1", "M", 5)
        self.demand("D", [("M", 20)])
        plan = self.svc.plan("D")
        types = [a["type"] for a in plan["adjustments"]]
        self.assertIn("wait_supply", types)

    def test_higher_priority_holder_is_not_preemptible(self) -> None:
        self.batch("B1", "M", 100)
        self.demand("HIGH", [("M", 100)], priority=9)
        self.svc.confirm("HIGH", {"policy": "ALL_OR_NOTHING"})
        self.demand("LOW", [("M", 10)], priority=1)
        plan = self.svc.plan("LOW")
        s = plan["all_shortages_blocked"][0]
        self.assertFalse(s["occupied_by"][0]["preemptible"])
        self.assertNotIn("release_lower_priority", [a["type"] for a in plan["adjustments"]])


class ReductionAndConservationTest(ReservationTestBase):
    def test_reduction_releases_surplus_conserving_totals(self) -> None:
        self.batch("B1", "M1", 100, expiry_date="2026-12-01")
        self.batch("B2", "M1", 50, expiry_date="2027-06-01")
        self.demand("D", [("M1", 120)])
        self.svc.confirm("D", {"policy": "ALL_OR_NOTHING"})
        self.assertEqual(self.available("M1"), 30)
        result = self.svc.reduce_demand("D", {"M1": 80})
        self.assertEqual(result["reductions"][0]["released_qty"], 40)
        self.assertEqual(self.available("M1"), 70)  # 30 + 释放的 40
        # 缩减释放优先从最晚到期批次拿回：B2 的 20 整行释放，B1 再释放 20
        occ = self.svc.occupancy("M1")["batches"]
        by_id = {b["batch_id"]: b for b in occ}
        self.assertEqual(by_id["B2"]["reserved"], 0)
        self.assertEqual(by_id["B1"]["reserved"], 80)

    def test_cannot_reduce_below_issued_qty(self) -> None:
        self.batch("B1", "M", 100)
        self.demand("D", [("M", 50)])
        self.svc.confirm("D", {"policy": "ALL_OR_NOTHING"})
        self.svc.issue("D", [{"material_id": "M", "qty": 30}])
        with self.assertRaises(ConflictError) as cm:
            self.svc.reduce_demand("D", {"M": 20})
        self.assertEqual(cm.exception.code, "BELOW_ISSUED")

    def test_issue_reduces_onhand_and_reservation_together(self) -> None:
        self.batch("B1", "M", 50)
        self.demand("D", [("M", 50)])
        self.svc.confirm("D", {"policy": "ALL_OR_NOTHING"})
        self.svc.issue("D", [{"material_id": "M", "qty": 20}])
        self.assertEqual(self.svc.get_batch("B1")["qty_on_hand"], 30)
        self.assertEqual(self.available("M"), 0)  # 剩余 30 仍被预留
        self.assertEqual(self.svc.get_demand("D")["status"], "履行中")

    def test_reconfirm_replaces_old_reservations_without_double_counting(self) -> None:
        self.batch("B1", "M", 100)
        self.demand("D", [("M", 60)])
        self.svc.confirm("D", {"policy": "PARTIAL"})
        self.assertEqual(self.available("M"), 40)
        # 缩减后再次确认不应叠加旧预留
        self.svc.reduce_demand("D", {"M": 30})
        self.svc.confirm("D", {"policy": "ALL_OR_NOTHING"})
        self.assertEqual(self.available("M"), 70)


class SubstituteTest(ReservationTestBase):
    def test_unapproved_substitute_rejected(self) -> None:
        self.batch("B1", "M", 10)
        self.batch("BS", "S", 100)
        self.demand("D", [("M", 10)])
        with self.assertRaises(DomainError) as cm:
            self.svc.plan("D", {"use_substitutes": {"M": "S"}})
        self.assertEqual(cm.exception.code, "SUBSTITUTE_NOT_APPROVED")

    def test_approved_substitute_uses_ratio_and_marks_line(self) -> None:
        self.batch("BS", "S", 100)
        self.demand("D", [{"material_id": "M", "qty": 10,
                           "substitutes": {"S": 2}}])
        self.svc.approve_substitute("D", "M", "S", 2)
        result = self.svc.confirm("D", {"policy": "ALL_OR_NOTHING",
                                        "use_substitutes": {"M": "S"}})
        row = result["reserved"][0]
        self.assertEqual(row["material_id"], "S")
        self.assertEqual(row["reserved_qty"], 20)  # 10 主料 × 转换比 2
        line = self.svc.get_demand("D")["lines"][0]
        self.assertEqual(line["status"], "替代料")
        self.assertEqual(line["use_substitute"], "S")

    def test_substitute_offered_as_adjustment(self) -> None:
        self.batch("B1", "M", 5)
        self.batch("BS", "S", 100)
        self.demand("D", [{"material_id": "M", "qty": 10,
                           "substitutes": {"S": 1}}])
        self.svc.approve_substitute("D", "M", "S", 1)
        plan = self.svc.plan("D")
        sub = next(a for a in plan["adjustments"] if a["type"] == "substitute")
        self.assertEqual(sub["substitute_material_id"], "S")
        self.assertEqual(sub["gain_qty"], 10)


class TimeoutAndHoldTest(ReservationTestBase):
    def test_tentative_hold_blocks_then_commits(self) -> None:
        self.batch("B1", "M", 50)
        self.demand("D", [("M", 50)])
        hold = self.svc.confirm("D", {"policy": "ALL_OR_NOTHING",
                                      "tentative": True, "ttl_seconds": 60})
        self.assertEqual(hold["state"], "tentative")
        self.assertEqual(self.available("M"), 0)
        committed = self.svc.commit_hold("D", hold["group_id"])
        self.assertEqual(committed["state"], "held")

    def test_timeout_auto_releases_hold(self) -> None:
        self.batch("B1", "M", 50)
        self.demand("D", [("M", 50)])
        self.svc.confirm("D", {"policy": "ALL_OR_NOTHING",
                               "tentative": True, "ttl_seconds": 60})
        self.clock.advance(61)
        result = self.svc.sweep_timeouts()
        self.assertEqual(result["timeout_released"], 1)
        self.assertEqual(self.available("M"), 50)

    def test_commit_after_timeout_rejected(self) -> None:
        self.batch("B1", "M", 50)
        self.demand("D", [("M", 50)])
        hold = self.svc.confirm("D", {"policy": "ALL_OR_NOTHING",
                                      "tentative": True, "ttl_seconds": 60})
        self.clock.advance(61)
        with self.assertRaises(ConflictError) as cm:
            self.svc.commit_hold("D", hold["group_id"])
        self.assertEqual(cm.exception.code, "HOLD_EXPIRED")


class ConcurrencyTest(ReservationTestBase):
    def test_concurrent_confirms_conserve_quantity(self) -> None:
        self.batch("B1", "M", 100)
        n = 10
        for i in range(n):
            self.demand(f"D{i}", [("M", 15)], priority=i)
        outcomes: list[dict] = []
        errors: list[Exception] = []

        def worker(i: int) -> None:
            try:
                outcomes.append(self.svc.confirm(
                    f"D{i}", {"policy": "ALL_OR_NOTHING"}, idem_key=f"k{i}"))
            except Exception as exc:  # noqa: BLE001
                errors.append(exc)

        threads = [threading.Thread(target=worker, args=(i,)) for i in range(n)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        self.assertFalse(errors)
        held = [o for o in outcomes if o["group_id"]]
        # 100 / 15 => 恰好 6 单成功（90），其余缺料，有效预留绝不超在库
        self.assertEqual(len(held), 6)
        self.assertEqual(100 - self.available("M"), 90)

    def test_concurrent_idempotent_retry_writes_once(self) -> None:
        self.batch("B1", "M", 100)
        self.demand("D", [("M", 10)])
        results: list[dict] = []

        def worker() -> None:
            results.append(self.svc.confirm(
                "D", {"policy": "ALL_OR_NOTHING"}, idem_key="SAME-KEY"))

        threads = [threading.Thread(target=worker) for _ in range(8)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        groups = {r["group_id"] for r in results}
        self.assertEqual(len(groups), 1)
        self.assertEqual(100 - self.available("M"), 10)

    def test_optimistic_version_blocks_stale_confirm(self) -> None:
        self.batch("B1", "M", 100)
        self.demand("D", [("M", 10)])
        self.svc.approve_substitute("D", "M", "S", 1)  # 版本 +1
        with self.assertRaises(ConflictError) as cm:
            self.svc.confirm("D", {"policy": "ALL_OR_NOTHING", "expected_version": 0})
        self.assertEqual(cm.exception.code, "VERSION_CONFLICT")


class OrphanRecoveryTest(ReservationTestBase):
    def _insert_orphan_group(self) -> None:
        conn = self.svc.store.conn()
        conn.execute("BEGIN IMMEDIATE")
        conn.execute(
            "INSERT INTO reservation_groups(group_id,demand_id,policy,state,complete,created_at)"
            " VALUES ('G_ORPHAN','D','PARTIAL','held',0,?)", (self.clock.t - 9999,))
        conn.execute(
            "INSERT INTO reservations(reservation_id,group_id,demand_id,line_material_id,"
            " material_id,batch_id,qty,state,created_at)"
            " VALUES ('R_ORPHAN','G_ORPHAN','D','M','M','B1',5,'held',?)",
            (self.clock.t - 9999,))
        conn.execute("COMMIT")

    def test_incomplete_group_cleaned_after_grace(self) -> None:
        self.batch("B1", "M", 100)
        self.demand("D", [("M", 10)])
        self.svc.confirm("D", {"policy": "ALL_OR_NOTHING"})
        self.assertEqual(self.available("M"), 90)
        self._insert_orphan_group()
        self.assertEqual(self.available("M"), 85)
        report = self.svc.recover_orphans()
        reasons = {a["reason"] for a in report["actions"]}
        self.assertIn("orphan_incomplete_group", reasons)
        self.assertEqual(self.available("M"), 90)

    def test_fresh_incomplete_group_spared_until_grace_passes(self) -> None:
        self.batch("B1", "M", 100)
        self.demand("D", [("M", 10)])
        self.svc.confirm("D", {"policy": "ALL_OR_NOTHING"})
        self._insert_orphan_group()
        # 刚创建（宽限期内，此处伪造的是旧时间 -> 改成当前时间再验证）
        conn = self.svc.store.conn()
        conn.execute("BEGIN IMMEDIATE")
        conn.execute("UPDATE reservation_groups SET created_at=? WHERE group_id='G_ORPHAN'",
                     (self.clock.t,))
        conn.execute("UPDATE reservations SET created_at=? WHERE reservation_id='R_ORPHAN'",
                     (self.clock.t,))
        conn.execute("COMMIT")
        report = self.svc.recover_orphans()
        self.assertFalse(any(a["reservation_id"] == "R_ORPHAN" for a in report["actions"]))
        # force 立即清理
        report = self.svc.recover_orphans(force=True)
        self.assertTrue(any(a["reservation_id"] == "R_ORPHAN" for a in report["actions"]))

    def test_missing_group_header_is_orphan(self) -> None:
        self.batch("B1", "M", 100)
        self.demand("D", [("M", 5)])
        self.svc.confirm("D", {"policy": "ALL_OR_NOTHING"})
        conn = self.svc.store.conn()
        conn.execute("BEGIN IMMEDIATE")
        conn.execute(
            "INSERT INTO reservations(reservation_id,group_id,demand_id,line_material_id,"
            " material_id,batch_id,qty,state,created_at)"
            " VALUES ('R_NOG','G_MISSING','D','M','M','B1',3,'held',?)", (self.clock.t,))
        conn.execute("COMMIT")
        report = self.svc.recover_orphans()
        self.assertTrue(any(a["reason"] == "orphan_group_missing" for a in report["actions"]))
        self.assertEqual(self.available("M"), 95)

    def test_overcommit_repairs_conservation(self) -> None:
        self.batch("B1", "M", 10)
        self.demand("D", [("M", 5)])
        self.svc.confirm("D", {"policy": "ALL_OR_NOTHING"})
        # 外部数据损坏：在库被下调到预留之下
        conn = self.svc.store.conn()
        conn.execute("BEGIN IMMEDIATE")
        conn.execute("UPDATE batches SET qty_on_hand=3 WHERE batch_id='B1'")
        conn.execute("COMMIT")
        report = self.svc.recover_orphans()
        self.assertTrue(any("overcommit" in a["reason"] for a in report["actions"]))
        self.assertGreaterEqual(self.available("M"), 0)


class InvariantGuardTest(unittest.TestCase):
    def test_write_txn_rejects_when_reservation_exceeds_onhand(self) -> None:
        store = Store(":memory:")
        svc = ReservationService(store)
        svc.register_batch({"batch_id": "B", "material_id": "M", "qty_on_hand": 100})
        # 外部数据损坏：有效预留超过在库
        conn = store.conn()
        conn.execute("BEGIN IMMEDIATE")
        conn.execute(
            "INSERT INTO reservation_groups(group_id,demand_id,policy,state,complete,created_at)"
            " VALUES ('G','D','ALL_OR_NOTHING','held',1,1.0)")
        conn.execute(
            "INSERT INTO reservations(reservation_id,group_id,demand_id,line_material_id,"
            " material_id,batch_id,qty,state,created_at)"
            " VALUES ('R','G','D','M','M','B',101,'held',1.0)")
        conn.execute("COMMIT")
        # 任意正常写事务提交前的守恒断言必须报错并回滚
        with self.assertRaises(InvariantViolation):
            svc.register_batch({"batch_id": "B2", "material_id": "M2", "qty_on_hand": 1})


if __name__ == "__main__":
    unittest.main()
