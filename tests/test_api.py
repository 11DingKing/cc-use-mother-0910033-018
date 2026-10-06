"""HTTP 接口集成测试。"""
from __future__ import annotations

import json
import sys
import threading
import unittest
import urllib.error
import urllib.request
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from reservation_service.api import create_server
from reservation_service.store import Store


class ApiTest(unittest.TestCase):
    def setUp(self) -> None:
        self.httpd = create_server("127.0.0.1", 0, store=Store(":memory:"))
        self.port = self.httpd.server_address[1]
        self.thread = threading.Thread(target=self.httpd.serve_forever, daemon=True)
        self.thread.start()

    def tearDown(self) -> None:
        self.httpd.shutdown()
        self.httpd.server_close()

    def call(self, method: str, path: str, body=None, headers=None):
        data = json.dumps(body).encode() if body is not None else None
        req = urllib.request.Request(
            f"http://127.0.0.1:{self.port}{path}", data=data,
            headers={"Content-Type": "application/json", **(headers or {})}, method=method)
        try:
            with urllib.request.urlopen(req) as resp:
                return resp.status, json.loads(resp.read())
        except urllib.error.HTTPError as exc:
            return exc.code, json.loads(exc.read())

    def test_full_flow_over_http(self) -> None:
        s, _ = self.call("POST", "/batches", {
            "batch_id": "B1", "material_id": "M1", "qty_on_hand": 100,
            "expiry_date": "2026-12-01"})
        self.assertEqual(s, 201)
        s, _ = self.call("POST", "/batches", {
            "batch_id": "B2", "material_id": "M2", "qty_on_hand": 5})
        self.assertEqual(s, 201)

        s, d = self.call("POST", "/demands", {
            "product": "P", "priority": 5,
            "lines": [{"material_id": "M1", "qty": 100}, {"material_id": "M2", "qty": 10}]})
        self.assertEqual(s, 201)
        did = d["demand_id"]

        # AON 缺料：整单不预留
        s, plan = self.call("POST", f"/demands/{did}/plan", {"policy": "ALL_OR_NOTHING"})
        self.assertEqual(s, 200)
        self.assertFalse(plan["feasible"])
        self.assertEqual(plan["all_shortages_blocked"][0]["gap"], 5)

        s, fail = self.call("POST", f"/demands/{did}/confirm", {"policy": "ALL_OR_NOTHING"})
        self.assertEqual(s, 200)
        self.assertIsNone(fail["group_id"])

        # PARTIAL 确认：M1 全留，M2 部分
        s, ok = self.call("POST", f"/demands/{did}/confirm", {"policy": "PARTIAL"})
        self.assertEqual(200, s)
        self.assertTrue(ok["group_id"])

        # 占用视图带 holder
        s, occ = self.call("GET", "/occupancy?material_id=M2")
        self.assertEqual(occ["batches"][0]["holders"][0]["demand_id"], did)

        # 缩减释放
        s, red = self.call("POST", f"/demands/{did}/reduce", {"reductions": {"M1": 40}})
        self.assertEqual(red["reductions"][0]["released_qty"], 60)

        # 发料
        s, iss = self.call("POST", f"/demands/{did}/issue",
                           {"items": [{"material_id": "M1", "qty": 40}]})
        self.assertEqual(s, 200)

        # 幂等头
        self.call("POST", "/demands", {"demand_id": "DX", "product": "P",
                                       "lines": [{"material_id": "M2", "qty": 1}]})
        s, a = self.call("POST", "/demands/DX/confirm", {"policy": "ALL_OR_NOTHING"},
                         headers={"Idempotency-Key": "abc"})
        s, b = self.call("POST", "/demands/DX/confirm", {"policy": "ALL_OR_NOTHING"},
                         headers={"Idempotency-Key": "abc"})
        self.assertEqual(a["group_id"], b["group_id"])

        # 运维端点
        s, rec = self.call("POST", "/internal/recover-orphans", {})
        self.assertEqual(s, 200)
        self.assertIn("cleaned", rec)
        s, swp = self.call("POST", "/internal/sweep-timeouts", {})
        self.assertEqual(s, 200)
        self.assertIn("timeout_released", swp)

    def test_error_envelope(self) -> None:
        s, body = self.call("GET", "/demands/NOPE")
        self.assertEqual(s, 404)
        self.assertEqual(body["error"]["code"], "NOT_FOUND")
        req = urllib.request.Request(
            f"http://127.0.0.1:{self.port}/demands", data=b"{",
            headers={"Content-Type": "application/json"}, method="POST")
        with self.assertRaises(urllib.error.HTTPError) as cm:
            urllib.request.urlopen(req)
        self.assertEqual(cm.exception.code, 400)


if __name__ == "__main__":
    unittest.main()
