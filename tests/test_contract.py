"""领域契约的基础回归测试。"""
from __future__ import annotations

import sys
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from domain_contract.validator import load_contract, summarize


class ContractTest(unittest.TestCase):
    def test_contract_is_complete(self) -> None:
        value = load_contract(ROOT / "domain" / "contract.json")
        result = summarize(value)
        self.assertGreaterEqual(result["actor_count"], 3)
        self.assertGreaterEqual(result["state_count"], 5)
        self.assertGreaterEqual(result["invariant_count"], 4)
        self.assertEqual(result["case_count"], 2)


if __name__ == "__main__":
    unittest.main()
