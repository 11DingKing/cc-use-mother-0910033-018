"""命令行检查领域契约。"""
from __future__ import annotations

import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from domain_contract.validator import load_contract, summarize


if __name__ == "__main__":
    target = Path(sys.argv[1]) if len(sys.argv) > 1 else ROOT / "domain" / "contract.json"
    print(json.dumps(summarize(load_contract(target)), ensure_ascii=False, sort_keys=True))
