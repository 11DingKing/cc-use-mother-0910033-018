"""领域契约校验工具。"""
from __future__ import annotations

import json
from pathlib import Path


REQUIRED = {"schema_version", "product", "source_context", "actors", "states", "invariants", "sample_cases", "tags"}


def load_contract(path: str | Path) -> dict:
    """读取并校验领域契约。"""
    value = json.loads(Path(path).read_text(encoding="utf-8"))
    missing = REQUIRED - value.keys()
    if missing:
        raise ValueError("领域契约缺少字段：" + "、".join(sorted(missing)))
    if value["schema_version"] != 1:
        raise ValueError("不支持的契约版本")
    for key in ("actors", "states", "invariants", "sample_cases", "tags"):
        if not isinstance(value[key], list) or not value[key]:
            raise ValueError(f"{key} 必须是非空列表")
    case_ids = [item.get("case_id") for item in value["sample_cases"]]
    if len(case_ids) != len(set(case_ids)):
        raise ValueError("样例编号不能重复")
    return value


def summarize(value: dict) -> dict:
    """生成稳定的契约摘要。"""
    return {
        "product": value["product"],
        "actor_count": len(value["actors"]),
        "state_count": len(value["states"]),
        "invariant_count": len(value["invariants"]),
        "case_count": len(value["sample_cases"]),
    }
