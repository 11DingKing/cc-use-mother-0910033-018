"""SQLite 存储层。

关键设计：
- 批次 ``qty_on_hand`` 不可被预留直接修改；可用量 = 在库 - 有效预留，保证数量守恒可推导。
- 同一预留组的全部明细与组头在单个事务内提交（原子多物料预留）。
- 组头 ``complete`` 标记崩溃边界：恢复任务把不完整且超过宽限期的组按孤儿清理。
"""
from __future__ import annotations

import json
import sqlite3
import threading
from datetime import date
from pathlib import Path
from typing import Any, Iterable, Optional

SCHEMA = """
CREATE TABLE IF NOT EXISTS demands (
    demand_id        TEXT PRIMARY KEY,
    product          TEXT NOT NULL,
    priority         INTEGER NOT NULL,
    status           TEXT NOT NULL,
    created_at       TEXT NOT NULL,
    deadline         TEXT,
    lock_version     INTEGER NOT NULL DEFAULT 0,
    hold_ttl_seconds REAL
);
CREATE TABLE IF NOT EXISTS demand_lines (
    demand_id      TEXT NOT NULL,
    idx            INTEGER NOT NULL,
    material_id    TEXT NOT NULL,
    qty            INTEGER NOT NULL CHECK (qty >= 0),
    use_substitute TEXT,
    status         TEXT NOT NULL,
    shortage_reason TEXT,
    substitutes    TEXT NOT NULL DEFAULT '{}',
    issued_qty     INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY (demand_id, idx)
);
CREATE TABLE IF NOT EXISTS batches (
    batch_id        TEXT PRIMARY KEY,
    material_id    TEXT NOT NULL,
    qty_on_hand    INTEGER NOT NULL CHECK (qty_on_hand >= 0),
    expiry_date     TEXT,
    received_date   TEXT,
    shelf_life_days INTEGER,
    location        TEXT NOT NULL DEFAULT '',
    quarantined     INTEGER NOT NULL DEFAULT 0
);
CREATE TABLE IF NOT EXISTS reservation_groups (
    group_id   TEXT PRIMARY KEY,
    demand_id  TEXT NOT NULL,
    policy     TEXT NOT NULL,
    state      TEXT NOT NULL,            -- tentative / held / released
    complete   INTEGER NOT NULL DEFAULT 0,
    created_at REAL NOT NULL,
    expires_at REAL
);
CREATE TABLE IF NOT EXISTS reservations (
    reservation_id  TEXT PRIMARY KEY,
    group_id        TEXT NOT NULL,
    demand_id       TEXT NOT NULL,
    line_material_id TEXT NOT NULL,
    material_id     TEXT NOT NULL,
    batch_id        TEXT NOT NULL,
    qty             INTEGER NOT NULL CHECK (qty > 0),
    state           TEXT NOT NULL,        -- tentative / held / consumed / released / expired
    tentative       INTEGER NOT NULL DEFAULT 0,
    created_at      REAL NOT NULL,
    expires_at      REAL,
    substitution_of TEXT,
    release_reason  TEXT
);
CREATE INDEX IF NOT EXISTS idx_res_batch    ON reservations(batch_id);
CREATE INDEX IF NOT EXISTS idx_res_demand   ON reservations(demand_id);
CREATE INDEX IF NOT EXISTS idx_res_material ON reservations(material_id, state);
CREATE INDEX IF NOT EXISTS idx_groups_demand ON reservation_groups(demand_id);
CREATE TABLE IF NOT EXISTS idempotency (
    idem_key  TEXT PRIMARY KEY,
    response  TEXT NOT NULL,
    created_at REAL NOT NULL
);
"""

ACTIVE_STATES = ("tentative", "held")


def _d(value: Optional[date]) -> Optional[str]:
    return value.isoformat() if value else None


def _parse_d(value: Optional[str]) -> Optional[date]:
    return date.fromisoformat(value) if value else None


class Store:
    """串行化写入的 SQLite 封装。读方法可在写事务内复用同一连接。"""

    def __init__(self, path: str | Path = ":memory:") -> None:
        self.path = str(path)
        if self.path != ":memory:":
            Path(self.path).parent.mkdir(parents=True, exist_ok=True)
        self._lock = threading.RLock()
        self._mem_conn: Optional[sqlite3.Connection] = None
        self._init_schema()

    # ---- 连接管理 -------------------------------------------------------

    def _connect(self) -> sqlite3.Connection:
        # 内存库共享单连接，会被多线程使用；全部访问由 self._lock 串行化保证安全。
        conn = sqlite3.connect(self.path, timeout=10, isolation_level=None,
                               check_same_thread=False)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA foreign_keys=ON")
        conn.execute("PRAGMA busy_timeout=10000")
        if self.path != ":memory:":
            conn.execute("PRAGMA journal_mode=WAL")
        return conn

    def conn(self) -> sqlite3.Connection:
        if self.path == ":memory:":
            if self._mem_conn is None:
                self._mem_conn = self._connect()
            return self._mem_conn
        return self._connect()

    def _init_schema(self) -> None:
        with self._lock:
            conn = self.conn()
            conn.executescript(SCHEMA)

    @property
    def lock(self) -> threading.RLock:
        return self._lock

    def begin_immediate(self, conn: sqlite3.Connection) -> None:
        conn.execute("BEGIN IMMEDIATE")

    # ---- 需求 -----------------------------------------------------------

    def insert_demand(self, conn: sqlite3.Connection, demand: dict[str, Any]) -> None:
        conn.execute(
            "INSERT INTO demands(demand_id, product, priority, status, created_at,"
            " deadline, lock_version, hold_ttl_seconds) VALUES (?,?,?,?,?,?,?,?)",
            (
                demand["demand_id"], demand["product"], demand["priority"],
                demand["status"], demand["created_at"], demand.get("deadline"),
                demand.get("lock_version", 0), demand.get("hold_ttl_seconds"),
            ),
        )
        for idx, line in enumerate(demand["lines"]):
            conn.execute(
                "INSERT INTO demand_lines(demand_id, idx, material_id, qty,"
                " use_substitute, status, shortage_reason, substitutes, issued_qty)"
                " VALUES (?,?,?,?,?,?,?,?,?)",
                (
                    demand["demand_id"], idx, line["material_id"], line["qty"],
                    line.get("use_substitute"), line.get("status", "需预留"),
                    line.get("shortage_reason"),
                    json.dumps(line.get("substitutes", {}), ensure_ascii=False),
                    line.get("issued_qty", 0),
                ),
            )

    def get_demand(self, conn: sqlite3.Connection, demand_id: str) -> Optional[dict[str, Any]]:
        row = conn.execute("SELECT * FROM demands WHERE demand_id=?", (demand_id,)).fetchone()
        if row is None:
            return None
        d = dict(row)
        d["lines"] = self.get_lines(conn, demand_id)
        return d

    def get_lines(self, conn: sqlite3.Connection, demand_id: str) -> list[dict[str, Any]]:
        rows = conn.execute(
            "SELECT * FROM demand_lines WHERE demand_id=? ORDER BY idx", (demand_id,)
        ).fetchall()
        out = []
        for r in rows:
            line = dict(r)
            line["substitutes"] = json.loads(line["substitutes"] or "{}")
            out.append(line)
        return out

    def list_demands(self, conn: sqlite3.Connection) -> list[dict[str, Any]]:
        rows = conn.execute("SELECT demand_id FROM demands ORDER BY demand_id").fetchall()
        return [self.get_demand(conn, r["demand_id"]) for r in rows]

    def update_line(self, conn: sqlite3.Connection, demand_id: str, material_id: str, **fields: Any) -> None:
        sets, vals = [], []
        for k, v in fields.items():
            if k == "substitutes":
                v = json.dumps(v, ensure_ascii=False)
            sets.append(f"{k}=?")
            vals.append(v)
        vals += [demand_id, material_id]
        conn.execute(
            f"UPDATE demand_lines SET {', '.join(sets)}"
            " WHERE demand_id=? AND material_id=?", vals
        )

    def bump_demand(self, conn: sqlite3.Connection, demand_id: str, status: Optional[str] = None,
                    *, deadline: Any = ..., hold_ttl: Any = ...) -> int:
        sets = ["lock_version=lock_version+1"]
        vals: list[Any] = []
        if status is not None:
            sets.append("status=?")
            vals.append(status)
        if deadline is not ...:
            sets.append("deadline=?")
            vals.append(deadline)
        if hold_ttl is not ...:
            sets.append("hold_ttl_seconds=?")
            vals.append(hold_ttl)
        vals.append(demand_id)
        conn.execute(f"UPDATE demands SET {', '.join(sets)} WHERE demand_id=?", vals)
        row = conn.execute("SELECT lock_version FROM demands WHERE demand_id=?", (demand_id,)).fetchone()
        return row["lock_version"]

    # ---- 批次 -----------------------------------------------------------

    def upsert_batch(self, conn: sqlite3.Connection, batch: dict[str, Any]) -> None:
        conn.execute(
            "INSERT INTO batches(batch_id, material_id, qty_on_hand, expiry_date,"
            " received_date, shelf_life_days, location, quarantined)"
            " VALUES (?,?,?,?,?,?,?,?)"
            " ON CONFLICT(batch_id) DO UPDATE SET"
            " material_id=excluded.material_id, qty_on_hand=excluded.qty_on_hand,"
            " expiry_date=excluded.expiry_date, received_date=excluded.received_date,"
            " shelf_life_days=excluded.shelf_life_days, location=excluded.location,"
            " quarantined=excluded.quarantined",
            (
                batch["batch_id"], batch["material_id"], batch["qty_on_hand"],
                _d(batch.get("expiry_date")), _d(batch.get("received_date")),
                batch.get("shelf_life_days"), batch.get("location", ""),
                1 if batch.get("quarantined") else 0,
            ),
        )

    def get_batches(self, conn: sqlite3.Connection, material_id: Optional[str] = None) -> list[dict[str, Any]]:
        if material_id is None:
            rows = conn.execute("SELECT * FROM batches ORDER BY material_id, batch_id").fetchall()
        else:
            rows = conn.execute(
                "SELECT * FROM batches WHERE material_id=? ORDER BY batch_id", (material_id,)
            ).fetchall()
        out = []
        for r in rows:
            b = dict(r)
            b["quarantined"] = bool(b["quarantined"])
            b["expiry_date"] = _parse_d(b["expiry_date"])
            b["received_date"] = _parse_d(b["received_date"])
            out.append(b)
        return out

    # ---- 预留组 / 预留 ---------------------------------------------------

    def insert_group(self, conn: sqlite3.Connection, group: dict[str, Any], complete: bool = False) -> None:
        conn.execute(
            "INSERT INTO reservation_groups(group_id, demand_id, policy, state, complete,"
            " created_at, expires_at) VALUES (?,?,?,?,?,?,?)",
            (
                group["group_id"], group["demand_id"], group["policy"],
                group["state"], 1 if complete else 0,
                group["created_at"], group.get("expires_at"),
            ),
        )

    def set_group_state(self, conn: sqlite3.Connection, group_id: str, state: str,
                        complete: Optional[bool] = None, expires_at: Any = ...) -> None:
        sets = ["state=?"]
        vals: list[Any] = [state]
        if complete is not None:
            sets.append("complete=?")
            vals.append(1 if complete else 0)
        if expires_at is not ...:
            sets.append("expires_at=?")
            vals.append(expires_at)
        vals.append(group_id)
        conn.execute(f"UPDATE reservation_groups SET {', '.join(sets)} WHERE group_id=?", vals)

    def get_group(self, conn: sqlite3.Connection, group_id: str) -> Optional[dict[str, Any]]:
        row = conn.execute(
            "SELECT * FROM reservation_groups WHERE group_id=?", (group_id,)
        ).fetchone()
        return dict(row) if row else None

    def list_groups(self, conn: sqlite3.Connection, demand_id: Optional[str] = None,
                    states: Iterable[str] = ACTIVE_STATES) -> list[dict[str, Any]]:
        states = tuple(states)
        q = "SELECT * FROM reservation_groups WHERE state IN (%s)" % ",".join("?" * len(states))
        params: list[Any] = list(states)
        if demand_id:
            q += " AND demand_id=?"
            params.append(demand_id)
        q += " ORDER BY created_at"
        return [dict(r) for r in conn.execute(q, params).fetchall()]

    def insert_reservation(self, conn: sqlite3.Connection, r: dict[str, Any]) -> None:
        conn.execute(
            "INSERT INTO reservations(reservation_id, group_id, demand_id, line_material_id,"
            " material_id, batch_id, qty, state, tentative, created_at, expires_at,"
            " substitution_of, release_reason)"
            " VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)",
            (
                r["reservation_id"], r["group_id"], r["demand_id"], r["line_material_id"],
                r["material_id"], r["batch_id"], r["qty"], r["state"],
                1 if r.get("tentative") else 0, r["created_at"], r.get("expires_at"),
                r.get("substitution_of"), r.get("release_reason"),
            ),
        )

    def list_reservations(self, conn: sqlite3.Connection, *, group_id: Optional[str] = None,
                          demand_id: Optional[str] = None, states: Iterable[str] = ACTIVE_STATES
                          ) -> list[dict[str, Any]]:
        states = tuple(states)
        q = "SELECT * FROM reservations WHERE state IN (%s)" % ",".join("?" * len(states))
        params: list[Any] = list(states)
        if group_id:
            q += " AND group_id=?"
            params.append(group_id)
        if demand_id:
            q += " AND demand_id=?"
            params.append(demand_id)
        q += " ORDER BY created_at, reservation_id"
        return [dict(r) for r in conn.execute(q, params).fetchall()]

    def release_reservations(self, conn: sqlite3.Connection, reservation_ids: list[str],
                             state: str = "released", reason: str = "") -> int:
        if not reservation_ids:
            return 0
        conn.executemany(
            "UPDATE reservations SET state=?, release_reason=? WHERE reservation_id=?",
            [(state, reason, rid) for rid in reservation_ids],
        )
        return len(reservation_ids)

    # ---- 守恒校验 --------------------------------------------------------

    def active_totals_by_batch(self, conn: sqlite3.Connection) -> dict[str, int]:
        rows = conn.execute(
            "SELECT batch_id, SUM(qty) AS q FROM reservations"
            " WHERE state IN ('tentative','held') GROUP BY batch_id"
        ).fetchall()
        return {r["batch_id"]: r["q"] for r in rows}

    def on_hand_by_batch(self, conn: sqlite3.Connection) -> dict[str, int]:
        rows = conn.execute("SELECT batch_id, qty_on_hand FROM batches").fetchall()
        return {r["batch_id"]: r["qty_on_hand"] for r in rows}

    # ---- 幂等与发料 ------------------------------------------------------

    def get_idem(self, conn: sqlite3.Connection, key: str) -> Optional[str]:
        row = conn.execute("SELECT response FROM idempotency WHERE idem_key=?", (key,)).fetchone()
        return row["response"] if row else None

    def put_idem(self, conn: sqlite3.Connection, key: str, response: str, now: float) -> None:
        conn.execute(
            "INSERT OR IGNORE INTO idempotency(idem_key, response, created_at) VALUES (?,?,?)",
            (key, response, now),
        )

    def add_issued_qty(self, conn: sqlite3.Connection, demand_id: str, material_id: str,
                       qty: int) -> None:
        conn.execute(
            "UPDATE demand_lines SET issued_qty=issued_qty+?"
            " WHERE demand_id=? AND material_id=?",
            (qty, demand_id, material_id),
        )
