"""SQLite 存储层：模式、连接管理、基础 CRUD。

并发控制：WAL 模式 + 写事务一律 BEGIN IMMEDIATE。多物料预留的
"扣减可用量 + 写入预留"在同一事务内完成，冲突时第二个写者阻塞/重试，
因此并发预留下批次的已预留数量不会超卖，总量天然守恒。
"""
from __future__ import annotations

import json
import sqlite3
import threading
import uuid
from collections.abc import Iterable
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

SCHEMA = """
CREATE TABLE IF NOT EXISTS orders (
    order_id        TEXT PRIMARY KEY,
    product         TEXT NOT NULL,
    priority        INTEGER NOT NULL,              -- 数值越大优先级越高
    state           TEXT NOT NULL,
    qty_required    REAL NOT NULL,
    due_date        TEXT,                          -- ISO8601，预留超时基准
    created_at      TEXT NOT NULL,
    updated_at      TEXT NOT NULL,
    version         INTEGER NOT NULL DEFAULT 0     -- 乐观锁，缩减时校验
);

CREATE TABLE IF NOT EXISTS order_demands (
    order_id        TEXT NOT NULL,
    material        TEXT NOT NULL,
    qty_required    REAL NOT NULL,
    seq             INTEGER NOT NULL DEFAULT 0,    -- BOM 行号
    PRIMARY KEY (order_id, material),
    FOREIGN KEY (order_id) REFERENCES orders(order_id)
);

CREATE TABLE IF NOT EXISTS materials (
    material        TEXT PRIMARY KEY,
    name            TEXT NOT NULL DEFAULT '',
    unit            TEXT NOT NULL DEFAULT ''
);

CREATE TABLE IF NOT EXISTS substitutes (
    -- material 的替代料为 substitute；替代关系本身在计划时按 BOM 行生效
    material        TEXT NOT NULL,
    substitute      TEXT NOT NULL,
    ratio           REAL NOT NULL DEFAULT 1.0,     -- 1 单位主料需 ratio 单位替代料
    approved        INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY (material, substitute)
);

CREATE TABLE IF NOT EXISTS batches (
    batch_id        TEXT PRIMARY KEY,
    material        TEXT NOT NULL,
    qty_on_hand     REAL NOT NULL,                 -- 在库总量（守恒锚点，不变）
    qty_reserved    REAL NOT NULL DEFAULT 0,       -- 被活跃预留占有的数量
    received_at     TEXT NOT NULL,                 -- 入库时间，FIFO 依据
    expiry_date     TEXT,                          -- 到期日，FEFO/保质期依据
    blocked         INTEGER NOT NULL DEFAULT 0,    -- 质量冻结
    CHECK (qty_reserved >= 0 AND qty_reserved <= qty_on_hand)
);
CREATE INDEX IF NOT EXISTS idx_batches_material ON batches(material);

CREATE TABLE IF NOT EXISTS reservations (
    reservation_id  TEXT PRIMARY KEY,
    order_id        TEXT NOT NULL,
    material        TEXT NOT NULL,                 -- 实际占用的物料（可能是替代料）
    demanded_material TEXT NOT NULL,               -- BOM 行上的需求物料
    batch_id        TEXT NOT NULL,
    qty             REAL NOT NULL CHECK (qty > 0),
    state           TEXT NOT NULL,                 -- HELD / CONFIRMED / RELEASED / CONSUMED
    created_at      TEXT NOT NULL,
    expires_at      TEXT,                          -- HELD 的 TTL / 预留最终期限
    released_at     TEXT,
    release_reason  TEXT,
    replaced_by     TEXT,                          -- 重新持有时指向新预留
    preempted_from  TEXT                           -- 抢占记录：被抢订单 id
);
CREATE INDEX IF NOT EXISTS idx_res_order ON reservations(order_id);
CREATE INDEX IF NOT EXISTS idx_res_batch ON reservations(batch_id);
CREATE INDEX IF NOT EXISTS idx_res_state ON reservations(state, expires_at);

CREATE TABLE IF NOT EXISTS reservation_groups (
    -- 一次多物料预留确认的原子单元
    group_id        TEXT PRIMARY KEY,
    order_id        TEXT NOT NULL,
    state           TEXT NOT NULL,                 -- HELD / CONFIRMED / RELEASED
    created_at      TEXT NOT NULL,
    confirmed_at    TEXT
);
CREATE TABLE IF NOT EXISTS reservation_group_items (
    group_id        TEXT NOT NULL,
    reservation_id  TEXT NOT NULL,
    PRIMARY KEY (group_id, reservation_id)
);

CREATE TABLE IF NOT EXISTS ledger (
    -- 预留数量流水：每次变动守恒校验都可回溯
    ledger_id       INTEGER PRIMARY KEY AUTOINCREMENT,
    reservation_id  TEXT NOT NULL,
    batch_id        TEXT NOT NULL,
    delta           REAL NOT NULL,                 -- +占有 / -释放
    reason          TEXT NOT NULL,
    created_at      TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS events (
    event_id        INTEGER PRIMARY KEY AUTOINCREMENT,
    order_id        TEXT,
    kind            TEXT NOT NULL,
    payload         TEXT NOT NULL,
    created_at      TEXT NOT NULL
);
"""


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


def iso(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).isoformat()


def parse_iso(value: str | None) -> datetime | None:
    if value is None:
        return None
    return datetime.fromisoformat(value)


class Store:
    """封装连接与基础读写。事务由 service 层显式控制。"""

    def __init__(self, path: str | Path = ":memory:") -> None:
        self.path = str(path)
        self._is_memory = self.path == ":memory:"
        self._mem_id = uuid.uuid4().hex
        self._local = threading.local()
        self._all_conns: list[sqlite3.Connection] = []
        self._conns_lock = threading.Lock()
        if not self._is_memory:
            Path(self.path).parent.mkdir(parents=True, exist_ok=True)
        # 初始化当前线程连接并建表
        self._init_conn(self.conn)

    @property
    def conn(self) -> sqlite3.Connection:
        """每线程一个连接：多线程下各自事务，靠 BEGIN IMMEDIATE 串行化写。

        :memory: 模式回退为单连接（仅适合单线程嵌入式用法与测试）。
        """
        existing = getattr(self._local, "conn", None)
        if existing is not None:
            return existing
        if self._is_memory:
            c = self._make_conn(":memory:")
            with self._conns_lock:
                self._all_conns.append(c)
        else:
            c = self._make_conn(self.path)
            with self._conns_lock:
                self._all_conns.append(c)
        self._init_conn(c)
        self._local.conn = c
        return c

    def _make_conn(self, target: str) -> sqlite3.Connection:
        if target == ":memory:":
            # 共享缓存内存库：本 Store 所有连接共享同一份数据，且无需落盘
            uri = f"file:reservation_mem_{self._mem_id}?mode=memory&cache=shared"
            return sqlite3.connect(uri, uri=True, isolation_level=None,
                                   timeout=30.0, check_same_thread=False)
        return sqlite3.connect(
            target,
            isolation_level=None,           # 手工事务
            timeout=30.0,
            check_same_thread=False,
        )

    def _init_conn(self, c: sqlite3.Connection) -> None:
        c.row_factory = sqlite3.Row
        if not self._is_memory:
            c.execute("PRAGMA journal_mode=WAL")
        c.execute("PRAGMA foreign_keys=ON")
        c.execute("PRAGMA busy_timeout=30000")
        c.executescript(SCHEMA)

    def close(self) -> None:
        with self._conns_lock:
            for c in self._all_conns:
                c.close()
            self._all_conns.clear()

    # ---- 事务原语 -------------------------------------------------------
    def begin_immediate(self) -> None:
        self.conn.execute("BEGIN IMMEDIATE")

    def commit(self) -> None:
        self.conn.commit()

    def rollback(self) -> None:
        self.conn.rollback()

    # ---- 工具 -----------------------------------------------------------
    @staticmethod
    def now_iso() -> str:
        return iso(utcnow())

    def event(self, tx: sqlite3.Connection, kind: str, order_id: str | None, payload: dict[str, Any]) -> None:
        tx.execute(
            "INSERT INTO events(order_id, kind, payload, created_at) VALUES (?,?,?,?)",
            (order_id, kind, json.dumps(payload, ensure_ascii=False), self.now_iso()),
        )

    # ---- orders / demands ----------------------------------------------
    def upsert_order(self, tx: sqlite3.Connection, order: dict[str, Any]) -> None:
        tx.execute(
            """INSERT INTO orders(order_id, product, priority, state, qty_required,
                                  due_date, created_at, updated_at, version)
               VALUES(:order_id,:product,:priority,:state,:qty_required,
                      :due_date,:created_at,:updated_at,0)
               ON CONFLICT(order_id) DO UPDATE SET
                  product=excluded.product, priority=excluded.priority,
                  state=excluded.state, qty_required=excluded.qty_required,
                  due_date=excluded.due_date, updated_at=excluded.updated_at,
                  version=orders.version+1""",
            order,
        )

    def get_order(self, order_id: str) -> sqlite3.Row | None:
        return self.conn.execute("SELECT * FROM orders WHERE order_id=?", (order_id,)).fetchone()

    def get_order_tx(self, tx: sqlite3.Connection, order_id: str) -> sqlite3.Row | None:
        return tx.execute("SELECT * FROM orders WHERE order_id=?", (order_id,)).fetchone()

    def list_demands(self, order_id: str) -> list[sqlite3.Row]:
        return list(self.conn.execute(
            "SELECT * FROM order_demands WHERE order_id=? ORDER BY seq", (order_id,)))

    def list_demands_tx(self, tx: sqlite3.Connection, order_id: str) -> list[sqlite3.Row]:
        return list(tx.execute(
            "SELECT * FROM order_demands WHERE order_id=? ORDER BY seq", (order_id,)))

    def replace_demands(self, tx: sqlite3.Connection, order_id: str, demands: Iterable[dict[str, Any]]) -> None:
        tx.execute("DELETE FROM order_demands WHERE order_id=?", (order_id,))
        tx.executemany(
            "INSERT INTO order_demands(order_id, material, qty_required, seq) VALUES(?,?,?,?)",
            [(order_id, d["material"], d["qty_required"], d.get("seq", i)) for i, d in enumerate(demands)],
        )

    # ---- materials / substitutes / batches ------------------------------
    def upsert_material(self, tx: sqlite3.Connection, material: str, name: str = "", unit: str = "") -> None:
        tx.execute(
            "INSERT INTO materials(material,name,unit) VALUES(?,?,?) "
            "ON CONFLICT(material) DO UPDATE SET name=excluded.name, unit=excluded.unit",
            (material, name, unit),
        )

    def set_substitute(self, tx: sqlite3.Connection, material: str, substitute: str,
                       ratio: float, approved: bool) -> None:
        tx.execute(
            "INSERT INTO substitutes(material,substitute,ratio,approved) VALUES(?,?,?,?) "
            "ON CONFLICT(material,substitute) DO UPDATE SET ratio=excluded.ratio, approved=excluded.approved",
            (material, substitute, ratio, 1 if approved else 0),
        )

    def list_substitutes(self, material: str) -> list[sqlite3.Row]:
        return list(self.conn.execute(
            "SELECT * FROM substitutes WHERE material=? ORDER BY substitute", (material,)))

    def upsert_batch(self, tx: sqlite3.Connection, b: dict[str, Any]) -> None:
        tx.execute(
            """INSERT INTO batches(batch_id, material, qty_on_hand, qty_reserved,
                                   received_at, expiry_date, blocked)
               VALUES(:batch_id,:material,:qty_on_hand,0,:received_at,:expiry_date,:blocked)
               ON CONFLICT(batch_id) DO UPDATE SET
                 material=excluded.material,
                 qty_on_hand=excluded.qty_on_hand,
                 received_at=excluded.received_at,
                 expiry_date=excluded.expiry_date,
                 blocked=excluded.blocked""",
            b,
        )

    def set_batch_blocked(self, tx: sqlite3.Connection, batch_id: str, blocked: bool) -> None:
        tx.execute("UPDATE batches SET blocked=? WHERE batch_id=?", (1 if blocked else 0, batch_id))

    def eligible_batches_tx(self, tx: sqlite3.Connection, material: str, as_of: str) -> list[sqlite3.Row]:
        """未冻结、未在 as_of 之前到期的批次，按调用方排序。"""
        rows = tx.execute(
            """SELECT * FROM batches
               WHERE material=? AND blocked=0
                 AND (expiry_date IS NULL OR date(expiry_date) >= date(?))
               ORDER BY
                 CASE WHEN expiry_date IS NULL THEN 1 ELSE 0 END,
                 expiry_date, received_at, batch_id""",
            (material, as_of[:10]),
        ).fetchall()
        return list(rows)

    # ---- reservations ---------------------------------------------------
    def insert_reservation(self, tx: sqlite3.Connection, r: dict[str, Any]) -> None:
        tx.execute(
            """INSERT INTO reservations(reservation_id, order_id, material, demanded_material,
                                        batch_id, qty, state, created_at, expires_at,
                                        released_at, release_reason, replaced_by, preempted_from)
               VALUES(:reservation_id,:order_id,:material,:demanded_material,
                      :batch_id,:qty,:state,:created_at,:expires_at,
                      NULL,NULL,NULL,NULL)""",
            r,
        )
        tx.execute(
            "UPDATE batches SET qty_reserved = qty_reserved + ? WHERE batch_id=?",
            (r["qty"], r["batch_id"]),
        )
        tx.execute(
            "INSERT INTO ledger(reservation_id,batch_id,delta,reason,created_at) VALUES(?,?,?,?,?)",
            (r["reservation_id"], r["batch_id"], r["qty"], "HOLD:" + r["state"], self.now_iso()),
        )

    def release_reservation_tx(self, tx: sqlite3.Connection, reservation_id: str,
                               reason: str, released_at: str,
                               preempted_from: str | None = None) -> sqlite3.Row | None:
        row = tx.execute(
            "SELECT * FROM reservations WHERE reservation_id=? AND state IN ('HELD','CONFIRMED')",
            (reservation_id,),
        ).fetchone()
        if row is None:
            return None
        tx.execute(
            """UPDATE reservations SET state='RELEASED', released_at=?, release_reason=?,
                                      preempted_from=COALESCE(?, preempted_from)
               WHERE reservation_id=?""",
            (released_at, reason, preempted_from, reservation_id),
        )
        tx.execute(
            "UPDATE batches SET qty_reserved = qty_reserved - ? WHERE batch_id=? AND qty_reserved >= ?",
            (row["qty"], row["batch_id"], row["qty"]),
        )
        tx.execute(
            "INSERT INTO ledger(reservation_id,batch_id,delta,reason,created_at) VALUES(?,?,?,?,?)",
            (reservation_id, row["batch_id"], -row["qty"], reason, released_at),
        )
        return row

    def active_for_order_tx(self, tx: sqlite3.Connection, order_id: str,
                            states: tuple[str, ...] = ("HELD", "CONFIRMED")) -> list[sqlite3.Row]:
        return list(tx.execute(
            f"SELECT * FROM reservations WHERE order_id=? AND state IN ({','.join('?'*len(states))})",
            (order_id, *states),
        ))

    def active_holders_tx(self, tx: sqlite3.Connection) -> list[sqlite3.Row]:
        """全部活跃占用及其订单优先级（分配快照用）。"""
        return list(tx.execute(
            """SELECT r.*, o.priority AS ord_priority
               FROM reservations r JOIN orders o ON o.order_id=r.order_id
               WHERE r.state IN ('HELD','CONFIRMED')"""))

    def reduce_reservation_qty_tx(self, tx: sqlite3.Connection, reservation_id: str,
                                  released_qty: float, reason: str, when: str,
                                  preempted_from: str | None) -> sqlite3.Row:
        """部分释放一条预留（抢占/缩减共用），剩余数量继续占用。"""
        row = tx.execute("SELECT * FROM reservations WHERE reservation_id=?", (reservation_id,)).fetchone()
        new_qty = round(row["qty"] - released_qty, 9)
        if new_qty <= 1e-9:
            return self.release_reservation_tx(tx, reservation_id, reason, when, preempted_from)  # type: ignore[return-value]
        tx.execute("UPDATE reservations SET qty=? WHERE reservation_id=?", (new_qty, reservation_id))
        tx.execute(
            "UPDATE batches SET qty_reserved=qty_reserved-? WHERE batch_id=? AND qty_reserved>=?",
            (released_qty, row["batch_id"], released_qty),
        )
        tx.execute(
            "INSERT INTO ledger(reservation_id,batch_id,delta,reason,created_at) VALUES(?,?,?,?,?)",
            (reservation_id, row["batch_id"], -released_qty, reason, when),
        )
        return row

    def consume_reservation_tx(self, tx: sqlite3.Connection, reservation_id: str,
                               consumed_qty: float, when: str) -> sqlite3.Row:
        """领料出库：在库与预留同步扣减，预留转 CONSUMED（整行或拆分剩余）。"""
        row = tx.execute("SELECT * FROM reservations WHERE reservation_id=?", (reservation_id,)).fetchone()
        if consumed_qty < row["qty"] - 1e-9:
            tx.execute("UPDATE reservations SET qty=? WHERE reservation_id=?",
                       (round(row["qty"] - consumed_qty, 9), reservation_id))
        else:
            consumed_qty = row["qty"]
            tx.execute(
                "UPDATE reservations SET state='CONSUMED', released_at=? WHERE reservation_id=?",
                (when, reservation_id),
            )
        tx.execute(
            "UPDATE batches SET qty_on_hand=qty_on_hand-?, qty_reserved=qty_reserved-? WHERE batch_id=?",
            (consumed_qty, consumed_qty, row["batch_id"]),
        )
        tx.execute(
            "INSERT INTO ledger(reservation_id,batch_id,delta,reason,created_at) VALUES(?,?,?,?,?)",
            (reservation_id, row["batch_id"], -consumed_qty, "CONSUME", when),
        )
        return row

    def set_group_state(self, tx: sqlite3.Connection, group_id: str, state: str,
                        confirmed_at: str | None = None) -> None:
        tx.execute("UPDATE reservation_groups SET state=?, confirmed_at=COALESCE(?,confirmed_at) WHERE group_id=?",
                   (state, confirmed_at, group_id))

    def list_groups_tx(self, tx: sqlite3.Connection) -> list[sqlite3.Row]:
        return list(tx.execute("SELECT * FROM reservation_groups"))

    def expired_active_tx(self, tx: sqlite3.Connection, now_iso: str) -> list[sqlite3.Row]:
        return list(tx.execute(
            """SELECT * FROM reservations
               WHERE state IN ('HELD','CONFIRMED') AND expires_at IS NOT NULL
                 AND expires_at <= ?""",
            (now_iso,),
        ))

    # ---- groups ---------------------------------------------------------
    def insert_group(self, tx: sqlite3.Connection, group_id: str, order_id: str,
                     state: str, created_at: str, confirmed_at: str | None = None) -> None:
        tx.execute(
            "INSERT INTO reservation_groups(group_id,order_id,state,created_at,confirmed_at) VALUES(?,?,?,?,?)",
            (group_id, order_id, state, created_at, confirmed_at),
        )

    def group_items(self, group_id: str) -> list[sqlite3.Row]:
        return list(self.conn.execute(
            """SELECT r.* FROM reservation_group_items gi
               JOIN reservations r ON r.reservation_id=gi.reservation_id
               WHERE gi.group_id=?""", (group_id,)))

    def attach_group_item(self, tx: sqlite3.Connection, group_id: str, reservation_id: str) -> None:
        tx.execute(
            "INSERT INTO reservation_group_items(group_id,reservation_id) VALUES(?,?)",
            (group_id, reservation_id),
        )

    # ---- 守恒校验 --------------------------------------------------------
    def conservation_violations_tx(self, tx: sqlite3.Connection) -> list[dict[str, Any]]:
        """事务内核对：批次已预留量 == 活跃预留之和；且不超在库、不为负。"""
        problems: list[dict[str, Any]] = []
        batch_rows = tx.execute(
            """SELECT b.*, COALESCE(SUM(CASE WHEN r.state IN ('HELD','CONFIRMED')
                                             THEN r.qty ELSE 0 END),0) AS active_sum
               FROM batches b
               LEFT JOIN reservations r ON r.batch_id=b.batch_id
               GROUP BY b.batch_id"""
        ).fetchall()
        for b in batch_rows:
            if abs(b["qty_reserved"] - b["active_sum"]) > 1e-9:
                problems.append({"type": "BATCH_SUM_MISMATCH", "batch_id": b["batch_id"],
                                 "qty_reserved": b["qty_reserved"], "active_sum": b["active_sum"]})
            if b["qty_reserved"] < -1e-9 or b["qty_reserved"] > b["qty_on_hand"] + 1e-9:
                problems.append({"type": "BATCH_LIMIT_VIOLATION", "batch_id": b["batch_id"],
                                 "qty_reserved": b["qty_reserved"], "qty_on_hand": b["qty_on_hand"]})
        led = tx.execute(
            """SELECT batch_id, SUM(delta) AS s FROM ledger GROUP BY batch_id"""
        ).fetchall()
        reserved_now = {b["batch_id"]: b["qty_reserved"] for b in batch_rows}
        for row in led:
            if abs(row["s"] - reserved_now.get(row["batch_id"], 0)) > 1e-9:
                problems.append({"type": "LEDGER_MISMATCH", "batch_id": row["batch_id"],
                                 "ledger_sum": row["s"], "reserved": reserved_now.get(row["batch_id"], 0)})
        return problems

    def set_order_state(self, tx: sqlite3.Connection, order_id: str, state: str, when: str) -> None:
        tx.execute("UPDATE orders SET state=?, updated_at=? WHERE order_id=?", (state, when, order_id))
