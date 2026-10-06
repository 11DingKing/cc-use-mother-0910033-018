"""领域模型：生产需求、库存批次、预留。"""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date
from enum import Enum
from typing import Optional


Qty = int  # 全部数量以最小计量单位的整数表示，杜绝浮点误差


class DemandStatus(str, Enum):
    DRAFT = "草拟"          # 可编辑需求行
    PENDING = "待确认"      # 已做过试算/预占，等待确认
    RELEASED = "已下达"     # 已正式预留
    FULFILLING = "履行中"   # 已开始发料扣减
    CLOSED = "已关闭"       # 终态


class LineStatus(str, Enum):
    REQUIRED = "需预留"
    RESERVED = "已预留"     # 按齐套策略成功预留
    SHORTAGE = "缺料"       # 原子预留失败或部分齐套被拒绝
    SUBSTITUTED = "替代料"  # 使用了已批准的替代物料
    CLOSED = "已关闭"


class AllocationPolicy(str, Enum):
    ALL_OR_NOTHING = "ALL_OR_NOTHING"  # 整单齐套：缺一种则全部不预留
    PARTIAL = "PARTIAL"                # 部分齐套：能留多少留多少，缺料行返回原因
    PARTIAL_PENDING = "PARTIAL_PENDING"  # 部分齐套但挂起：只成功行落库，缺料行等待，可再决策


class ReservationState(str, Enum):
    HELD = "held"        # 正式预留，占用库存
    CONSUMED = "consumed"
    RELEASED = "released"
    EXPIRED = "expired"  # 批次过期导致预留失效


@dataclass
class DemandLine:
    material_id: str
    qty: Qty
    # 已批准的替代料：material_id -> (替代料编号 -> 转换比 1 主料 = ratio 替代料)
    substitutes: dict[str, int] = field(default_factory=dict)
    use_substitute: Optional[str] = None  # 确认时选定的替代料
    status: LineStatus = LineStatus.REQUIRED
    shortage_reason: Optional[str] = None

    @property
    def effective_material(self) -> str:
        return self.use_substitute or self.material_id

    @property
    def effective_qty(self) -> Qty:
        if self.use_substitute is None:
            return self.qty
        return self.qty * self.substitutes[self.use_substitute]


@dataclass
class Demand:
    demand_id: str
    product: str
    priority: int           # 数值越大优先级越高
    status: DemandStatus
    lines: list[DemandLine]
    created_at: str
    deadline: Optional[str] = None   # ISO 日期，用于保质/效期匹配
    lock_version: int = 0           # 乐观锁版本
    confirm_deadline: Optional[str] = None  # 待确认预占的超时时刻（epoch 秒，字符串存储）


@dataclass
class Batch:
    batch_id: str
    material_id: str
    qty_on_hand: Qty
    expiry_date: Optional[date] = None   # 保质/有效期
    shelf_life_days: Optional[int] = None
    received_date: Optional[date] = None
    location: str = ""
    quarantined: bool = False            # 质检不合格隔离，不可预留


@dataclass
class Reservation:
    """对单个批次的单物料占用。多物料预留共享 reservation_group 实现原子性。"""

    reservation_id: str
    group_id: str
    demand_id: str
    line_material_id: str          # 需求行上的主料编号（替代时与 batch.material_id 不同）
    material_id: str               # 实际占用的物料编号
    batch_id: str
    qty: Qty
    state: ReservationState
    created_at: float              # epoch 秒
    expires_at: Optional[float] = None   # 超时释放时刻
    tentative: bool = False        # True=待确认的临时预占，False=正式预留
    substitution_of: Optional[str] = None  # 非空表示该占用替代的主料


@dataclass
class ShortageDetail:
    material_id: str
    required: Qty
    available: Qty
    gap: Qty
    reasons: list[str]
    occupied_by: list[dict]        # 占用来源：被哪些需求/批次占用


@dataclass
class AdjustmentOption:
    """可行调整方案。"""

    type: str                      # release_lower_priority / substitute / wait_supply / reduce_qty
    description: str
    material_id: str
    gain_qty: Qty = 0
    target_demand_id: Optional[str] = None
    substitute_material_id: Optional[str] = None
    requires_approval: bool = False
