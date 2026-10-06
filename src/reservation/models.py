"""领域常量：状态机、齐套策略、批次策略、缺料原因码、释放原因。"""
from __future__ import annotations

import enum


class OrderState(str, enum.Enum):
    """生产订单状态，与 domain/contract.json 的 states 对齐。"""

    DRAFT = "DRAFT"                       # 草拟
    PENDING_CONFIRM = "PENDING_CONFIRM"   # 待确认（已下临时持有）
    RELEASED = "RELEASED"                 # 已下达（预留已确认）
    FULFILLING = "FULFILLING"             # 履行中（已有领料出库）
    CLOSED = "CLOSED"                     # 已关闭


STATE_CN = {
    OrderState.DRAFT: "草拟",
    OrderState.PENDING_CONFIRM: "待确认",
    OrderState.RELEASED: "已下达",
    OrderState.FULFILLING: "履行中",
    OrderState.CLOSED: "已关闭",
}


class ReservationState(str, enum.Enum):
    HELD = "HELD"            # 临时持有（计划确认前，带 TTL）
    CONFIRMED = "CONFIRMED"  # 已确认的正式预留
    RELEASED = "RELEASED"    # 已释放（审计留存）
    CONSUMED = "CONSUMED"    # 已领料出库


class KitPolicy(str, enum.Enum):
    ALL_OR_NOTHING = "ALL_OR_NOTHING"  # 齐套才确认，否则整单回滚
    PARTIAL = "PARTIAL"                # 允许部分齐套


class BatchStrategy(str, enum.Enum):
    FEFO = "FEFO"  # 先到期先出（默认）
    FIFO = "FIFO"  # 先入库先出


class ShortageReason(str, enum.Enum):
    INSUFFICIENT_TOTAL = "INSUFFICIENT_TOTAL"          # 合格总量本身不足
    RESERVED_BY_OTHERS = "RESERVED_BY_OTHERS"          # 被其他订单占用
    EXPIRED_STOCK = "EXPIRED_STOCK"                    # 批次在需求日之前到期
    BLOCKED_LOT = "BLOCKED_LOT"                        # 批次被质量冻结
    SUBSTITUTE_NOT_APPROVED = "SUBSTITUTE_NOT_APPROVED"  # 有替代料但未批准


class ReleaseReason(str, enum.Enum):
    TEMP_HOLD_TIMEOUT = "TEMP_HOLD_TIMEOUT"    # 临时持有超时
    RESERVE_TIMEOUT = "RESERVE_TIMEOUT"        # 正式预留超过订单截止时间
    PREEMPTED = "PREEMPTED"                    # 被更高优先级订单抢占
    ORDER_REDUCED = "ORDER_REDUCED"            # 订单缩减释放
    ORDER_CLOSED = "ORDER_CLOSED"              # 订单关闭
    ORDER_COMPLETED = "ORDER_COMPLETED"        # 订单完工，尾量释放
    REPLACED_HOLD = "REPLACED_HOLD"            # 重新持有，替换旧持有
    ORPHAN_CLEANUP = "ORPHAN_CLEANUP"          # 孤儿预留恢复任务清理


ACTIVE_STATES = (ReservationState.HELD.value, ReservationState.CONFIRMED.value)
