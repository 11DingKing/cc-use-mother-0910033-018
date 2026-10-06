"""生产领料预留服务端。

领域子包：
- models：状态、策略、缺料原因码等领域常量
- store：SQLite 模式与连接管理
- allocation：批次选择与多物料齐套分配引擎（纯计算）
- service：领域服务（事务边界、数量守恒、超时与孤儿恢复）
- server：JSON over HTTP 接口
"""
from .service import ReservationService, DomainError
from .store import Store

__all__ = ["ReservationService", "DomainError", "Store"]
