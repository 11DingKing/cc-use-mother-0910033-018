"""领域错误类型。"""
from __future__ import annotations

from typing import Any


class DomainError(Exception):
    """所有业务规则违反的基类，携带机器可读错误码与 HTTP 状态。"""

    def __init__(
        self,
        code: str,
        message: str,
        *,
        status: int = 400,
        details: dict[str, Any] | None = None,
    ) -> None:
        super().__init__(message)
        self.code = code
        self.message = message
        self.status = status
        self.details = details or {}

    def to_dict(self) -> dict[str, Any]:
        return {
            "error": {
                "code": self.code,
                "message": self.message,
                "details": self.details,
            }
        }


class NotFoundError(DomainError):
    def __init__(self, entity: str, entity_id: str) -> None:
        super().__init__(
            "NOT_FOUND",
            f"{entity} 不存在：{entity_id}",
            status=404,
            details={"entity": entity, "id": entity_id},
        )


class ConflictError(DomainError):
    def __init__(self, code: str, message: str, *, details: dict[str, Any] | None = None) -> None:
        super().__init__(code, message, status=409, details=details)


class InvariantViolation(RuntimeError):
    """数量守恒等关键不变量被破坏，属于服务端内部严重错误。"""
