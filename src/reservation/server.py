"""JSON over HTTP 接口（stdlib http.server，零依赖）。

路由：
  POST /orders                      创建生产订单（含多物料需求）
  GET  /orders/{id}                 订单详情：需求、覆盖率、活跃预留、历史
  POST /materials                   登记物料
  POST /batches                     登记库存批次（在库量、入库时间、到期日）
  POST /batches/{id}/block          质量冻结/解冻
  POST /substitutes/approve         批准/撤销替代料
  POST /orders/{id}/plan            计划诊断（dry-run）：缺料原因/占用来源/调整方案
  POST /orders/{id}/hold            临时持有（带 TTL，默认 300s）
  POST /orders/{id}/confirm         原子确认多物料预留（policy/strategy/preempt）
  POST /orders/{id}/reduce          订单缩减（只减不增，守恒释放）
  POST /orders/{id}/issue           领料出库
  POST /orders/{id}/complete        完工释放尾量
  POST /orders/{id}/close           关闭订单
  POST /sweep/expired               超时释放清扫（可由定时器调用）
  POST /recover/orphans             孤儿预留恢复任务
  GET  /conservation                数量守恒校验
  GET  /batches?material=           批次与占用查询
  GET  /health                      健康检查

并发安全：写事务一律 BEGIN IMMEDIATE + busy_timeout，多线程下第二个写者
等待锁后重新读到最新 qty_reserved，因此不会超卖。
"""
from __future__ import annotations

import json
import re
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any, Callable
from urllib.parse import parse_qs, urlparse

from .models import BatchStrategy, KitPolicy
from .service import DomainError, ReservationService
from .store import Store

Handler = Callable[["ApiHandler", dict[str, str], dict[str, Any], dict[str, list[str]]], Any]


class ApiHandler(BaseHTTPRequestHandler):
    service: ReservationService  # 由 make_server 注入到类上

    server_version = "ReservationServer/1.0"

    # ---- 路由表（顺序匹配，支持 {param}） --------------------------------
    ROUTES: list[tuple[str, re.Pattern[str], Handler]] = []

    def log_message(self, fmt: str, *args: Any) -> None:  # 安静一点
        return

    def _read_json(self) -> dict[str, Any]:
        length = int(self.headers.get("Content-Length") or 0)
        if length == 0:
            return {}
        raw = self.rfile.read(length)
        try:
            body = json.loads(raw.decode("utf-8"))
        except json.JSONDecodeError as exc:
            raise DomainError("BAD_JSON", f"请求体不是合法 JSON：{exc}", http_status=400)
        if not isinstance(body, dict):
            raise DomainError("BAD_BODY", "请求体必须是 JSON 对象", http_status=400)
        return body

    def _send(self, status: int, payload: Any) -> None:
        data = json.dumps(payload, ensure_ascii=False, indent=2).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def _handle(self, method: str) -> None:
        parsed = urlparse(self.path)
        query = parse_qs(parsed.query)
        for m, pattern, handler in self.ROUTES:
            if m != method:
                continue
            match = pattern.fullmatch(parsed.path)
            if match:
                body = self._read_json() if method == "POST" else {}
                try:
                    result = handler(self, match.groupdict(), body, query)
                except DomainError as exc:
                    self._send(exc.http_status, {
                        "error": exc.code, "message": exc.message, "details": exc.details,
                    })
                except ValueError as exc:
                    self._send(400, {"error": "BAD_VALUE", "message": str(exc)})
                else:
                    self._send(200, result if result is not None else {"ok": True})
                return
        self._send(404, {"error": "NOT_FOUND", "message": f"无此路由：{method} {parsed.path}"})

    def do_GET(self) -> None:  # noqa: N802
        self._handle("GET")

    def do_POST(self) -> None:  # noqa: N802
        self._handle("POST")


def route(method: str, path: str) -> Callable[[Handler], Handler]:
    parts = re.split(r"(\{\w+\})", path)
    regex = "".join(f"(?P<{p[1:-1]}>[^/]+)" if p.startswith("{") else re.escape(p)
                    for p in parts)
    pattern = re.compile(regex)

    def deco(fn: Handler) -> Handler:
        ApiHandler.ROUTES.append((method, pattern, fn))
        return fn

    return deco


def _q(query: dict[str, list[str]], key: str, default: str | None = None) -> str | None:
    return query.get(key, [default])[0]


def _qb(query: dict[str, list[str]], key: str, default: bool = False) -> bool:
    return _q(query, key, str(default)).lower() in ("1", "true", "yes", "y")


# =====================================================================
# 路由实现
# =====================================================================
@route("GET", "/health")
def health(h: ApiHandler, p, b, q) -> dict[str, Any]:
    return {"status": "ok"}


@route("POST", "/materials")
def create_material(h: ApiHandler, p, b, q) -> dict[str, Any]:
    h.service.register_material(b["material"], b.get("name", ""), b.get("unit", ""))
    return {"material": b["material"], "name": b.get("name", ""), "unit": b.get("unit", "")}


@route("POST", "/batches")
def create_batch(h: ApiHandler, p, b, q) -> dict[str, Any]:
    return h.service.add_batch(
        b["batch_id"], b["material"], float(b["qty_on_hand"]),
        b["received_at"], b.get("expiry_date"), bool(b.get("blocked", False)),
    )


@route("POST", "/batches/{batch_id}/block")
def block_batch(h: ApiHandler, p, b, q) -> dict[str, Any]:
    return h.service.set_batch_blocked(p["batch_id"], bool(b.get("blocked", True)))


@route("GET", "/batches")
def list_batches(h: ApiHandler, p, b, q) -> dict[str, Any]:
    return {"batches": h.service.list_batches(_q(q, "material"))}


@route("POST", "/substitutes/approve")
def approve_sub(h: ApiHandler, p, b, q) -> dict[str, Any]:
    return h.service.approve_substitute(
        b["material"], b["substitute"], float(b.get("ratio", 1.0)),
        bool(b.get("approved", True)),
    )


@route("POST", "/orders")
def create_order(h: ApiHandler, p, b, q) -> dict[str, Any]:
    return h.service.create_order(
        b["order_id"], b.get("product", ""), b["demands"],
        priority=int(b.get("priority", 0)), due_date=b.get("due_date"),
        qty_required=float(b["qty_required"]) if b.get("qty_required") is not None else None,
    )


@route("GET", "/orders/{order_id}")
def get_order(h: ApiHandler, p, b, q) -> dict[str, Any]:
    return h.service.get_order(p["order_id"])


def _plan_kwargs(body: dict[str, Any], query: dict[str, list[str]]) -> dict[str, Any]:
    return {
        "strategy": body.get("strategy") or _q(query, "strategy", BatchStrategy.FEFO.value),
        "preempt": bool(body.get("preempt", _qb(query, "preempt", False))),
        "as_of": body.get("as_of") or _q(query, "as_of"),
    }


@route("POST", "/orders/{order_id}/plan")
def plan_order(h: ApiHandler, p, b, q) -> dict[str, Any]:
    return h.service.plan(p["order_id"], **_plan_kwargs(b, q))


@route("POST", "/orders/{order_id}/hold")
def hold_order(h: ApiHandler, p, b, q) -> dict[str, Any]:
    kwargs = _plan_kwargs(b, q)
    kwargs["ttl_seconds"] = int(b["ttl_seconds"]) if b.get("ttl_seconds") is not None else None
    return h.service.hold(p["order_id"], **kwargs)


@route("POST", "/orders/{order_id}/confirm")
def confirm_order(h: ApiHandler, p, b, q) -> dict[str, Any]:
    kwargs = _plan_kwargs(b, q)
    kwargs["policy"] = b.get("policy", KitPolicy.ALL_OR_NOTHING.value)
    return h.service.confirm(p["order_id"], **kwargs)


@route("POST", "/orders/{order_id}/reduce")
def reduce_order(h: ApiHandler, p, b, q) -> dict[str, Any]:
    return h.service.reduce_order(p["order_id"], b["demands"])


@route("POST", "/orders/{order_id}/issue")
def issue(h: ApiHandler, p, b, q) -> dict[str, Any]:
    return h.service.issue_materials(p["order_id"], b["items"])


@route("POST", "/orders/{order_id}/complete")
def complete(h: ApiHandler, p, b, q) -> dict[str, Any]:
    return h.service.complete_order(p["order_id"])


@route("POST", "/orders/{order_id}/close")
def close(h: ApiHandler, p, b, q) -> dict[str, Any]:
    return h.service.close_order(p["order_id"])


@route("POST", "/sweep/expired")
def sweep(h: ApiHandler, p, b, q) -> dict[str, Any]:
    return h.service.sweep_expired()


@route("POST", "/recover/orphans")
def recover(h: ApiHandler, p, b, q) -> dict[str, Any]:
    return h.service.recover_orphans()


@route("GET", "/conservation")
def conservation(h: ApiHandler, p, b, q) -> dict[str, Any]:
    result = h.service.conservation_check()
    result["ok"] = not result["violations"]
    return result


def make_server(host: str, port: int, db_path: str) -> tuple[ThreadingHTTPServer, Store]:
    store = Store(db_path)
    service = ReservationService(store)

    class BoundHandler(ApiHandler):
        pass

    BoundHandler.service = service
    BoundHandler.ROUTES = ApiHandler.ROUTES
    httpd = ThreadingHTTPServer((host, port), BoundHandler)
    return httpd, store


def main(argv: list[str] | None = None) -> None:
    import argparse

    parser = argparse.ArgumentParser(description="生产领料预留服务端")
    parser.add_argument("--host", default="0.0.0.0")
    parser.add_argument("--port", type=int, default=8080)
    parser.add_argument("--db", default="data/reservation.db")
    args = parser.parse_args(argv)
    httpd, _store = make_server(args.host, args.port, args.db)
    print(f"生产领料预留服务监听 http://{args.host}:{args.port} （数据库 {args.db}）")
    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        httpd.server_close()


if __name__ == "__main__":
    main()
