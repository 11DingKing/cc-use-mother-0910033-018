"""HTTP 接口层（标准库 http.server，无外部依赖）。

路由：
- POST   /demands                      创建生产需求
- GET    /demands                      需求列表
- GET    /demands/{id}                 需求详情
- POST   /demands/{id}/plan            试算（缺料原因/占用来源/调整方案）
- POST   /demands/{id}/confirm         原子确认多物料预留（支持幂等键）
- POST   /demands/{id}/commit-hold     临时预占转正式
- POST   /demands/{id}/reduce          订单缩减
- POST   /demands/{id}/release         手动释放
- POST   /demands/{id}/close           关闭订单
- POST   /demands/{id}/substitutes     批准替代料
- POST   /demands/{id}/issue           发料扣减
- POST   /batches                      登记/更新库存批次
- GET    /batches                      批次列表（?material_id=）
- GET    /occupancy                    占用来源视图（?material_id=）
- POST   /internal/sweep-timeouts      超时/过期释放
- POST   /internal/recover-orphans     孤儿预留恢复
"""
from __future__ import annotations

import json
import re
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any, Callable, Optional
from urllib.parse import urlparse

from .errors import DomainError
from .service import ReservationService
from .store import Store


def _json_default(value: Any) -> Any:
    if hasattr(value, "isoformat"):
        return value.isoformat()
    return str(value)


class _Handler(BaseHTTPRequestHandler):
    service: ReservationService  # 由工厂函数注入到类上

    def log_message(self, fmt: str, *args: Any) -> None:  # 静默，测试输出更干净
        return

    # ---- 输入输出 -------------------------------------------------------

    def _send(self, payload: Any, status: int = 200) -> None:
        body = json.dumps(payload, ensure_ascii=False, default=_json_default).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _read_json(self) -> dict[str, Any]:
        length = int(self.headers.get("Content-Length") or 0)
        if length == 0:
            return {}
        raw = self.rfile.read(length)
        try:
            value = json.loads(raw.decode("utf-8"))
        except json.JSONDecodeError as exc:
            raise DomainError("BAD_JSON", f"请求体不是合法 JSON：{exc}")
        if not isinstance(value, dict):
            raise DomainError("BAD_BODY", "请求体必须是 JSON 对象")
        return value

    def _idempotency_key(self) -> Optional[str]:
        return self.headers.get("Idempotency-Key")

    # ---- 路由 -----------------------------------------------------------

    def do_GET(self) -> None:  # noqa: N802
        self._dispatch("GET")

    def do_POST(self) -> None:  # noqa: N802
        self._dispatch("POST")

    def _dispatch(self, method: str) -> None:
        try:
            path = urlparse(self.path).path.rstrip("/") or "/"
            query = urlparse(self.path).query
            svc = self.service
            m = re.fullmatch(r"/demands/([^/]+)", path)
            if method == "POST" and path == "/demands":
                return self._send(svc.create_demand(self._read_json()), 201)
            if method == "GET" and path == "/demands":
                return self._send({"demands": svc.list_demands()})
            if m and method == "GET":
                return self._send(svc.get_demand(m.group(1)))
            if (mm := re.fullmatch(r"/demands/([^/]+)/([a-z-]+)", path)):
                if method != "POST":
                    raise DomainError("METHOD_NOT_ALLOWED", f"{path} 仅支持 POST", status=405)
                did, action = mm.group(1), mm.group(2)
                body = self._read_json()
                return self._send(self._action(svc, action, did, body, query))
            if method == "POST" and path == "/batches":
                return self._send(svc.register_batch(self._read_json()), 201)
            if method == "GET" and path == "/batches":
                material_id = self._qs(query, "material_id")
                return self._send({"batches": svc.list_batches(material_id)})
            if method == "GET" and path == "/occupancy":
                return self._send(svc.occupancy(self._qs(query, "material_id")))
            if method == "POST" and path == "/internal/sweep-timeouts":
                return self._send(svc.sweep_timeouts())
            if method == "POST" and path == "/internal/recover-orphans":
                body = self._read_json()
                return self._send(svc.recover_orphans(force=bool(body.get("force"))))
            raise DomainError("NOT_FOUND", f"路径不存在：{path}", status=404)
        except DomainError as exc:
            self._send(exc.to_dict(), exc.status)
        except Exception as exc:  # noqa: BLE001
            self._send({"error": {"code": "INTERNAL", "message": str(exc)}}, 500)

    def _action(self, svc: ReservationService, action: str, demand_id: str,
                body: dict[str, Any], query: str):
        if action == "plan":
            return svc.plan(demand_id, body)
        if action == "confirm":
            return svc.confirm(demand_id, body, idem_key=self._idempotency_key())
        if action == "commit-hold":
            return svc.commit_hold(demand_id, body.get("group_id"))
        if action == "reduce":
            return svc.reduce_demand(
                demand_id, body["reductions"], body.get("expected_version"))
        if action == "release":
            return svc.release_demand(demand_id, body.get("reason", "manual"))
        if action == "close":
            return svc.close_demand(demand_id)
        if action == "substitutes":
            return svc.approve_substitute(
                demand_id, body["material_id"],
                body["substitute_material_id"], int(body.get("ratio", 1)),
            )
        if action == "issue":
            return svc.issue(demand_id, body["items"])
        raise DomainError("NOT_FOUND", f"动作不存在：{action}", status=404)

    @staticmethod
    def _qs(query: str, key: str) -> Optional[str]:
        for pair in query.split("&"):
            if "=" in pair:
                k, v = pair.split("=", 1)
                if k == key:
                    return v
        return None


def create_server(host: str = "127.0.0.1", port: int = 0, *,
                  store: Optional[Store] = None,
                  service: Optional[ReservationService] = None,
                  clock: Optional[Callable[[], float]] = None) -> ThreadingHTTPServer:
    store = store or Store(":memory:")
    svc = service or ReservationService(store, clock=clock)

    handler = type("BoundHandler", (_Handler,), {"service": svc})
    httpd = ThreadingHTTPServer((host, port), handler)
    httpd.service = svc  # type: ignore[attr-defined]
    httpd.store = store  # type: ignore[attr-defined]
    return httpd


def run(host: str = "0.0.0.0", port: int = 8080, db_path: str = "data/reservation.db") -> None:
    store = Store(db_path)
    httpd = create_server(host, port, store=store)
    print(f"生产领料预留服务监听 http://{host}:{port}（数据库 {db_path}）")
    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        httpd.server_close()
