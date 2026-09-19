"""月饼工坊产销协同的运行入口。

在原有健康检查之上挂载产销协同 API（见 commands.App）。
仅依赖 Python 标准库：``python3 service.py --port 8000 --db mooncake.db``。
"""

import argparse
import json
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlparse

from commands import App, all_views, box_view, freeze_view, lot_view, order_view, worker_view
from domain import DomainError, NotFound, ValidationFailed
from eventstore import EventStore

SERVICE_ID = "mooncake-coordination"
SERVICE_NAME = "月饼工坊产销协同"

DB_PATH = "mooncake_coordination.db"
_APP = None


def health_payload():
    """返回稳定的服务身份信息。"""
    return {"status": "ok", "service": SERVICE_ID, "name": SERVICE_NAME}


def get_app():
    """惰性创建应用：只访问 /health 时不产生数据库文件。"""
    global _APP
    if _APP is None:
        _APP = App(EventStore(DB_PATH))
    return _APP


def reset_app(app=None):
    """测试辅助：替换或清空全局应用。"""
    global _APP
    _APP = app


class Handler(BaseHTTPRequestHandler):
    """健康检查与产销协同 HTTP 接口。"""

    server_version = "MooncakeCoord/1.0"

    # -- 基础框架 -----------------------------------------------------------

    def do_GET(self):
        self._dispatch("GET")

    def do_POST(self):
        self._dispatch("POST")

    def _dispatch(self, method):
        parsed = urlparse(self.path)
        path = parsed.path.rstrip("/") or "/"
        query = {k: v[0] for k, v in parse_qs(parsed.query).items()}
        if path == "/health":
            if method != "GET":
                self._json(405, {"error": "健康检查仅支持 GET"})
                return
            self._json(200, health_payload())
            return
        route = ROUTES.get((method, path))
        if route is None:
            match = _match_dynamic(method, path)
            if match is None:
                self.send_error(404)
                return
            handler, kwargs = match
        else:
            handler, kwargs = route
        data = {}
        if method == "POST":
            try:
                raw = self.rfile.read(int(self.headers.get("Content-Length") or 0))
                data = json.loads(raw.decode("utf-8")) if raw else {}
                if not isinstance(data, dict):
                    raise ValueError("请求体必须是 JSON 对象")
            except (ValueError, UnicodeDecodeError) as exc:
                self._json(400, {"error": f"请求体不是合法 JSON：{exc}"})
                return
        try:
            handler(self, get_app(), data, query, kwargs)
        except NotFound as exc:
            self._json(404, {"error": str(exc)})
        except ValidationFailed as exc:
            self._json(400, {"error": str(exc)})
        except DomainError as exc:
            self._json(409, {"error": str(exc)})

    def _json(self, status, payload):
        body = json.dumps(payload, ensure_ascii=False, default=str).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _created(self, event, duplicated):
        self._json(200 if duplicated else 201,
                   {"event": event, "duplicated": duplicated})

    def log_message(self, *_args):
        return

    # -- 命令接口 -----------------------------------------------------------

    @staticmethod
    def h_register_material(h, app, data, _q, _k):
        h._created(*app.register_material(data))

    @staticmethod
    def h_register_worker(h, app, data, _q, _k):
        h._created(*app.register_worker(data))

    @staticmethod
    def h_receive_order(h, app, data, _q, _k):
        h._created(*app.receive_order(data))

    @staticmethod
    def h_cancel_order(h, app, data, _q, k):
        data.setdefault("order_id", k["order_id"])
        h._created(*app.cancel_order(data))

    @staticmethod
    def h_schedule(h, app, data, _q, _k):
        h._created(*app.schedule_production(data))

    @staticmethod
    def h_consume(h, app, data, _q, _k):
        h._created(*app.consume_material(data))

    @staticmethod
    def h_split(h, app, data, _q, _k):
        h._created(*app.split_lot(data))

    @staticmethod
    def h_merge(h, app, data, _q, _k):
        h._created(*app.merge_lots(data))

    @staticmethod
    def h_produce(h, app, data, _q, _k):
        h._created(*app.produce_lot(data))

    @staticmethod
    def h_finish(h, app, data, _q, _k):
        h._created(*app.finish_lot(data))

    @staticmethod
    def h_pack_box(h, app, data, _q, _k):
        h._created(*app.pack_box(data))

    @staticmethod
    def h_quality(h, app, data, _q, _k):
        h._created(*app.quality_inspection(data))

    @staticmethod
    def h_allergen(h, app, data, _q, _k):
        h._created(*app.allergen_alert(data))

    @staticmethod
    def h_freeze(h, app, data, _q, _k):
        h._created(*app.manual_freeze(data))

    @staticmethod
    def h_release(h, app, data, _q, _k):
        h._created(*app.release_freeze(data))

    @staticmethod
    def h_rework_start(h, app, data, _q, _k):
        h._created(*app.start_rework(data))

    @staticmethod
    def h_rework_complete(h, app, data, _q, k):
        data.setdefault("rework_id", k["rework_id"])
        h._created(*app.complete_rework(data))

    @staticmethod
    def h_ship(h, app, data, _q, _k):
        h._created(*app.ship(data))

    @staticmethod
    def h_return(h, app, data, _q, k):
        data.setdefault("shipment_id", k["shipment_id"])
        h._created(*app.receive_return(data))

    @staticmethod
    def h_report(h, app, data, _q, _k):
        h._created(*app.add_report(data))

    @staticmethod
    def h_effort(h, app, data, _q, _k):
        h._created(*app.record_effort(data))

    # -- 查询接口 -----------------------------------------------------------

    @staticmethod
    def h_state(h, app, _d, _q, _k):
        h._json(200, all_views(app.state()))

    @staticmethod
    def h_events(h, app, _d, query, _k):
        up_to = query.get("up_to_seq")
        events = app.store.events(int(up_to) if up_to else None)
        h._json(200, {"events": events, "count": len(events)})

    @staticmethod
    def h_lots(h, app, _d, _q, _k):
        state = app.state()
        h._json(200, {"lots": [lot_view(l, state) for l in state.lots.values()]})

    @staticmethod
    def h_lot(h, app, _d, _q, k):
        lot = app.state().lots.get(k["code"])
        if lot is None:
            h._json(404, {"error": f"批次不存在：{k['code']}"})
            return
        h._json(200, lot_view(lot, app.state()))

    @staticmethod
    def h_boxes(h, app, _d, _q, _k):
        h._json(200, {"boxes": [box_view(b) for b in app.state().boxes.values()]})

    @staticmethod
    def h_box(h, app, _d, _q, k):
        box = app.state().boxes.get(k["code"])
        if box is None:
            h._json(404, {"error": f"箱码不存在：{k['code']}"})
            return
        h._json(200, box_view(box))

    @staticmethod
    def h_orders(h, app, _d, _q, _k):
        h._json(200, {"orders": [order_view(o) for o in app.state().orders.values()]})

    @staticmethod
    def h_order(h, app, _d, _q, k):
        order = app.state().orders.get(k["order_id"])
        if order is None:
            h._json(404, {"error": f"订单不存在：{k['order_id']}"})
            return
        h._json(200, order_view(order, detailed=True))

    @staticmethod
    def h_replay(h, app, _d, query, k):
        version = query.get("version")
        try:
            version = int(version) if version is not None else -1
        except ValueError:
            h._json(400, {"error": "version 必须是整数下标"})
            return
        h._json(200, app.replay_order_decision(k["order_id"], version))

    @staticmethod
    def h_workers(h, app, _d, _q, _k):
        h._json(200, {"workers": [worker_view(w) for w in app.state().workers.values()]})

    @staticmethod
    def h_worker(h, app, _d, _q, k):
        worker = app.state().workers.get(k["code"])
        if worker is None:
            h._json(404, {"error": f"村民/工人不存在：{k['code']}"})
            return
        h._json(200, worker_view(worker))

    @staticmethod
    def h_payroll(h, app, _d, _q, k):
        h._json(200, app.payroll(k["code"]))

    @staticmethod
    def h_shipments(h, app, _d, _q, k):
        state = app.state()
        if not k:
            h._json(200, {"shipment_ids": sorted(state.shipments)})
            return
        ship = state.shipments.get(k["shipment_id"])
        if ship is None:
            h._json(404, {"error": f"出库单不存在：{k['shipment_id']}"})
            return
        h._json(200, {
            "shipment_id": ship.shipment_id,
            "order_id": ship.order_id,
            "store": ship.store,
            "state": ship.state,
            "business_time": ship.business_time,
            "items": ship.items,
            "returned": ship.returned,
        })

    @staticmethod
    def h_trace(h, app, _d, query, _k):
        code = query.get("code")
        if not code:
            h._json(400, {"error": "必须提供 code 参数"})
            return
        h._json(200, app.trace(code))

    @staticmethod
    def h_timeline(h, app, _d, query, _k):
        code = query.get("code")
        if not code:
            h._json(400, {"error": "必须提供 code 参数"})
            return
        h._json(200, app.timeline(code))

    @staticmethod
    def h_reconciliation(h, app, _d, query, _k):
        h._json(200, {"rows": app.reconciliation(query.get("order_id"))})

    @staticmethod
    def h_freezes(h, app, _d, query, _k):
        state = app.state()
        events_by_seq = {e["seq"]: e for e in state.events}
        records = []
        for fr in state.freezes:
            view = freeze_view(fr, events_by_seq[fr.id])
            if query.get("active") == "1" and not view["active"]:
                continue
            records.append(view)
        h._json(200, {"freezes": records})


# (method, path) -> (handler, 固定参数)
ROUTES = {
    ("POST", "/v1/materials"): (Handler.h_register_material, None),
    ("POST", "/v1/workers"): (Handler.h_register_worker, None),
    ("POST", "/v1/orders"): (Handler.h_receive_order, None),
    ("POST", "/v1/schedules"): (Handler.h_schedule, None),
    ("POST", "/v1/consumptions"): (Handler.h_consume, None),
    ("POST", "/v1/lots/split"): (Handler.h_split, None),
    ("POST", "/v1/lots/merge"): (Handler.h_merge, None),
    ("POST", "/v1/lots/produce"): (Handler.h_produce, None),
    ("POST", "/v1/lots/finish"): (Handler.h_finish, None),
    ("POST", "/v1/boxes"): (Handler.h_pack_box, None),
    ("POST", "/v1/quality-inspections"): (Handler.h_quality, None),
    ("POST", "/v1/allergens"): (Handler.h_allergen, None),
    ("POST", "/v1/freezes"): (Handler.h_freeze, None),
    ("POST", "/v1/freezes/release"): (Handler.h_release, None),
    ("POST", "/v1/reworks"): (Handler.h_rework_start, None),
    ("POST", "/v1/shipments"): (Handler.h_ship, None),
    ("POST", "/v1/reports"): (Handler.h_report, None),
    ("POST", "/v1/efforts"): (Handler.h_effort, None),
    ("GET", "/v1/state"): (Handler.h_state, None),
    ("GET", "/v1/events"): (Handler.h_events, None),
    ("GET", "/v1/lots"): (Handler.h_lots, None),
    ("GET", "/v1/boxes"): (Handler.h_boxes, None),
    ("GET", "/v1/orders"): (Handler.h_orders, None),
    ("GET", "/v1/workers"): (Handler.h_workers, None),
    ("GET", "/v1/shipments"): (Handler.h_shipments, {}),
    ("GET", "/v1/trace"): (Handler.h_trace, None),
    ("GET", "/v1/timeline"): (Handler.h_timeline, None),
    ("GET", "/v1/reconciliation"): (Handler.h_reconciliation, None),
    ("GET", "/v1/freezes"): (Handler.h_freezes, None),
}

# 动态路径：(method, 前缀段数匹配) —— 保持显式、可读。
_DYNAMIC = [
    ("POST", "/v1/orders/", "/cancel", "order_id", Handler.h_cancel_order),
    ("POST", "/v1/reworks/", "/complete", "rework_id", Handler.h_rework_complete),
    ("POST", "/v1/shipments/", "/returns", "shipment_id", Handler.h_return),
    ("GET", "/v1/lots/", None, "code", Handler.h_lot),
    ("GET", "/v1/boxes/", None, "code", Handler.h_box),
    ("GET", "/v1/workers/", None, "code", Handler.h_worker),
    ("GET", "/v1/workers/", "/payroll", "code", Handler.h_payroll),
    ("GET", "/v1/orders/", None, "order_id", Handler.h_order),
    ("GET", "/v1/orders/", "/replay-decision", "order_id", Handler.h_replay),
    ("GET", "/v1/shipments/", None, "shipment_id", Handler.h_shipments),
]


def _match_dynamic(method, path):
    for m, prefix, suffix, key, handler in _DYNAMIC:
        if m != method or not path.startswith(prefix):
            continue
        rest = path[len(prefix):]
        if suffix is None:
            if "/" in rest or not rest:
                continue
            return handler, {key: rest}
        # 形如 /v1/workers/<code>/payroll
        if suffix and rest.endswith(suffix):
            ident = rest[: -len(suffix)].rstrip("/")
            if ident and "/" not in ident:
                return handler, {key: ident}
    return None


def main():
    global DB_PATH
    parser = argparse.ArgumentParser(description=SERVICE_NAME)
    parser.add_argument("--port", type=int, default=8000)
    parser.add_argument("--db", default=DB_PATH, help="SQLite 数据库路径，默认本地文件")
    parser.add_argument("--check", action="store_true")
    args = parser.parse_args()
    if args.check:
        assert health_payload()["service"] == SERVICE_ID
        print("基础检查通过")
        return
    DB_PATH = args.db
    ThreadingHTTPServer(("0.0.0.0", args.port), Handler).serve_forever()


if __name__ == "__main__":
    main()
