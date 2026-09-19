"""HTTP 路由层：把 JSON 请求映射到领域命令，统一错误格式。"""

import re

from domain import Domain, DomainError
from eventstore import EventStore


class Application:
    """产销协同应用：持有领域对象，按路由分发请求。"""

    def __init__(self, store=None, seed=True):
        self.domain = Domain(store or EventStore(), seed=seed)
        d = self.domain
        self.routes = [
            # 主数据
            ("GET", r"^/api/steps$", lambda b, q, m: {"steps": list(d.steps.values())}),
            ("POST", r"^/api/steps$", lambda b, q, m: d.define_step(b)),
            ("GET", r"^/api/equipment$", lambda b, q, m: {"equipment": list(d.equipment.values())}),
            ("POST", r"^/api/equipment$", lambda b, q, m: d.register_equipment(b)),
            ("GET", r"^/api/shifts$", lambda b, q, m: {"shifts": list(d.shifts.values())}),
            ("POST", r"^/api/shifts$", lambda b, q, m: d.define_shift(b)),
            ("GET", r"^/api/workers$", lambda b, q, m: {"workers": list(d.workers.values())}),
            ("POST", r"^/api/workers$", lambda b, q, m: d.register_worker(b)),
            # 原料批次
            ("GET", r"^/api/materials$", lambda b, q, m: {"materials": d.list_materials()}),
            ("POST", r"^/api/materials/receive$", lambda b, q, m: d.receive_material(b)),
            ("GET", r"^/api/materials/(?P<id>[^/]+)$", lambda b, q, m: d.material_view(m.group("id"))),
            # 订单与排产
            ("GET", r"^/api/orders$", lambda b, q, m: {"orders": d.list_orders()}),
            ("POST", r"^/api/orders$", lambda b, q, m: d.create_order(b)),
            ("GET", r"^/api/orders/(?P<id>[^/]+)$", lambda b, q, m: d.order_view(m.group("id"))),
            ("POST", r"^/api/orders/(?P<id>[^/]+)/commit$", lambda b, q, m: d.commit_order(m.group("id"), b)),
            ("GET", r"^/api/orders/(?P<id>[^/]+)/replay$", lambda b, q, m: d.replay_order(m.group("id"), at=q.get("at"))),
            # 生产：扫码投料 / 工序加工 / 装箱
            ("POST", r"^/api/scan/consume$", lambda b, q, m: d.consume_material(b)),
            ("POST", r"^/api/lots/transform$", lambda b, q, m: d.transform_lots(b)),
            ("GET", r"^/api/lots/(?P<id>[^/]+)$", lambda b, q, m: d.lot_view(m.group("id"))),
            ("GET", r"^/api/lots/(?P<id>[^/]+)/trace$", lambda b, q, m: d.trace_lot(m.group("id"))),
            ("POST", r"^/api/boxes/pack$", lambda b, q, m: d.pack_box(b)),
            ("GET", r"^/api/boxes/(?P<code>[^/]+)$", lambda b, q, m: d.box_view(m.group("code"))),
            ("GET", r"^/api/boxes/(?P<code>[^/]+)/trace$", lambda b, q, m: d.trace_box(m.group("code"))),
            # 质检 / 冻结 / 召回
            ("GET", r"^/api/quality/checks$", lambda b, q, m: {"checks": d.list_checks(q.get("target_type"), q.get("target_id"))}),
            ("POST", r"^/api/quality/checks$", lambda b, q, m: d.record_check(b)),
            ("POST", r"^/api/holds$", lambda b, q, m: d.place_hold(b)),
            ("POST", r"^/api/holds/(?P<id>[^/]+)/release$", lambda b, q, m: d.release_hold(m.group("id"), b)),
            ("GET", r"^/api/recall$", lambda b, q, m: d.recall(q.get("target_type"), q.get("target_id"))),
            # 出库 / 退货 / 返工
            ("GET", r"^/api/shipments$", lambda b, q, m: {"shipments": list(d.shipments.values())}),
            ("POST", r"^/api/shipments$", lambda b, q, m: d.ship(b)),
            ("GET", r"^/api/returns$", lambda b, q, m: {"returns": list(d.returns.values())}),
            ("POST", r"^/api/returns$", lambda b, q, m: d.record_return(b)),
            ("POST", r"^/api/rework$", lambda b, q, m: d.record_rework(b)),
            # 报告补录
            ("GET", r"^/api/reports$", lambda b, q, m: {"reports": d.list_reports(q.get("ref_type"), q.get("ref_id"))}),
            ("POST", r"^/api/reports$", lambda b, q, m: d.file_report(b)),
            # 工时与报酬
            ("POST", r"^/api/time-entries$", lambda b, q, m: d.record_time_entry(b)),
            ("GET", r"^/api/wages$", lambda b, q, m: d.wages(q.get("worker_id"), q.get("from"), q.get("to"))),
            # 事件审计
            ("GET", r"^/api/events$", lambda b, q, m: {"events": d.list_events(q.get("type"), q.get("limit"))}),
        ]
        self.routes = [(method, re.compile(pattern), fn) for method, pattern, fn in self.routes]

    def handle(self, method, path, query, body):
        """返回 (status, payload)；未匹配的路由和业务错误都转成 JSON。"""
        for route_method, pattern, handler in self.routes:
            if route_method != method:
                continue
            match = pattern.match(path)
            if not match:
                continue
            try:
                return 200, handler(body or {}, query or {}, match)
            except DomainError as exc:
                return exc.status, {"error": str(exc)}
        return 404, {"error": f"未找到路由: {method} {path}"}
