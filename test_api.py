"""HTTP 层冒烟测试：健康检查保持不变，业务接口走 JSON 路由。"""

import json
import threading
import unittest
from http.server import ThreadingHTTPServer
from urllib.error import HTTPError
from urllib.request import Request, urlopen

from api import Application
from eventstore import EventStore
from service import Handler, health_payload


class ApiHandler(Handler):
    """挂接应用的测试 Handler，不影响契约测试使用的原始 Handler。"""


class ApiTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        ApiHandler.application = Application(EventStore(), seed=True)
        cls.server = ThreadingHTTPServer(("127.0.0.1", 0), ApiHandler)
        cls.thread = threading.Thread(target=cls.server.serve_forever, daemon=True)
        cls.thread.start()
        cls.base_url = f"http://127.0.0.1:{cls.server.server_port}"

    @classmethod
    def tearDownClass(cls):
        cls.server.shutdown()
        cls.server.server_close()
        cls.thread.join(timeout=2)
        ApiHandler.application = None

    def get(self, path):
        try:
            with urlopen(f"{self.base_url}{path}", timeout=2) as response:
                return response.status, json.load(response)
        except HTTPError as error:
            return error.code, json.load(error)

    def post(self, path, payload, raw=None):
        body = raw if raw is not None else json.dumps(payload).encode("utf-8")
        request = Request(
            f"{self.base_url}{path}", data=body,
            headers={"Content-Type": "application/json"}, method="POST",
        )
        try:
            with urlopen(request, timeout=2) as response:
                return response.status, json.load(response)
        except HTTPError as error:
            return error.code, json.load(error)

    def test_health_still_available(self):
        status, payload = self.get("/health")
        self.assertEqual(status, 200)
        self.assertEqual(payload, health_payload())

    def test_unknown_api_route_returns_json_404(self):
        status, payload = self.get("/api/nope")
        self.assertEqual(status, 404)
        self.assertIn("error", payload)

    def test_invalid_json_body_is_400(self):
        status, payload = self.post("/api/orders", {}, raw=b"{not json")
        self.assertEqual(status, 400)
        self.assertIn("error", payload)

    def test_order_to_shipment_flow(self):
        self.post("/api/shifts", {"shift_id": "SHIFT-API", "date": "2026-09-19", "name": "早班"})
        status, _ = self.post("/api/materials/receive", {"batch_id": "MB-API", "material": "黑猪叉烧", "qty": 100})
        self.assertEqual(status, 200)
        status, order = self.post("/api/orders", {
            "order_id": "SO-API", "channel": "游客团购",
            "items": [{"product": "叉烧月饼", "qty": 50}], "due": "2026-09-24",
        })
        self.assertEqual(order["order"]["status"], "已接收")
        status, committed = self.post("/api/orders/SO-API/commit", {
            "materials": [{"batch_id": "MB-API", "qty": 20}],
            "equipment": [{"equip_id": "EQ-OVEN-01", "shift_id": "SHIFT-API", "planned_qty": 50}],
            "route": ["ST-01", "ST-05"], "shifts": ["SHIFT-API"], "decided_by": "生产经理",
        })
        self.assertEqual(status, 200)
        self.assertEqual(committed["order"]["status"], "已承诺")
        scan = {"event_key": "api-scan-1", "batch_id": "MB-API", "qty": 5, "lot_id": "LOT-API",
                "product": "叉烧馅", "step_id": "ST-03", "shift_id": "SHIFT-API", "worker_id": "W-001",
                "order_id": "SO-API"}
        status, first = self.post("/api/scan/consume", scan)
        self.assertFalse(first["deduplicated"])
        status, second = self.post("/api/scan/consume", scan)
        self.assertTrue(second["deduplicated"])
        _, material = self.get("/api/materials/MB-API")
        self.assertEqual(material["consumed"], 5)

    def test_business_error_has_status_and_message(self):
        status, payload = self.post("/api/orders/SO-MISSING/commit", {"materials": []})
        self.assertEqual(status, 404)
        self.assertIn("订单不存在", payload["error"])
        status, payload = self.post("/api/materials/receive", {"qty": 10})
        self.assertEqual(status, 400)
        self.assertIn("error", payload)

    def test_wages_endpoint(self):
        self.post("/api/shifts", {"shift_id": "SHIFT-WAGE", "date": "2026-09-19", "name": "中班"})
        self.post("/api/time-entries", {
            "event_key": "api-te-1", "worker_id": "W-002", "shift_id": "SHIFT-WAGE",
            "step_id": "ST-04", "hours": 6, "output_qty": 300,
        })
        status, payload = self.get("/api/wages?worker_id=W-002")
        self.assertEqual(status, 200)
        wage = payload["wages"][0]
        self.assertEqual(wage["total_hours"], 6)
        self.assertEqual(wage["amount_due"], 120)


if __name__ == "__main__":
    unittest.main()
