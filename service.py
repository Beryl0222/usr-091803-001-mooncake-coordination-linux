"""月饼工坊产销协同的运行入口与 HTTP 适配层。"""

import argparse
import json
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, unquote, urlparse

from api import Application
from eventstore import EventStore

SERVICE_ID = "mooncake-coordination"
SERVICE_NAME = "月饼工坊产销协同"


def health_payload():
    """返回稳定的服务身份信息。"""
    return {"status": "ok", "service": SERVICE_ID, "name": SERVICE_NAME}


class Handler(BaseHTTPRequestHandler):
    """保留健康检查，并把 /api/* 请求转交给产销协同应用。"""

    application = None  # 由 main() 注入；未注入时仅暴露健康检查

    def do_GET(self):
        self._dispatch("GET")

    def do_POST(self):
        self._dispatch("POST")

    def _dispatch(self, method):
        parsed = urlparse(self.path)
        path = unquote(parsed.path)
        if method == "GET" and path == "/health":
            self._send_json(200, health_payload())
            return
        app = type(self).application
        if app is None:
            self.send_error(404)
            return
        try:
            body = self._read_json()
        except ValueError:
            self._send_json(400, {"error": "请求体不是合法 JSON"})
            return
        query = {key: values[0] for key, values in parse_qs(parsed.query).items()}
        status, payload = app.handle(method, path, query, body)
        self._send_json(status, payload)

    def _read_json(self):
        length = int(self.headers.get("Content-Length") or 0)
        if length == 0:
            return {}
        raw = self.rfile.read(length)
        try:
            return json.loads(raw.decode("utf-8"))
        except (ValueError, UnicodeDecodeError) as exc:
            raise ValueError("invalid json") from exc

    def _send_json(self, status, obj):
        body = json.dumps(obj, ensure_ascii=False).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, *_args):
        return


def main():
    parser = argparse.ArgumentParser(description=SERVICE_NAME)
    parser.add_argument("--port", type=int, default=8000)
    parser.add_argument("--db", default="data/mooncake-events.jsonl", help="事件日志文件路径")
    parser.add_argument("--check", action="store_true")
    args = parser.parse_args()
    if args.check:
        assert health_payload()["service"] == SERVICE_ID
        Application(EventStore())  # 验证领域装配可用
        print("基础检查通过")
        return
    Handler.application = Application(EventStore(args.db))
    ThreadingHTTPServer(("0.0.0.0", args.port), Handler).serve_forever()


if __name__ == "__main__":
    main()
