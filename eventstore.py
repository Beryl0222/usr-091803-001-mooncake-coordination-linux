"""只追加事件存储：所有业务变化先落事件，再折叠成投影。

事件写入 JSONL 文件后不可修改，服务重启时按顺序回放即可恢复状态；
质检、返工、出库、退货、报告补录等历史记录因此永远不会被覆盖。
"""

import json
import os
import threading
from datetime import datetime, timezone


def utc_now():
    """返回带时区的 ISO 时间戳，字符串可安全按字典序比较。"""
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


class EventStore:
    """只追加的事件日志，可选地持久化到本地 JSONL 文件。"""

    def __init__(self, path=None):
        self.path = path
        self._events = []
        self._by_key = {}
        self._lock = threading.RLock()
        if path and os.path.exists(path):
            with open(path, encoding="utf-8") as fh:
                for line in fh:
                    line = line.strip()
                    if line:
                        self._remember(json.loads(line))

    def _remember(self, record):
        self._events.append(record)
        key = record.get("payload", {}).get("event_key")
        if key:
            self._by_key.setdefault(key, record)

    def append(self, type_, payload, at=None):
        """追加一条事件并落盘，返回带序号的完整记录。"""
        with self._lock:
            record = {
                "seq": len(self._events) + 1,
                "type": type_,
                "at": at or utc_now(),
                "payload": payload,
            }
            if self.path:
                directory = os.path.dirname(self.path)
                if directory:
                    os.makedirs(directory, exist_ok=True)
                with open(self.path, "a", encoding="utf-8") as fh:
                    fh.write(json.dumps(record, ensure_ascii=False) + "\n")
            self._remember(record)
            return record

    def all(self):
        """按写入顺序返回全部事件。"""
        return list(self._events)

    def find_by_key(self, event_key):
        """按幂等键查找已处理的事件，用于离线补传去重。"""
        if not event_key:
            return None
        return self._by_key.get(event_key)
