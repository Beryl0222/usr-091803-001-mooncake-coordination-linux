"""只增事件存储。

所有业务事实都以事件形式追加到 SQLite，事件一旦写入便不可修改或删除
（数据库触发器层面拒绝 UPDATE/DELETE）。每个事件携带：

* seq          全局递增序号，决定重放顺序；
* event_id     幂等键，扫描枪断网补传时重复提交只会生效一次；
* business_time 业务发生时间（允许补录历史）；
* recorded_at  系统实际记录时间，重放排产决定时以 seq 还原"当时"；
* payload      事件内容（JSON）。
"""

import json
import sqlite3
import threading
import uuid

SCHEMA = """
CREATE TABLE IF NOT EXISTS events (
    seq            INTEGER PRIMARY KEY AUTOINCREMENT,
    event_id       TEXT NOT NULL UNIQUE,
    event_type     TEXT NOT NULL,
    business_time TEXT NOT NULL,
    recorded_at    TEXT NOT NULL,
    payload        TEXT NOT NULL
);
CREATE TRIGGER IF NOT EXISTS events_no_update BEFORE UPDATE ON events
BEGIN
    SELECT RAISE(ABORT, 'events 为只增账本，禁止修改');
END;
CREATE TRIGGER IF NOT EXISTS events_no_delete BEFORE DELETE ON events
BEGIN
    SELECT RAISE(ABORT, 'events 为只增账本，禁止删除');
END;
"""


class EventStore:
    """线程安全的只增事件仓库。默认内存库，便于测试。"""

    def __init__(self, path=":memory:"):
        self._lock = threading.RLock()
        # check_same_thread=False：服务本身多线程，写操作由 self._lock 串行化。
        self._conn = sqlite3.connect(path, check_same_thread=False)
        self._conn.row_factory = sqlite3.Row
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.execute("PRAGMA foreign_keys=ON")
        self._conn.executescript(SCHEMA)
        self._conn.commit()

    def find_event(self, event_id):
        """按幂等键取事件，不存在返回 None。"""
        with self._lock:
            row = self._conn.execute(
                "SELECT * FROM events WHERE event_id=?", (event_id,)
            ).fetchone()
        return _row_to_event(row) if row is not None else None

    def append(self, event_type, payload, *, event_id=None, business_time, recorded_at):
        """追加一条事件。

        返回 (event, duplicated)：相同 event_id 已存在时 duplicated=True，
        原事件原样返回、不重复扣料、不产生第二条记录。
        """
        event_id = event_id or str(uuid.uuid4())
        body = json.dumps(payload, ensure_ascii=False, sort_keys=True)
        with self._lock:
            existing = self._conn.execute(
                "SELECT * FROM events WHERE event_id=?", (event_id,)
            ).fetchone()
            if existing is not None:
                return _row_to_event(existing), True
            cur = self._conn.execute(
                "INSERT INTO events (event_id, event_type, business_time, recorded_at, payload)"
                " VALUES (?,?,?,?,?)",
                (event_id, event_type, business_time, recorded_at, body),
            )
            self._conn.commit()
            row = self._conn.execute(
                "SELECT * FROM events WHERE seq=?", (cur.lastrowid,)
            ).fetchone()
            return _row_to_event(row), False

    def events(self, up_to_seq=None):
        """按序号读取事件；up_to_seq 用于把账本重放到某个历史时刻。"""
        with self._lock:
            if up_to_seq is None:
                rows = self._conn.execute(
                    "SELECT * FROM events ORDER BY seq"
                ).fetchall()
            else:
                rows = self._conn.execute(
                    "SELECT * FROM events WHERE seq<=? ORDER BY seq", (up_to_seq,)
                ).fetchall()
        return [_row_to_event(r) for r in rows]

    def max_seq(self):
        with self._lock:
            row = self._conn.execute("SELECT MAX(seq) AS m FROM events").fetchone()
            return row["m"] or 0

    def close(self):
        with self._lock:
            self._conn.close()


def _row_to_event(row):
    return {
        "seq": row["seq"],
        "event_id": row["event_id"],
        "type": row["event_type"],
        "business_time": row["business_time"],
        "recorded_at": row["recorded_at"],
        "payload": json.loads(row["payload"]),
    }
