"""splunk-sim storage: indexed events and fired alerts in SQLite (WAL)."""

from __future__ import annotations

import json
import sqlite3
import threading
import time
from pathlib import Path
from typing import Any, Iterator

from sims.splunk_sim.spl import Event

SCHEMA = """
CREATE TABLE IF NOT EXISTS events (
    id INTEGER PRIMARY KEY, time REAL NOT NULL, indextime REAL NOT NULL, idx TEXT NOT NULL,
    sourcetype TEXT NOT NULL, source TEXT NOT NULL, host TEXT NOT NULL, raw TEXT NOT NULL, fields TEXT
);
CREATE INDEX IF NOT EXISTS events_time ON events(time);
CREATE INDEX IF NOT EXISTS events_st_time ON events(sourcetype, time);
CREATE INDEX IF NOT EXISTS events_indextime ON events(indextime);
CREATE TABLE IF NOT EXISTS fired_alerts (
    id INTEGER PRIMARY KEY, name TEXT NOT NULL, trigger_time REAL NOT NULL, expires REAL NOT NULL,
    severity INTEGER NOT NULL, sid TEXT NOT NULL, result_count INTEGER NOT NULL, entity TEXT, message TEXT
);
"""


def _like(pattern: str) -> str:
    return pattern.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_").replace("*", "%")


class EventStore:
    def __init__(self, path: str | Path):
        if str(path) != ":memory:":
            Path(path).parent.mkdir(parents=True, exist_ok=True)
        self.db = sqlite3.connect(str(path), check_same_thread=False)
        self.db.execute("PRAGMA journal_mode=WAL")
        self.db.execute("PRAGMA synchronous=NORMAL")
        self.db.executescript(SCHEMA)
        self.lock = threading.Lock()

    def close(self) -> None:
        self.db.close()

    # ------------------------------------------------------------ events

    def add(self, events: list[dict[str, Any]], indextime: float | None = None) -> int:
        """events: {time, index, sourcetype, source, host, raw, fields}."""
        now = time.time() if indextime is None else indextime
        rows = [(float(e["time"]), now, e["index"], e["sourcetype"], e["source"], e["host"], e["raw"],
                 json.dumps(e.get("fields") or {}, separators=(",", ":"))) for e in events]
        with self.lock, self.db:
            self.db.executemany(
                "INSERT INTO events(time, indextime, idx, sourcetype, source, host, raw, fields) VALUES (?,?,?,?,?,?,?,?)",
                rows)
        return len(rows)

    def candidates(self, earliest: float | None, latest: float | None, prefilter: dict[str, list],
                   index_earliest: float | None = None, limit: int = 200_000) -> Iterator[Event]:
        """Events in the time range that match the search's metadata constraints, newest first.
        The full predicate is applied afterwards in Python."""
        where, args = [], []
        if earliest is not None:
            where.append("time >= ?"); args.append(earliest)
        if latest is not None:
            where.append("time <= ?"); args.append(latest)
        if index_earliest is not None:
            where.append("indextime > ?"); args.append(index_earliest)
        column = {"index": "idx", "sourcetype": "sourcetype", "source": "source", "host": "host"}
        for name, constraints in prefilter.items():
            for c in constraints:
                values = c if isinstance(c, list) else [c]
                where.append("(" + " OR ".join(f"{column[name]} LIKE ? ESCAPE '\\'" for _ in values) + ")")
                args.extend(_like(v) for v in values)
        sql = "SELECT time, idx, sourcetype, source, host, raw, fields, indextime FROM events"
        if where:
            sql += " WHERE " + " AND ".join(where)
        sql += " ORDER BY time DESC LIMIT ?"
        args.append(limit)
        with self.lock:
            rows = self.db.execute(sql, args).fetchall()
        for t, idx, st, src, host, raw, fields, it in rows:
            yield Event(t, idx, st, src, host, raw, json.loads(fields) if fields else {}, it)

    def summary(self, since: float) -> list[dict[str, Any]]:
        with self.lock:
            rows = self.db.execute(
                "SELECT idx, sourcetype, COUNT(*), MAX(time), MAX(indextime) FROM events WHERE indextime >= ? "
                "GROUP BY idx, sourcetype ORDER BY idx, sourcetype", (since,)).fetchall()
        return [{"index": i, "sourcetype": s, "count": n, "latest_time": t, "latest_indextime": it}
                for i, s, n, t, it in rows]

    def trim(self, older_than: float) -> int:
        with self.lock, self.db:
            n = self.db.execute("DELETE FROM events WHERE indextime < ?", (older_than,)).rowcount
            self.db.execute("DELETE FROM fired_alerts WHERE expires < ?", (time.time(),))
        return n

    # ------------------------------------------------------------ fired alerts

    def fire(self, name: str, trigger_time: float, expires: float, severity: int, sid: str,
             result_count: int, entity: str, message: str) -> None:
        with self.lock, self.db:
            self.db.execute(
                "INSERT INTO fired_alerts(name, trigger_time, expires, severity, sid, result_count, entity, message) "
                "VALUES (?,?,?,?,?,?,?,?)", (name, trigger_time, expires, severity, sid, result_count, entity, message))

    def clear_fired(self, name: str | None = None) -> int:
        with self.lock, self.db:
            if name:
                return self.db.execute("DELETE FROM fired_alerts WHERE name = ?", (name,)).rowcount
            return self.db.execute("DELETE FROM fired_alerts").rowcount

    def fired(self, name: str | None = None, now: float | None = None) -> list[dict[str, Any]]:
        now = time.time() if now is None else now
        sql = ("SELECT name, trigger_time, expires, severity, sid, result_count, entity, message "
               "FROM fired_alerts WHERE expires >= ?")
        args: list[Any] = [now]
        if name:
            sql += " AND name = ?"
            args.append(name)
        with self.lock:
            rows = self.db.execute(sql + " ORDER BY trigger_time DESC", args).fetchall()
        keys = ("name", "trigger_time", "expires", "severity", "sid", "result_count", "entity", "message")
        return [dict(zip(keys, r)) for r in rows]
