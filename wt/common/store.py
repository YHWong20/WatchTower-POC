"""SQLite evidence store: investigations, evidence, watch items, the audit log and the LLM cache.

One small file per deployment (WT_DB_PATH). Writes are tiny, so calls are synchronous and
guarded by a lock; that is fine inside the agents' event loops.
"""

from __future__ import annotations

import json
import sqlite3
import threading
from pathlib import Path
from typing import Any

from wt.common.contracts import EvidenceItem, Investigation, dump, utcnow

SCHEMA = """
CREATE TABLE IF NOT EXISTS investigations (
    id          TEXT PRIMARY KEY,
    opened_at   TEXT NOT NULL,
    updated_at  TEXT NOT NULL,
    playbook    TEXT NOT NULL,
    data        TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS evidence (
    investigation_id TEXT NOT NULL,
    id               TEXT NOT NULL,
    domain           TEXT NOT NULL,
    source           TEXT NOT NULL,
    query            TEXT NOT NULL,
    collected_at     TEXT NOT NULL,
    data             TEXT NOT NULL,
    PRIMARY KEY (investigation_id, id)
);
CREATE TABLE IF NOT EXISTS watch_items (
    id          TEXT PRIMARY KEY,
    opened_at   TEXT NOT NULL,
    updated_at  TEXT NOT NULL,
    status      TEXT NOT NULL,
    confidence  REAL NOT NULL,
    data        TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS audit (
    seq          INTEGER PRIMARY KEY AUTOINCREMENT,
    ts           TEXT NOT NULL,
    kind         TEXT NOT NULL,
    actor        TEXT NOT NULL,
    target       TEXT NOT NULL,
    input        TEXT NOT NULL,
    input_hash   TEXT NOT NULL,
    duration_ms  INTEGER NOT NULL,
    ok           INTEGER NOT NULL,
    detail       TEXT
);
CREATE TABLE IF NOT EXISTS llm_cache (
    key         TEXT PRIMARY KEY,
    job         TEXT NOT NULL,
    model       TEXT NOT NULL,
    created_at  TEXT NOT NULL,
    response    TEXT NOT NULL
);
"""


class Store:
    def __init__(self, path: str | Path = ":memory:"):
        if str(path) != ":memory:":
            Path(path).parent.mkdir(parents=True, exist_ok=True)
        self._db = sqlite3.connect(str(path), check_same_thread=False, isolation_level=None)
        self._db.row_factory = sqlite3.Row
        self._lock = threading.Lock()
        with self._lock:
            self._db.execute("PRAGMA journal_mode=WAL")
            self._db.executescript(SCHEMA)

    def close(self) -> None:
        self._db.close()

    def _exec(self, sql: str, args: tuple = ()) -> sqlite3.Cursor:
        with self._lock:
            return self._db.execute(sql, args)

    def _rows(self, sql: str, args: tuple = ()) -> list[sqlite3.Row]:
        with self._lock:
            return self._db.execute(sql, args).fetchall()

    # ------------------------------------------------------------ investigations

    def next_investigation_id(self) -> str:
        row = self._rows("SELECT MAX(CAST(SUBSTR(id, 5) AS INTEGER)) AS n FROM investigations")[0]
        return f"INV-{(row['n'] or 0) + 1:04d}"

    def save_investigation(self, inv: Investigation) -> None:
        """Upsert the investigation and every evidence item it carries."""
        now = utcnow().isoformat()
        self._exec(
            "INSERT INTO investigations (id, opened_at, updated_at, playbook, data) VALUES (?,?,?,?,?) "
            "ON CONFLICT(id) DO UPDATE SET updated_at=excluded.updated_at, playbook=excluded.playbook, "
            "data=excluded.data",
            (inv.id, inv.opened_at.isoformat(), now, inv.playbook, json.dumps(dump(inv))),
        )
        for ev in inv.evidence:
            self.add_evidence(inv.id, ev)

    def get_investigation(self, inv_id: str) -> Investigation | None:
        rows = self._rows("SELECT data FROM investigations WHERE id=?", (inv_id,))
        return Investigation.model_validate_json(rows[0]["data"]) if rows else None

    def list_investigations(self, limit: int = 50) -> list[Investigation]:
        rows = self._rows("SELECT data FROM investigations ORDER BY opened_at DESC LIMIT ?", (limit,))
        return [Investigation.model_validate_json(r["data"]) for r in rows]

    # ------------------------------------------------------------ evidence

    def next_evidence_id(self, investigation_id: str) -> str:
        row = self._rows(
            "SELECT MAX(CAST(SUBSTR(id, 4) AS INTEGER)) AS n FROM evidence WHERE investigation_id=?",
            (investigation_id,),
        )[0]
        return f"ev-{(row['n'] or 0) + 1}"

    def add_evidence(self, investigation_id: str, ev: EvidenceItem) -> None:
        self._exec(
            "INSERT OR REPLACE INTO evidence (investigation_id, id, domain, source, query, collected_at, data) "
            "VALUES (?,?,?,?,?,?,?)",
            (investigation_id, ev.id, ev.domain, ev.source, ev.query, ev.collected_at.isoformat(),
             json.dumps(dump(ev))),
        )

    def evidence_for(self, investigation_id: str) -> list[EvidenceItem]:
        rows = self._rows(
            "SELECT data FROM evidence WHERE investigation_id=? ORDER BY CAST(SUBSTR(id, 4) AS INTEGER)",
            (investigation_id,),
        )
        return [EvidenceItem.model_validate_json(r["data"]) for r in rows]

    # ------------------------------------------------------------ watch items
    # The watch-item model arrives with the Parent Agent (phase 5); stored as JSON until then.

    def save_watch_item(self, item: dict[str, Any]) -> None:
        now = utcnow().isoformat()
        self._exec(
            "INSERT INTO watch_items (id, opened_at, updated_at, status, confidence, data) VALUES (?,?,?,?,?,?) "
            "ON CONFLICT(id) DO UPDATE SET updated_at=excluded.updated_at, status=excluded.status, "
            "confidence=excluded.confidence, data=excluded.data",
            (item["id"], item.get("opened_at", now), now, item.get("status", "open"),
             float(item.get("confidence", 0.0)), json.dumps(item, default=str)),
        )

    def get_watch_item(self, item_id: str) -> dict[str, Any] | None:
        rows = self._rows("SELECT data FROM watch_items WHERE id=?", (item_id,))
        return json.loads(rows[0]["data"]) if rows else None

    def list_watch_items(self, status: str | None = None, limit: int = 50) -> list[dict[str, Any]]:
        if status:
            rows = self._rows(
                "SELECT data FROM watch_items WHERE status=? ORDER BY updated_at DESC LIMIT ?", (status, limit)
            )
        else:
            rows = self._rows("SELECT data FROM watch_items ORDER BY updated_at DESC LIMIT ?", (limit,))
        return [json.loads(r["data"]) for r in rows]

    # ------------------------------------------------------------ audit (written via wt.common.audit)

    def insert_audit(self, row: dict[str, Any]) -> int:
        cur = self._exec(
            "INSERT INTO audit (ts, kind, actor, target, input, input_hash, duration_ms, ok, detail) "
            "VALUES (:ts, :kind, :actor, :target, :input, :input_hash, :duration_ms, :ok, :detail)",
            row,
        )
        return cur.lastrowid

    def audit_rows(self, kind: str | None = None, limit: int = 200) -> list[dict[str, Any]]:
        if kind:
            rows = self._rows("SELECT * FROM audit WHERE kind=? ORDER BY seq DESC LIMIT ?", (kind, limit))
        else:
            rows = self._rows("SELECT * FROM audit ORDER BY seq DESC LIMIT ?", (limit,))
        return [dict(r) for r in rows]

    # ------------------------------------------------------------ LLM cache (written via wt.common.llm)

    def cache_get(self, key: str) -> str | None:
        rows = self._rows("SELECT response FROM llm_cache WHERE key=?", (key,))
        return rows[0]["response"] if rows else None

    def cache_put(self, key: str, job: str, model: str, response: str) -> None:
        self._exec(
            "INSERT OR REPLACE INTO llm_cache (key, job, model, created_at, response) VALUES (?,?,?,?,?)",
            (key, job, model, utcnow().isoformat(), response),
        )
