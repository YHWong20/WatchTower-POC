"""Audit log: every query, probe and LLM call, with an inputs hash and its duration.

Usage:
    async with audit.timed("query", "network-agent", "solarwinds", swql) as rec:
        rows = ...
        rec.detail = f"{len(rows)} rows"
    rec.audit_id   # "aud-42", referenced from Investigation.audit_ids

A refused probe is recorded with kind="refused" so the attempt is visible.
"""

from __future__ import annotations

import hashlib
import time
from contextlib import asynccontextmanager
from dataclasses import dataclass
from typing import AsyncIterator, Literal

from wt.common.contracts import utcnow
from wt.common.store import Store

Kind = Literal["query", "probe", "llm", "refused"]


def input_hash(text: str) -> str:
    return hashlib.sha256(text.encode()).hexdigest()[:16]


@dataclass
class AuditRecord:
    detail: str | None = None
    audit_id: str | None = None
    duration_ms: int = 0


class Audit:
    def __init__(self, store: Store):
        self.store = store

    def record(
        self,
        kind: Kind,
        actor: str,
        target: str,
        input: str,
        duration_ms: int = 0,
        ok: bool = True,
        detail: str | None = None,
    ) -> str:
        seq = self.store.insert_audit(
            {
                "ts": utcnow().isoformat(),
                "kind": kind,
                "actor": actor,
                "target": target,
                "input": input,
                "input_hash": input_hash(input),
                "duration_ms": duration_ms,
                "ok": int(ok),
                "detail": (detail or "")[:2000] or None,
            }
        )
        return f"aud-{seq}"

    @asynccontextmanager
    async def timed(self, kind: Kind, actor: str, target: str, input: str) -> AsyncIterator[AuditRecord]:
        rec = AuditRecord()
        start = time.perf_counter()
        ok = False
        try:
            yield rec
            ok = True
        except Exception as exc:
            rec.detail = f"{type(exc).__name__}: {exc}"
            raise
        finally:
            rec.duration_ms = int((time.perf_counter() - start) * 1000)
            rec.audit_id = self.record(kind, actor, target, input, rec.duration_ms, ok, rec.detail)
