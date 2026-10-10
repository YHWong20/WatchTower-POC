"""Helpers to run domain agents on recorded (or recording-shaped) Splunk/SolarWinds responses."""

from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta
from pathlib import Path

import httpx

from wt.agents.base import AgentContext
from wt.common.audit import Audit
from wt.common.catalog import load_sources, load_topology
from wt.common.config import PROFILES, load_settings
from wt.common.sources.base import render_template
from wt.common.sources.recording import ReplayTransport
from wt.common.sources.solarwinds import SolarWindsClient
from wt.common.sources.splunk import SplunkClient
from wt.common.store import Store

EXPORT = "/services/search/jobs/export"
SWIS = "/SolarWinds/InformationService/v3/Json/Query"


def splunk_body(rows: list[dict]) -> str:
    if not rows:
        return '{"preview":false,"lastrow":true}\n'
    return "\n".join(json.dumps({"preview": False, "offset": i, "result": r}) for i, r in enumerate(rows)) + "\n"


def record(sources, key: str, rows: list[dict], tool: str | None = None, **params) -> dict:
    sig = sources.signals[key]
    tool = tool or (sig.primary if isinstance(sig.primary, str) else sig.primary[0])
    query = render_template(sources.template(key, tool), **params)
    if tool == "splunk":
        return {"tool": "splunk", "path": EXPORT, "query": query, "status": 200, "body": splunk_body(rows)}
    return {"tool": "solarwinds", "path": SWIS, "query": query, "status": 200, "body": json.dumps({"results": rows})}


def context(records: list[dict], now: float, store: Store | None = None, profile: str = "lite",
            strict: bool = False) -> AgentContext:
    settings = load_settings()
    settings = settings.__class__(**{**settings.__dict__, "profile": PROFILES[profile]})
    store = store or Store(":memory:")
    audit = Audit(store)
    sources = load_sources()
    splunk_t = ReplayTransport([r for r in records if r["tool"] == "splunk"], strict=strict)
    sw_t = ReplayTransport([r for r in records if r["tool"] == "solarwinds"], strict=strict)
    return AgentContext(
        settings=settings, store=store, audit=audit, bus=None,
        splunk=SplunkClient("http://splunk", "t", sources, audit, transport=splunk_t),
        solarwinds=SolarWindsClient("http://solarwinds", "u", "p", sources, audit, transport=sw_t),
        sources=sources, topology=load_topology(profile=PROFILES[profile]), clock=lambda: now,
    )


def iso(t: float) -> str:
    """SWIS DateTime format."""
    return datetime.fromtimestamp(t, UTC).strftime("%Y-%m-%dT%H:%M:%S.0000000Z")


def splunk_time(t: float) -> str:
    return datetime.fromtimestamp(t, UTC).strftime("%Y-%m-%dT%H:%M:%S.000+00:00")


def recorded(path: Path) -> list[dict]:
    return [json.loads(line) for line in path.read_text().splitlines() if line.strip()]
