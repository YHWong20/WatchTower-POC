"""What every domain agent shares: the task and evidence messages, the watch loop with signal
de-duplication, the task worker, baselines, and a small /health + /signals HTTP endpoint.

A domain agent covers one technology, never one device. Subclasses implement:
    watch_once(now) -> list[Signal]      deterministic watch rules (no LLM)
    gather(task)    -> EvidenceResult    evidence on request: reuse first, probe second
"""

from __future__ import annotations

import asyncio
import logging
import os
import socket
import statistics
import time
import uuid
from collections import deque
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Callable, Literal

import yaml
from fastapi import FastAPI
from pydantic import BaseModel, Field

from wt.common import bus as streams
from wt.common.audit import Audit
from wt.common.bus import Bus
from wt.common.catalog import Sources, Topology, load_sources, load_topology
from wt.common.config import Settings
from wt.common.contracts import Domain, Signal, Tool, dump, utcnow
from wt.common.sources.base import QueryResult, render_template
from wt.common.sources.recording import RecordingTransport
from wt.common.sources.solarwinds import SolarWindsClient
from wt.common.sources.splunk import SplunkClient
from wt.common.store import Store

log = logging.getLogger("wt.agents")
Clock = Callable[[], float]


# ---------------------------------------------------------------- messages on wt.tasks / wt.evidence


class Task(BaseModel):
    """A request from the Parent Agent for one piece of evidence (a sources.yaml signal)."""

    task_id: str = Field(default_factory=lambda: f"task-{uuid.uuid4().hex[:10]}")
    investigation_id: str
    to: Domain
    signal: str                                 # key in contracts/sources.yaml
    params: dict[str, Any] = {}
    needs_live: bool = False                    # the playbook wants current state: probe
    allow_probe: bool = False                   # the Parent Agent's probe budget allows a fallback probe
    max_age_s: int = 120                        # reused data older than this counts as stale
    why: str | None = None                      # which hypothesis/fact asked for it


class EvidenceResult(BaseModel):
    """A domain agent's answer to a Task. The Parent Agent numbers it (ev-N) into the chain."""

    task_id: str
    investigation_id: str
    signal: str
    domain: Domain
    agent: str
    ok: bool = True
    source: Tool
    query: str
    reason_for_probe: str | None = None
    collected_at: datetime = Field(default_factory=utcnow)
    excerpt: str = ""
    finding: str
    facts: dict[str, Any] = {}                 # structured values the predicates read
    audit_id: str | None = None
    error: str | None = None


# ---------------------------------------------------------------- context


@dataclass
class AgentContext:
    settings: Settings
    store: Store
    audit: Audit
    bus: Bus | None
    splunk: SplunkClient
    solarwinds: SolarWindsClient
    sources: Sources
    topology: Topology
    clock: Clock = time.time

    @classmethod
    def build(cls, settings: Settings, actor: str, bus: Bus | None = None, store: Store | None = None) -> AgentContext:
        store = store or Store(settings.db_path)
        audit = Audit(store)
        sources = load_sources(settings.contracts_dir)
        record = os.environ.get("WT_RECORD_SOURCES")      # record every Splunk/SolarWinds exchange (fixtures)
        rec = (lambda tool: RecordingTransport(record, tool=tool)) if record else (lambda tool: None)
        return cls(
            settings=settings, store=store, audit=audit, bus=bus,
            splunk=SplunkClient(settings.splunk_url, settings.splunk_token, sources, audit, actor=actor,
                                verify=settings.verify_tls, transport=rec("splunk")),
            solarwinds=SolarWindsClient(settings.solarwinds_url, settings.solarwinds_user, settings.solarwinds_password,
                                        sources, audit, actor=actor, verify=settings.verify_tls,
                                        transport=rec("solarwinds")),
            sources=sources, topology=load_topology(settings.contracts_dir, settings.profile),
        )

    def now(self) -> datetime:
        return datetime.fromtimestamp(self.clock(), UTC)


def load_rules(path: Path) -> dict[str, Any]:
    return yaml.safe_load(path.read_text())


def parse_window(text: str) -> int:
    """'5m' -> 300."""
    unit = {"s": 1, "m": 60, "h": 3600, "d": 86400}[text[-1]]
    return int(text[:-1]) * unit


# ---------------------------------------------------------------- baselines


class Baselines:
    """Rolling samples per (metric, entity, hour of week) in SQLite. Samples taken while a rule
    is hitting are not added, so an incident does not teach the baseline that it is normal."""

    def __init__(self, store: Store, keep: int = 200):
        self.store, self.keep = store, keep

    @staticmethod
    def bucket(ts: float) -> str:
        dt = datetime.fromtimestamp(ts, UTC)
        return f"{dt.weekday()}-{dt.hour:02d}"

    def add(self, key: str, ts: float, value: float) -> None:
        b = self.bucket(ts)
        values = self.store.baseline_get(key, b)
        values.append(round(float(value), 3))
        self.store.baseline_put(key, b, values[-self.keep:])

    def samples(self, key: str, ts: float, min_samples: int = 10) -> list[float]:
        """This hour-of-week's samples, or all hours' when this hour has too few."""
        values = self.store.baseline_get(key, self.bucket(ts))
        return values if len(values) >= min_samples else self.store.baseline_all(key)

    def mean(self, key: str, ts: float, default: float = 0.0) -> float:
        values = self.samples(key, ts)
        return round(statistics.fmean(values), 2) if values else default

    def p95(self, key: str, ts: float, min_samples: int) -> float | None:
        values = sorted(self.samples(key, ts))
        if len(values) < min_samples:
            return None
        return values[min(len(values) - 1, int(round(0.95 * (len(values) - 1))))]


# ---------------------------------------------------------------- the agent


class DomainAgent:
    name: str = "agent"
    domain: Domain = "network"
    rules_file: Path

    def __init__(self, ctx: AgentContext, rules: dict[str, Any] | None = None):
        self.ctx = ctx
        self.rules = rules if rules is not None else load_rules(self.rules_file)
        self.interval_s = float(self.rules.get("interval_s", 60))
        self.repeat_s = float(self.rules.get("repeat_s", 120))
        self.baselines = Baselines(ctx.store)
        self.last_sent: dict[tuple[str, str], tuple[float, float]] = {}    # (rule, entity) -> (ts, value)
        self.recent: deque[Signal] = deque(maxlen=200)
        self.status: dict[str, Any] = {"last_watch": None, "last_watch_ok": None, "watch_error": None,
                                       "stale": [], "tasks_handled": 0, "last_task": None, "probes": 0}

    # ------------------------------------------------------------ to implement

    async def watch_once(self, now: float) -> list[Signal]:
        raise NotImplementedError

    async def gather(self, task: Task) -> EvidenceResult:
        raise NotImplementedError

    def describe(self) -> str:
        """One line for the Copilot's agent list, e.g. '1 switch · SolarWinds + Splunk'."""
        return ""

    # ------------------------------------------------------------ helpers for subclasses

    async def splunk(self, key: str, **params: Any) -> QueryResult:
        return await self.ctx.splunk.search_signal(key, **params)

    async def swql(self, key: str, **params: Any) -> QueryResult:
        return await self.ctx.solarwinds.query_signal(key, **params)

    def query_text(self, key: str, tool: str | None = None, **params: Any) -> str:
        return render_template(self.ctx.sources.template(key, tool), **params)

    def signal(self, rule: str, kind: str, entity: str, severity: str, summary: str, value: float,
               baseline: float, tool: Tool, query: str, window_s: int | None = None, ts: float | None = None) -> Signal:
        return Signal(ts=datetime.fromtimestamp(ts or self.ctx.clock(), UTC), domain=self.domain, agent=self.name,
                      rule=rule, entity={"kind": kind, "name": entity}, severity=severity, summary=summary[:200],
                      value=round(float(value), 2), baseline=round(float(baseline), 2), window_s=window_s,
                      source={"tool": tool, "query": query})

    def sample(self, rows: list[tuple[str, str, float]], now: float) -> None:
        """Record metric samples (metric, entity, value) for sparklines and the Copilot timeline."""
        self.ctx.store.add_samples([(m, e, now, float(v)) for m, e, v in rows])

    # ------------------------------------------------------------ publishing

    def should_send(self, sig: Signal, now: float) -> bool:
        """New hits always go out; an ongoing hit is repeated every repeat_s (with fresh values)
        so the Parent Agent's 10-minute window keeps it; state changes are sent once each."""
        key = (sig.rule, sig.entity.name if sig.severity != "state_change" else f"{sig.entity.name}@{sig.summary}")
        last = self.last_sent.get(key)
        if last is None or now - last[0] >= self.repeat_s and sig.severity != "state_change":
            self.last_sent[key] = (now, sig.value)
            return True
        return False

    async def publish(self, signals: list[Signal], now: float) -> list[Signal]:
        sent = []
        for sig in signals:
            if not self.should_send(sig, now):
                continue
            self.ctx.store.save_signal(sig)
            if self.ctx.bus:
                await self.ctx.bus.publish(streams.SIGNALS, sig)
            self.recent.appendleft(sig)
            sent.append(sig)
            log.info("%s signal %s %s: %s", self.name, sig.rule, sig.entity.name, sig.summary)
        return sent

    # ------------------------------------------------------------ loops

    async def watch_loop(self) -> None:
        while True:
            started = time.monotonic()
            now = self.ctx.clock()
            try:
                signals = await self.watch_once(now)
                await self.publish(signals, now)
                self.status.update(last_watch=now, last_watch_ok=True, watch_error=None)
            except Exception as exc:                      # keep watching; show it in /health
                log.exception("%s watch failed", self.name)
                self.status.update(last_watch=now, last_watch_ok=False, watch_error=f"{type(exc).__name__}: {exc}")
            if int(now) % 600 < self.interval_s:
                self.ctx.store.trim_samples(now - 6 * 3600)
            await asyncio.sleep(max(self.interval_s - (time.monotonic() - started), 1))

    async def handle(self, task: Task) -> EvidenceResult:
        try:
            result = await self.gather(task)
        except Exception as exc:
            log.exception("%s task %s (%s) failed", self.name, task.task_id, task.signal)
            result = EvidenceResult(task_id=task.task_id, investigation_id=task.investigation_id, signal=task.signal,
                                    domain=self.domain, agent=self.name, ok=False, source="splunk", query="",
                                    finding=f"could not collect {task.signal}: {type(exc).__name__}",
                                    error=f"{type(exc).__name__}: {exc}")
        self.status["tasks_handled"] += 1
        self.status["last_task"] = self.ctx.clock()
        if result.source in ("probe_cli", "probe_k8s_api") and result.ok:
            self.status["probes"] += 1
        return result

    async def task_loop(self) -> None:
        bus = self.ctx.bus
        assert bus is not None
        group = self.name                        # each agent reads every task and keeps its own
        consumer = socket.gethostname()
        async for msg in bus.consume([streams.TASKS], group, consumer):
            try:
                task = Task.model_validate(msg.data)
            except ValueError:
                await bus.ack(group, msg)
                continue
            if task.to == self.domain:
                result = await self.handle(task)
                await bus.publish(streams.EVIDENCE, result)
            await bus.ack(group, msg)

    # ------------------------------------------------------------ HTTP

    def health(self) -> dict[str, Any]:
        now = self.ctx.clock()
        last = self.status["last_watch"]
        hour_ago = datetime.fromtimestamp(now - 3600, UTC).isoformat()
        recent = self.ctx.store.list_signals(since=hour_ago, agent=self.name, limit=500)
        return {
            "agent": self.name, "domain": self.domain, "describe": self.describe(),
            "status": "ok" if self.status["last_watch_ok"] and last and now - last < 3 * self.interval_s
            else "starting" if last is None else "degraded",
            "interval_s": self.interval_s, "last_watch_age_s": round(now - last, 1) if last else None,
            "watch_error": self.status["watch_error"], "stale": self.status["stale"],
            "signals_last_hour": len(recent), "active_rules": sorted({s.rule for s in recent}),
            "entities": sorted({s.entity.name for s in recent}),
            "tasks_handled": self.status["tasks_handled"], "probes": self.status["probes"],
        }

    def app(self) -> FastAPI:
        app = FastAPI(title=self.name, docs_url=None, redoc_url=None, openapi_url=None)

        @app.get("/health")
        def health():
            return self.health()

        @app.get("/signals")
        def signals(limit: int = 50):
            return [dump(s) for s in self.ctx.store.list_signals(agent=self.name, limit=limit)]

        return app


Severity = Literal["weak", "notable", "state_change"]
