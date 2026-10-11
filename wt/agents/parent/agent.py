"""Parent Agent: correlates signals into watch items and runs investigations.

Loops:
  signals   wt.signals -> Correlator -> watch items (SQLite + wt.watch); opens an investigation
            when a watch item reaches 0.7 or a node changes state
  evidence  wt.evidence -> the investigation waiting for that task
  sre       wt.sre: {"type": "investigate", "request_id", node | switch+port | alarm | watch_item_id}
                    {"type": "dismiss", "watch_item_id"}
  expire    re-scores and closes watch items as signals age out
"""

from __future__ import annotations

import asyncio
import logging
import socket
import time
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from fastapi import FastAPI

from wt.agents.base import EvidenceResult, Task, load_rules
from wt.agents.parent.correlator import Correlator, WatchItem
from wt.agents.parent.investigator import Investigator, Request
from wt.common import bus as streams
from wt.common.audit import Audit
from wt.common.bus import Bus
from wt.common.catalog import load_escalation, load_playbooks, load_sources, load_topology
from wt.common.config import Settings
from wt.common.contracts import Signal
from wt.common.llm import LLMClient
from wt.common.store import Store

log = logging.getLogger("wt.parent")
HERE = Path(__file__).resolve().parent


class BusGatherer:
    """Publishes tasks on wt.tasks and hands each wt.evidence reply to the investigation waiting for it."""

    def __init__(self, bus: Bus):
        self.bus = bus
        self.waiting: dict[str, asyncio.Queue] = {}

    async def loop(self) -> None:
        group = "parent-agent"
        async for msg in self.bus.consume([streams.EVIDENCE], group, socket.gethostname()):
            try:
                res = EvidenceResult.model_validate(msg.data)
                q = self.waiting.get(res.task_id)
                if q:
                    q.put_nowait(res)
            except ValueError:
                pass
            await self.bus.ack(group, msg)

    async def gather(self, tasks: list[Task], timeout_s: float, on_result) -> None:
        q: asyncio.Queue = asyncio.Queue()
        pending = {t.task_id: t for t in tasks}
        for t in tasks:
            self.waiting[t.task_id] = q
            await self.bus.publish(streams.TASKS, t)
        deadline = time.monotonic() + timeout_s
        try:
            while pending:
                try:
                    res = await asyncio.wait_for(q.get(), max(deadline - time.monotonic(), 0.01))
                except TimeoutError:
                    log.warning("evidence timeout: no reply for %s", ", ".join(t.signal for t in pending.values()))
                    break
                task = pending.pop(res.task_id, None)
                if task:
                    await on_result(task, res)
        finally:
            for t in tasks:
                self.waiting.pop(t.task_id, None)


class ParentAgent:
    name = "parent-agent"

    def __init__(self, settings: Settings, store: Store, bus: Bus | None, investigator: Investigator,
                 correlator: Correlator, rules: dict[str, Any], gatherer: BusGatherer | None = None):
        self.settings, self.store, self.bus = settings, store, bus
        self.inv, self.corr, self.rules = investigator, correlator, rules
        self.gatherer = gatherer
        self.running: dict[str, asyncio.Task] = {}
        self.status: dict[str, Any] = {"signals_seen": 0, "investigations": 0, "last_signal": None}
        for raw in store.list_watch_items(limit=200):          # pick up open watch items after a restart
            item = WatchItem.model_validate(raw)
            if item.status not in ("closed", "dismissed"):
                correlator.items[item.id] = item

    @classmethod
    def build(cls, settings: Settings, bus: Bus) -> ParentAgent:
        store = Store(settings.db_path)
        audit = Audit(store)
        rules = load_rules(HERE / "rules.yaml")
        topology = load_topology(settings.contracts_dir, settings.profile)
        playbooks = load_playbooks(settings.playbooks_dir)
        escalation = load_escalation(settings.contracts_dir)
        llm = LLMClient(settings.llm_base_url, settings.llm_model, audit, store, enabled=settings.llm_enabled,
                        timeout_s=settings.llm_timeout_s, actor="parent-agent", api_key=settings.llm_api_key)
        gatherer = BusGatherer(bus)

        async def publish(event: dict) -> None:
            await bus.publish(streams.INVESTIGATIONS, event)

        inv = Investigator(store, topology, load_sources(settings.contracts_dir), playbooks, escalation, rules,
                           gatherer, llm if settings.llm_enabled else None, publish=publish)
        corr = Correlator(topology, rules, playbooks, escalation, new_id=lambda: cls._next_watch_id(store))
        return cls(settings, store, bus, inv, corr, rules, gatherer)

    @staticmethod
    def _next_watch_id(store: Store) -> str:
        n = store.kv_get("watch_counter", 0) + 1
        store.kv_put("watch_counter", n)
        return f"WI-{n:04d}"

    # ------------------------------------------------------------ watch items

    async def save(self, item: WatchItem) -> None:
        self.store.save_watch_item(item.model_dump(mode="json"))
        if self.bus:
            await self.bus.publish(streams.WATCH, item.model_dump(mode="json"))     # the whole item: replayable

    async def on_signal(self, sig: Signal) -> WatchItem | None:
        age = (datetime.now(UTC) - sig.ts).total_seconds()
        if age > self.rules["correlation"]["window_s"]:
            return None                     # history replayed after a restart: too old to correlate or act on
        self.status["signals_seen"] += 1
        self.status["last_signal"] = sig.ts.isoformat()
        item = self.corr.add(sig)
        await self.save(item)
        cfg = self.rules["investigate"]
        state_change = sig.severity == "state_change" and cfg["on_state_change"]
        # A node changing state always gets an investigation (unless one for it is still running);
        # a watch item crossing 0.7 gets one once.
        if state_change or (item.investigation_id is None and item.confidence >= cfg["confidence"]):
            mode = "incident_investigation" if state_change else "proactive_watch"
            inv_id = await self.start(running_only=state_change, req=Request(
                mode=mode, node=item.node, switch=item.switch, port=item.port, watch_item=item,
                requested_by=(f'WatchTower, {sig.ts:%H:%M} UTC: "{item.node} went NotReady. Investigating."' if state_change
                              else f'WatchTower, {sig.ts:%H:%M} UTC: "watch item {item.id} reached '
                                   f'{item.confidence:.2f}. Investigating."'),
                trigger_signal=sig.rule, signal_ids=[sig.id]))
            item.investigation_id = inv_id
            item.status = "investigating"
            await self.save(item)
        return item

    # ------------------------------------------------------------ investigations

    def recent_for(self, node: str | None, running_only: bool = False) -> str | None:
        if not node:
            return None
        rec = self.store.kv_get(f"inv_by_node:{node}")
        if not rec:
            return None
        if running_only:
            return rec["id"] if rec["id"] in self.running else None
        return rec["id"] if time.time() - rec["at"] < self.rules["investigate"]["dedupe_s"] else None

    async def start(self, req: Request, force: bool = False, running_only: bool = False) -> str:
        """Start (or reuse) an investigation; returns its id at once, the work runs in the background.
        Reuses one for the same node opened within dedupe_s (running_only: only one still running)."""
        node = req.node or (self.inv.topo.node_on_port(req.switch, req.port) if req.switch and req.port else None)
        existing = None if force else self.recent_for(node, running_only)
        if existing:
            return existing
        inv_id = await self.inv.new_id()
        if node:
            self.store.kv_put(f"inv_by_node:{node}", {"id": inv_id, "at": time.time()})
        self.status["investigations"] += 1

        async def run() -> None:
            try:
                await self.inv.investigate(req, inv_id)
            except Exception as exc:
                log.exception("investigation %s failed", inv_id)
                await self.inv.progress(inv_id, f"Investigation failed: {type(exc).__name__}: {exc}", status="failed")
            finally:
                self.running.pop(inv_id, None)

        self.running[inv_id] = asyncio.create_task(run())
        return inv_id

    async def on_sre(self, msg: dict[str, Any]) -> None:
        kind = msg.get("type")
        if kind == "investigate":
            item = None
            if msg.get("watch_item_id"):
                raw = self.store.get_watch_item(msg["watch_item_id"])
                item = WatchItem.model_validate(raw) if raw else None
            alarm = msg.get("alarm")
            node = msg.get("node") or (item.node if item else None)
            if not node and alarm and alarm.get("entity"):
                try:
                    node = self.inv.topo.node_name(alarm["entity"])
                except KeyError:
                    node = None
            req = Request(mode="proactive_watch" if item and not alarm else "incident_investigation",
                          node=node, switch=msg.get("switch") or (item.switch if item else None),
                          port=msg.get("port") or (item.port if item else None), alarm=alarm, watch_item=item,
                          requested_by=msg.get("requested_by") or "SRE")
            inv_id = await self.start(req, force=bool(msg.get("force")))
            if item:
                item.investigation_id, item.status = inv_id, "investigating"
                self.corr.items[item.id] = item
                await self.save(item)
            self.store.kv_put(f"request:{msg.get('request_id')}", {"investigation_id": inv_id})
            if self.bus:
                await self.bus.publish(streams.INVESTIGATIONS, {"request_id": msg.get("request_id"), "id": inv_id})
        elif kind == "dismiss" and msg.get("watch_item_id"):
            raw = self.store.get_watch_item(msg["watch_item_id"])
            if raw:
                item = WatchItem.model_validate(raw)
                item.status = "dismissed"
                self.corr.items.pop(item.id, None)
                await self.save(item)

    # ------------------------------------------------------------ loops

    async def signal_loop(self) -> None:
        group = self.name
        async for msg in self.bus.consume([streams.SIGNALS], group, socket.gethostname()):
            try:
                await self.on_signal(Signal.model_validate(msg.data))
            except Exception:
                log.exception("signal %s not handled", msg.id)
            await self.bus.ack(group, msg)

    async def sre_loop(self) -> None:
        group = self.name
        async for msg in self.bus.consume([streams.SRE], group, socket.gethostname()):
            try:
                await self.on_sre(msg.data)
            except Exception:
                log.exception("SRE request %s not handled", msg.id)
            await self.bus.ack(group, msg)

    async def expire_loop(self) -> None:
        while True:
            await asyncio.sleep(30)
            for item in self.corr.expire(datetime.now(UTC)):
                await self.save(item)

    async def run(self) -> None:
        await asyncio.gather(self.signal_loop(), self.sre_loop(), self.expire_loop(), self.gatherer.loop())

    def app(self) -> FastAPI:
        app = FastAPI(title=self.name, docs_url=None, redoc_url=None, openapi_url=None)

        @app.get("/health")
        def health():
            open_items = [i for i in self.corr.items.values() if i.status not in ("closed", "dismissed")]
            return {"agent": self.name, "status": "ok", "signals_seen": self.status["signals_seen"],
                    "last_signal": self.status["last_signal"], "watch_items_open": len(open_items),
                    "cross_domain_items": sum(1 for i in open_items if i.scope == "cross_domain"),
                    "investigations_started": self.status["investigations"],
                    "running": sorted(self.running), "llm": self.settings.llm_model if self.settings.llm_enabled else "off"}

        return app
