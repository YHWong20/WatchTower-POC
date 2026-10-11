"""make replay NAME=x [SPEED=10]: replay a recording into a fresh Redis and show it in the Copilot.

  player     reads demo/recordings/<NAME>/events.jsonl.gz and republishes every wt.* event into
             the replay Redis on the recorded schedule (sped up), moving the replay clock along;
             alarm, agent and sample snapshots go straight to the replay database
  projector  consumes the replay Redis like the Parent Agent's readers do and writes the
             Copilot's database: signals, watch items, investigation progress, evidence, reports
  copilot    the normal Copilot in replay mode: recording clock, no LLM, no Splunk/SolarWinds
SPEED=0 jumps straight to the end state.
"""

from __future__ import annotations

import asyncio
import gzip
import json
import time
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from wt.common import bus as streams
from wt.common.bus import Bus
from wt.common.contracts import EvidenceItem, Investigation, Signal
from wt.common.store import Store
from wt.replay.recorder import RECORDINGS


def load(name: str) -> tuple[dict, list[dict]]:
    d = RECORDINGS / name
    if not (d / "events.jsonl.gz").exists():
        raise SystemExit(f"no recording {d} (make record NAME={name})")
    meta = json.loads((d / "meta.json").read_text()) if (d / "meta.json").exists() else {"name": name}
    with gzip.open(d / "events.jsonl.gz", "rt") as f:
        events = [json.loads(line) for line in f if line.strip()]
    events.sort(key=lambda e: e["t"])
    return meta, events


class Projector:
    """Applies events to the Copilot's database, as the live agents would have written it."""

    def __init__(self, store: Store):
        self.store = store

    def apply(self, stream: str, data: dict[str, Any], t: float) -> None:
        if stream == streams.SIGNALS:
            self.store.save_signal(Signal.model_validate(data))
        elif stream == streams.WATCH and "opened_at" in data:
            self.store.save_watch_item(data)
        elif stream == streams.INVESTIGATIONS:
            self.investigation_event(data, t)

    def investigation_event(self, data: dict[str, Any], t: float) -> None:
        if "request_id" in data and "text" not in data:
            self.store.kv_put(f"request:{data['request_id']}", {"investigation_id": data["id"]})
            return
        inv_id = data["id"]
        if data.get("evidence_item"):
            self.store.add_evidence(inv_id, EvidenceItem.model_validate(data["evidence_item"]))
        p = self.store.kv_get(f"progress:{inv_id}", {"status": "running", "steps": []})
        p["status"] = data.get("status", p["status"])
        p["steps"].append({"t": datetime.fromtimestamp(t, UTC).strftime("%H:%M:%S"), "text": data.get("text", "")})
        p.update({k: v for k, v in data.items()
                  if k not in ("id", "status", "text", "evidence_item", "investigation", "timeline")})
        self.store.kv_put(f"progress:{inv_id}", p)
        if data.get("investigation"):
            self.store.save_investigation(Investigation.model_validate(data["investigation"]))
            self.store.kv_put(f"timeline:{inv_id}", data.get("timeline") or [])

    def snapshot(self, kind: str, data: Any) -> None:
        if kind == "alarms":
            self.store.kv_put("replay:alarms", data)
        elif kind == "agents":
            self.store.kv_put("replay:agents", data)
        elif kind == "samples":
            self.store.add_samples([tuple(r) for r in data])


class Player:
    def __init__(self, name: str, store: Store, bus: Bus, speed: float = 10.0):
        self.meta, self.events = load(name)
        self.store, self.bus, self.speed = store, bus, speed
        self.projector = Projector(store)
        self.position = self.events[0]["t"] if self.events else time.time()
        self.done = asyncio.Event()

    @property
    def start(self) -> float:
        return self.events[0]["t"] if self.events else time.time()

    @property
    def end(self) -> float:
        return self.events[-1]["t"] if self.events else time.time()

    async def fresh(self) -> None:
        await self.bus.r.flushdb()                       # a fresh Redis: the replay's own instance

    async def play(self) -> None:
        wall0 = time.monotonic()
        for ev in self.events:
            if self.speed > 0:
                due = (ev["t"] - self.start) / self.speed
                while (wait := due - (time.monotonic() - wall0)) > 0:
                    self.tick(self.start + (time.monotonic() - wall0) * self.speed)
                    await asyncio.sleep(min(wait, 0.5))
            self.tick(ev["t"])
            if ev["kind"] == "bus":
                await self.bus.publish(ev["stream"], ev["data"])
            else:
                self.projector.snapshot(ev["kind"], ev["data"])
        self.tick(self.end)
        self.done.set()

    def tick(self, t: float) -> None:
        if t >= self.position:
            self.position = t
            self.store.kv_put("replay:now", t)

    async def drain(self) -> int:
        """Project everything waiting in the replay Redis, without blocking (tests, SPEED=0)."""
        group, n = "replay-projector", 0
        for s in streams.STREAMS:
            await self.bus.ensure_group(s, group)
        while batch := await self.bus.read(list(streams.STREAMS), group, "player", count=500, block_ms=None):
            for msg in batch:
                self.projector.apply(msg.stream, msg.data, self.position)
                await self.bus.ack(group, msg)
                n += 1
        return n

    async def project(self) -> None:
        """Consume the replay Redis and write the Copilot's database (stream id time = publish time,
        so the recording's own time is taken from the replay clock instead)."""
        group = "replay-projector"
        async for msg in self.bus.consume(list(streams.STREAMS), group, "player", block_ms=500):
            try:
                self.projector.apply(msg.stream, msg.data, self.position)
            finally:
                await self.bus.ack(group, msg)


def recorded_label(meta: dict) -> str:
    if "started" not in meta:
        return meta.get("name", "")
    a = datetime.fromtimestamp(meta["started"], UTC)
    b = datetime.fromtimestamp(meta.get("ended", meta["started"]), UTC)
    return f"recorded {a:%Y-%m-%d %H:%M}–{b:%H:%M} UTC"


def path_for(name: str) -> Path:
    return RECORDINGS / name
