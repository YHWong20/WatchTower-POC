"""make record NAME=x: capture a run for replay.

Writes demo/recordings/<NAME>/events.jsonl.gz, one JSON line per event with its wall time:
  {"t": 1791…, "kind": "bus", "stream": "wt.signals", "data": {...}}       every wt.* stream event
  {"t": …, "kind": "alarms", "data": {...}}       the stand-ins' alerts as the Copilot sees them (every 10 s)
  {"t": …, "kind": "agents", "data": [...]}       the agents' /health (every 10 s)
  {"t": …, "kind": "samples", "data": [[metric, entity, ts, value], ...]}   sparkline/timeline samples
and meta.json (name, profile, start/end). Reads Redis and the Copilot's read-only JSON endpoints
from the Mac, so it needs nothing inside the containers.
"""

from __future__ import annotations

import asyncio
import gzip
import json
import os
import signal
import time
from pathlib import Path

import httpx
import redis.asyncio as aioredis

from wt.common.bus import STREAMS
from wt.common.config import REPO_ROOT

RECORDINGS = REPO_ROOT / "demo" / "recordings"


class Recorder:
    def __init__(self, name: str, redis_url: str = "redis://127.0.0.1:6379/0", copilot: str = "http://127.0.0.1:8080",
                 poll_s: float = 10.0):
        self.dir = RECORDINGS / name
        self.dir.mkdir(parents=True, exist_ok=True)
        self.name = name
        self.redis = aioredis.from_url(redis_url, decode_responses=True, socket_timeout=30)
        self.http = httpx.AsyncClient(base_url=copilot, timeout=20)
        self.poll_s = poll_s
        self.out = gzip.open(self.dir / "events.jsonl.gz", "wt")
        self.counts: dict[str, int] = {}
        self.stop = asyncio.Event()
        self.started = time.time()

    def write(self, kind: str, data, stream: str | None = None, t: float | None = None) -> None:
        line = {"t": round(t or time.time(), 3), "kind": kind, **({"stream": stream} if stream else {}), "data": data}
        self.out.write(json.dumps(line, separators=(",", ":"), default=str) + "\n")
        key = stream or kind
        self.counts[key] = self.counts.get(key, 0) + 1

    async def bus(self) -> None:
        last = {s: "$" for s in STREAMS}
        while not self.stop.is_set():
            reply = await self.redis.xread(last, block=1000, count=200)
            for stream, entries in reply or []:
                for msg_id, fields in entries:
                    last[stream] = msg_id
                    # the stream id carries the publish time in ms
                    self.write("bus", json.loads(fields["data"]), stream, t=int(msg_id.split("-")[0]) / 1000)

    async def snapshots(self) -> None:
        since = time.time() - 3600                      # an hour of history so sparklines start full
        while not self.stop.is_set():
            for kind, path in (("alarms", "/api/alarms"), ("agents", "/api/agents")):
                try:
                    self.write(kind, (await self.http.get(path)).json())
                except (httpx.HTTPError, ValueError):
                    pass
            try:
                rows = (await self.http.get("/api/samples", params={"since": since})).json()
                if rows:
                    since = max(r[2] for r in rows)
                    self.write("samples", rows)
            except (httpx.HTTPError, ValueError):
                pass
            try:
                await asyncio.wait_for(self.stop.wait(), self.poll_s)
            except TimeoutError:
                pass

    async def run(self, duration_s: float | None = None) -> dict:
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGINT, signal.SIGTERM):
            loop.add_signal_handler(sig, self.stop.set)
        if duration_s:
            loop.call_later(duration_s, self.stop.set)
        print(f"recording to {self.dir} (Ctrl-C to stop)", flush=True)
        tasks = [asyncio.create_task(self.bus()), asyncio.create_task(self.snapshots())]
        await self.stop.wait()
        for t in tasks:
            t.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        self.out.close()
        meta = {"name": self.name, "profile": os.environ.get("WT_PROFILE", "lite"), "started": self.started,
                "ended": time.time(), "events": self.counts}
        (self.dir / "meta.json").write_text(json.dumps(meta, indent=1))
        await self.redis.aclose()
        await self.http.aclose()
        print(f"recorded {sum(self.counts.values())} events: {self.counts}", flush=True)
        return meta
