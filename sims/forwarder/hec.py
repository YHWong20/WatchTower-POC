"""HEC output with batching, retry and checkpoints that move only after Splunk acknowledges."""

from __future__ import annotations

import asyncio
import json
import logging
from collections import Counter, deque
from pathlib import Path
from typing import Any, Callable

import httpx

log = logging.getLogger("forwarder")


class Checkpoints:
    """Where each input got to (log file timestamp, journal cursor, event time), in a JSON file,
    so a restarted forwarder neither re-sends nor skips."""

    def __init__(self, path: Path):
        self.path = path
        try:
            self.data: dict[str, Any] = json.loads(path.read_text())
        except (OSError, ValueError):
            self.data = {}
        self.dirty = False

    def get(self, key: str, default: Any = None) -> Any:
        return self.data.get(key, default)

    def set(self, key: str, value: Any) -> None:
        self.data[key] = value
        self.dirty = True

    def save(self) -> None:
        if self.dirty:
            tmp = self.path.with_suffix(".tmp")
            tmp.write_text(json.dumps(self.data))
            tmp.replace(self.path)
            self.dirty = False

    async def loop(self, every_s: float = 5) -> None:
        while True:
            await asyncio.sleep(every_s)
            self.save()


class HecSender:
    def __init__(self, url: str, token: str, batch: int = 500, flush_s: float = 1.0, max_queue: int = 20000,
                 transport: httpx.AsyncBaseTransport | None = None):
        self.http = httpx.AsyncClient(base_url=url, headers={"Authorization": f"Splunk {token}"}, timeout=15,
                                      transport=transport)
        self.batch, self.flush_s, self.max_queue = batch, flush_s, max_queue
        self.queue: deque[tuple[dict, Callable[[], None] | None]] = deque()
        self.wake = asyncio.Event()
        self.dropped = 0
        self.shipped: Counter[str] = Counter()

    def send(self, event: dict[str, Any], on_ack: Callable[[], None] | None = None) -> None:
        if len(self.queue) >= self.max_queue:
            self.queue.popleft()
            self.dropped += 1
        self.queue.append((event, on_ack))
        if len(self.queue) >= self.batch:
            self.wake.set()

    async def flush(self) -> bool:
        """Send one batch. Returns False if Splunk did not accept it (the batch stays queued)."""
        if not self.queue:
            return True
        items = [self.queue[i] for i in range(min(self.batch, len(self.queue)))]
        body = "\n".join(json.dumps(ev, separators=(",", ":"), default=str) for ev, _ in items)
        try:
            resp = await self.http.post("/services/collector/event", content=body.encode())
        except httpx.HTTPError as exc:
            log.warning("HEC unreachable (%s); %d events queued", type(exc).__name__, len(self.queue))
            return False
        if resp.status_code != 200:
            log.warning("HEC refused a batch: %s %s", resp.status_code, resp.text[:200])
            if resp.status_code in (400,):          # a bad event: drop the batch rather than loop on it
                for _ in items:
                    self.queue.popleft()
            return False
        for _ in items:
            ev, cb = self.queue.popleft()
            self.shipped[ev.get("sourcetype", "?")] += 1
            if cb:
                cb()
        return True

    async def run(self) -> None:
        backoff = 1.0
        while True:
            try:
                await asyncio.wait_for(self.wake.wait(), self.flush_s)
            except TimeoutError:
                pass
            self.wake.clear()
            while self.queue:
                if not await self.flush():
                    await asyncio.sleep(backoff)
                    backoff = min(backoff * 2, 30)
                    break
                backoff = 1.0

    async def report(self, every_s: float = 60) -> None:
        """A line per minute in `make logs`: what reached Splunk, by sourcetype."""
        while True:
            await asyncio.sleep(every_s)
            if self.shipped or self.queue:
                parts = ", ".join(f"{k} {v}" for k, v in sorted(self.shipped.items()))
                log.info("shipped in the last %ds: %s; queued %d, dropped %d", every_s, parts or "nothing",
                         len(self.queue), self.dropped)
                self.shipped.clear()

    async def close(self) -> None:
        await self.http.aclose()
