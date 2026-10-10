"""Shared types for read-only probes."""

from __future__ import annotations

import json
import time
from datetime import datetime
from typing import Any, Literal

from pydantic import BaseModel, Field

from wt.common.contracts import utcnow


class ProbeRefused(PermissionError):
    """The command or call is not read-only, so it was never sent."""


class ProbeThrottled(RuntimeError):
    """The device was probed less than min_interval_per_device_s ago."""


class ProbeResult(BaseModel):
    tool: Literal["probe_cli", "probe_k8s_api"]
    target: str                  # device or API server
    query: str                   # exact CLI command or "GET /api/v1/..."
    output: str = ""             # raw CLI output
    data: Any = None             # parsed JSON for API calls
    collected_at: datetime = Field(default_factory=utcnow)
    duration_ms: int = 0
    audit_id: str | None = None

    def excerpt(self, limit: int = 2000) -> str:
        text = self.output if self.data is None else json.dumps(self.data, separators=(",", ":"), default=str)
        return text if len(text) <= limit else text[: limit - 15] + "\n…(truncated)"


class Throttle:
    """At most one probe per device every `interval_s` (sources.yaml probe_policy)."""

    def __init__(self, interval_s: float):
        self.interval_s = interval_s
        self._last: dict[str, float] = {}

    def check(self, device: str) -> None:
        last = self._last.get(device)
        now = time.monotonic()
        if last is not None and now - last < self.interval_s:
            raise ProbeThrottled(f"{device} probed {now - last:.0f}s ago (min {self.interval_s:.0f}s)")
        self._last[device] = now
