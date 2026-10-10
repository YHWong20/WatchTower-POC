"""Splunk adapter: SPL export search and fired alerts (REST API on :8089).

Works against splunk-sim in the PoC and real Splunk later; only the URL and token change.
Queries come from contracts/sources.yaml; agents call search_signal("api_svc_timeouts", node=..., window="30m").
"""

from __future__ import annotations

import json
from typing import Any

import httpx

from wt.common.audit import Audit
from wt.common.catalog import Sources
from wt.common.sources.base import QueryResult, render_template

EXPORT_PATH = "/services/search/jobs/export"
FIRED_ALERTS_PATH = "/services/alerts/fired_alerts"


class SplunkClient:
    def __init__(
        self,
        base_url: str,
        token: str,
        sources: Sources,
        audit: Audit,
        actor: str = "watchtower",
        verify: bool = False,
        timeout_s: float = 30.0,
        transport: httpx.AsyncBaseTransport | None = None,
    ):
        self.sources = sources
        self.audit = audit
        self.actor = actor
        self.http = httpx.AsyncClient(
            base_url=base_url,
            headers={"Authorization": f"Bearer {token}"},
            verify=verify,
            timeout=timeout_s,
            transport=transport,
        )

    async def close(self) -> None:
        await self.http.aclose()

    async def search(self, spl: str) -> QueryResult:
        """Run an SPL search through the export endpoint and return its final results."""
        search = spl if spl.lstrip().startswith(("search ", "|")) else f"search {spl}"
        async with self.audit.timed("query", self.actor, "splunk", spl) as rec:
            resp = await self.http.post(EXPORT_PATH, data={"search": search, "output_mode": "json"})
            resp.raise_for_status()
            rows = _parse_export(resp.text)
            rec.detail = f"{len(rows)} rows"
        return QueryResult(tool="splunk", query=spl, rows=rows, duration_ms=rec.duration_ms, audit_id=rec.audit_id)

    async def search_signal(self, key: str, **params: Any) -> QueryResult:
        """Run the SPL that sources.yaml defines for a signal, e.g. node_ready_events."""
        return await self.search(render_template(self.sources.template(key, "splunk"), **params))

    async def fired_alerts(self) -> QueryResult:
        path = self.sources.signals["active_alerts"].query_splunk or FIRED_ALERTS_PATH
        async with self.audit.timed("query", self.actor, "splunk", f"GET {path}") as rec:
            resp = await self.http.get(path, params={"output_mode": "json"})
            resp.raise_for_status()
            rows = [{"name": e.get("name"), **(e.get("content") or {})} for e in resp.json().get("entry", [])]
            rec.detail = f"{len(rows)} alerts"
        return QueryResult(
            tool="splunk", query=f"GET {path}", rows=rows, duration_ms=rec.duration_ms, audit_id=rec.audit_id
        )


    async def fired_alert_instances(self) -> QueryResult:
        """Every triggered instance (name, time, entity, message) from /services/alerts/fired_alerts/-."""
        path = (self.sources.signals["active_alerts"].query_splunk or FIRED_ALERTS_PATH) + "/-"
        async with self.audit.timed("query", self.actor, "splunk", f"GET {path}") as rec:
            resp = await self.http.get(path, params={"output_mode": "json"})
            resp.raise_for_status()
            rows = [{"sid": e.get("name"), **(e.get("content") or {})} for e in resp.json().get("entry", [])]
            rec.detail = f"{len(rows)} alert instances"
        return QueryResult(
            tool="splunk", query=f"GET {path}", rows=rows, duration_ms=rec.duration_ms, audit_id=rec.audit_id
        )


def _parse_export(body: str) -> list[dict[str, Any]]:
    """The export endpoint streams one JSON object per line. With preview results, keep only the
    final ones; skip messages and blank lines."""
    rows, final = [], []
    for line in body.splitlines():
        line = line.strip()
        if not line:
            continue
        obj = json.loads(line)
        if "result" not in obj:
            continue
        rows.append(obj["result"])
        if not obj.get("preview", False):
            final.append(obj["result"])
    return final or rows
