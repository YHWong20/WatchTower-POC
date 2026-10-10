"""SolarWinds adapter: SWQL over the SWIS REST API (:17778).

Works against solarwinds-sim in the PoC and Orion later; only the URL and credentials change.
Queries come from contracts/sources.yaml; agents call query_signal("interface_discards", if_id=..., minutes=60).
"""

from __future__ import annotations

from typing import Any

import httpx

from wt.common.audit import Audit
from wt.common.catalog import Sources
from wt.common.sources.base import QueryResult, render_template

QUERY_PATH = "/SolarWinds/InformationService/v3/Json/Query"


class SolarWindsClient:
    def __init__(
        self,
        base_url: str,
        username: str,
        password: str,
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
            base_url=base_url, auth=(username, password), verify=verify, timeout=timeout_s, transport=transport
        )

    async def close(self) -> None:
        await self.http.aclose()

    async def query(self, swql: str, parameters: dict[str, Any] | None = None) -> QueryResult:
        async with self.audit.timed("query", self.actor, "solarwinds", swql) as rec:
            resp = await self.http.post(QUERY_PATH, json={"query": swql, "parameters": parameters or {}})
            resp.raise_for_status()
            rows = resp.json().get("results", [])
            rec.detail = f"{len(rows)} rows"
        return QueryResult(
            tool="solarwinds", query=swql, rows=rows, duration_ms=rec.duration_ms, audit_id=rec.audit_id
        )

    async def query_signal(self, key: str, **params: Any) -> QueryResult:
        """Run the SWQL that sources.yaml defines for a signal, e.g. interface_discards."""
        return await self.query(render_template(self.sources.template(key, "solarwinds"), **params))

    async def active_alerts(self) -> QueryResult:
        return await self.query(self.sources.template("active_alerts", "solarwinds"))
