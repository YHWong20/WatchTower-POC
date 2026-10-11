"""splunk-sim: the Splunk stand-in. HEC ingest, export search with an SPL subset, saved searches
on a 60 s scheduler, and fired alerts. Same paths and response shapes as Splunk Enterprise, so
wt/common/sources/splunk.py works unchanged against real Splunk later.

    HEC  :8088  POST /services/collector/event          Authorization: Splunk <hec token>
    REST :8089  POST /services/search/jobs/export        search=..., output_mode=json
                GET  /services/alerts/fired_alerts[/<name>]
                GET  /services/saved/searches
                GET  /services/server/info
    REST auth: Authorization: Bearer <token> (WatchTower), or basic auth (admin:demo for people).
    Both ports serve the same app; it is all read-only apart from HEC ingest.

Environment:
  SPLUNK_API_PORT 8089   SPLUNK_HEC_PORT 8088   SPLUNK_TOKENS wt-poc-token   SPLUNK_USERS admin:demo
  SPLUNK_HEC_TOKEN wt-hec-token   SPLUNK_DB $SIM_STATE_DIR/splunk.db   SPLUNK_RETENTION_H 48
  SPLUNK_SCHED_S 60   SPLUNK_SAVED_SEARCHES sims/splunk_sim/saved_searches.yaml
"""

from __future__ import annotations

import asyncio
import base64
import hashlib
import json
import logging
import os
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs

import yaml
from fastapi import Depends, FastAPI, HTTPException, Request
from fastapi.responses import JSONResponse, PlainTextResponse

from sims.common.state import http_server, state_dir
from sims.splunk_sim import spl
from sims.splunk_sim.store import EventStore

log = logging.getLogger("splunk-sim")
HERE = Path(__file__).resolve().parent


# ---------------------------------------------------------------- saved searches


@dataclass
class SavedSearch:
    name: str
    search: str
    earliest: str = "-10m"
    severity: int = 3
    entity_field: str | None = None
    expires: str = "24h"
    description: str = ""

    def __post_init__(self):
        self.parsed = spl.parse(self.search)       # fail at start-up, not at 3 a.m.


def load_saved_searches(path: str | Path) -> list[SavedSearch]:
    data = yaml.safe_load(Path(path).read_text()) or {}
    return [SavedSearch(name=name, **spec) for name, spec in data.items()]


class Scheduler:
    """Runs every saved search over the events indexed since its previous run."""

    def __init__(self, store: EventStore, searches: list[SavedSearch], interval_s: float = 60):
        self.store, self.searches, self.interval_s = store, searches, interval_s
        self.last_run = time.time()

    def run_once(self, now: float | None = None) -> list[str]:
        now = time.time() if now is None else now
        since, self.last_run = self.last_run, now
        fired = []
        for s in self.searches:
            t0, t1 = s.parsed.window(now, s.earliest, None)
            cands = self.store.candidates(t0, t1, s.parsed.prefilter, index_earliest=since)
            rows = spl.run(s.parsed, cands, now=now, earliest=s.earliest, index_window=(since, now))
            if not rows:
                continue
            entities = sorted({str(r.get(s.entity_field)) for r in rows if s.entity_field and r.get(s.entity_field)})
            sid = "scheduler__admin__search__" + hashlib.md5(f"{s.name}{now}".encode()).hexdigest()[:12] + f"_at_{int(now)}"
            message = str(rows[0].get("message") or rows[0].get("_raw", ""))[:300]
            self.store.fire(s.name, now, now + spl.span_seconds(s.expires), s.severity, sid,
                            len(rows), ", ".join(entities), message)
            fired.append(s.name)
            log.info("alert %s fired: %d results, entity %s", s.name, len(rows), entities)
        return fired

    async def loop(self) -> None:
        while True:
            await asyncio.sleep(self.interval_s)
            try:
                await asyncio.to_thread(self.run_once)
            except Exception:                         # keep scheduling; log the bad search
                log.exception("scheduler run failed")


# ---------------------------------------------------------------- HEC parsing


class HecError(Exception):
    def __init__(self, status: int, code: int, text: str):
        self.status, self.code, self.text = status, code, text


def parse_hec(body: str, received: float | None = None) -> list[dict[str, Any]]:
    """HEC accepts one or more JSON objects back to back. Returns store rows."""
    received = time.time() if received is None else received
    dec, pos, out = json.JSONDecoder(), 0, []
    body = body.strip()
    if not body:
        raise HecError(400, 5, "No data")
    while pos < len(body):
        while pos < len(body) and body[pos].isspace():
            pos += 1
        if pos >= len(body):
            break
        try:
            obj, pos = dec.raw_decode(body, pos)
        except ValueError:
            raise HecError(400, 6, "Invalid data format") from None
        if not isinstance(obj, dict) or "event" not in obj:
            raise HecError(400, 12, "Event field is required")
        ev = obj["event"]
        if ev in ("", None):
            raise HecError(400, 13, "Event field cannot be blank")
        try:
            t = float(obj.get("time", received))
        except (TypeError, ValueError):
            raise HecError(400, 6, "Invalid data format") from None
        fields = obj.get("fields") or {}
        if not isinstance(fields, dict):
            raise HecError(400, 6, "Invalid data format")
        out.append({
            "time": t, "index": obj.get("index") or "main", "sourcetype": obj.get("sourcetype") or "httpevent",
            "source": obj.get("source") or "http:hec", "host": obj.get("host") or "unknown",
            "raw": ev if isinstance(ev, str) else json.dumps(ev, separators=(",", ":")),
            "fields": {k: v for k, v in fields.items() if v not in (None, "")},
        })
    return out


# ---------------------------------------------------------------- app


def create_app(store: EventStore, searches: list[SavedSearch], tokens: set[str], users: dict[str, str],
               hec_tokens: set[str]) -> FastAPI:
    app = FastAPI(title="splunk-sim", docs_url=None, redoc_url=None, openapi_url=None)

    @app.exception_handler(HecError)
    async def hec_error(_request: Request, exc: HecError):
        return JSONResponse({"text": exc.text, "code": exc.code}, status_code=exc.status)

    def rest_auth(request: Request) -> str:
        header = request.headers.get("authorization", "")
        scheme, _, cred = header.partition(" ")
        if scheme.lower() in ("bearer", "splunk") and cred in tokens:
            return "token"
        if scheme.lower() == "basic":
            try:
                user, _, pw = base64.b64decode(cred).decode().partition(":")
            except ValueError:
                user, pw = "", ""
            if users.get(user) == pw:
                return user
        raise HTTPException(401, detail="call not properly authenticated")

    @app.exception_handler(HTTPException)
    async def rest_error(_request: Request, exc: HTTPException):
        return JSONResponse({"messages": [{"type": "ERROR" if exc.status_code < 500 else "FATAL", "text": exc.detail}]},
                            status_code=exc.status_code)

    # -------------------------------------------------------- HEC

    @app.get("/services/collector/health")
    def hec_health():
        return {"text": "HEC is healthy", "code": 17}

    @app.post("/services/collector/event")
    @app.post("/services/collector")
    @app.post("/services/collector/event/1.0")
    async def hec(request: Request):
        scheme, _, token = request.headers.get("authorization", "").partition(" ")
        if not token:
            raise HecError(401, 2, "Token is required")
        if scheme.lower() != "splunk" or token not in hec_tokens:
            raise HecError(403, 4, "Invalid token")
        rows = parse_hec((await request.body()).decode("utf-8", "replace"))
        await asyncio.to_thread(store.add, rows)
        return {"text": "Success", "code": 0}

    # -------------------------------------------------------- search

    @app.api_route("/services/search/jobs/export", methods=["GET", "POST"])
    async def export(request: Request, _who: str = Depends(rest_auth)):
        params = dict(request.query_params)
        if request.method == "POST":                      # application/x-www-form-urlencoded, as Splunk takes it
            form = parse_qs((await request.body()).decode("utf-8", "replace"), keep_blank_values=True)
            params.update({k: v[-1] for k, v in form.items()})
        query = params.get("search", "").strip()
        if not query:
            raise HTTPException(400, detail="Missing search parameter")
        if query.startswith("|"):
            raise HTTPException(400, detail="Generating commands are not supported by splunk-sim")
        if not query.lower().startswith("search"):
            raise HTTPException(400, detail="Search must start with 'search' (the export endpoint requires it)")
        try:
            rows = await asyncio.to_thread(_search, store, query, params.get("earliest_time"), params.get("latest_time"))
        except spl.SplError as exc:
            raise HTTPException(400, detail=f"Error in 'search' command: {exc}") from None
        lines = [json.dumps({"preview": False, "offset": i, **({"lastrow": True} if i == len(rows) - 1 else {}),
                             "result": r}, separators=(",", ":")) for i, r in enumerate(rows)]
        if not lines:
            lines = [json.dumps({"preview": False, "lastrow": True})]
        return PlainTextResponse("\n".join(lines) + "\n", media_type="application/json")

    # -------------------------------------------------------- alerts

    def entry_list(path: str, entries: list[dict]) -> dict:
        return {"origin": f"https://splunk:8089{path}", "entry": entries,
                "paging": {"total": len(entries), "perPage": 30, "offset": 0}}

    @app.get("/services/alerts/fired_alerts")
    def fired_alerts(_who: str = Depends(rest_auth)):
        by_name: dict[str, list[dict]] = {}
        for a in store.fired():
            by_name.setdefault(a["name"], []).append(a)
        entries = [{"name": name, "content": {"triggered_alert_count": len(items), "severity": items[0]["severity"],
                                              "trigger_time": int(items[0]["trigger_time"]),
                                              "entity": items[0]["entity"]}}
                   for name, items in sorted(by_name.items())]
        return entry_list("/services/alerts/fired_alerts", entries)

    @app.get("/services/alerts/fired_alerts/{name}")
    def fired_alert_instances(name: str, _who: str = Depends(rest_auth)):
        """Each triggered instance; name "-" lists every alert's instances."""
        entries = [{"name": a["sid"], "content": {
            "savedsearch_name": a["name"], "sid": a["sid"], "severity": a["severity"],
            "trigger_time": int(a["trigger_time"]), "trigger_time_rendered": spl.iso(a["trigger_time"]),
            "expiration_time_rendered": spl.iso(a["expires"]), "triggered_alerts": 1,
            "result_count": a["result_count"], "entity": a["entity"], "message": a["message"]}}
            for a in store.fired(None if name == "-" else name)]
        return entry_list(f"/services/alerts/fired_alerts/{name}", entries)

    @app.delete("/services/alerts/fired_alerts/{name}")
    def delete_fired(name: str, who: str = Depends(rest_auth)):
        """Splunk's own way to delete triggered alerts (an admin action; make demo-reset uses it).
        Token users (WatchTower) cannot: they are read-only."""
        if who == "token":
            raise HTTPException(403, detail="the WatchTower token is read-only")
        n = store.clear_fired(None if name == "-" else name)
        return entry_list(f"/services/alerts/fired_alerts/{name}", [{"name": name, "content": {"deleted": n}}])

    @app.get("/services/saved/searches")
    def saved(_who: str = Depends(rest_auth)):
        return entry_list("/services/saved/searches", [
            {"name": s.name, "content": {"search": s.search, "cron_schedule": "* * * * *", "is_scheduled": True,
                                         "alert.severity": s.severity, "alert.expires": s.expires,
                                         "dispatch.earliest_time": s.earliest, "description": s.description}}
            for s in searches])

    @app.get("/services/server/info")
    def info(_who: str = Depends(rest_auth)):
        return entry_list("/services/server/info", [
            {"name": "server-info", "content": {"serverName": "splunk-sim", "version": "9.3.0-sim",
                                                "product_type": "enterprise"}}])

    @app.get("/sim/summary")
    def summary(minutes: int = 5, _who: str = Depends(rest_auth)):
        """Not a Splunk endpoint: what arrived in the last N minutes, for lab/health.sh."""
        return {"now": time.time(), "sourcetypes": store.summary(time.time() - minutes * 60)}

    return app


def _search(store: EventStore, query: str, earliest: str | None, latest: str | None) -> list[dict]:
    search = spl.parse(query)
    now = time.time()
    t0, t1 = search.window(now, earliest, latest)
    i0, _ = search.index_window(now)
    return spl.run(search, store.candidates(t0, t1, search.prefilter, index_earliest=i0), now=now,
                   earliest=earliest, latest=latest)


# ---------------------------------------------------------------- main


def _csv(name: str, default: str) -> list[str]:
    return [x.strip() for x in os.environ.get(name, default).split(",") if x.strip()]


async def retention(store: EventStore, hours: float) -> None:
    while True:
        n = await asyncio.to_thread(store.trim, time.time() - hours * 3600)
        if n:
            log.info("retention: removed %d events older than %.0f h", n, hours)
        await asyncio.sleep(600)


async def serve() -> None:
    store = EventStore(os.environ.get("SPLUNK_DB", str(state_dir() / "splunk.db")))
    searches = load_saved_searches(os.environ.get("SPLUNK_SAVED_SEARCHES", str(HERE / "saved_searches.yaml")))
    app = create_app(
        store, searches,
        tokens=set(_csv("SPLUNK_TOKENS", "wt-poc-token")),
        users=dict(u.split(":", 1) for u in _csv("SPLUNK_USERS", "admin:demo")),
        hec_tokens=set(_csv("SPLUNK_HEC_TOKEN", "wt-hec-token")),
    )
    sched = Scheduler(store, searches, float(os.environ.get("SPLUNK_SCHED_S", "60")))
    servers = [http_server(app, int(os.environ.get(var, default)))
               for var, default in (("SPLUNK_API_PORT", "8089"), ("SPLUNK_HEC_PORT", "8088"))]
    log.info("splunk-sim up: REST :%s, HEC :%s, %d saved searches (%s)",
             os.environ.get("SPLUNK_API_PORT", "8089"), os.environ.get("SPLUNK_HEC_PORT", "8088"),
             len(searches), ", ".join(s.name for s in searches))
    await asyncio.gather(*(s.serve() for s in servers), sched.loop(),
                         retention(store, float(os.environ.get("SPLUNK_RETENTION_H", "48"))))
