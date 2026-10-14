"""Copilot: the SRE's web UI (FastAPI + Jinja2 + HTMX + Chart.js, all served locally).

Screens (wireframes/proactive-watch.html, wireframes/incident-investigation.html):
  /                       proactive watch: alarms from the existing tools (Investigate on each),
                          domain agents, watch items with evidence cards and sparklines, what
                          happens next, signals over the last hour, Ask box
  /investigations/<id>    live progress while evidence arrives, then the report
Read-only towards the infrastructure: the only actions start or dismiss WatchTower's own work
(investigate, explain, ask, not relevant). Nothing here can change a switch or the cluster.
"""

from __future__ import annotations

import asyncio
import os
import re
import time
import uuid
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import httpx
from fastapi import FastAPI, Form, Request
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse, Response
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

from wt.agents.parent.correlator import WatchItem, watch_item_text
from wt.common import bus as streams
from wt.common import jobs
from wt.common.audit import Audit
from wt.common.bus import Bus
from wt.common.catalog import load_sources, load_topology
from wt.common.config import Settings
from wt.common.contracts import Investigation
from wt.common.llm import LLMClient
from wt.common.sources.solarwinds import SolarWindsClient
from wt.common.sources.splunk import SplunkClient
from wt.common.store import Store

HERE = Path(__file__).resolve().parent
TAGS = {"splunk": ("SPLUNK", "s-spl"), "solarwinds": ("SOLARWINDS", "s-sw"), "probe_cli": ("PROBE · CLI", "s-cli"),
        "probe_k8s_api": ("PROBE · API", "s-api")}
METRIC_OF_RULE = {"ocp.api_svc_timeouts": "svc_timeouts", "net.output_drops_rising": "out_discards",
                  "ocp.lease_update_failures": "lease_failures", "ocp.etcd_latency": "etcd_warnings",
                  "net.errors_rising": "in_errors", "net.util_vs_baseline": "util"}
STOP = set("a an the is are was were be been of to in on for and or not no it its this that what why how did does do "
           "any anything else there with from at by as we i you me my about only some has have had can could "
           "was were when where which who whom".split())


def sparkline(points: list[tuple[float, float]], width: int = 220, height: int = 30) -> str:
    """Inline SVG polyline (server-side: no JS per card)."""
    if len(points) < 2:
        return ""
    t0, t1 = points[0][0], points[-1][0]
    vmax = max(v for _, v in points) or 1
    xy = " ".join(f"{(t - t0) / max(t1 - t0, 1) * (width - 4) + 2:.1f},{height - 3 - v / vmax * (height - 6):.1f}"
                  for t, v in points)
    return (f'<svg class="spark" viewBox="0 0 {width} {height}" preserveAspectRatio="none">'
            f'<polyline points="{xy}" fill="none" stroke="currentColor" stroke-width="1.6"/></svg>')


class Clock:
    """Wall-clock time, or the recording's time while replaying (set by wt/replay/player.py)."""

    def __init__(self) -> None:
        self.store: Store | None = None           # replay: read "replay:now" from this store

    def now(self) -> float:
        if self.store is not None:
            t = self.store.kv_get("replay:now")
            if t:
                return float(t)
        return time.time()


CLOCK = Clock()


def hhmm(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, (int, float)):
        dt = datetime.fromtimestamp(value, UTC)
    elif isinstance(value, datetime):
        dt = value
    else:
        dt = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    return dt.astimezone(UTC).strftime("%H:%M")


def ago(value: Any) -> str:
    if value is None:
        return "never"
    t = value if isinstance(value, (int, float)) else datetime.fromisoformat(str(value).replace("Z", "+00:00")).timestamp()
    s = CLOCK.now() - t
    if s < 90:
        return f"{int(s)} s ago"
    if s < 5400:
        return f"{int(s // 60)} min ago"
    if s < 172800:
        return f"{int(s // 3600)} h ago"
    return f"{int(s // 86400)} days ago"


def code_spans(text: str) -> str:
    """Wrap error strings and port names in <span class=k> like the wireframes (input is escaped first)."""
    from markupsafe import escape
    out = str(escape(text))
    out = re.sub(r"(i/o timeout|failed to update lease|Gi\d+/\d+/\d+|dial tcp [\d.:]+)", r'<span class="k">\1</span>', out)
    return out


class Copilot:
    def __init__(self, settings: Settings, bus: Bus | None, store: Store | None = None,
                 splunk: SplunkClient | None = None, solarwinds: SolarWindsClient | None = None,
                 llm: LLMClient | None = None, agent_urls: list[str] | None = None, replay: dict | None = None):
        self.settings = settings
        self.replay = replay                      # {"name", "speed", "recorded"} in replay mode
        self.bus = bus
        self.store = store or Store(settings.db_path)
        audit = Audit(self.store)
        sources = load_sources(settings.contracts_dir)
        self.topology = load_topology(settings.contracts_dir, settings.profile)
        self.splunk = splunk or SplunkClient(settings.splunk_url, settings.splunk_token, sources, audit, actor="copilot")
        self.solarwinds = solarwinds or SolarWindsClient(settings.solarwinds_url, settings.solarwinds_user,
                                                         settings.solarwinds_password, sources, audit, actor="copilot")
        self.llm = llm or (LLMClient(settings.llm_base_url, settings.llm_model, audit, self.store,
                                     timeout_s=settings.llm_timeout_s, actor="copilot", api_key=settings.llm_api_key)
                           if settings.llm_enabled else None)
        self.agent_urls = agent_urls if agent_urls is not None else [
            u for u in os.environ.get("WT_AGENT_URLS", "http://127.0.0.1:8101,http://127.0.0.1:8102,"
                                                       "http://127.0.0.1:8103").split(",") if u]
        self.cache: dict[str, tuple[float, Any]] = {}
        self.http = httpx.AsyncClient(timeout=3)
        if replay:
            self.llm = None                       # a replay needs neither the lab nor the model
            CLOCK.store = self.store

    def now(self) -> float:
        return CLOCK.now()

    async def cached(self, key: str, ttl: float, fn):
        hit = self.cache.get(key)
        if hit and time.time() - hit[0] < ttl:
            return hit[1]
        value = await fn()
        self.cache[key] = (time.time(), value)
        return value

    # ------------------------------------------------------------ data

    async def alarms(self) -> dict[str, Any]:
        if self.replay:
            return self.store.kv_get("replay:alarms", {"splunk": [], "solarwinds": [], "splunk_ok": True,
                                                       "solarwinds_ok": True, "splunk_last": None})

        async def load():
            out: dict[str, Any] = {"splunk": [], "solarwinds": [], "splunk_ok": True, "solarwinds_ok": True,
                                   "splunk_last": None}
            try:
                rows = (await self.splunk.fired_alert_instances()).rows
                out["splunk_last"] = max((r.get("trigger_time", 0) for r in rows), default=None)
                out["splunk"] = [r for r in rows if r.get("trigger_time", 0) > self.now() - 6 * 3600]
                for r in out["splunk"]:                       # the entity WatchTower can investigate: a node
                    ents = [e.strip() for e in str(r.get("entity") or "").split(",") if e.strip()]
                    r["node"] = next((e for e in ents if self.node_of(e)), ents[0] if ents else "")
            except (httpx.HTTPError, ValueError):
                out["splunk_ok"] = False
            try:
                out["solarwinds"] = (await self.solarwinds.active_alerts()).rows
            except (httpx.HTTPError, ValueError):
                out["solarwinds_ok"] = False
            return out
        return await self.cached("alarms", 10, load)

    def node_of(self, name: str) -> str | None:
        try:
            return self.topology.node_name(name)
        except KeyError:
            return None

    async def agents(self) -> list[dict[str, Any]]:
        if self.replay:
            return self.store.kv_get("replay:agents", [])

        async def one(url: str) -> dict[str, Any]:
            try:
                return (await self.http.get(f"{url}/health")).json()
            except (httpx.HTTPError, ValueError):
                return {"agent": url.split("//")[-1].split(":")[0], "status": "down"}
        return await self.cached("agents", 5, lambda: asyncio.gather(*(one(u) for u in self.agent_urls)))

    async def llm_status(self) -> str:
        if self.replay:
            return "replay"
        if not self.llm:
            return "off"

        async def probe():
            base = self.settings.llm_base_url.rsplit("/v1", 1)[0]
            try:
                tags = (await self.http.get(f"{base}/api/tags")).json()
                names = {m.get("name") for m in tags.get("models", [])}
                return "online" if self.settings.llm_model in names else "model missing"
            except (httpx.HTTPError, ValueError):
                return "offline"
        return await self.cached("llm", 15, probe)

    def watch_items(self) -> list[WatchItem]:
        items = [WatchItem.model_validate(r) for r in self.store.list_watch_items(limit=50)]
        recent = datetime.fromtimestamp(self.now(), UTC) - timedelta(hours=2)
        items = [i for i in items if i.status not in ("dismissed",) and (i.status != "closed" or i.updated_at > recent)]
        rank = {"investigating": 0, "emerging": 1, "watching": 2, "closed": 3}
        return sorted(items, key=lambda i: (rank.get(i.status, 4), -i.confidence))

    def series(self, metric: str, entity: str, minutes: int = 60) -> list[tuple[float, float]]:
        now = self.now()
        return [(t, v) for t, v in self.store.samples(metric, entity, now - minutes * 60) if t <= now]

    def cards(self, item: WatchItem) -> list[dict[str, Any]]:
        out = []
        for s in sorted(item.signals, key=lambda s: (s.domain != "openshift", s.ts)):
            metric = METRIC_OF_RULE.get(s.rule)
            pts = self.series(metric, s.entity.name) if metric else []
            tag, cls = TAGS[s.source.tool]
            out.append({"tag": tag, "cls": cls, "agent": "Infra Agent" if s.domain == "openshift" else "Network Agent",
                        "html": code_spans(s.summary), "spark": sparkline(pts), "rule": s.rule, "id": s.id,
                        "query": s.source.query, "at": hhmm(s.ts)})
        return out

    def investigation(self, inv_id: str) -> Investigation | None:
        return self.store.get_investigation(inv_id)

    # ------------------------------------------------------------ ask

    def context_for(self, inv_id: str | None) -> list[tuple[str, str]]:
        ctx: list[tuple[str, str]] = []
        inv = self.store.get_investigation(inv_id) if inv_id else None
        if inv is None and not inv_id:
            recent = self.store.list_investigations(limit=1)
            inv = (recent[0] if recent and recent[0].opened_at > datetime.fromtimestamp(self.now(), UTC) - timedelta(hours=3)
                   else None)
        if inv:
            ctx.append((inv.id, f"Conclusion: {inv.conclusion.likely_cause}. {inv.conclusion.what_happened}"))
            ctx += [(e.id, f"{e.source}: {e.finding}") for e in inv.evidence]
            ctx += [(c.id, c.text) for c in inv.correlations]
            ctx += [(inv.id + "/" + h.id, f"Hypothesis {h.statement}: confidence {h.confidence:.2f}") for h in inv.hypotheses[:4]]
        elif inv_id:
            ctx += [(e.id, f"{e.source}: {e.finding}") for e in self.store.evidence_for(inv_id)]
        for item in self.watch_items()[:3]:
            ctx.append((item.id, f"Watch item ({item.status}, {item.confidence}): {item.title}. {item.correlation}"))
            ctx += [(s.id, s.summary) for s in item.signals[-4:]]
        return ctx

    @staticmethod
    def stems(text: str) -> set[str]:
        out = set()
        for w in re.findall(r"[a-z0-9/.\-]+", text.lower()):
            if w in STOP or len(w) < 3:
                continue
            for suf in ("ing", "ed", "es", "s"):
                if w.endswith(suf) and len(w) - len(suf) >= 3:
                    w = w[: -len(suf)]
                    break
            out.add(w)
        return out

    def rank(self, question: str, ctx: list[tuple[str, str]]) -> list[tuple[int, str, str]]:
        """Context items sharing words with the question, best first (retrieval decides whether
        WatchTower has evidence at all)."""
        q = self.stems(question)
        scored = [(len(q & self.stems(text)), cid, text) for cid, text in ctx]
        kind = {"ev": 0, "cor": 1}                # on a tie, cite evidence before correlations and summaries
        return sorted([x for x in scored if x[0] > 0], key=lambda x: (-x[0], kind.get(x[1].split("-")[0], 2)))

    @staticmethod
    def rule_answer(ranked: list[tuple[int, str, str]]) -> jobs.Answer:
        """Extractive answer: the best-matching stored findings, with their ids."""
        if not ranked:
            return jobs.Answer(answer=jobs.NO_EVIDENCE, cited=[], has_evidence=False)
        best = [ranked[0]] + [r for r in ranked[1:2] if r[0] == ranked[0][0]]
        return jobs.Answer(answer=" ".join(f"{t} ({cid})." for _, cid, t in best)[:600], cited=[c for _, c, _ in best],
                           has_evidence=True)

    async def ask(self, question: str, inv_id: str | None) -> tuple[jobs.Answer, str]:
        ranked = self.rank(question, self.context_for(inv_id))
        if not ranked:
            return jobs.Answer(answer=jobs.NO_EVIDENCE, cited=[], has_evidence=False), "rule"
        if self.llm:
            ans = await jobs.answer(self.llm, question, [(cid, t) for _, cid, t in ranked[:5]])
            if ans and ans.has_evidence:
                return ans, "llm"
        return self.rule_answer(ranked), "rule"

    # ------------------------------------------------------------ requests to the Parent Agent

    async def request_investigation(self, **fields: Any) -> str:
        request_id = uuid.uuid4().hex[:12]
        if self.replay:                           # no Parent Agent: open what the recording investigated
            entity = fields.get("node") or (fields.get("alarm") or {}).get("entity") or ""
            node = self.node_of(entity) if entity else None
            for inv in self.store.list_investigations(limit=50):
                text = " ".join([inv.conclusion.likely_cause, inv.trigger.requested_by or "",
                                 (inv.trigger.alarm.entity if inv.trigger.alarm else "") or ""])
                if node is None or node in text or entity in text:
                    self.store.kv_put(f"request:{request_id}", {"investigation_id": inv.id})
                    break
            return request_id
        if self.bus:
            await self.bus.publish(streams.SRE, {"type": "investigate", "request_id": request_id, **fields})
        return request_id


def create_app(settings: Settings, bus: Bus | None, copilot: Copilot | None = None) -> FastAPI:
    cp = copilot or Copilot(settings, bus)
    app = FastAPI(title="WatchTower Copilot", docs_url=None, redoc_url=None, openapi_url=None)
    app.mount("/static", StaticFiles(directory=HERE / "static"), name="static")
    tpl = Jinja2Templates(directory=HERE / "templates")
    tpl.env.filters.update(hhmm=hhmm, ago=ago, code=code_spans)
    tpl.env.globals.update(TAGS=TAGS, profile=settings.profile.name, model=settings.llm_model)

    async def common(request: Request, subtitle: str) -> dict[str, Any]:
        alarms = await cp.alarms()
        return {"request": request, "subtitle": subtitle, "llm": await cp.llm_status(),
                "splunk_ok": alarms["splunk_ok"], "solarwinds_ok": alarms["solarwinds_ok"],
                "now": datetime.fromtimestamp(cp.now(), UTC).strftime("%H:%M:%S"), "replay": cp.replay}

    # -------------------------------------------------------- proactive watch

    @app.get("/", response_class=HTMLResponse)
    async def watch(request: Request, explain: str = "", ask: str = ""):
        """?explain=WI-0005 and ?ask=… open the page with that answer loaded (shareable links)."""
        return tpl.TemplateResponse(request, "watch.html", {**await common(request, "Proactive watch"),
                                                            "explain": explain, "ask": ask})

    @app.get("/partials/alarms", response_class=HTMLResponse)
    async def p_alarms(request: Request):
        alarms = await cp.alarms()
        items = cp.watch_items()
        port_util = None
        top = next((i for i in items if i.port), None)
        if top:
            pts = cp.series("util", f"{top.switch}:{top.port}", 10)
            port_util = (top.port, pts[-1][1]) if pts else None
        return tpl.TemplateResponse(request, "partials/alarms.html", {"a": alarms, "port_util": port_util,
                                                                      "nodes": cp.topology.nodes()})

    @app.get("/partials/agents", response_class=HTMLResponse)
    async def p_agents(request: Request):
        return tpl.TemplateResponse(request, "partials/agents.html", {"agents": await cp.agents()})

    @app.get("/partials/watch", response_class=HTMLResponse)
    async def p_watch(request: Request):
        items = cp.watch_items()
        return tpl.TemplateResponse(request, "partials/watch.html",
                                    {"items": items, "cards": {i.id: cp.cards(i) for i in items}})

    @app.get("/partials/next", response_class=HTMLResponse)
    async def p_next(request: Request):
        items = [i for i in cp.watch_items() if i.status != "closed"]
        return tpl.TemplateResponse(request, "partials/next.html", {"item": items[0] if items else None})

    @app.get("/api/timeline")
    async def timeline(item: str | None = None):
        """Signals over the last hour for the top watch item (or every node and cabled port)."""
        items = cp.watch_items()
        it = next((i for i in items if i.id == item), items[0] if items else None)
        series = []
        if it and it.node:
            series.append(("i/o timeouts per 5 min (Splunk)", "svc_timeouts", it.node, "#1F4E6B"))
            series.append(("lease failures per 5 min (Splunk)", "lease_failures", it.node, "#0E9F8E"))
        if it and it.port:
            series.append((f"output drops per poll on {it.port} (SolarWinds)", "out_discards", f"{it.switch}:{it.port}", "#E89B2C"))
        if not series:
            palette = ["#1F4E6B", "#0E9F8E", "#E89B2C", "#D9534F", "#7A5BA6"]
            for i, name in enumerate(cp.topology.nodes()):
                series.append((f"i/o timeouts {name} (Splunk)", "svc_timeouts", name, palette[i % len(palette)]))
        return {"datasets": [{"label": lbl, "color": color, "axis": "y2" if m == "out_discards" else "y",
                              "points": [{"x": int(t * 1000), "y": v} for t, v in cp.series(m, e)]}
                             for lbl, m, e, color in series]}

    @app.post("/watch/{item_id}/explain", response_class=HTMLResponse)
    async def explain(request: Request, item_id: str):
        raw = cp.store.get_watch_item(item_id)
        if not raw:
            return HTMLResponse("<div class='muted small'>Watch item not found.</div>")
        item = WatchItem.model_validate(raw)
        by, text = "rule", None
        if cp.llm:
            res = await jobs.summarise_watch(cp.llm, watch_item_text(item))
            if res:
                by, text = "llm", res.summary
        if text is None:
            text = f"{item.title}. {item.correlation} {item.impact}".strip()
        return tpl.TemplateResponse(request, "partials/explained.html",
                                    {"text": text, "by": by, "ids": [s.id for s in item.signals]})

    @app.post("/watch/{item_id}/dismiss", response_class=HTMLResponse)
    async def dismiss(item_id: str):
        if cp.bus:
            await cp.bus.publish(streams.SRE, {"type": "dismiss", "watch_item_id": item_id})
        return HTMLResponse("<div class='muted small'>Marked not relevant. WatchTower stops tracking this item.</div>")

    # -------------------------------------------------------- investigate

    @app.post("/investigate")
    async def investigate(request: Request, node: str = Form(""), switch: str = Form(""), port: str = Form(""),
                          watch_item_id: str = Form(""), alarm_tool: str = Form(""), alarm_name: str = Form(""),
                          alarm_entity: str = Form(""), alarm_fired_at: str = Form(""), force: str = Form("")):
        alarm = ({"tool": alarm_tool, "name": alarm_name, "entity": alarm_entity, "fired_at": alarm_fired_at}
                 if alarm_name else None)
        rid = await cp.request_investigation(
            node=node or None, switch=switch or None, port=port or None, watch_item_id=watch_item_id or None,
            alarm=alarm, force=bool(force),
            requested_by=f"SRE via Copilot, {datetime.fromtimestamp(cp.now(), UTC).strftime('%H:%M')} UTC"
                         + (f': "{alarm_tool.title()} alert {alarm_name} on {alarm_entity}. Investigate."' if alarm else ""))
        target = f"/pending/{rid}"
        if request.headers.get("hx-request"):
            return Response(headers={"HX-Redirect": target})
        return RedirectResponse(target, status_code=303)

    @app.get("/pending/{rid}", response_class=HTMLResponse)
    async def pending(request: Request, rid: str):
        found = cp.store.kv_get(f"request:{rid}")
        if found:
            return RedirectResponse(f"/investigations/{found['investigation_id']}", status_code=303)
        return tpl.TemplateResponse(request, "pending.html", {**await common(request, "Investigation"), "rid": rid})

    @app.get("/api/request/{rid}")
    async def request_status(rid: str):
        return cp.store.kv_get(f"request:{rid}") or {}

    @app.get("/investigations", response_class=HTMLResponse)
    async def investigations(request: Request):
        invs = cp.store.list_investigations(limit=30)
        return tpl.TemplateResponse(request, "list.html", {**await common(request, "Investigations"), "invs": invs})

    def inv_context(inv_id: str) -> dict[str, Any]:
        inv = cp.investigation(inv_id)
        progress = cp.store.kv_get(f"progress:{inv_id}", {"status": "unknown", "steps": []})
        ctx: dict[str, Any] = {"inv_id": inv_id, "inv": inv, "progress": progress,
                               "evidence": inv.evidence if inv else cp.store.evidence_for(inv_id),
                               "timeline": cp.store.kv_get(f"timeline:{inv_id}", [])}
        if inv:
            ev = {e.id: e for e in inv.evidence}
            ctx["ev"] = ev
            ctx["domains"] = sorted({e.domain for e in inv.evidence})
            ctx["sources"] = sorted({TAGS[e.source][0].title().replace("Solarwinds", "SolarWinds")
                                     for e in inv.evidence if not e.source.startswith("probe")})
            playbook_hyps = {h.id: h for h in inv.hypotheses}
            ctx["bars"] = playbook_hyps
        return ctx

    @app.get("/investigations/{inv_id}", response_class=HTMLResponse)
    async def investigation(request: Request, inv_id: str, ask: str = ""):
        return tpl.TemplateResponse(request, "investigation.html",
                                    {**await common(request, f"Investigation {inv_id}"), **inv_context(inv_id),
                                     "ask": ask})

    @app.get("/partials/investigation/{inv_id}", response_class=HTMLResponse)
    async def p_investigation(request: Request, inv_id: str):
        ctx = inv_context(inv_id)
        if ctx["inv"] is not None:                  # finished: reload the whole page into the report
            return Response(headers={"HX-Refresh": "true"})
        return tpl.TemplateResponse(request, "partials/progress.html", ctx)

    @app.get("/investigations/{inv_id}/note", response_class=Response)
    async def note(inv_id: str):
        inv = cp.investigation(inv_id)
        return Response(inv.escalation.note_markdown if inv and inv.escalation.note_markdown else "",
                        media_type="text/markdown")

    @app.get("/api/investigations/{inv_id}")
    async def inv_json(inv_id: str):
        inv = cp.investigation(inv_id)
        return JSONResponse(inv.model_dump(mode="json", exclude_none=True) if inv else
                            {"progress": cp.store.kv_get(f"progress:{inv_id}")})

    # -------------------------------------------------------- ask

    @app.post("/ask", response_class=HTMLResponse)
    async def ask(request: Request, question: str = Form(...), inv_id: str = Form("")):
        q = question.strip()
        m = re.match(r"^investigate\s+(\S+)", q, re.I)
        if m:
            target = m.group(1)
            fields: dict[str, Any] = {}
            try:
                fields["node"] = cp.topology.node_name(target)
            except KeyError:
                sw, _, port = target.partition(":")
                fields.update(switch=sw, port=port) if port else fields.update(node=target)
            rid = await cp.request_investigation(**fields, requested_by=f"SRE via Ask: {q}"[:200])
            return Response(headers={"HX-Redirect": f"/pending/{rid}"})
        ans, by = await cp.ask(q, inv_id or None)
        return tpl.TemplateResponse(request, "partials/answer.html", {"q": q, "a": ans, "by": by, "inv_id": inv_id})

    @app.get("/api/alarms")
    async def api_alarms():
        return await cp.alarms()

    @app.get("/api/agents")
    async def api_agents():
        return await cp.agents()

    @app.get("/api/samples")
    async def api_samples(since: float = 0):
        """Metric samples written after `since` (epoch), for make record."""
        return [list(r) for r in cp.store.samples_since(since)]

    @app.get("/api/now")
    async def api_now():
        return {"now": cp.now(), "replay": cp.replay}

    @app.get("/health")
    async def health():
        return {"agent": "copilot", "status": "ok", "replay": bool(cp.replay)}

    return app
