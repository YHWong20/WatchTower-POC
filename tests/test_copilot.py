"""Copilot: both screens render from stored investigations and watch items, Investigate goes to
the Parent Agent over wt.sre, Ask answers only from stored evidence (citing ids) and refuses
otherwise, and nothing in the UI can change the infrastructure."""

import json
from datetime import UTC, datetime
from pathlib import Path

import httpx
import pytest

from tests.test_correlator import corr, sig
from tests.test_investigation import investigator, load_case
from wt.agents.parent.investigator import Request
from wt.common.config import load_settings
from wt.copilot.app import Copilot, create_app

CASE = Path("eval/cases/lite-worker1-flap/case.yaml")


class FakeBus:
    def __init__(self):
        self.sent = []

    async def publish(self, stream, payload):
        self.sent.append((stream, payload))


@pytest.fixture
async def ui(store):
    inv = await investigator(load_case(CASE), store).investigate(Request(**load_case(CASE)["request"]))
    c = corr()
    now = datetime.now(UTC)
    c.add(sig("ocp.api_svc_timeouts", "node", "worker-1", "openshift", now))
    item = c.add(sig("net.output_drops_rising", "interface", "cisco-sim-1:Gi1/0/13", "network", now, tool="solarwinds"))
    store.save_watch_item(item.model_dump(mode="json"))
    store.add_samples([("svc_timeouts", "worker-1", now.timestamp() - 60 * i, float(i)) for i in range(10)])

    def tools(request: httpx.Request) -> httpx.Response:
        if request.url.path.endswith("/fired_alerts/-"):
            return httpx.Response(200, json={"entry": [{"name": "sid1", "content": {
                "savedsearch_name": "NodeNotReady", "trigger_time": int(now.timestamp()) - 120,
                "entity": "demo-web-1, ocp-sim-worker", "trigger_time_rendered": "x"}}]})
        if request.url.path.endswith("/Json/Query"):
            return httpx.Response(200, json={"results": []})
        return httpx.Response(200, json={"status": "ok"})

    settings = load_settings()
    bus = FakeBus()
    cp = Copilot(settings, bus, store=store, llm=None, agent_urls=[])
    cp.llm = None
    for client in (cp.splunk, cp.solarwinds):
        client.http._transport = httpx.MockTransport(tools)
    cp.http = httpx.AsyncClient(transport=httpx.MockTransport(tools))
    app = create_app(settings, bus, cp)
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://copilot") as http:
        yield http, inv, item, bus


async def test_proactive_watch_screen(ui):
    http, inv, item, _ = ui
    page = await http.get("/")
    assert page.status_code == 200 and "Watch items" in page.text and "Read-only" in page.text
    watch = (await http.get("/partials/watch")).text
    assert "EMERGING · cross-domain" in watch and item.title in watch and "<svg class=\"spark\"" in watch
    assert "SPLUNK" in watch and "SOLARWINDS" in watch and "CORRELATION" in watch and "Explain in plain words" in watch
    alarms = (await http.get("/partials/alarms")).text
    assert "NodeNotReady" in alarms and 'name="alarm_entity" value="ocp-sim-worker"' in alarms   # the node, not the pod
    nxt = (await http.get("/partials/next")).text
    assert "queue stats interface Gi1/0/13" in nxt
    tl = (await http.get("/api/timeline")).json()
    assert any(d["points"] for d in tl["datasets"])


async def test_investigation_report(ui):
    http, inv, _, _ = ui
    page = (await http.get(f"/investigations/{inv.id}")).text
    for part in ("Likely cause:", "CONFIDENCE: HIGH", "Evidence chain", "PROBE · CLI", "Hypotheses",
                 "What happened", "Recommended next steps (read-only)", "Network team · P3", "Copy escalation note"):
        assert part in page, part
    assert all(f'id="{e.id}"' in page for e in inv.evidence)          # every item is on the page, linkable
    assert inv.evidence[0].query.split()[0] in page                     # with its exact query
    note = (await http.get(f"/investigations/{inv.id}/note")).text
    assert note.startswith(f"**WatchTower {inv.id}") and "Evidence:" in note


async def test_investigate_and_explain_go_through_watchtower_only(ui):
    http, inv, item, bus = ui
    r = await http.post("/investigate", data={"alarm_tool": "splunk", "alarm_name": "NodeNotReady",
                                              "alarm_entity": "ocp-sim-worker"}, headers={"hx-request": "true"})
    assert r.headers["HX-Redirect"].startswith("/pending/")
    stream, msg = bus.sent[-1]
    assert stream == "wt.sre" and msg["type"] == "investigate" and msg["alarm"]["entity"] == "ocp-sim-worker"
    ex = (await http.post(f"/watch/{item.id}/explain")).text
    assert "In plain words" in ex and "rule text" in ex                  # LLM off: rule fallback


@pytest.mark.parametrize("question,expect", [
    ("did worker-1 reboot?", "No Rebooted or Starting events"),
    ("why did only some calls fail?", "every call to the kubernetes Service leaves the node"),
    ("what is the BGP state on the core router?", "I don&#39;t have evidence for that."),
])
async def test_ask_answers_only_from_stored_evidence(ui, question, expect):
    http, inv, _, _ = ui
    r = (await http.post("/ask", data={"question": question, "inv_id": inv.id})).text
    assert expect in r
    if "evidence for that" not in r:
        assert "cites" in r and ("ev-" in r or "cor-" in r)


async def test_ask_investigate_command(ui):
    http, _, _, bus = ui
    r = await http.post("/ask", data={"question": "investigate worker-1"})
    assert r.headers["HX-Redirect"].startswith("/pending/") and bus.sent[-1][1]["node"] == "worker-1"


def test_no_route_can_change_the_infrastructure():
    """The only writes go to WatchTower's own bus (investigate, dismiss); no PUT/PATCH/DELETE."""
    app = create_app(load_settings(), FakeBus(), Copilot(load_settings(), FakeBus(), llm=None, agent_urls=[]))
    posts = sorted(r.path for r in app.routes if "POST" in getattr(r, "methods", set()))
    assert posts == ["/ask", "/investigate", "/watch/{item_id}/dismiss", "/watch/{item_id}/explain"]
    assert not [r for r in app.routes if getattr(r, "methods", set()) & {"PUT", "PATCH", "DELETE"}]


async def test_a_closed_cross_domain_item_shows_as_closed(ui, store):
    http, _, item, _ = ui
    store.save_watch_item(item.model_copy(update={"status": "closed"}).model_dump(mode="json"))
    watch = (await http.get("/partials/watch")).text
    assert "CLOSED" in watch and "EMERGING" not in watch


async def test_deep_links_load_an_explanation_or_answer(ui):
    http, inv, item, _ = ui
    page = (await http.get("/", params={"explain": item.id, "ask": "did it reboot?"})).text
    assert f'hx-post="/watch/{item.id}/explain" hx-trigger="load"' in page and "did it reboot?" in page
    report = (await http.get(f"/investigations/{inv.id}", params={"ask": "why only some calls?"})).text
    assert 'hx-trigger="load"' in report and "why only some calls?" in report
