"""Record and replay: events captured from the bus and the stand-ins are replayed into a fresh
Redis, projected into a new database, and the Copilot (replay mode: recording clock, no LLM, no
Splunk/SolarWinds) shows the same watch item and report."""

import gzip
import json
from datetime import UTC, datetime, timedelta
from pathlib import Path

import fakeredis.aioredis
import httpx
import pytest

from tests.test_correlator import corr, sig
from tests.test_investigation import LITE, investigator, load_case
from wt.agents.parent.investigator import Request
from wt.common import bus as streams
from wt.common.bus import Bus
from wt.common.config import load_settings
from wt.common.contracts import dump
from wt.common.store import Store
from wt.copilot import app as copilot_app
from wt.copilot.app import Copilot, create_app
from wt.replay import player as player_mod
from wt.replay import recorder as recorder_mod
from wt.replay.player import Player


@pytest.fixture
def recording(tmp_path, monkeypatch, store):
    """A recording made the way the recorder writes it, from a real investigation and watch item."""
    monkeypatch.setattr(recorder_mod, "RECORDINGS", tmp_path)
    monkeypatch.setattr(player_mod, "RECORDINGS", tmp_path)
    t0 = datetime(2026, 10, 11, 2, 30, tzinfo=UTC)
    c = corr()
    s1 = sig("ocp.api_svc_timeouts", "node", "worker-1", "openshift", t0)
    s2 = sig("net.output_drops_rising", "interface", "cisco-sim-1:Gi1/0/13", "network", t0 + timedelta(seconds=90),
             tool="solarwinds")
    c.add(s1)
    item = c.add(s2)
    case = load_case(LITE)
    events = []

    async def capture(event):
        events.append({"t": t0.timestamp() + 600 + len(events), "kind": "bus", "stream": streams.INVESTIGATIONS,
                       "data": event})
    inv_ = investigator(case, store)
    inv_.publish = capture                       # what the Parent Agent publishes on wt.investigations
    return {"dir": tmp_path, "t0": t0, "item": item, "signals": [s1, s2], "case": case, "inv_": inv_,
            "events": events, "capture": capture}


async def write_recording(rec):
    inv = await rec["inv_"].investigate(Request(**rec["case"]["request"]))
    t0 = rec["t0"].timestamp()
    ev = [{"t": t0, "kind": "alarms", "data": {"splunk": [], "solarwinds": [], "splunk_ok": True,
                                                 "solarwinds_ok": True, "splunk_last": None}},
          {"t": t0, "kind": "agents", "data": [{"agent": "network-agent", "status": "ok", "describe": "1 switch",
                                                "signals_last_hour": 1, "active_rules": ["net.output_drops_rising"]}]},
          {"t": t0 + 1, "kind": "samples", "data": [["svc_timeouts", "worker-1", t0 - 60 * i, float(i)] for i in range(5)]}]
    for s in rec["signals"]:
        ev.append({"t": s.ts.timestamp(), "kind": "bus", "stream": streams.SIGNALS, "data": dump(s)})
    ev.append({"t": t0 + 100, "kind": "bus", "stream": streams.WATCH, "data": rec["item"].model_dump(mode="json")})
    ev += rec["events"]
    ev.append({"t": t0 + 700, "kind": "alarms", "data": {
        "splunk": [{"savedsearch_name": "NodeNotReady", "trigger_time": t0 + 650, "entity": "ocp-sim-worker",
                    "node": "ocp-sim-worker"}], "solarwinds": [], "splunk_ok": True, "solarwinds_ok": True,
        "splunk_last": t0 + 650}})
    d = rec["dir"] / "rehearsal"
    d.mkdir()
    with gzip.open(d / "events.jsonl.gz", "wt") as f:
        for e in ev:
            f.write(json.dumps(e) + "\n")
    (d / "meta.json").write_text(json.dumps({"name": "rehearsal", "started": t0, "ended": t0 + 700}))
    return inv


async def test_replay_reproduces_both_screens(recording, tmp_path, monkeypatch):
    live_inv = await write_recording(recording)
    replay_store = Store(tmp_path / "replay.db")
    bus = Bus(fakeredis.aioredis.FakeRedis(decode_responses=True))
    player = Player("rehearsal", replay_store, bus, speed=0)          # jump to the end state
    await player.fresh()
    await player.play()
    assert await player.drain() > 0                 # read back out of the replay Redis into the new database

    replay = {"name": "rehearsal", "speed": 10, "recorded": "recorded"}
    cp = Copilot(load_settings(), bus, store=replay_store, llm=None, agent_urls=[], replay=replay)
    app = create_app(load_settings(), bus, cp)
    try:
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://c") as http:
            page = (await http.get("/")).text
            assert "REPLAY · recording <b>rehearsal</b>" in page and "replay" in page
            watch = (await http.get("/partials/watch")).text
            assert recording["item"].title in watch and "EMERGING · cross-domain" in watch
            assert "<svg class=\"spark\"" in watch                          # samples came from the recording
            alarms = (await http.get("/partials/alarms")).text
            assert 'data-alarm="NodeNotReady"' in alarms and "50 s ago" in alarms   # recording clock, not wall clock
            agents = (await http.get("/partials/agents")).text
            assert "Network Agent" in agents
            report = (await http.get(f"/investigations/{live_inv.id}")).text
            assert live_inv.conclusion.likely_cause.split(",")[0] in report.replace("&#39;", "'")
            assert "Network team · P3" in report and all(f'id="{e.id}"' in report for e in live_inv.evidence)
            # the replayed report is the recorded document, field for field
            assert replay_store.get_investigation(live_inv.id) == live_inv
            # Investigate on the replayed alarm opens the recorded investigation (no Parent Agent running)
            r = await http.post("/investigate", data={"alarm_tool": "splunk", "alarm_name": "NodeNotReady",
                                                      "alarm_entity": "ocp-sim-worker"})
            rid = r.headers["location"].rsplit("/", 1)[-1]
            assert (await http.get(f"/api/request/{rid}")).json() == {"investigation_id": live_inv.id}
            ans = (await http.post("/ask", data={"question": "did worker-1 reboot?", "inv_id": live_inv.id})).text
            assert "No Rebooted or Starting events" in ans
    finally:
        copilot_app.CLOCK.store = None


async def test_recorder_writes_bus_events_with_their_publish_time(tmp_path, monkeypatch):
    monkeypatch.setattr(recorder_mod, "RECORDINGS", tmp_path)
    fake = fakeredis.aioredis.FakeRedis(decode_responses=True)
    rec = recorder_mod.Recorder("x", poll_s=0.05)
    rec.redis = fake
    rec.http = httpx.AsyncClient(transport=httpx.MockTransport(
        lambda r: httpx.Response(200, json=[["svc_timeouts", "worker-1", 1.0, 2.0]] if r.url.path == "/api/samples"
                                 else {"splunk": []})), base_url="http://c")
    import asyncio
    task = asyncio.create_task(rec.run(duration_s=0.6))
    await asyncio.sleep(0.2)
    await Bus(fake).publish(streams.SIGNALS, dump(sig("ocp.api_svc_timeouts", "node", "worker-1", "openshift",
                                                      datetime.now(UTC))))
    meta = await task
    lines = [json.loads(x) for x in gzip.open(tmp_path / "x" / "events.jsonl.gz", "rt")]
    assert meta["events"]["wt.signals"] == 1 and {"alarms", "agents", "samples"} <= set(meta["events"])
    bus_ev = next(e for e in lines if e["kind"] == "bus")
    assert bus_ev["stream"] == "wt.signals" and abs(bus_ev["t"] - datetime.now(UTC).timestamp()) < 5
