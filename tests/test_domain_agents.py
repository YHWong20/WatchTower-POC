"""Domain agents on recorded-shape Splunk/SolarWinds responses: each watch rule with a hit, a
miss and a stale-data case, and the evidence handlers (reuse first, probe only when live)."""

import json
from pathlib import Path
from datetime import UTC, datetime

import pytest

from tests.agent_fixtures import context, iso, record, splunk_time
from wt.agents.base import Task
from wt.agents.network.agent import NetworkAgent, parse_queue_stats, streak
from wt.agents.openshift.agent import OpenShiftAgent
from wt.common.catalog import load_sources
from wt.common.probes.base import ProbeResult

NOW = datetime(2026, 10, 10, 13, 0, tzinfo=UTC).timestamp()
SOURCES = load_sources()


def err_rows(port_drops: dict[str, list[int]], age_s: float = 30, crc: dict[str, list[int]] | None = None):
    """watch_interface_errors rows: one per poll per port, newest poll age_s ago."""
    rows = []
    for port, drops in port_drops.items():
        n = len(drops)
        for i, d in enumerate(drops):
            t = NOW - age_s - (n - 1 - i) * 60
            rows.append({"InterfaceID": 100 + int(port.split("/")[-1]), "Port": port, "Switch": "cisco-sim-1",
                         "DateTime": iso(t), "InDiscards": 0, "OutDiscards": d,
                         "InErrors": (crc or {}).get(port, [0] * n)[i], "OutErrors": 0})
    return sorted(rows, key=lambda r: r["DateTime"])


def traffic_rows(util: dict[str, float], age_s: float = 30):
    return [{"InterfaceID": 1, "Port": p, "Switch": "cisco-sim-1", "DateTime": iso(NOW - age_s),
             "InPercentUtil": u / 2, "OutPercentUtil": u} for p, u in util.items()]


def net_agent(errors, traffic, links=(), now=NOW):
    recs = [record(SOURCES, "watch_interface_errors", errors, minutes=10),
            record(SOURCES, "watch_interface_traffic", traffic, minutes=10),
            record(SOURCES, "watch_link_events", list(links), window="10m")]
    return NetworkAgent(context(recs, now), ssh=object())


# ---------------------------------------------------------------- Network Agent watch rules


async def test_output_drops_rising_hit():
    agent = net_agent(err_rows({"Gi1/0/13": [0, 120, 410, 760], "Gi1/0/10": [0, 0, 0, 0]}), traffic_rows({"Gi1/0/13": 46}))
    sigs = await agent.watch_once(NOW)
    assert [(s.rule, s.entity.name) for s in sigs] == [("net.output_drops_rising", "cisco-sim-1:Gi1/0/13")]
    s = sigs[0]
    assert s.value == 3 and s.severity == "weak" and s.source.tool == "solarwinds" and "SELECT" in s.source.query
    assert "rising 3 polls in a row (760 in the last poll); utilisation average 46%; feeds worker-1" in s.summary


async def test_output_drops_miss_when_the_streak_is_broken():
    agent = net_agent(err_rows({"Gi1/0/13": [200, 300, 0, 50]}), traffic_rows({"Gi1/0/13": 20}))
    assert await agent.watch_once(NOW) == []


async def test_stale_solarwinds_data_runs_no_metric_rule():
    agent = net_agent(err_rows({"Gi1/0/13": [100, 200, 300, 400]}, age_s=400), traffic_rows({"Gi1/0/13": 46}, age_s=400))
    assert await agent.watch_once(NOW) == []
    assert "newest poll 400 s old" in agent.status["stale"][0]


async def test_errors_rising_and_link_flap():
    links = [{"_time": splunk_time(NOW - 100 * i), "host": "cisco-sim-1",
              "_raw": f"00004{i}: cisco-sim-1: *Oct 10 12:5{i}:00.000 UTC: %LINK-3-UPDOWN: Interface "
                      f"GigabitEthernet1/0/13, changed state to {'down' if i % 2 else 'up'}"} for i in range(1, 3)]
    agent = net_agent(err_rows({"Gi1/0/13": [0, 0, 0]}, crc={"Gi1/0/13": [0, 5, 9]}), traffic_rows({}), links)
    rules = sorted((s.rule, s.entity.name) for s in await agent.watch_once(NOW))
    assert rules == [("net.errors_rising", "cisco-sim-1:Gi1/0/13"), ("net.link_flap", "cisco-sim-1:Gi1/0/13")]
    one = net_agent(err_rows({"Gi1/0/13": [0, 0, 0]}), traffic_rows({}), links[:1])
    assert await one.watch_once(NOW) == []                  # a single link change is not a flap


async def test_utilisation_vs_baseline_needs_history():
    agent = net_agent(err_rows({"Gi1/0/13": [0, 0, 0]}), traffic_rows({"Gi1/0/13": 85}))
    assert await agent.watch_once(NOW) == []                # no baseline yet
    for _ in range(30):
        agent.baselines.add("util:cisco-sim-1:Gi1/0/13", NOW, 30)
    sigs = await agent.watch_once(NOW)
    assert [s.rule for s in sigs] == ["net.util_vs_baseline"] and sigs[0].baseline == 30


def test_streak_and_queue_parser(tmp_path):
    assert streak([0, 1, 2, 3]) == 3 and streak([1, 0]) == 0 and streak([]) == 0
    text = Path("eval/fixtures/cli/show_qos_queue_stats_gi1-0-13.txt").read_text()
    assert parse_queue_stats(text) == {0: 0, 1: 18422, 2: 0}


async def test_repeat_suppression():
    agent = net_agent(err_rows({"Gi1/0/13": [1, 2, 3]}), traffic_rows({}))
    sigs = await agent.watch_once(NOW)
    assert len(await agent.publish(sigs, NOW)) == 1
    assert await agent.publish(sigs, NOW + 60) == []        # same hit within repeat_s
    assert len(await agent.publish(sigs, NOW + 130)) == 1


# ---------------------------------------------------------------- OpenShift Agent watch rules


def ocp_agent(calls, lease=(), etcd=(), events=(), now=NOW):
    recs = [record(SOURCES, "watch_api_svc_calls", list(calls), window="5m"),
            record(SOURCES, "watch_lease_failures", list(lease), window="5m"),
            record(SOURCES, "watch_etcd_warnings", list(etcd), window="5m"),
            record(SOURCES, "watch_node_events", list(events), window="5m")]
    return OpenShiftAgent(context(recs, now))


def calls(host, ok, failed):
    return [{"host": host, "ok": "true", "count": str(ok)}] + ([{"host": host, "ok": "false", "count": str(failed)}]
                                                               if failed else [])


async def test_api_svc_timeouts_hit_names_the_node():
    agent = ocp_agent(calls("ocp-sim-worker", 290, 9) + calls("ocp-sim-control-plane", 300, 0))
    sigs = await agent.watch_once(NOW)
    assert [(s.rule, s.entity.name) for s in sigs] == [("ocp.api_svc_timeouts", "worker-1")]
    assert sigs[0].value == 9 and sigs[0].baseline == 0 and sigs[0].source.tool == "splunk"
    assert "9 × i/o timeout" in sigs[0].summary


async def test_api_svc_timeouts_miss_and_baseline():
    agent = ocp_agent(calls("ocp-sim-worker", 298, 2) + calls("ocp-sim-control-plane", 300, 0))
    assert await agent.watch_once(NOW) == []                # 2 <= baseline 0 + 2
    for _ in range(20):
        agent.baselines.add("svc_timeouts:ocp-sim-worker", NOW, 8)
    noisy = ocp_agent(calls("ocp-sim-worker", 290, 9) + calls("ocp-sim-control-plane", 300, 0))
    noisy.ctx.store = agent.ctx.store
    noisy.baselines = agent.baselines
    assert await noisy.watch_once(NOW) == []                # 9 <= baseline 8 + 2: normal for this node


async def test_stale_node_is_reported_and_not_judged():
    agent = ocp_agent(calls("ocp-sim-control-plane", 300, 0))
    assert await agent.watch_once(NOW) == []
    assert agent.status["stale"] == ["Splunk: no svc-prober lines from worker-1 in 5m"]


async def test_lease_etcd_and_node_not_ready():
    events = [{"_time": splunk_time(NOW - 40), "involvedObject.name": "ocp-sim-worker", "reason": "NodeNotReady",
               "message": "Node ocp-sim-worker status is now: NodeNotReady"},
              {"_time": splunk_time(NOW - 5), "involvedObject.name": "ocp-sim-worker", "reason": "NodeReady",
               "message": "Node ocp-sim-worker status is now: NodeReady"}]
    agent = ocp_agent(calls("ocp-sim-worker", 300, 0) + calls("ocp-sim-control-plane", 300, 0),
                      lease=[{"host": "ocp-sim-worker", "count": "3"}],
                      etcd=[{"host": "ocp-sim-control-plane", "count": "4"}], events=events)
    sigs = {s.rule: s for s in await agent.watch_once(NOW)}
    assert set(sigs) == {"ocp.lease_update_failures", "ocp.node_not_ready"}     # 4 etcd warnings: under threshold
    assert sigs["ocp.node_not_ready"].severity == "state_change" and sigs["ocp.node_not_ready"].entity.name == "worker-1"
    sent = await agent.publish(list(sigs.values()), NOW)
    assert len(await agent.publish(list(sigs.values()), NOW + 300)) == 1     # the state change is sent once only


# ---------------------------------------------------------------- evidence on request


def task(signal, to="openshift", **params):
    return Task(investigation_id="INV-0001", to=to, signal=signal, params=params)


async def test_flap_facts_from_splunk_events():
    rows = [{"_time": splunk_time(NOW - 100), "reason": "NodeNotReady", "message": "x"},
            {"_time": splunk_time(NOW - 44), "reason": "NodeReady", "message": "y"}]
    recs = [record(SOURCES, "node_ready_events", rows, node="ocp-sim-worker", window="30m")]
    agent = OpenShiftAgent(context(recs, NOW))
    res = await agent.handle(task("node_ready_events", node="ocp-sim-worker", window="30m"))
    assert res.ok and res.source == "splunk" and res.facts["gap_s"] == 56 and res.facts["flaps"] == 1
    assert "NotReady for 56 s (marked after 40 s without a lease renewal), below the 300 s eviction threshold" in res.finding


async def test_reboot_check_answers_from_splunk_only():
    q2 = SOURCES.signals["node_reboot_check"].model_extra["corroborate"]
    recs = [record(SOURCES, "node_reboot_check", [], node="ocp-sim-worker", window="30m"),
            {**record(SOURCES, "node_reboot_check", [], node="ocp-sim-worker", window="30m"),
             "query": q2.format(node="ocp-sim-worker", window="30m")}]
    agent = OpenShiftAgent(context(recs, NOW), k8s=object())
    res = await agent.handle(task("node_reboot_check", node="ocp-sim-worker", window="30m"))
    assert res.source == "splunk" and res.facts["rebooted"] is False and "no reboot" in res.finding
    assert agent.status["probes"] == 0


async def test_api_calls_partial_failure_facts():
    rows = [{"_time": splunk_time(NOW - 60 * (5 - i)), "true": str(ok), "false": str(bad)}
            for i, (ok, bad) in enumerate([(60, 0), (58, 2), (0, 60), (50, 10), (60, 0)])]
    recs = [record(SOURCES, "api_svc_calls", rows, node="ocp-sim-worker", window="30m")]
    res = await OpenShiftAgent(context(recs, NOW)).handle(task("api_svc_calls", node="ocp-sim-worker", window="30m"))
    assert res.facts["failed"] == 72 and res.facts["minutes_affected"] == 3
    assert res.facts["success_rate_affected"] == pytest.approx(108 / 180, abs=0.001)


async def test_interface_discards_and_queue_probe():
    lookup = record(SOURCES, "interface_lookup", [{"InterfaceID": 113, "Name": "Gi1/0/13", "Status": 1}],
                    switch="cisco-sim-1", port="Gi1/0/13")
    rows = [{"DateTime": iso(NOW - 60 * (6 - i)), "InDiscards": 0, "OutDiscards": d, "InErrors": 0, "OutErrors": 0}
            for i, d in enumerate([0, 120, 410, 760, 1190, 1505])]
    disc = record(SOURCES, "interface_discards", rows, if_id=113, minutes=30)

    class FakeSsh:
        calls = []

        async def run(self, host, command):
            self.calls.append((host, command))
            return ProbeResult(tool="probe_cli", target=host, query=command,
                               output=Path("eval/fixtures/cli/show_qos_queue_stats_gi1-0-13.txt").read_text())

    ssh = FakeSsh()
    agent = NetworkAgent(context([lookup, disc], NOW), ssh=ssh)
    res = await agent.handle(task("interface_discards", "network", switch="cisco-sim-1", port="Gi1/0/13", minutes=30))
    assert res.source == "solarwinds" and res.facts["rising_polls"] == 5 and res.facts["latest_two_rising"]
    assert res.facts["errors_flat"] and not res.facts["stale"] and ssh.calls == []      # fresh: no probe
    q = await agent.handle(Task(investigation_id="INV-0001", to="network", signal="queue_drops", needs_live=True,
                                params={"switch": "cisco-sim-1", "port": "Gi1/0/13"}))
    assert q.source == "probe_cli" and q.reason_for_probe and q.facts["top_share"] == 1.0
    assert ssh.calls == [("cisco-sim-1:22", "show platform hardware fed switch active qos queue stats interface Gi1/0/13")]


async def test_unknown_evidence_is_a_failed_result_not_a_crash():
    res = await OpenShiftAgent(context([], NOW)).handle(task("bogus", node="x"))
    assert not res.ok and "bogus" in res.error


# ---------------------------------------------------------------- the recorded phase A run (lab/verify_watch.py)

RECORDED = "eval/fixtures/recorded/phase4-watch-lite.jsonl"
FAULT_START = 1791642267            # cp-congest.sh phase A began (data/faults/last.jsonl of that run)


async def test_recorded_phase_a_raises_both_signals_from_reuse_only():
    """Replays the agents' real Splunk/SolarWinds exchanges from the phase 4 verification run
    (lite lab, ports trimmed to the cabled ones): both watch signals, no probe, within 5 min."""
    from tests.agent_fixtures import recorded
    recs = recorded(Path(RECORDED))
    seen: dict[str, float] = {}
    for agent_cls, first_query in ((OpenShiftAgent, "index=ocp sourcetype=kube:container:svc-prober"),
                                   (NetworkAgent, "SELECT E.InterfaceID")):
        cycles = [r["at"] for r in recs if r["query"].startswith(first_query)]
        clock = {"t": cycles[0]}
        ctx = context(recs, cycles[0], strict=True)
        ctx.clock = lambda: clock["t"]
        agent = agent_cls(ctx, ssh=object()) if agent_cls is NetworkAgent else agent_cls(ctx)
        for at in cycles:
            clock["t"] = at
            for s in await agent.watch_once(at):
                assert s.source.tool in ("splunk", "solarwinds")
                if at >= FAULT_START:      # the recording starts 5 min earlier, while an aborted run's timeouts aged out
                    seen.setdefault(f"{s.rule} {s.entity.name}", at)
        assert ctx.splunk.http._transport.misses == [] and ctx.solarwinds.http._transport.misses == []
    assert "ocp.api_svc_timeouts worker-1" in seen and "net.output_drops_rising cisco-sim-1:Gi1/0/13" in seen
    for key in ("ocp.api_svc_timeouts worker-1", "net.output_drops_rising cisco-sim-1:Gi1/0/13"):
        assert 0 < seen[key] - FAULT_START <= 300, (key, seen[key] - FAULT_START)
