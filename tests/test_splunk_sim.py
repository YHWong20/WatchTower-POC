"""splunk-sim: the SPL subset, HEC ingest, export search and scheduled alerts, driven through the
phase-1 SplunkClient so the adapter and the stand-in stay compatible."""

import json
import time
from datetime import UTC, datetime

import httpx
import pytest

from sims.forwarder import parse
from sims.splunk_sim import spl
from sims.splunk_sim.server import HecError, Scheduler, SavedSearch, create_app, load_saved_searches, parse_hec
from sims.splunk_sim.store import EventStore
from wt.common.sources.base import placeholders, render_template
from wt.common.sources.splunk import SplunkClient

NOW = datetime(2026, 10, 10, 13, 0, tzinfo=UTC).timestamp()
NODE = "ocp-sim-worker"
PARAMS = {"node": NODE, "window": "1h", "switch": "cisco-sim-1", "port": "Gi1/0/13"}


def k8s_event(reason, name=NODE, kind="Node", message="", t=NOW - 60, ns="", host="", uid=None):
    stamp = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(t))
    return {"metadata": {"name": f"{name}.{reason}", "namespace": ns or "default", "uid": uid or f"{name}-{reason}",
                         "resourceVersion": "1", "managedFields": [{"manager": "x"}]},
            "involvedObject": {"kind": kind, "name": name, "namespace": ns}, "reason": reason, "message": message,
            "source": {"component": "kubelet" if host else "node-controller", "host": host},
            "lastTimestamp": stamp, "type": "Normal"}


def hec_rows(events):
    return parse_hec("\n".join(json.dumps(e) for e in events), received=NOW)


@pytest.fixture
def sample():
    s = EventStore(":memory:")
    prober = lambda ok, t: {"node": NODE, "target": "https://10.96.0.1:443/readyz", "ok": ok, "ms": 2001 if not ok else 30,
                            "err": None if ok else "dial tcp 10.96.0.1:443: i/o timeout"}
    meta = parse.container_log_meta("svc-prober-abcde_wt-lab_svc-prober-" + "a" * 64 + ".log")
    events = [parse.container_event(NODE, "svc-prober.log", meta, int((NOW - 600 + 5 * i) * 1e9),
                                    json.dumps(prober(i % 3 != 0, i))) for i in range(100)]
    events.append(parse.journal_event({"MESSAGE": 'E1010 12:59:01.000000     231 controller.go:145] "Failed to update lease" '
                                                  'err="Put \\"https://ocp-sim-control-plane:6443/apis\\": context deadline exceeded"',
                                       "__REALTIME_TIMESTAMP": str(int((NOW - 59) * 1e6)), "_HOSTNAME": NODE}, NODE))
    events.append(parse.journal_event({"MESSAGE": "Started kubelet.service - kubelet: The Kubernetes Node Agent.",
                                       "__REALTIME_TIMESTAMP": str(int((NOW - 7200) * 1e6)), "_HOSTNAME": NODE}, NODE))
    events.append(parse.kube_event(k8s_event("NodeNotReady", message=f"Node {NODE} status is now: NodeNotReady")))
    events.append(parse.kube_event(k8s_event("NodeReady", t=NOW - 15, host=NODE)))
    events.append(parse.kube_event(k8s_event("TaintManagerEviction", name="demo-web-1", kind="Pod", ns="wt-lab",
                                             message="Cancelling deletion of Pod wt-lab/demo-web-1")))
    events.append(parse.kube_event(k8s_event("Started", name="kube-proxy-x", kind="Pod", ns="kube-system", host=NODE)))
    etcd = parse.container_log_meta("etcd-ocp-sim-control-plane_kube-system_etcd-" + "b" * 64 + ".log")
    events.append(parse.container_event("ocp-sim-control-plane", "etcd.log", etcd, int((NOW - 30) * 1e9),
                                        json.dumps({"level": "warn", "msg": "apply request took too long", "took": "1.2s"})))
    events.append(parse.syslog_event(b"<187>000042: cisco-sim-1: *Oct 10 12:58:00.123 UTC: %LINK-3-UPDOWN: "
                                     b"Interface GigabitEthernet1/0/13, changed state to down", "10.0.0.5", NOW - 100))
    s.add(hec_rows(events), indextime=NOW - 1)
    yield s
    s.close()


def search(sample, q, **kw):
    s = spl.parse(q if q.startswith("search") else "search " + q)
    t0, t1 = s.window(NOW)
    return spl.run(s, sample.candidates(t0, t1, s.prefilter), now=NOW, **kw)


# ---------------------------------------------------------------- SPL semantics


def spl_templates(sources):
    for key, sig in sources.signals.items():
        for attr in ("query", "prod_query", "corroborate"):
            q = getattr(sig, attr, None)
            if q and not q.startswith(("SELECT", "/")):
                yield key, attr, render_template(q, **{p: PARAMS[p] for p in placeholders(q)})


def test_every_spl_template_in_sources_runs(sources, sample):
    """Each Splunk query in contracts/sources.yaml parses and runs; those with evidence in the
    sample data return it."""
    expect_rows = {"api_svc_timeouts", "lease_update_failures", "node_ready_events", "pod_evictions",
                   "static_pod_restarts", "switch_syslog"}
    ran = set()
    for key, attr, q in spl_templates(sources):
        rows = search(sample, q)
        ran.add(key)
        if key in expect_rows and attr == "query":
            assert rows, f"{key} returned nothing"
    assert {"api_svc_timeouts", "etcd_latency", "ovn_errors", "node_pressure", "node_reboot_check"} <= ran


def test_timechart_counts_timeouts_per_minute_with_zero_buckets(sample):
    rows = search(sample, f'index=ocp sourcetype=kube:container:svc-prober host={NODE} "i/o timeout" earliest=-15m '
                         "| timechart span=1m count")
    assert len(rows) == 15 and all(set(r) == {"_time", "count"} for r in rows)
    assert sum(int(r["count"]) for r in rows) == 34           # every third call failed
    assert rows[0]["count"] == "0" and rows[0]["_time"].endswith("+00:00")


def test_terms_phrases_in_wildcards_and_fields(sample):
    assert len(search(sample, 'index=ocp sourcetype=kubelet ("failed to update lease" OR "error updating node status")')) == 1
    assert search(sample, "index=ocp sourcetype=kube:events reason IN (NodeNotReady, NodeReady) "
                         f"involvedObject.name={NODE}")[0]["reason"] == "NodeReady"   # newest first
    assert len(search(sample, "sourcetype=kube:container:svc* ok=false")) == 34
    assert len(search(sample, "sourcetype=kube:container:svc-prober ms>2000")) == 34
    assert len(search(sample, "sourcetype=kube:container:svc-prober NOT ok=false")) == 66
    assert search(sample, "index=network sourcetype=cisco:ios mnemonic=UPDOWN")[0]["facility"] == "LINK"
    assert not search(sample, "index=network Gi1/0/13")                  # whole tokens only, as in Splunk
    assert search(sample, 'index=network "%LINK"')
    rows = search(sample, 'index=ocp sourcetype=kubelet "lease" | table _time host err')
    assert rows[0]["host"] == NODE and "context deadline exceeded" in rows[0]["err"]


def test_indexed_fields_and_stats_by_two_fields(sample):
    rows = search(sample, f"index=ocp sourcetype=kube:events namespace IN (kube-system) source_host={NODE} "
                         "reason IN (Killing, Started) | stats count by involvedObject.name, reason")
    assert rows == [{"involvedObject.name": "kube-proxy-x", "reason": "Started", "count": "1"}]
    assert search(sample, "index=ocp sourcetype=kube:container:etcd | stats count by msg") == [
        {"msg": "apply request took too long", "count": "1"}]
    assert search(sample, "index=nothing | stats count") == [{"count": "0"}]


def test_time_modifiers(sample):
    assert len(search(sample, "index=ocp sourcetype=kubelet earliest=-1h")) == 1
    assert len(search(sample, "index=ocp sourcetype=kubelet earliest=-3h latest=-1h")) == 1
    assert spl.parse_time("-1h@h", NOW + 125) == NOW - 3600
    assert spl.parse_time("10/10/2026:13:00:00", 0) == NOW


def test_event_rows_drop_object_metadata(sample):
    row = search(sample, "sourcetype=kube:events reason=NodeNotReady")[0]
    assert row["involvedObject.name"] == NODE and row["sourcetype"] == "kube:events"
    assert not any(k.startswith("metadata.managedFields") for k in row) and "managedFields" not in row["_raw"]


@pytest.mark.parametrize("bad", ['index=ocp "unbalanced', "index=ocp | eval x=1", "index=ocp | stats foo(x)",
                                 "index=ocp earliest=-5parsecs", "| inputlookup x", "search"])
def test_unsupported_spl_is_an_error(bad):
    with pytest.raises(spl.SplError):
        spl.parse(bad).window(NOW)


# ---------------------------------------------------------------- HEC and the REST API


def test_hec_parsing_rules():
    rows = parse_hec('{"event": "a", "host": "h", "index": "ocp", "time": 5}{"event": {"x": 1}, "fields": {"k": ""}}')
    assert [r["raw"] for r in rows] == ["a", '{"x":1}'] and rows[1]["index"] == "main" and rows[1]["fields"] == {}
    for body, code in (("", 5), ("{not json", 6), ('{"host": "h"}', 12), ('{"event": ""}', 13)):
        with pytest.raises(HecError) as exc:
            parse_hec(body)
        assert exc.value.code == code


@pytest.fixture
def app_store():
    s = EventStore(":memory:")
    yield s
    s.close()


@pytest.fixture
def app(app_store):
    searches = load_saved_searches("sims/splunk_sim/saved_searches.yaml")
    return create_app(app_store, searches, tokens={"wt-poc-token"}, users={"admin": "demo"}, hec_tokens={"hec"})


async def test_hec_then_search_through_the_wt_adapter(app, app_store, sources, audit):
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://splunk") as http:
        assert (await http.post("/services/collector/event", content="{}")).json()["code"] == 2
        bad = await http.post("/services/collector/event", content='{"event":"x"}', headers={"Authorization": "Splunk no"})
        assert bad.status_code == 403
        ev = parse.kube_event(k8s_event("NodeNotReady", t=time.time() - 30))
        ok = await http.post("/services/collector/event", content=json.dumps(ev), headers={"Authorization": "Splunk hec"})
        assert ok.json() == {"text": "Success", "code": 0}
        assert (await http.post("/services/search/jobs/export", data={"search": "search index=ocp"})).status_code == 401
        basic = await http.post("/services/search/jobs/export", auth=("admin", "demo"),
                                data={"search": "search index=* | stats count by sourcetype", "output_mode": "json"})
        assert '"sourcetype":"kube:events"' in basic.text

    client = SplunkClient("http://splunk", "wt-poc-token", sources, audit, transport=transport)
    res = await client.search_signal("node_ready_events", node=NODE, window="10m")
    assert res.rows[0]["reason"] == "NodeNotReady" and res.audit_id
    with pytest.raises(httpx.HTTPStatusError) as exc:
        await client.search("index=ocp | eval x=1")
    assert exc.value.response.status_code == 400 and "eval" in exc.value.response.text
    await client.close()


async def test_scheduler_fires_node_not_ready_once_and_wt_reads_it(app, app_store, sources, audit):
    searches = load_saved_searches("sims/splunk_sim/saved_searches.yaml")
    assert {s.name for s in searches} >= {"NodeNotReady"}
    now = time.time()                    # fired_alerts hides expired alerts against the wall clock
    sched = Scheduler(app_store, searches)
    sched.last_run = now - 60
    # a stale NodeNotReady (older than the search's earliest) must not fire
    app_store.add(hec_rows([parse.kube_event(k8s_event("NodeNotReady", t=now - 3600, uid="old"))]), indextime=now - 30)
    assert sched.run_once(now) == []
    app_store.add(hec_rows([parse.kube_event(k8s_event("NodeNotReady", t=now + 20)),
                            parse.kube_event(k8s_event("NodeNotReady", name="demo-web-1", kind="Pod", ns="wt-lab",
                                                       t=now + 20, message="Node is not ready"))]), indextime=now + 30)
    assert sched.run_once(now + 60) == ["NodeNotReady"]
    assert sched.run_once(now + 120) == []                     # same event, already alerted
    fired = app_store.fired(now=now + 120)
    assert fired[0]["entity"] == NODE and fired[0]["severity"] == 4

    transport = httpx.ASGITransport(app=app)
    client = SplunkClient("http://splunk", "wt-poc-token", sources, audit, transport=transport)
    res = await client.fired_alerts()
    assert res.rows == [{"name": "NodeNotReady", "triggered_alert_count": 1, "severity": 4,
                         "trigger_time": int(now + 60), "entity": NODE}]
    await client.close()


def test_saved_searches_do_not_fire_on_congestion_symptoms(sample):
    """Phase A of the fault produces svc-prober timeouts, lease failures and switch drops: none
    of the saved searches match those."""
    sched = Scheduler(EventStore(":memory:"), [s for s in load_saved_searches("sims/splunk_sim/saved_searches.yaml")])
    events = [ev for ev in sample.candidates(None, None, {}) if ev.sourcetype in ("kube:container:svc-prober", "kubelet")]
    sched.store.add([{"time": e.time, "index": e.index, "sourcetype": e.sourcetype, "source": e.source,
                      "host": e.host, "raw": e.raw, "fields": e.indexed} for e in events], indextime=NOW)
    sched.last_run = NOW - 60
    assert sched.run_once(NOW + 1) == []


def test_saved_search_with_bad_spl_fails_at_load():
    with pytest.raises(spl.SplError):
        SavedSearch(name="x", search="index=ocp | eval y=1")
