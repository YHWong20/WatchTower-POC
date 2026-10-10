"""Phase 3 "done when", part 1: every SPL and SWQL template in contracts/sources.yaml runs against
splunk-sim and solarwinds-sim and returns data. Uses the phase-1 adapters (SplunkClient,
SolarWindsClient) over the host ports, exactly as the agents will.

    uv run python lab/verify_sources.py [--window 24h]

Each template is in one of three groups:
  data      returns rows on a healthy lab (metrics, lookups, switch syslog, kubelet start lines).
  incident  rows only once the incident has happened in the window (timeouts, lease failures,
            NotReady/Ready, cancelled evictions, the NodeNotReady alert). Required when
            data/faults/last.jsonl shows a fault inside the window (run `make verify-flap` first);
            otherwise reported.
  absence   a "did X happen?" check that is empty on a healthy cluster (reboot, pressure, static
            pod restarts, etcd slowness, active alerts). The query must run, and the stream it
            reads must hold data, so "empty" means "it did not happen", not "we cannot see".
Exit 0 when every required check holds.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import re
import sys
import tempfile
import time
from pathlib import Path

from wt.common.audit import Audit
from wt.common.catalog import load_sources
from wt.common.sources.base import placeholders, render_template
from wt.common.sources.solarwinds import SolarWindsClient
from wt.common.sources.splunk import SplunkClient
from wt.common.store import Store

ROOT = Path(__file__).resolve().parents[1]
GROUP = {
    ("api_svc_timeouts", "query"): "incident", ("api_svc_timeouts", "prod_query"): "prod",
    ("lease_update_failures", "query"): "incident", ("node_ready_events", "query"): "incident",
    ("node_reboot_check", "query"): "absence", ("node_reboot_check", "corroborate"): "data",
    ("static_pod_restarts", "query"): "absence", ("pod_evictions", "query"): "incident",
    ("etcd_latency", "query"): "absence", ("ovn_errors", "query"): "n/a", ("node_pressure", "query"): "absence",
    ("switch_syslog", "query"): "data",
    ("node_cpu_memory", "query"): "data", ("interface_discards", "query"): "data",
    ("interface_utilisation", "query"): "data", ("interface_lookup", "query"): "data",
    ("active_alerts", "query_solarwinds"): "absence", ("active_alerts", "query_splunk"): "incident",
}
NOTE = {
    "prod": "OpenShift wording (172.30.0.1); kind's VIP is 10.96.0.1, so it only has to run",
    "n/a": "kind runs kindnet, not OVN-Kubernetes: no ovnkube logs exist in the lab; it only has to run",
}


def _hhmm(t: float | None) -> str:
    return time.strftime("%H:%M:%S", time.localtime(t)) if t else "never"


def mark(ok: bool | None) -> str:
    return {True: "   ok  ", False: "   FAIL", None: "   --  "}[ok]


def stream_of(spl: str) -> str:
    """The index/sourcetype/host part of a query: the data stream it reads."""
    terms = re.findall(r"\b(?:index|sourcetype|host)=\S+", spl)
    window = re.search(r"earliest=\S+", spl)
    return " ".join(terms + ([window.group(0)] if window else []))


def splunk_count(rows: list[dict]) -> int:
    if rows and "count" in rows[0] and len(rows[0]) <= 4:       # stats/timechart: add up the counts
        return sum(int(r.get("count") or 0) for r in rows)
    return len(rows)


async def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--window", default="24h", help="Splunk earliest window for the templates")
    ap.add_argument("--minutes", type=int, default=60, help="SolarWinds window for the templates")
    args = ap.parse_args()
    profile = os.environ.get("WT_PROFILE", "lite")
    node = "ocp-sim-worker" if profile == "lite" else "ocp-sim-control-plane2"
    port = "Gi1/0/13" if profile == "lite" else "Gi1/0/11"
    params = {"node": node, "window": args.window, "switch": "cisco-sim-1", "port": port, "minutes": args.minutes}

    window_s = int(args.window[:-1]) * {"m": 60, "h": 3600, "d": 86400}[args.window[-1]]
    fault_at = None
    last = ROOT / "data" / "faults" / "last.jsonl"
    if last.exists():
        marks = [json.loads(line) for line in last.read_text().splitlines() if line.strip()]
        starts = [m["at"] for m in marks if m["phase"] == "A" and m.get("node") == node]
        if starts and time.time() - starts[-1] < window_s - 300:
            fault_at = starts[-1]
    print(f"profile {profile}: node {node}, switch cisco-sim-1 {port}, window -{args.window}")
    print(f"incident in window: {'yes, fault began ' + _hhmm(fault_at) if fault_at else 'no (incident checks are informational; run make verify-flap first)'}")

    tmp = tempfile.TemporaryDirectory()
    shipping_since = None
    store = Store(Path(tmp.name) / "audit.db")
    audit = Audit(store)
    sources = load_sources()
    splunk = SplunkClient("http://127.0.0.1:8089", os.environ.get("WT_SPLUNK_TOKEN", "wt-poc-token"), sources, audit,
                          actor="verify-sources")
    sw = SolarWindsClient("http://127.0.0.1:17778", "watchtower", "watchtower", sources, audit, actor="verify-sources")
    failures = 0

    try:
        first = (await splunk.search("index=ocp sourcetype=kube:container:svc-prober | stats min(_indextime) as first")).rows
        shipping_since = float(first[0]["first"]) if first and first[0].get("first") else None
        if fault_at and (shipping_since is None or shipping_since > fault_at):
            print(f"   the forwarder started shipping after that fault ({_hhmm(shipping_since)}), so splunk-sim "
                  "missed its Kubernetes events: incident checks are informational")
            fault_at = None
        lookup = await sw.query_signal("interface_lookup", switch="cisco-sim-1", port=port)
        params["if_id"] = lookup.rows[0]["InterfaceID"] if lookup.rows else 0

        print("\n Splunk (SPL)")
        for key, sig in sources.signals.items():
            for attr in ("query", "prod_query", "corroborate", "query_splunk"):
                text = getattr(sig, attr, None)
                if not text or text.startswith("SELECT"):
                    continue
                group = GROUP.get((key, attr), "data")
                q = render_template(text, **{p: params[p] for p in placeholders(text)})
                if key == "etcd_latency":
                    q = q.replace(f"host={node}", "host=ocp-sim-control-plane")   # etcd runs on the masters
                try:
                    if q.startswith("/"):
                        res = await splunk.fired_alerts()
                        rows = [r for r in res.rows if not fault_at or r.get("trigger_time", 0) >= fault_at]
                        n, stream_n, stream = len(rows), None, ""
                    else:
                        res = await splunk.search(q)
                        n = splunk_count(res.rows)
                        stream = stream_of(q)
                        stream_n = splunk_count((await splunk.search(f"{stream} | stats count")).rows)
                except Exception as exc:                           # report every template, keep going
                    print(f"{mark(False)} {key}.{attr}: {type(exc).__name__}: {exc}")
                    failures += 1
                    continue
                ok, why = check(group, n, stream_n, fault_at)
                failures += ok is False
                extra = f"; stream [{stream}] {stream_n} events" if stream else ""
                print(f"{mark(ok)} {key + '.' + attr:<34} {group:<8} {n:>5} {'hits' if not q.startswith('/') else 'alerts'}{extra}{why}")

        print("\n SolarWinds (SWQL)")
        for key, sig in sources.signals.items():
            for attr in ("query", "query_solarwinds"):
                text = getattr(sig, attr, None)
                if not text or not text.startswith("SELECT"):
                    continue
                group = GROUP.get((key, attr), "data")
                targets = [params["node"], "cisco-sim-1"] if key == "node_cpu_memory" else [None]
                for target in targets:
                    p = dict(params, **({"node": target} if target else {}))
                    q = render_template(text, **{k: p[k] for k in placeholders(text)})
                    try:
                        rows = (await sw.query(q)).rows
                    except Exception as exc:
                        print(f"{mark(False)} {key}: {type(exc).__name__}: {exc}")
                        failures += 1
                        continue
                    label = f"{key}({target})" if target else key
                    if key == "node_cpu_memory" and target == params["node"]:
                        ok, why = True, " (the OpenShift node is not a SolarWinds node here: the probe fallback applies)"
                    else:
                        ok, why = check(group, len(rows), 1, fault_at)
                    failures += ok is False
                    sample = f" e.g. {json.dumps(rows[-1], separators=(',', ':'))[:110]}" if rows else ""
                    print(f"{mark(ok)} {label:<34} {group:<8} {len(rows):>5} rows{why}{sample}")
    finally:
        await splunk.close()
        await sw.close()
        store.close()
        tmp.cleanup()

    print("\nPASS" if not failures else f"\nFAIL ({failures})")
    return 1 if failures else 0


def check(group: str, n: int, stream_n: int | None, fault_at: float | None) -> tuple[bool | None, str]:
    if group in ("prod", "n/a"):
        return True, f" ({NOTE[group]})"
    if group == "data":
        return n > 0, "" if n else " (expected data)"
    if group == "incident":
        if fault_at:
            return n > 0, "" if n else " (expected after the fault)"
        return None, " (no incident in the window)"
    # absence: the stream must be there; empty means it did not happen
    if stream_n == 0:
        return False, " (its stream has no data, so empty would mean blind, not healthy)"
    return True, " (nothing happened, as expected)" if n == 0 else " (it happened: worth a look)"


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
