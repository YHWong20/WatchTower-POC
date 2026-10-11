"""Build the three variant eval cases from the recorded lite incident (eval/cases/lite-worker1-flap).

    uv run python eval/make_variants.py

Each variant edits the recorded evidence so a different cause is right; the rest of the
recording stays as it was. They check that the ranking follows the evidence, not the playbook's
favourite:
  crc-link-fault        network-port-congestion on Gi1/0/13: CRC errors rising and link flaps,
                        few drops, normal utilisation                    -> link_fault, high
  etcd-slow-disk        ocp-node-flap on master-1 (lite): etcd "slow fdatasync" warnings, every
                        call fails (not partial), no port drops          -> etcd_slow_disk, high
  node-cpu-starvation   ocp-node-flap on worker-1: SystemOOM / memory pressure events, CPU 95 %
                        (node monitored in SolarWinds), no port drops    -> node_resource_starvation, high
"""

from __future__ import annotations

import copy
import json
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parent
SRC = ROOT / "cases" / "lite-worker1-flap"


def load() -> dict[str, dict]:
    return {r["signal"]: r for r in map(json.loads, (SRC / "evidence.jsonl").read_text().splitlines())}


def write(name: str, evidence: dict[str, dict], request: dict, expect: dict, description: str) -> None:
    out = ROOT / "cases" / name
    out.mkdir(parents=True, exist_ok=True)
    (out / "evidence.jsonl").write_text("".join(json.dumps(r) + "\n" for r in evidence.values()))
    case = {"name": name, "description": description, "profile": "lite", "variant_of": SRC.name,
            "request": request, "expect": expect}
    (out / "case.yaml").write_text(yaml.safe_dump(case, sort_keys=False))
    print(f"{out.relative_to(ROOT.parent)}: {len(evidence)} evidence answers")


def no_drops(ev: dict) -> None:
    f = ev["interface_discards"]["facts"]
    n = f["polls"]
    f.update(out_discards=[0] * n, rising_polls=0, latest_streak=0, total_out_discards=0, latest_two_rising=False,
             first_drop_at=None)
    ev["interface_discards"]["finding"] = "no output drops on Gi1/0/13 in %d polls; InErrors and OutErrors flat" % n
    ev.pop("queue_drops", None)


def crc_link_fault(base: dict) -> None:
    ev = {k: copy.deepcopy(base[k]) for k in ("interface_discards", "node_ready_events", "api_svc_calls",
                                              "lease_update_failures")}
    f = ev["interface_discards"]["facts"]
    n = f["polls"]
    crc = [0] * (n - 6) + [40, 95, 120, 160, 210, 260]
    f.update(in_errors=crc, in_errors_polls=6, errors_flat=False, out_discards=[0] * (n - 2) + [12, 9],
             rising_polls=2, latest_streak=2, total_out_discards=21, latest_two_rising=True)
    ev["interface_discards"]["finding"] = "OutDiscards on Gi1/0/13 > 0 in 2 consecutive polls (total 21); InErrors > 0 in 6 polls (CRC)"
    ev["interface_utilisation"] = {**copy.deepcopy(base["interface_discards"]), "signal": "interface_utilisation",
                                   "query": base["interface_discards"]["query"].replace("InterfaceErrors", "InterfaceTraffic"),
                                   "finding": "Utilisation on cisco-sim-1 Gi1/0/13: average 21.0%, peak 24.5% over 30 polls",
                                   "facts": {"avg_util": 21.0, "peak_util": 24.5, "polls": 30}, "excerpt": ""}
    ev["switch_syslog"] = {**copy.deepcopy(base["node_ready_events"]), "domain": "network", "agent": "network-agent",
                           "signal": "switch_syslog",
                           "query": 'index=network sourcetype=cisco:ios host=cisco-sim-1 (Gi1/0/13 OR "%LINK" OR "%LINEPROTO") earliest=-30m',
                           "excerpt": '{"_time":"…","_raw":"000231: cisco-sim-1: *Oct 11 02:31:07.120 UTC: %LINK-3-UPDOWN: Interface GigabitEthernet1/0/13, changed state to down"}',
                           "finding": "3 link-down message(s) for Gi1/0/13 in Splunk", "facts": {"link_down": 3, "lines": 6}}
    ev["active_alerts"] = {**copy.deepcopy(base["node_cpu_memory"]), "domain": "network", "agent": "network-agent",
                           "signal": "active_alerts", "query": "SELECT AlertActiveID, TriggeredDateTime, TriggeredMessage FROM Orion.AlertActive",
                           "finding": "No active SolarWinds alerts", "facts": {"count": 0}, "excerpt": ""}
    write("crc-link-fault", ev, {"mode": "incident_investigation", "switch": "cisco-sim-1", "port": "Gi1/0/13",
                                 "playbook": "network-port-congestion", "requested_by": "eval"},
          {"top_key": "link_fault", "band": "high", "min_confidence": 0.7, "team": "Network team"},
          "VARIANT of lite-worker1-flap: CRC errors and link flaps on Gi1/0/13, almost no drops, normal utilisation.")


def etcd_slow_disk(base: dict) -> None:
    ev = copy.deepcopy(base)
    no_drops(ev)
    ev["etcd_latency"]["facts"] = {"by_msg": {"slow fdatasync": 41, "apply request took too long": 63}, "fsync": 41,
                                   "peer": 0, "total": 104, "etcd_on_node": True}
    ev["etcd_latency"]["finding"] = "etcd on master-1: 41 × slow fdatasync, 63 × apply request took too long"
    ev["etcd_latency"]["excerpt"] = '{"msg":"slow fdatasync","count":"41"}\n{"msg":"apply request took too long","count":"63"}'
    c = ev["api_svc_calls"]["facts"]
    c.update(success_rate_affected=0.04, failed_in_affected=int(c["calls_in_affected"] * 0.96))
    ev["api_svc_calls"]["finding"] = "calls from master-1 to the kubernetes Service failed in the affected minutes: 4% succeeded (not partial)"
    ev["api_svc_timeouts_by_node"]["facts"].update(by_node={"master-1": 300}, nodes_with_timeouts=["master-1"],
                                                    all_masters=False, only_node=True)
    for r in ev.values():                         # the incident is on master-1 now
        r.update({k: v.replace("worker-1", "master-1").replace("ocp-sim-worker", "ocp-sim-control-plane")
                  for k, v in r.items() if isinstance(v, str)})
    write("etcd-slow-disk", ev, {"mode": "incident_investigation", "node": "master-1", "requested_by": "eval",
                                 "alarm": {"tool": "splunk", "name": "NodeNotReady", "entity": "ocp-sim-control-plane"}},
          {"top_key": "etcd_slow_disk", "band": "high", "min_confidence": 0.7, "team": "Platform team"},
          "VARIANT of lite-worker1-flap moved to master-1: etcd slow fdatasync warnings, every call fails, no port drops.")


def node_cpu_starvation(base: dict) -> None:
    ev = copy.deepcopy(base)
    no_drops(ev)
    ev["node_pressure"]["facts"] = {"count": 3, "reasons": ["NodeHasInsufficientMemory", "SystemOOM"]}
    ev["node_pressure"]["finding"] = "Pressure events on worker-1: NodeHasInsufficientMemory, SystemOOM"
    ev["node_pressure"]["excerpt"] = '{"reason":"SystemOOM","message":"System OOM encountered, victim process: java"}'
    ev["node_cpu_memory"]["facts"] = {"monitored": True, "avg_load": 95.0, "memory_pct": 97.0}
    ev["node_cpu_memory"]["finding"] = "worker-1 CPU 95.0% average, memory 97.0% (SolarWinds)"
    c = ev["api_svc_calls"]["facts"]
    c.update(success_rate_affected=0.1)
    write("node-cpu-starvation", ev, {"mode": "incident_investigation", "node": "worker-1", "requested_by": "eval",
                                      "alarm": {"tool": "splunk", "name": "NodeNotReady", "entity": "ocp-sim-worker"}},
          {"top_key": "node_resource_starvation", "band": "high", "min_confidence": 0.7, "team": "Platform team"},
          "VARIANT of lite-worker1-flap: SystemOOM and memory pressure events, CPU 95% in SolarWinds, no port drops.")


if __name__ == "__main__":
    base = load()
    crc_link_fault(base)
    etcd_slow_disk(base)
    node_cpu_starvation(base)
