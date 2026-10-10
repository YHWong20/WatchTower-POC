"""Derive the master-2 eval case from the recorded lite incident (the full profile needs 16 GB).

    uv run python eval/derive_master2.py

Takes eval/cases/lite-worker1-flap and maps it onto the full profile: worker-1 -> master-2,
ocp-sim-worker -> ocp-sim-control-plane2, Gi1/0/13 -> Gi1/0/11. Three things change beyond names,
each because a master differs from a worker:
  - Service calls: in the minutes with timeouts about 1 in 3 calls succeeds (the local API server),
    the "2 in 3" pattern the full profile produces;
  - etcd runs on a master: a few "apply request took too long" warnings, no fsync warnings;
  - one flap only (the recording's window also held the previous run's flap).
The result is marked derived: true in case.yaml. Replace it with a recording from a 16 GB machine.
"""

from __future__ import annotations

import json
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parent
SRC, DST = ROOT / "cases" / "lite-worker1-flap", ROOT / "cases" / "master2-flap-derived"
NAMES = [("ocp-sim-worker", "ocp-sim-control-plane2"), ("worker-1", "master-2"), ("Gi1/0/13", "Gi1/0/11"),
         ("InterfaceID = 113", "InterfaceID = 111")]


def rename(value):
    if isinstance(value, str):
        for a, b in NAMES:
            value = value.replace(a, b)
        return value
    if isinstance(value, list):
        return [rename(v) for v in value]
    if isinstance(value, dict):
        return {rename(k): rename(v) for k, v in value.items()}
    return value


def main() -> None:
    out = []
    for line in (SRC / "evidence.jsonl").read_text().splitlines():
        r = rename(json.loads(line))
        f = r["facts"]
        if r["signal"] == "node_ready_events":
            f["flaps"] = 1
            f["events"] = f["events"][-2:]
            r["finding"] = r["finding"].split("; ")[0] if "flaps in the window" in r["finding"] else r["finding"]
            r["finding"] = r["finding"].replace("; 2 flaps in the window", "")
        elif r["signal"] == "api_svc_calls":
            calls = f["calls_in_affected"]
            failed = round(calls * 0.64)
            extra = failed - f["failed_in_affected"]
            f.update(failed_in_affected=failed, failed=f["failed"] + extra, success_rate_affected=round(1 - failed / calls, 3))
            r["finding"] = (f"{f['failed']} of {f['calls']} calls from master-2 to the kubernetes Service timed out in "
                            f"{f['minutes_affected']} min; in those minutes {f['success_rate_affected']:.0%} still "
                            "succeeded (partial failure)")
        elif r["signal"] == "api_svc_timeouts_by_node":
            n = sum(f["by_node"].values())
            f.update(by_node={"master-2": n}, nodes_with_timeouts=["master-2"], all_masters=False, only_node=True)
        elif r["signal"] == "etcd_latency":
            f.update(by_msg={"apply request took too long": 3}, fsync=0, peer=0, total=3, etcd_on_node=True)
            r["finding"] = "etcd on master-2: 3 × apply request took too long"
        out.append(r)
    DST.mkdir(parents=True, exist_ok=True)
    (DST / "evidence.jsonl").write_text("".join(json.dumps(r) + "\n" for r in out))
    case = yaml.safe_load((SRC / "case.yaml").read_text())
    case.update(name=DST.name, profile="full", derived=True,
                description="DERIVED from lite-worker1-flap by eval/derive_master2.py (names mapped to master-2 / "
                            "Gi1/0/11, 2-in-3 Service call pattern, etcd on the node, one flap). The full profile "
                            "needs 16 GB; replace with a recording from a 16 GB machine.")
    case["request"].update(node="master-2", alarm={"tool": "splunk", "name": "NodeNotReady",
                                                   "entity": "ocp-sim-control-plane2"})
    (DST / "case.yaml").write_text(yaml.safe_dump(case, sort_keys=False))
    print(f"{DST}: {len(out)} evidence answers")


if __name__ == "__main__":
    main()
