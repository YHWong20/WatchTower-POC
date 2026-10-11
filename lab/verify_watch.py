"""Phase 4 "done when": with the fault's phase A running, wt.signals gets ocp.api_svc_timeouts for
the fault node and net.output_drops_rising for its switch port within 5 minutes, both from
Splunk/SolarWinds queries (no probes).

    uv run python lab/verify_watch.py [--keep-fault]

Needs the lab (make lab-up && make up) with the agents running. Starts
lab/faults/cp-congest.sh <node> (full length, phase A = 6 min), reads wt.signals from that
moment, and stops the fault once both signals are in (or after 5 minutes) unless --keep-fault.
Also checks that the agents ran no probe while watching (audit log in the agents' database).
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import signal
import subprocess
import sys
import time
from pathlib import Path

import redis.asyncio as aioredis

ROOT = Path(__file__).resolve().parents[1]
LIMIT_S = 300


async def main() -> int:
    signal.signal(signal.SIGTERM, lambda *_: sys.exit(143))   # stopped from outside: still clear the fault
    ap = argparse.ArgumentParser()
    ap.add_argument("--keep-fault", action="store_true", help="let the fault run to the end")
    args = ap.parse_args()
    profile = os.environ.get("WT_PROFILE", "lite")
    kind_node, node, port = (("ocp-sim-worker", "worker-1", "Gi1/0/13") if profile == "lite"
                             else ("ocp-sim-control-plane2", "master-2", "Gi1/0/11"))
    want = {("ocp.api_svc_timeouts", node), ("net.output_drops_rising", f"cisco-sim-1:{port}")}

    r = aioredis.from_url("redis://127.0.0.1:6379/0", decode_responses=True, socket_timeout=30)
    last_id = (await r.xrevrange("wt.signals", count=1) or [("0-0", {})])[0][0]
    print(f"starting the fault on {kind_node} (phase A runs 6 min); watching wt.signals for {LIMIT_S} s")
    (ROOT / "data" / "faults").mkdir(parents=True, exist_ok=True)
    fault_log = open(ROOT / "data" / "faults" / "verify-watch.log", "w")      # not a pipe: --keep-fault outlives us
    fault = subprocess.Popen([str(ROOT / "lab/faults/cp-congest.sh"), kind_node], cwd=ROOT,
                             stdout=fault_log, stderr=subprocess.STDOUT, start_new_session=True)
    start = time.time()
    seen: dict[tuple[str, str], dict] = {}
    others: list[dict] = []
    try:
        while time.time() - start < LIMIT_S and not want <= set(seen):
            reply = await r.xread({"wt.signals": last_id}, block=5000, count=50)
            for _, entries in reply or []:
                for msg_id, fields in entries:
                    last_id = msg_id
                    sig = json.loads(fields["data"])
                    key = (sig["rule"], sig["entity"]["name"])
                    t = time.time() - start
                    print(f"  +{t:5.0f} s  {sig['agent']:<16} {sig['rule']:<26} {sig['entity']['name']:<22} "
                          f"[{sig['source']['tool']}] {sig['summary']}")
                    if key in want and key not in seen:
                        seen[key] = {**sig, "_after_s": t}
                    else:
                        others.append(sig)
    finally:
        if not args.keep_fault:
            fault.send_signal(signal.SIGTERM)          # the script's trap clears netem and the switch
            fault.wait(timeout=30)
        await r.aclose()

    ok = True
    print()
    for rule, entity in sorted(want):
        sig = seen.get((rule, entity))
        if not sig:
            print(f"   FAIL {rule} for {entity}: not seen within {LIMIT_S} s")
            ok = False
            continue
        tool = sig["source"]["tool"]
        good = tool in ("splunk", "solarwinds") and sig["_after_s"] <= LIMIT_S
        ok &= good
        print(f"   {'ok  ' if good else 'FAIL'} {rule} for {entity} after {sig['_after_s']:.0f} s, from {tool}:")
        print(f"        {sig['source']['query']}")
    probes = [s for s in [*seen.values(), *others] if s["source"]["tool"].startswith("probe")]
    print(f"   {'ok  ' if not probes else 'FAIL'} no probe-sourced signals ({len(seen) + len(others)} signals in total)")
    ok &= not probes
    db = Path(os.environ.get("WT_DB_PATH", ROOT / "data" / "watchtower.db"))
    if db.exists():
        import sqlite3
        since = time.strftime("%Y-%m-%dT%H:%M:%S", time.gmtime(start))
        n = sqlite3.connect(db).execute("SELECT COUNT(*) FROM audit WHERE kind='probe' AND ts >= ?", (since,)).fetchone()[0]
        print(f"   {'ok  ' if n == 0 else 'FAIL'} audit log: {n} probes by the agents since the fault began ({db})")
        ok &= n == 0
    print("\nPASS" if ok else "\nFAIL")
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
