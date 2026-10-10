"""Phase 2 "done when" check: run the congestion fault and verify the node flap.

    uv run python lab/verify_flap.py [node] [--full-length] [--analyse-only]

Runs lab/faults/cp-congest.sh <node> --investigate (about 6.5 min), polls the node's Ready
condition every 2 s, then checks:
  1. the node went NotReady (Ready=False/Unknown) during the fault and is Ready again;
  2. the cluster events have NodeNotReady and NodeReady for it, and no Rebooted or Starting
     event since the fault began (no reboot, no kubelet restart);
  3. no pods were evicted (NotReady stayed under the 300 s eviction threshold);
  4. svc-prober on that node logged i/o timeouts, reported per fault phase.
     On a master in the full profile, phase C should fail about 2 in 3 calls (the call to its
     own API server stays local). On the lite worker every call crosses the switch.
  5. neither splunk-sim nor solarwinds-sim fired an alert during phase A (the existing tools
     stay quiet while congestion creeps); alerts in later phases are listed (splunk-sim's
     NodeNotReady is expected after the burst), and splunk-sim holds the NotReady/Ready events.
Exit 0 when 1-5 hold.
"""

from __future__ import annotations

import argparse
import json
import os
import re
import subprocess
import sys
import threading
import time
from datetime import datetime
from pathlib import Path

import httpx

ROOT = Path(__file__).resolve().parents[1]
CTX = ["--context", "kind-ocp-sim"]


def kubectl(*args: str) -> str:
    return subprocess.run(["kubectl", *CTX, *args], capture_output=True, text=True, check=True).stdout


def ready(node: str) -> str:
    try:
        return kubectl("get", "node", node, "-o",
                       "jsonpath={.status.conditions[?(@.type==\"Ready\")].status}").strip() or "?"
    except subprocess.CalledProcessError:
        return "?"


def ts(value: str | None) -> float | None:
    if not value:
        return None
    return datetime.fromisoformat(value.replace("Z", "+00:00")).timestamp()


def event_time(e: dict) -> float | None:
    times = [ts(e.get(k)) for k in ("lastTimestamp", "eventTime", "firstTimestamp")]
    times += [ts((e.get("series") or {}).get("lastObservedTime"))]
    return max((t for t in times if t), default=None)


def hhmmss(t: float) -> str:
    return time.strftime("%H:%M:%S", time.localtime(t))


def main() -> int:
    profile = os.environ.get("WT_PROFILE", "lite")
    ap = argparse.ArgumentParser()
    ap.add_argument("node", nargs="?", default="ocp-sim-worker" if profile == "lite" else "ocp-sim-control-plane2")
    ap.add_argument("--full-length", action="store_true", help="run the 15-min proactive-watch timing")
    ap.add_argument("--analyse-only", action="store_true", help="check the last run in data/faults/last.jsonl")
    args = ap.parse_args()
    node = args.node
    phases_file = ROOT / "data" / "faults" / "last.jsonl"
    transitions: list[tuple[float, str]] = []

    if not args.analyse_only:
        cmd = [str(ROOT / "lab" / "faults" / "cp-congest.sh"), node] + ([] if args.full_length else ["--investigate"])
        print(f"running {' '.join(cmd[1:])} (Ctrl-C clears the fault)")
        proc = subprocess.Popen(cmd, cwd=ROOT)
        stop = threading.Event()

        def poll() -> None:
            last = None
            while not stop.is_set():
                state = ready(node)
                if state != last:
                    transitions.append((time.time(), state))
                    print(f"{time.strftime('%H:%M:%S')} {node} Ready={state}", flush=True)
                    last = state
                stop.wait(2)

        t = threading.Thread(target=poll, daemon=True)
        t.start()
        try:
            proc.wait()
        except KeyboardInterrupt:
            proc.wait()
            stop.set()
            return 130
        deadline = time.time() + 180
        while ready(node) != "True" and time.time() < deadline:
            time.sleep(2)
        time.sleep(3)
        stop.set()
        t.join()

    phases = [json.loads(line) for line in phases_file.read_text().splitlines() if line.strip()]
    starts = {p["phase"]: p["at"] for p in phases}
    start, end = starts["A"], starts.get("clear", time.time())
    ok = True
    print(f"\nfault on {node} from {hhmmss(start)} to {hhmmss(end)}")

    # 1. NotReady then Ready -------------------------------------------------------------------
    events = json.loads(kubectl("get", "events", "-A", "--field-selector", f"involvedObject.name={node}", "-o", "json"))["items"]
    recent = [(event_time(e), e["reason"], (e.get("source") or {}).get("component") or e.get("reportingComponent", ""))
              for e in events if (event_time(e) or 0) >= start - 1]
    recent.sort(key=lambda x: x[0])
    not_ready = [t for t, r, _ in recent if r == "NodeNotReady"]
    ready_ev = [t for t, r, _ in recent if r == "NodeReady" and not_ready and t > not_ready[0]]
    went_down = any(s in ("False", "Unknown") for _, s in transitions) or bool(not_ready)
    final = ready(node)
    check(went_down and final == "True", f"1. {node} went NotReady and is Ready again (now Ready={final})")
    if transitions:
        print("   Ready over time: " + ", ".join(f"{hhmmss(t)} {s}" for t, s in transitions))
    ok &= went_down and final == "True"
    if not_ready and ready_ev:
        gap = ready_ev[0] - not_ready[0]
        print(f"   NodeNotReady {hhmmss(not_ready[0])}, NodeReady {hhmmss(ready_ev[0])}: {gap:.0f} s NotReady "
              f"(eviction starts at 300 s)")

    # 2. no reboot, no kubelet restart -----------------------------------------------------------
    restarts = [(t, r, c) for t, r, c in recent if r in ("Rebooted", "Starting")]
    check(not restarts, "2. no Rebooted or Starting event since the fault began")
    ok &= not restarts
    for t, r, c in restarts:
        print(f"   ! {hhmmss(t)} {r} from {c}")
    for t, r, c in recent:
        print(f"   {hhmmss(t)} {r:<24} {c}")

    # 3. no evictions ----------------------------------------------------------------------------
    all_events = json.loads(kubectl("get", "events", "-A", "-o", "json"))["items"]
    # TaintManagerEviction also reports "Cancelling deletion of Pod ..." when the node comes back
    # before the toleration (300 s) runs out: that is the opposite of an eviction.
    window = [e for e in all_events if (event_time(e) or 0) >= start - 1]
    evictions = [e for e in window if e["reason"] in ("Evicted", "Preempted")
                 or (e["reason"] == "TaintManagerEviction" and not (e.get("message") or "").startswith("Cancelling"))]
    cancelled = [e for e in window if e["reason"] == "TaintManagerEviction" and e not in evictions]
    check(not evictions, f"3. no pods evicted ({len(cancelled)} pending deletions cancelled when the node came back)")
    ok &= not evictions
    for e in evictions:
        print(f"   ! {e['reason']}: {e.get('message')}")

    # 4. svc-prober timeouts per phase -----------------------------------------------------------
    pod = kubectl("-n", "wt-lab", "get", "pods", "-l", "app=svc-prober", "--field-selector", f"spec.nodeName={node}",
                  "-o", "jsonpath={.items[0].metadata.name}").strip()
    since = max(int(time.time() - start) + 30, 60)
    lines = kubectl("-n", "wt-lab", "logs", pod, "--timestamps", f"--since={since}s").splitlines()
    calls = []
    for line in lines:
        stamp, _, body = line.partition(" ")
        try:
            rec = json.loads(body)
        except json.JSONDecodeError:
            continue
        calls.append((ts(stamp[:26] + "Z" if "." in stamp else stamp), rec))
    order = [p for p in ("A", "B", "C", "D", "clear") if p in starts]
    timeouts = 0
    print("   svc-prober on", node)
    for i, phase in enumerate(order):
        lo = starts[phase]
        hi = starts[order[i + 1]] if i + 1 < len(order) else lo + 120
        window = [r for t, r in calls if t and lo <= t < hi]
        failed = [r for r in window if not r["ok"]]
        timeouts += sum(1 for r in failed if "i/o timeout" in (r["err"] or ""))
        rate = len(failed) / len(window) if window else 0
        label = {"A": "creep", "B": "worse", "C": "burst", "D": "partial", "clear": "after clear"}[phase]
        print(f"   phase {phase} {label:<12} {len(window):>3} calls, {len(failed):>3} failed ({rate:.0%})")
    errs = sorted({re.sub(r":\d+->", ":*->", r["err"]) for _, r in calls if r["err"]})
    for e in errs[:4]:
        print(f"     e.g. {e}")
    check(timeouts > 0, f"4. svc-prober logged {timeouts} i/o timeouts during the fault")
    ok &= timeouts > 0

    # 5. the existing tools' alerts ------------------------------------------------------------
    ok &= check_alerts(node, starts, order)

    print("\nPASS" if ok else "\nFAIL")
    return 0 if ok else 1


def check_alerts(node: str, starts: dict, order: list[str]) -> bool:
    """Alerts from splunk-sim (fired alerts) and solarwinds-sim (AlertHistory), by fault phase."""
    a_start, a_end = starts["A"], starts.get("B", starts.get("clear", time.time()))
    try:
        with httpx.Client(timeout=15) as http:
            fired = http.get("http://127.0.0.1:8089/services/alerts/fired_alerts/-", auth=("admin", "demo"),
                             params={"output_mode": "json"}).raise_for_status().json()["entry"]
            since = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(a_start))
            history = http.post("http://127.0.0.1:17778/SolarWinds/InformationService/v3/Json/Query",
                                auth=("watchtower", "watchtower"),
                                json={"query": "SELECT TimeStamp, Message FROM Orion.AlertHistory "
                                               f"WHERE EventType = 0 AND TimeStamp >= '{since}' ORDER BY TimeStamp"}
                                ).raise_for_status().json()["results"]
            ready = http.post("http://127.0.0.1:8089/services/search/jobs/export", auth=("admin", "demo"),
                              data={"output_mode": "json", "search": "search index=ocp sourcetype=kube:events "
                                    f"reason IN (NodeNotReady, NodeReady) involvedObject.name={node} "
                                    f"earliest={int(a_start)} | stats count by reason"}).raise_for_status().text
    except (httpx.HTTPError, KeyError, ValueError) as exc:
        check(False, f"5. could not read the stand-ins' alerts ({type(exc).__name__}: {exc}); make up?")
        return False
    alerts = [(e["content"]["trigger_time"], "splunk-sim", e["content"]["savedsearch_name"]) for e in fired
              if e["content"]["trigger_time"] >= a_start]
    alerts += [(ts(h["TimeStamp"]), "solarwinds-sim", h["Message"]) for h in history]

    def phase_of(t: float) -> str:
        current = "before"
        for p in order:
            if t >= starts[p]:
                current = p
        return current

    in_a = [a for a in alerts if a_start <= a[0] < a_end]
    check(not in_a, f"5. no alert from splunk-sim or solarwinds-sim during phase A ({len(alerts)} during the whole run)")
    for t, tool, what in sorted(alerts):
        print(f"   {'!' if t < a_end else ' '} {hhmmss(t)} phase {phase_of(t):<5} {tool:<15} {what}")
    counts = {json.loads(line)["result"]["reason"]: json.loads(line)["result"]["count"]
              for line in ready.splitlines() if '"result"' in line}
    print(f"   splunk-sim holds {node} NodeNotReady x{counts.get('NodeNotReady', 0)}, "
          f"NodeReady x{counts.get('NodeReady', 0)} since the fault began (via the forwarder)")
    return not in_a


def check(cond: bool, text: str) -> None:
    print(("   ok   " if cond else "   FAIL ") + text)


if __name__ == "__main__":
    sys.exit(main())
