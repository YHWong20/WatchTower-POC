"""Phase 6 "done when" (and the master prompt's definition of done) through the Copilot, live.

    uv run python lab/verify_copilot.py [--no-screenshots]

Runs the full-length fault on the profile's node and checks, as an SRE would see it:
  Proactive watch: within 6 minutes a cross-domain watch item names the node and its switch port
    with confidence >= 0.5, while splunk-sim and solarwinds-sim have fired no alert and the node
    is still Ready; the item uses no probes.
  Incident investigation: after the flap the NodeNotReady alert shows in the Copilot; pressing
    Investigate gives, within 3 minutes, a report whose top hypothesis is network congestion on the
    node's path at >= 0.7, that states no reboot, lists >= 3 read-only next steps, recommends the
    Network team at P3, takes >= 85% of its evidence from Splunk/SolarWinds with <= 1 probe.
  Ask: a follow-up question is answered with evidence ids.
Screenshots (headless Chrome) go to docs/screenshots/. The fault is cleared at the end.
"""

from __future__ import annotations

import argparse
import json
import os
import re
import shutil
import signal
import subprocess
import sys
import time
from pathlib import Path

import httpx

ROOT = Path(__file__).resolve().parents[1]
UI = "http://127.0.0.1:8080"
CHROME = "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome"
results: list[tuple[bool, str]] = []


def check(ok: bool, text: str) -> bool:
    results.append((ok, text))
    print(("   ok   " if ok else "   FAIL ") + text, flush=True)
    return ok


def shot(name: str, path: str, height: int = 1000) -> None:
    chrome = CHROME if Path(CHROME).exists() else shutil.which("chromium") or shutil.which("google-chrome")
    if not chrome:
        return
    out = ROOT / "docs" / "screenshots" / f"{name}.png"
    out.parent.mkdir(parents=True, exist_ok=True)
    subprocess.run([chrome, "--headless=new", "--disable-gpu", "--hide-scrollbars", f"--screenshot={out}",
                    f"--window-size=1440,{height}", "--virtual-time-budget=8000", UI + path],
                   capture_output=True, timeout=60)
    print(f"        screenshot {out.relative_to(ROOT)}")


def text(html: str) -> str:
    return re.sub(r"\s+", " ", re.sub(r"<[^>]+>", " ", html)).replace("&#39;", "'")


def ready(node: str) -> str:
    r = subprocess.run(["kubectl", "--context", "kind-ocp-sim", "get", "node", node, "-o",
                        "jsonpath={.status.conditions[?(@.type==\"Ready\")].status}"], capture_output=True, text=True)
    return r.stdout.strip() or "?"


def main() -> int:
    signal.signal(signal.SIGTERM, lambda *_: sys.exit(143))   # stopped from outside: still clear the fault
    ap = argparse.ArgumentParser()
    ap.add_argument("--no-screenshots", action="store_true")
    args = ap.parse_args()
    snap = (lambda *a, **k: None) if args.no_screenshots else shot
    profile = os.environ.get("WT_PROFILE", "lite")
    kind_node, node, port = (("ocp-sim-worker", "worker-1", "Gi1/0/13") if profile == "lite"
                             else ("ocp-sim-control-plane2", "master-2", "Gi1/0/11"))
    http = httpx.Client(base_url=UI, timeout=60, transport=httpx.HTTPTransport(retries=3))
    before = {i["id"] for i in json.loads(subprocess.run(
        ["curl", "-s", "http://127.0.0.1:8103/health"], capture_output=True, text=True).stdout or "{}").get("running", [])}
    log = open(ROOT / "data" / "faults" / "verify-copilot.log", "w")
    print(f"fault on {kind_node} (full length: A 6 min, B 4 min, burst 75 s, D 3 min)", flush=True)
    fault = subprocess.Popen([str(ROOT / "lab/faults/cp-congest.sh"), kind_node], cwd=ROOT, stdout=log,
                             stderr=subprocess.STDOUT, start_new_session=True)
    start = time.time()
    try:
        # ---------------------------------------------------------------- proactive watch
        print("\n proactive watch", flush=True)
        item = None
        while time.time() - start < 360:
            watch = text(http.get("/partials/watch").text)
            m = re.search(r"EMERGING · cross-domain (WI-\d+).*?Confidence: \w+ \(([\d.]+)\)\s*(.*?)\s{0,2}No service impact", watch)
            if m and node in m.group(3) and float(m.group(2)) >= 0.5:
                item = (m.group(1), float(m.group(2)), m.group(3).strip(), time.time() - start)
                break
            time.sleep(10)
        if check(item is not None, f"cross-domain watch item for {node} at >= 0.5 within 6 min"
                 + (f": {item[0]} after {item[3]:.0f} s, confidence {item[1]}, \"{item[2]}\"" if item else "")):
            new = [m for m in re.finditer(r'data-alarm="([^"]+)" (?:data-entity="[^"]*" )?data-trigger="([^"]+)"',
                                          http.get("/partials/alarms").text)
                   if m.group(1) == "solarwinds" or float(m.group(2)) >= start]
            check(not new, "splunk-sim and solarwinds-sim have fired no alarm since the fault began")
            check(ready(kind_node) == "True", f"{node} is still Ready")
            watch_html = http.get("/partials/watch").text
            check(port in watch_html and "100% reused, 0 probes" in text(watch_html),
                  f"the item names {port} and uses no probes (100% reused)")
            ex = text(http.post(f"/watch/{item[0]}/explain").text)
            check("In plain words" in ex, "Explain in plain words: " + ex[:160])
            snap("proactive-watch", "/", 900)

        # ---------------------------------------------------------------- incident investigation
        print("\n incident investigation", flush=True)
        alarm_seen = None
        while time.time() - start < 1000 and not alarm_seen:
            for m in re.finditer(r'data-alarm="NodeNotReady" data-entity="([^"]*)" data-trigger="(\d+)"',
                                 http.get("/partials/alarms").text):
                if m.group(1) == kind_node and int(m.group(2)) >= start:
                    alarm_seen = time.time()
            if not alarm_seen:
                time.sleep(10)
        check(alarm_seen is not None, f"the Splunk NodeNotReady alert for {kind_node} shows in the Copilot"
              + (f" ({alarm_seen - start:.0f} s into the fault)" if alarm_seen else ""))
        if alarm_seen:
            pressed = time.time()
            r = http.post("/investigate", data={"alarm_tool": "splunk", "alarm_name": "NodeNotReady",
                                                "alarm_entity": kind_node}, follow_redirects=False)
            pending = r.headers.get("location", "")
            inv_id = None
            while time.time() - pressed < 30 and not inv_id:
                inv_id = http.get(f"/api/request/{pending.rsplit('/', 1)[-1]}").json().get("investigation_id")
                time.sleep(1)
            check(bool(inv_id), f"Investigate opened {inv_id}")
            snap("investigation-progress", f"/investigations/{inv_id}", 800)
            inv = None
            while time.time() - pressed < 180:
                data = http.get(f"/api/investigations/{inv_id}").json()
                if "hypotheses" in data:
                    inv = data
                    break
                time.sleep(3)
            if check(inv is not None, f"report ready {time.time() - pressed:.0f} s after pressing Investigate (limit 180 s)"):
                top = inv["hypotheses"][0]
                check("Congestion" in top["statement"] and node in top["statement"] and top["confidence"] >= 0.7,
                      f"top hypothesis: {top['statement']} ({top['confidence']})")
                said = (inv["conclusion"]["likely_cause"] + " " + inv["conclusion"]["what_happened"]).lower()
                check("without a reboot" in said or "did not reboot" in said or "no reboot" in said,
                      "states the node did not reboot")
                check(len(inv["recommended_actions"]) >= 3 and all(a["kind"] == "diagnostic" for a in inv["recommended_actions"]),
                      f"{len(inv['recommended_actions'])} read-only next steps")
                esc = inv["escalation"]
                check((esc["team"], esc["priority"]) == ("Network team", "P3"), f"escalation {esc['team']} {esc['priority']}")
                reuse = inv["reuse"]
                check(reuse["rate"] >= 0.85 and reuse["probed"] <= 1,
                      f"reuse {reuse['reused']}/{reuse['reused'] + reuse['probed']} ({reuse['rate']:.0%}), {reuse['probed']} probe")
                check(all(e.get("query") and e.get("source") for e in inv["evidence"]),
                      "every evidence item carries its source and exact query")
                snap("incident-investigation", f"/investigations/{inv_id}", 1250)
                ans = text(http.post("/ask", data={"question": "why did only some calls fail?", "inv_id": inv_id}).text)
                check("cites" in ans, "Ask: " + ans[:200])
                ans = text(http.post("/ask", data={"question": "what is the BGP state on the core router?",
                                                   "inv_id": inv_id}).text)
                check("don't have evidence" in ans, "Ask (no evidence): " + ans[:120])
    finally:
        fault.send_signal(signal.SIGTERM)
        fault.wait(timeout=30)
        print("\nfault cleared", flush=True)
    failed = [t for ok, t in results if not ok]
    print("\nPASS" if not failed else f"\nFAIL ({len(failed)})")
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
