"""Screenshot every stage of a demo run (run it next to `make demo`).

    uv run python lab/demo_screenshots.py [--out docs/demo]

Takes a baseline, then watches the lab and captures each stage as it happens:
  quiet baseline -> first weak signal -> cross-domain watch item -> explain in plain words ->
  node NotReady + Splunk alarm -> investigation while evidence arrives -> report -> Ask ->
  escalation note -> after the fault clears.
Writes <out>/NN-stage.png and <out>/README.md (a captioned gallery). Headless Chrome; the
explanation and the answer use the Copilot's ?explain= and ?ask= links.
"""

from __future__ import annotations

import argparse
import json
import re
import subprocess
import sys
import time
from datetime import UTC, datetime
from pathlib import Path

import httpx

ROOT = Path(__file__).resolve().parents[1]
UI = "http://127.0.0.1:8080"
CHROME = "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome"
FAULT = ROOT / "data" / "faults" / "last.jsonl"


class Shots:
    def __init__(self, out: Path):
        self.out = out
        self.out.mkdir(parents=True, exist_ok=True)
        for old in self.out.glob("*.png"):
            old.unlink()
        self.items: list[tuple[str, str, str]] = []
        self.start = time.time()

    def take(self, name: str, path: str, caption: str, height: int = 900, budget_ms: int = 8000) -> None:
        n = len(self.items)
        file = f"{n:02d}-{name}.png"
        subprocess.run([CHROME, "--headless=new", "--disable-gpu", "--hide-scrollbars", f"--screenshot={self.out / file}",
                        f"--window-size=1440,{height}", f"--virtual-time-budget={budget_ms}", UI + path],
                       capture_output=True, timeout=120)
        at = datetime.now(UTC).strftime("%H:%M:%S")
        self.items.append((file, caption, at))
        print(f"{at} UTC  +{time.time() - self.start:5.0f} s  {file}: {caption}", flush=True)

    def gallery(self, title: str, notes: list[str]) -> None:
        lines = [f"# {title}", "", *notes, ""]
        for file, caption, at in self.items:
            lines += [f"## {file[:2]}. {caption}", "", f"_{at} UTC_", "", f"![{caption}]({file})", ""]
        (self.out / "README.md").write_text("\n".join(lines))


def get(http: httpx.Client, path: str) -> str:
    try:
        return http.get(path).text
    except httpx.HTTPError:
        return ""


def fault_marks() -> dict[str, float]:
    if not FAULT.exists():
        return {}
    return {m["phase"]: m["at"] for m in map(json.loads, FAULT.read_text().splitlines()) if m}


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", default="docs/demo")
    ap.add_argument("--timeout", type=float, default=3600)
    args = ap.parse_args()
    shots = Shots(ROOT / args.out)
    http = httpx.Client(base_url=UI, timeout=30)
    began = time.time()
    shots.take("baseline", "/", "Quiet baseline: no alarms in Splunk or SolarWinds, agents quiet, nothing to watch")

    print("waiting for the fault to start (make demo)…", flush=True)
    while fault_marks().get("A", 0) < began and time.time() - began < args.timeout:
        time.sleep(5)
    fault_at = fault_marks().get("A", time.time())
    stage, inv_id, alarm_shot = "signal", None, False
    alarm_rx = r'data-alarm="NodeNotReady" data-entity="[^"]*" data-trigger="([\d.]+)"'
    while time.time() - began < args.timeout:
        watch = get(http, "/partials/watch")
        if stage == "signal" and re.search(r"WATCHING|EMERGING", watch):
            shots.take("first-signal", "/", "First weak signal: a watch item at low confidence; the tools are still quiet")
            stage = "cross"
        if stage == "cross" and "EMERGING · cross-domain" in watch:
            m = re.search(r"EMERGING · cross-domain</span>\s*<span[^>]*>(WI-\d+)", watch)
            shots.take("cross-domain", "/", "Cross-domain watch item: Service timeouts (Splunk) + drops on the switch "
                       "port that feeds the node (SolarWinds), linked by topology; 100% reused, no probes")
            if m:
                shots.take("explain", f"/?explain={m.group(1)}", "Explain in plain words (LLM job 1, from the signals only)",
                           budget_ms=40000)
            stage = "notready"
        if stage in ("notready", "running"):
            if not alarm_shot and any(float(t) >= fault_at for t in re.findall(alarm_rx, get(http, "/partials/alarms"))):
                shots.take("notready-alarm", "/", "The node flapped: Splunk's NodeNotReady alert appears with an "
                           "Investigate button; the watch item turns to INVESTIGATING")
                alarm_shot = True
            try:
                running = http.get("http://127.0.0.1:8103/health").json().get("running") or []
            except (httpx.HTTPError, ValueError):
                running = []
            if stage == "notready" and running:
                inv_id = running[0]
                shots.take("investigation-live", f"/investigations/{inv_id}", "WatchTower opened an investigation on "
                           "the state change by itself: tasks go to the domain agents, evidence arrives live",
                           budget_ms=3000)
                stage = "running"
            if stage == "running" and "hypotheses" in get(http, f"/api/investigations/{inv_id}") and alarm_shot:
                shots.take("report", f"/investigations/{inv_id}", "The report: likely cause, no reboot, evidence chain "
                           "with sources, hypotheses, timeline, read-only next steps, escalation", height=1300)
                shots.take("ask", f"/investigations/{inv_id}?ask=why did only some calls fail?",
                           "Ask: answered only from the stored evidence, citing ids", height=1300, budget_ms=40000)
                shots.take("escalation-note", f"/investigations/{inv_id}/note",
                           "Copy escalation note: the Markdown for the Network team's ticket", height=1100)
                stage = "after"
        if stage == "after" and fault_marks().get("clear", 0) >= fault_at:
            time.sleep(60)
            shots.take("after", "/", "After the fault clears: the alert stays in Splunk, the item links to its "
                       "investigation, the signals age out")
            break
        time.sleep(2 if stage in ("notready", "running") else 5)
    marks = fault_marks()
    notes = [f"Live run on the lite lab (8 GB MacBook Air), {datetime.fromtimestamp(began, UTC):%Y-%m-%d %H:%M} UTC: "
             "`make demo` (full storyline) with these screenshots taken as each stage happened "
             "(`lab/demo_screenshots.py`).",
             "Fault phases (UTC): " + ", ".join(f"{p} {datetime.fromtimestamp(t, UTC):%H:%M:%S}" for p, t in marks.items())]
    shots.gallery("WatchTower demo, stage by stage", notes)
    print(f"gallery: {(shots.out / 'README.md').relative_to(ROOT)}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
