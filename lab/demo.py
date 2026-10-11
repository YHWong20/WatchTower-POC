"""make demo [MODE=investigate] [NAME=x]: the Builders Day storyline, narrated and recorded.

1. Preflight: make health must pass, and no watch item may still be open from an earlier run
   (it would absorb the new signals); waits up to 20 minutes for one to close.
2. Starts recording (demo/recordings/<NAME>, default demo-<date>-<time>), then runs the fault
   through the Copilot exactly as lab/verify_copilot.py does: proactive watch item while the
   existing tools stay quiet, the node flaps, Investigate on the Splunk alert, the report, Ask.
   Every step of the definition of done is checked and printed.
3. Keeps recording 30 s after the report, stops, and prints how to replay it.
MODE=investigate is the short version: flap at about 3 minutes, incident investigation only.
"""

from __future__ import annotations

import argparse
import re
import signal
import subprocess
import sys
import time
from datetime import datetime
from pathlib import Path

import httpx

ROOT = Path(__file__).resolve().parents[1]


def say(text: str) -> None:
    print(f"\n== {datetime.now():%H:%M:%S}  {text}", flush=True)


def open_items() -> list[str]:
    try:
        html = httpx.get("http://127.0.0.1:8080/partials/watch", timeout=10).text
    except httpx.HTTPError:
        return ["(Copilot not answering)"]
    # any item not yet closed (a cross-domain item drops back to WATCHING as one domain's signals age out)
    return re.findall(r"(?:INVESTIGATING|EMERGING|WATCHING)[^·]*·[^W]*(WI-\d+)", html) or (
        ["open"] if re.search(r"INVESTIGATING|EMERGING|WATCHING", html) else [])


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--investigate", action="store_true")
    ap.add_argument("--name", default=None)
    ap.add_argument("--no-wait", action="store_true", help="do not wait for open watch items to close")
    args = ap.parse_args()
    signal.signal(signal.SIGTERM, lambda *_: sys.exit(143))
    name = args.name or f"demo-{datetime.now():%Y%m%d-%H%M}"

    say("Preflight: make health")
    for attempt in range(4):                     # agents report "starting" for their first watch cycle
        if subprocess.run(["lab/health.sh"], cwd=ROOT, stdout=subprocess.DEVNULL).returncode == 0:
            break
        if attempt == 3:
            print("   health check failed: run make health to see why (make clear removes a leftover fault)")
            return 1
        print("   health check not passing yet; trying again in 20 s", flush=True)
        time.sleep(20)
    print("   health OK")
    deadline = time.time() + 1200
    while (items := open_items()) and not args.no_wait and time.time() < deadline:
        print(f"   waiting for open watch item(s) {', '.join(items)} to close (15 quiet minutes after their last signal)",
              flush=True)
        time.sleep(60)
    if open_items() and not args.no_wait:
        print("   a watch item is still open; rerun later or with --no-wait")
        return 1

    say(f"Recording to demo/recordings/{name}")
    rec = subprocess.Popen([sys.executable, "-m", "wt.replay", "record", name], cwd=ROOT)
    time.sleep(3)
    try:
        say("Storyline: " + ("short version (flap at ~3 min)" if args.investigate else
                             "full version (~14 min): proactive watch, then the flap and the investigation"))
        print("   Open http://localhost:8080 and follow along. The checks below are the definition of done.")
        cmd = [sys.executable, "lab/verify_copilot.py"] + (["--investigate"] if args.investigate else [])
        result = subprocess.run(cmd, cwd=ROOT).returncode
        say("Recording 30 more seconds so the replay ends on the finished report")
        time.sleep(30)
    finally:
        rec.send_signal(signal.SIGINT)
        rec.wait(timeout=30)
    say("Done" + (": every check passed" if result == 0 else ": some checks failed (see above)"))
    print(f"   Replay it with the lab stopped:  make stop && make replay NAME={name}   (make up to restart)")
    return result


if __name__ == "__main__":
    sys.exit(main())
