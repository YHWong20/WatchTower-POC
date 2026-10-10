"""Turn a recorded investigation (WT_RECORD_EVIDENCE lines) into an eval case.

    uv run python eval/make_case.py eval/recordings/<run>/evidence.jsonl INV-0002 eval/cases/<name> \
        --node worker-1 --profile lite --alarm splunk:NodeNotReady:ocp-sim-worker

Keeps the last successful answer per signal for that investigation, and writes
eval/cases/<name>/evidence.jsonl and case.yaml (request + expected outcome, edit as needed).
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path

import yaml


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("recording")
    ap.add_argument("investigation_id")
    ap.add_argument("out")
    ap.add_argument("--node", required=True)
    ap.add_argument("--profile", default="lite")
    ap.add_argument("--alarm", default="", help="tool:name:entity")
    ap.add_argument("--description", default="")
    args = ap.parse_args()
    by_signal: dict[str, dict] = {}
    for line in Path(args.recording).read_text().splitlines():
        r = json.loads(line)
        if r["investigation_id"] == args.investigation_id and r.get("ok", True):
            by_signal[r["signal"]] = r
    out = Path(args.out)
    out.mkdir(parents=True, exist_ok=True)
    (out / "evidence.jsonl").write_text("".join(json.dumps(r) + "\n" for r in by_signal.values()))
    alarm = dict(zip(("tool", "name", "entity"), args.alarm.split(":"))) if args.alarm else None
    case = {
        "name": out.name, "description": args.description, "profile": args.profile,
        "request": {"mode": "incident_investigation", "node": args.node, "alarm": alarm, "requested_by": "eval"},
        "expect": {"top_key": "network_path_congestion", "min_confidence": 0.7, "team": "Network team",
                   "priority": "P3", "no_reboot": True, "no_eviction": True, "min_reuse": 0.85, "max_probes": 1},
    }
    (out / "case.yaml").write_text(yaml.safe_dump(case, sort_keys=False))
    print(f"{out}: {len(by_signal)} evidence answers ({', '.join(sorted(by_signal))})")


if __name__ == "__main__":
    main()
