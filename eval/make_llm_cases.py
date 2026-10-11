"""Build eval/llm_cases.yaml (jobs 1, 3 and 6) from a recorded rehearsal.

    uv run python eval/make_llm_cases.py [--recording rehearsal]

Takes the recording's cross-domain watch item and its investigation report (demo/recordings/<name>):
  summarise_watch  the watch item as the Parent Agent saw it; the summary must name the node and port
  interpret        recorded evidence findings against expectations with a known verdict
  answer           questions about the investigation with the evidence ids that answer them,
                   plus questions the evidence cannot answer (must refuse)
"""

from __future__ import annotations

import argparse
import gzip
import json
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[1]


def main() -> None:
    import sys
    sys.path.insert(0, str(ROOT))
    from wt.agents.parent.correlator import WatchItem, watch_item_text

    ap = argparse.ArgumentParser()
    ap.add_argument("--recording", default="rehearsal")
    args = ap.parse_args()
    path = ROOT / "demo" / "recordings" / args.recording / "events.jsonl.gz"
    events = [json.loads(x) for x in gzip.open(path, "rt") if x.strip()]
    items = [WatchItem.model_validate(e["data"]) for e in events if e.get("stream") == "wt.watch" and "opened_at" in e["data"]]
    cross = [i for i in items if i.scope == "cross_domain" and i.status == "emerging"]
    item = max(cross, key=lambda i: (len(i.signals), i.confidence)) if cross else items[-1]
    inv = next(e["data"]["investigation"] for e in reversed(events)
               if e.get("stream") == "wt.investigations" and e["data"].get("investigation"))
    ev = {e["id"]: e for e in inv["evidence"]}

    def find(text: str) -> str:
        return next(i for i, e in ev.items() if text in e["query"] or text in e["finding"])

    reboot, flap, calls = find("Rebooted"), find("NodeNotReady"), find("count by ok")
    evict, drops, queue = find("TaintManagerEviction"), find("InterfaceErrors"), find("qos queue stats")
    context = [[e["id"], f"{e['source']}: {e['finding']}"] for e in inv["evidence"]]
    context += [[c["id"], c["text"]] for c in inv["correlations"]]
    why = next((c["id"] for c in inv["correlations"] if "kubernetes Service" in c["text"]), calls)
    cases = {
        "source": f"demo/recordings/{args.recording} ({inv['id']}, {item.id})",
        "summarise_watch": [{"input": watch_item_text(item), "must_mention": [item.node, item.port]}],
        "interpret": [
            {"expectation": "calls to the local API server endpoint also time out", "verdict": "neutral",
             "evidence": f"{ev[calls]['finding']} (worker-1 has no API server of its own)"},
            {"expectation": "drops concentrated in one egress queue", "verdict": "supports",
             "evidence": ev[queue]["finding"] + "\n" + ev[queue]["excerpt"][-600:]},
            {"expectation": "the node rebooted or the kubelet restarted", "verdict": "refutes",
             "evidence": ev[reboot]["finding"]},
            {"expectation": "'slow fdatasync' count > 0 on the node's etcd", "verdict": "refutes",
             "evidence": "No etcd slowness warnings on master-1 in 30 min: 0 × slow fdatasync, 0 × apply request took too long"},
        ],
        "answer": [
            {"question": "Did worker-1 reboot?", "cite_any": [reboot], "context": context},
            {"question": "How long was worker-1 NotReady?", "cite_any": [flap], "context": context},
            {"question": "Were any pods evicted?", "cite_any": [evict], "context": context},
            {"question": "Why did only some calls to the API fail?", "cite_any": [why, calls], "context": context},
            {"question": "Are packets being dropped on the switch port?", "cite_any": [drops, queue], "context": context},
            {"question": "What is the BGP state on the core router?", "refuse": True, "context": context},
            {"question": "Which change ticket caused this?", "refuse": True, "context": context},
        ],
    }
    out = ROOT / "eval" / "llm_cases.yaml"
    out.write_text("# Built by eval/make_llm_cases.py from a recorded rehearsal. Jobs 1, 3 and 6 for make eval.\n"
                   + yaml.safe_dump(cases, sort_keys=False, width=140, allow_unicode=True))
    print(f"{out.relative_to(ROOT)}: {sum(len(v) for v in cases.values() if isinstance(v, list))} cases from {cases['source']}")


if __name__ == "__main__":
    main()
