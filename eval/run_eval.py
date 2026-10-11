"""make eval MODEL=x [BASE_URL=http://127.0.0.1:11434/v1] [API_KEY=...]: score a model on the six LLM jobs.

Works with any OpenAI-compatible endpoint (Ollama in the PoC, the on-prem Gemma later). No lab
needed: every input comes from recorded runs (eval/cases, eval/llm_cases.yaml). The LLM cache is
empty for each run, so every call reaches the model.

  Investigation cases (eval/cases/*): the full Investigator on recorded evidence, with the model:
    jobs 2 (extra hypotheses), 4 (explain + adjustment), 5 (next steps + note); scored on
    top-hypothesis accuracy and confidence-band accuracy, against the LLM-off baseline.
  Job cases (eval/llm_cases.yaml, built from the recorded rehearsal by eval/make_llm_cases.py):
    job 1 (summarise a watch item), job 3 (interpret evidence), job 6 (answer, citations).
Reports, per job: calls, valid JSON on the first try, usable answers (after the one retry and
the guards), task score, p50/p95 latency. Writes eval/results/<model>-<time>.md and .json.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import statistics
import sys
import time
from collections import defaultdict
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import yaml
from pydantic import BaseModel

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from wt.agents.base import load_rules                                        # noqa: E402
from wt.agents.parent.agent import HERE as PARENT                           # noqa: E402
from wt.agents.parent.investigator import FixtureGatherer, Investigator, Request, label   # noqa: E402
from wt.common import jobs                                                   # noqa: E402
from wt.common.audit import Audit                                            # noqa: E402
from wt.common.catalog import load_escalation, load_playbooks, load_sources, load_topology  # noqa: E402
from wt.common.config import PROFILES                                        # noqa: E402
from wt.common.llm import LLMClient                                          # noqa: E402
from wt.common.store import Store                                            # noqa: E402

JOBS = ["summarise_watch", "extra_hypotheses", "interpret", "explain", "next_steps", "answer"]


class MeasuredLLM(LLMClient):
    """LLMClient that records, per call: job, latency, whether the first reply was valid JSON for
    the schema, and whether an answer came back at all."""

    def __init__(self, *a, **kw):
        super().__init__(*a, **kw)
        self.calls: list[dict[str, Any]] = []

    async def complete(self, job, system, examples, user, schema):
        before = len(self.store.audit_rows(kind="llm", limit=10_000))
        t0 = time.perf_counter()
        answer = await super().complete(job, system, examples, user, schema)
        ms = (time.perf_counter() - t0) * 1000
        rows = self.store.audit_rows(kind="llm", limit=10_000)            # newest first
        rows = rows[: len(rows) - before]                                   # this call's attempts
        first_invalid = any("attempt 1 invalid" in r["input"] for r in rows)
        transport_fail = any(r["input"].startswith(f"{job} attempt 1") and not r["ok"] and "invalid" not in r["input"]
                             for r in rows)
        self.calls.append({"job": job, "ms": ms, "valid_first": not first_invalid and not transport_fail,
                           "answered": answer is not None})
        return answer


def pct(xs: list[float], q: float) -> float | None:
    if not xs:
        return None
    xs = sorted(xs)
    return xs[min(len(xs) - 1, int(round(q * (len(xs) - 1))))]


def make_llm(args, store: Store) -> MeasuredLLM:
    return MeasuredLLM(args.base_url, args.model, Audit(store), store, timeout_s=args.timeout, max_tokens=600,
                       actor="eval", api_key=args.api_key)


async def run_cases(args, llm: MeasuredLLM | None, store: Store) -> list[dict[str, Any]]:
    out = []
    playbooks = load_playbooks()
    for path in sorted((ROOT / "eval" / "cases").glob("*/case.yaml")):
        case = yaml.safe_load(path.read_text())
        evidence = [json.loads(x) for x in (path.parent / "evidence.jsonl").read_text().splitlines() if x.strip()]
        inv_ = Investigator(store, load_topology(profile=PROFILES[case.get("profile", "lite")]), load_sources(),
                            playbooks, load_escalation(), load_rules(PARENT / "rules.yaml"), FixtureGatherer(evidence), llm)
        t0 = time.perf_counter()
        inv = await inv_.investigate(Request(**case["request"]))
        top = inv.hypotheses[0]
        key = next((h.key for h in playbooks[inv.playbook].hypotheses if h.id == top.id), top.id)
        exp = case["expect"]
        out.append({"case": path.parent.name, "derived": bool(case.get("derived") or case.get("variant_of")),
                    "expected": exp["top_key"], "got": key, "confidence": top.confidence,
                    "confidence_rule": top.confidence_rule, "band": label(top.confidence),
                    "expected_band": exp.get("band", "high"), "top_ok": key == exp["top_key"],
                    "band_ok": label(top.confidence) == exp.get("band", "high"),
                    "llm_extra": sum(1 for h in inv.hypotheses if h.proposed_by == "llm"),
                    "adjustment": top.llm_adjustment.delta if top.llm_adjustment else None,
                    "seconds": round(time.perf_counter() - t0, 1)})
    return out


async def run_jobs(args, llm: MeasuredLLM) -> dict[str, list[bool]]:
    """Task scores for jobs 1, 3, 6 from eval/llm_cases.yaml."""
    cases = yaml.safe_load((ROOT / "eval" / "llm_cases.yaml").read_text())
    score: dict[str, list[bool]] = defaultdict(list)
    citations: list[bool] = []
    for c in cases.get("summarise_watch", []):
        res = await jobs.summarise_watch(llm, c["input"])
        score["summarise_watch"].append(bool(res) and all(k.lower() in res.summary.lower() for k in c["must_mention"]))
    for c in cases.get("interpret", []):
        res = await jobs.interpret(llm, c["expectation"], c["evidence"])
        score["interpret"].append(bool(res) and res.verdict == c["verdict"])
    for c in cases.get("answer", []):
        ctx = [tuple(x) for x in c["context"]]
        res = await jobs.answer(llm, c["question"], ctx)
        if c.get("refuse"):
            ok = bool(res) and not res.has_evidence
        else:
            ok = bool(res) and res.has_evidence and bool(set(res.cited) & set(c["cite_any"]))
        score["answer"].append(ok)
        citations.append(ok)
    score["_citations"] = citations
    return score


def table(rows: list[list[Any]], head: list[str]) -> str:
    cells = [head] + [[("" if v is None else str(v)) for v in r] for r in rows]
    w = [max(len(r[i]) for r in cells) for i in range(len(head))]
    line = lambda r: "| " + " | ".join(v.ljust(w[i]) for i, v in enumerate(r)) + " |"
    return "\n".join([line(head), "|" + "|".join("-" * (x + 2) for x in w) + "|"] + [line(r) for r in cells[1:]])


async def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--model", default="gemma3:4b")
    ap.add_argument("--base-url", default="http://127.0.0.1:11434/v1")
    ap.add_argument("--api-key", default=None)
    ap.add_argument("--timeout", type=float, default=180)
    args = ap.parse_args()
    started = datetime.now(UTC)
    print(f"WatchTower eval · {args.model} at {args.base_url} · {started:%Y-%m-%d %H:%M} UTC", flush=True)

    baseline = await run_cases(args, None, Store(":memory:"))          # LLM off: rules only
    store = Store(":memory:")
    llm = make_llm(args, store)
    try:
        await llm.http.get("/models", timeout=10)
    except Exception as exc:
        print(f"endpoint not reachable: {exc}")
        return 2
    with_llm = await run_cases(args, llm, store)
    job_scores = await run_jobs(args, llm)
    await llm.close()

    by_job: dict[str, list[dict]] = defaultdict(list)
    for c in llm.calls:
        by_job[c["job"]].append(c)
    # task scores for jobs 2, 4, 5 come from the investigation runs
    job_scores.setdefault("extra_hypotheses", [r["llm_extra"] <= 2 for r in with_llm])
    job_scores.setdefault("explain", [r["adjustment"] is None or abs(r["adjustment"]) <= 0.1 for r in with_llm])
    job_scores.setdefault("next_steps", [c["answered"] for c in by_job["next_steps"]])
    job_rows = []
    for job in JOBS:
        calls = by_job.get(job, [])
        lat = [c["ms"] for c in calls]
        sc = job_scores.get(job, [])
        job_rows.append([job, len(calls),
                         f"{sum(c['valid_first'] for c in calls) / len(calls):.0%}" if calls else "-",
                         f"{sum(c['answered'] for c in calls) / len(calls):.0%}" if calls else "-",
                         f"{sum(sc)}/{len(sc)}" if sc else "-",
                         f"{pct(lat, 0.5):,.0f}" if lat else "-", f"{pct(lat, 0.95):,.0f}" if lat else "-"])
    all_calls = llm.calls
    lat_all = [c["ms"] for c in all_calls]
    valid = sum(c["valid_first"] for c in all_calls) / len(all_calls) if all_calls else 0
    cites = job_scores.get("_citations", [])
    top_acc = sum(r["top_ok"] for r in with_llm) / len(with_llm)
    band_acc = sum(r["band_ok"] for r in with_llm) / len(with_llm)
    base_top = sum(r["top_ok"] for r in baseline) / len(baseline)
    summary = {
        "model": args.model, "base_url": args.base_url, "at": started.isoformat(), "calls": len(all_calls),
        "valid_json_rate": round(valid, 3), "citation_accuracy": round(sum(cites) / len(cites), 3) if cites else None,
        "top_hypothesis_accuracy": round(top_acc, 3), "confidence_band_accuracy": round(band_acc, 3),
        "top_hypothesis_accuracy_llm_off": round(base_top, 3),
        "latency_ms_p50": round(pct(lat_all, 0.5) or 0), "latency_ms_p95": round(pct(lat_all, 0.95) or 0),
    }
    case_rows = [[r["case"] + (" *" if r["derived"] else ""), r["expected"], r["got"], f"{r['confidence']:.2f}",
                  f"{b['confidence']:.2f}", r["band"], "ok" if r["top_ok"] and r["band_ok"] else "MISS",
                  "" if r["adjustment"] is None else f"{r['adjustment']:+.2f}", r["llm_extra"], r["seconds"]]
                 for r, b in zip(with_llm, baseline)]
    report = "\n".join([
        f"# WatchTower eval: {args.model}", "",
        f"{args.base_url} · {started:%Y-%m-%d %H:%M} UTC · {len(all_calls)} model calls", "",
        f"**valid JSON (first try) {valid:.0%} · citation accuracy {summary['citation_accuracy'] or 0:.0%} · "
        f"top hypothesis {top_acc:.0%} (rules only: {base_top:.0%}) · confidence band {band_acc:.0%} · "
        f"latency p50 {summary['latency_ms_p50'] / 1000:.1f} s, p95 {summary['latency_ms_p95'] / 1000:.1f} s**", "",
        "## Jobs", "",
        table(job_rows, ["job", "calls", "valid JSON 1st try", "usable answer", "task score", "p50 ms", "p95 ms"]), "",
        "## Investigations (recorded evidence; * = derived or variant case)", "",
        table(case_rows, ["case", "expected top", "got", "conf (LLM)", "conf (rules)", "band", "result",
                          "LLM adj", "LLM hyps", "s"]), "",
        "Task scores: summarise = names the node and port; interpret = right verdict; answer = cites an expected "
        "id or refuses when it should; extra hypotheses = at most two; explain = adjustment within ±0.10; "
        "next steps = valid catalogue keys.",
    ])
    print("\n" + report, flush=True)
    out = ROOT / "eval" / "results"
    out.mkdir(exist_ok=True)
    stem = f"{args.model.replace(':', '-').replace('/', '-')}-{started:%Y%m%d-%H%M}"
    (out / f"{stem}.md").write_text(report + "\n")
    (out / f"{stem}.json").write_text(json.dumps({"summary": summary, "jobs": job_rows, "cases": with_llm,
                                                  "baseline": baseline}, indent=1, default=str))
    print(f"\nsaved eval/results/{stem}.md and .json")
    return 0


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
