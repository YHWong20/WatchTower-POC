"""make eval end to end against a fake OpenAI-compatible endpoint: every case runs, invalid JSON
on a first reply is counted (and retried), citations and refusals are scored, the report has the
summary numbers and tables."""

import json
import sys
from pathlib import Path

import httpx
import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "eval"))
import run_eval  # noqa: E402

pytestmark = pytest.mark.skipif(not (ROOT / "eval" / "llm_cases.yaml").exists(), reason="eval/llm_cases.yaml not built")


def fake_model():
    seen = {"explain": 0}

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path.endswith("/models"):
            return httpx.Response(200, json={"data": [{"id": "fake"}]})
        body = json.loads(request.content)
        job = body["response_format"]["json_schema"]["name"]
        user = body["messages"][-1]["content"]
        if job == "summarise_watch":
            node = "worker-1" if "worker-1" in user else "master-2"
            ans = {"summary": f"{node}'s API calls time out while Gi1/0/13 drops packets."}
        elif job == "extra_hypotheses":
            ans = {"hypotheses": []}
        elif job == "interpret":
            ans = {"verdict": "refutes" if "reboot" in user or "fdatasync" in user else "supports", "reason": "x"}
        elif job == "explain":
            seen["explain"] += 1
            if seen["explain"] == 1:                             # one invalid first reply: retried
                return httpx.Response(200, json={"choices": [{"message": {"content": "not json"}}]})
            ans = {"likely_cause": "c", "what_happened": "w", "adjustment": {"delta": 0.05, "reason": "r"}}
        elif job == "next_steps":
            offered = user.split("Catalogue: ", 1)[1].split(".", 1)[0].split(", ")
            ans = {"steps": [{"key": k, "why": "w"} for k in offered[:3]], "note_summary": "n"}
        else:                                                    # answer: cite the first context id, or refuse
            ids = [line[1:].split("]")[0] for line in user.splitlines() if line.startswith("[")]
            refuse = "BGP" in user or "change ticket" in user
            ans = {"answer": "x", "cited": [] if refuse else ids[:1], "has_evidence": not refuse}
        return httpx.Response(200, json={"choices": [{"message": {"content": json.dumps(ans)}}]})
    return handler


async def test_eval_runs_every_case_and_reports(monkeypatch, tmp_path, capsys):
    handler = fake_model()
    real = run_eval.MeasuredLLM.__init__

    def init(self, *a, **kw):
        real(self, *a, **{**kw, "transport": httpx.MockTransport(handler)})
    monkeypatch.setattr(run_eval.MeasuredLLM, "__init__", init)
    monkeypatch.setattr(run_eval, "ROOT", ROOT)
    monkeypatch.setattr(sys, "argv", ["run_eval.py", "--model", "fake", "--base-url", "http://fake/v1"])
    results = ROOT / "eval" / "results"
    before = set(results.glob("fake-*")) if results.exists() else set()
    assert await run_eval.main() == 0
    out = capsys.readouterr().out
    for f in set(results.glob("fake-*")) - before:
        data = json.loads(f.read_text()) if f.suffix == ".json" else None
        if data:
            s = data["summary"]
            assert s["top_hypothesis_accuracy"] == 1.0 and s["top_hypothesis_accuracy_llm_off"] == 1.0
            assert 0.9 < s["valid_json_rate"] < 1.0                  # the one invalid explain reply
            assert s["citation_accuracy"] is not None
        f.unlink()
    assert "| job " in out and "| case " in out and "valid JSON (first try)" in out
    assert "crc-link-fault *" in out and "lite-worker1-flap " in out
