"""Phase 5 "done when", on recorded evidence (eval/cases): the investigation validates against
investigation.schema.json; H1 network congestion ranks first at >= 0.7 with the LLM off and on;
the conclusion says no reboot and no eviction; escalation is Network team P3; reuse >= 0.85 with
at most one probe."""

import json
from pathlib import Path

import httpx
import jsonschema
import pytest
import yaml

from wt.agents.base import load_rules
from wt.agents.parent.agent import HERE
from wt.agents.parent.investigator import FixtureGatherer, Investigator, Request
from wt.common.audit import Audit
from wt.common.catalog import load_escalation, load_playbooks, load_sources, load_topology
from wt.common.config import PROFILES, REPO_ROOT
from wt.common.contracts import dump
from wt.common.llm import LLMClient

CASES = sorted((REPO_ROOT / "eval" / "cases").glob("*/case.yaml"))
LITE = REPO_ROOT / "eval" / "cases" / "lite-worker1-flap" / "case.yaml"
SCHEMA = json.loads((REPO_ROOT / "contracts" / "investigation.schema.json").read_text())


def fake_llm(store, delta: float = -0.1) -> LLMClient:
    """An LLM that answers every job validly, and adjusts the top hypothesis by `delta` (worst case)."""
    answers = {
        "extra_hypotheses": {"hypotheses": [{"statement": "A bulk transfer to the node saturates the port in bursts",
                                             "why": "Drops with flat CRC fit bursty bulk traffic"}]},
        "explain": {"likely_cause": "Congestion on the node's switch port delayed its API calls and lease renewals.",
                    "what_happened": "The node had a transient NotReady (node flap) and recovered without a reboot; "
                                     "no control-plane pods restarted and no pods were evicted.",
                    "adjustment": {"delta": delta, "reason": "test adjustment"}},
        "next_steps": None,                          # filled per request from the catalogue it offers
        "interpret": {"verdict": "neutral", "reason": "n/a"},
    }

    def handler(request: httpx.Request) -> httpx.Response:
        body = json.loads(request.content)
        job = body["response_format"]["json_schema"]["name"]
        answer = answers[job]
        if job == "next_steps":                  # pick the first three keys the prompt offers
            offered = body["messages"][-1]["content"].split("Catalogue: ", 1)[1].split(".", 1)[0].split(", ")
            answer = {"steps": [{"key": k, "why": "test"} for k in offered[:3]], "note_summary": "test note"}
        return httpx.Response(200, json={"choices": [{"message": {"content": json.dumps(answer)}}]})

    return LLMClient("http://llm/v1", "fake", Audit(store), store, transport=httpx.MockTransport(handler))


def investigator(case: dict, store, llm=None) -> Investigator:
    evidence = [json.loads(line) for line in (case["dir"] / "evidence.jsonl").read_text().splitlines() if line.strip()]
    return Investigator(store, load_topology(profile=PROFILES[case["profile"]]), load_sources(), load_playbooks(),
                        load_escalation(), load_rules(HERE / "rules.yaml"), FixtureGatherer(evidence), llm)


def load_case(path: Path) -> dict:
    case = yaml.safe_load(path.read_text())
    case["dir"] = path.parent
    return case


@pytest.mark.parametrize("case_path", CASES, ids=[p.parent.name for p in CASES])
@pytest.mark.parametrize("llm_on", [False, True], ids=["llm-off", "llm-on"])
async def test_case(case_path, llm_on, store):
    case = load_case(case_path)
    inv = await investigator(case, store, fake_llm(store) if llm_on else None).investigate(Request(**case["request"]))
    jsonschema.validate(dump(inv), SCHEMA)
    exp = case["expect"]
    top = inv.hypotheses[0]
    pb = load_playbooks()[inv.playbook]
    top_key = next(h.key for h in pb.hypotheses if h.id == top.id)
    assert top_key == exp["top_key"] and top.confidence >= exp["min_confidence"], [(h.id, h.confidence) for h in inv.hypotheses]
    assert inv.conclusion.confidence_label == exp.get("band", "high")
    assert inv.llm_used is llm_on
    text = f"{inv.conclusion.likely_cause} {inv.conclusion.what_happened}".lower()
    if exp.get("no_reboot"):
        assert "without a reboot" in text or "did not reboot" in text
    if exp.get("no_eviction"):
        assert "not evicted" in text or "no pods were evicted" in text
    assert inv.escalation.team == exp["team"]
    if "priority" in exp:
        assert inv.escalation.priority == exp["priority"]
    if "min_reuse" in exp:
        assert inv.reuse.rate >= exp["min_reuse"] and inv.reuse.probed <= exp["max_probes"]
    assert len(inv.recommended_actions) >= 3 and all(a.kind == "diagnostic" for a in inv.recommended_actions)
    # every statement links to evidence: the conclusion cites at least two items from two sources
    ev = {e.id: e for e in inv.evidence}
    assert len({ev[r].source for r in inv.conclusion.refs if r in ev}) >= 2
    for h in inv.hypotheses:
        assert set(h.supporting + h.refuting) <= set(ev)
    assert "**Evidence:**" in inv.escalation.note_markdown and inv.id in inv.escalation.note_markdown
    if llm_on:
        assert top.llm_adjustment and top.llm_adjustment.delta == -0.1 and top.confidence_rule - top.confidence == pytest.approx(0.1)
        assert any(h.proposed_by == "llm" and h.confidence == 0 for h in inv.hypotheses)


async def test_inconclusive_when_evidence_is_missing(store):
    case = load_case(LITE)
    inv_ = investigator(case, store)
    inv_.gatherer = FixtureGatherer([])          # the agents answered nothing
    inv = await inv_.investigate(Request(**case["request"]))
    assert inv.hypotheses[0].confidence < 0.4 and inv.conclusion.likely_cause.startswith("Inconclusive")
    assert inv.escalation.team == "SRE shift lead"
