"""The six LLM jobs. Each is a short system prompt, two few-shot examples, a Pydantic schema
(sent as the JSON schema Ollama constrains decoding with), a guard on the answer, and the
caller's rule-based fallback when the LLM is off, unreachable or wrong.

    1 summarise_watch      a watch item in plain words
    2 extra_hypotheses     up to two hypotheses the playbook does not have (start at 0)
    3 interpret            evidence without a deterministic predicate: supports / refutes / neutral
    4 explain              likely cause and what happened (incl. reboot or not), ±0.10 adjustment
    5 next_steps           3-5 steps from the playbook's action catalogue + a note summary
    6 answer               SRE questions, only from stored investigations and watch items

Prompts stay small: the lite profile runs gemma3:1b with a 2k context.
"""

from __future__ import annotations

import re
from typing import Literal

from pydantic import BaseModel, Field

from wt.common.llm import Example, LLMClient

# ---------------------------------------------------------------- schemas


class WatchSummary(BaseModel):
    summary: str = Field(max_length=400, description="2-3 plain sentences for an SRE")


class ExtraHypothesis(BaseModel):
    statement: str = Field(max_length=160)
    why: str = Field(max_length=200)


class ExtraHypotheses(BaseModel):
    hypotheses: list[ExtraHypothesis] = Field(max_length=2)


class Interpretation(BaseModel):
    verdict: Literal["supports", "refutes", "neutral"]
    reason: str = Field(max_length=200)


class Adjustment(BaseModel):
    delta: float = Field(ge=-0.1, le=0.1)
    reason: str = Field(max_length=200)


class Explanation(BaseModel):
    likely_cause: str = Field(max_length=300)
    what_happened: str = Field(max_length=800)
    adjustment: Adjustment


class Step(BaseModel):
    key: str
    why: str = Field(max_length=200)


class NextSteps(BaseModel):
    steps: list[Step] = Field(min_length=3, max_length=5)
    note_summary: str = Field(max_length=500)


class Answer(BaseModel):
    answer: str = Field(max_length=600)
    cited: list[str] = Field(max_length=6, description="evidence ids like ev-3 or watch item ids")
    has_evidence: bool


NO_EVIDENCE = "I don't have evidence for that."


# ---------------------------------------------------------------- 1 summarise a watch item

SUMMARY_SYSTEM = (
    "You explain a WatchTower watch item to the team lead in 2-3 short sentences. Use only the "
    "signals given. Say what is happening, why the signals are linked, and what could happen next. "
    "No advice to change anything. Reply with JSON only."
)
SUMMARY_EXAMPLES = [
    Example("Watch item: cross-domain, node master-3, confidence 0.6.\nSignals:\n- 18 x i/o timeout from master-3 to "
            "the kubernetes Service in 5m (baseline 0)\n- Output drops on cisco-sim-1 Gi1/0/12 rising 4 polls; "
            "utilisation 40%\nLink: Gi1/0/12 feeds master-3.",
            WatchSummary(summary="master-3's calls to the API are starting to time out, and the switch port that "
                                 "feeds it (Gi1/0/12) is dropping packets while its average load looks normal. The "
                                 "two started together, so the port is the likely bottleneck. If it worsens, master-3 "
                                 "may miss its 40 s heartbeat and go NotReady.")),
    Example("Watch item: single-domain, interface cisco-sim-2:Gi1/0/1, confidence 0.2.\nSignals:\n- Utilisation on "
            "cisco-sim-2 Gi1/0/1 at 71%, baseline p95 45% for this hour",
            WatchSummary(summary="The uplink Gi1/0/1 on cisco-sim-2 is busier than usual for this hour. Nothing "
                                 "else is affected and no infra signal is linked, so this is only worth watching.")),
]


async def summarise_watch(llm: LLMClient, text: str) -> WatchSummary | None:
    return await llm.complete("summarise_watch", SUMMARY_SYSTEM, SUMMARY_EXAMPLES, text, WatchSummary)


# ---------------------------------------------------------------- 2 extra hypotheses

EXTRA_SYSTEM = (
    "You help diagnose an infrastructure/Cisco incident. Given the playbook hypotheses and the evidence "
    "findings, propose at most two OTHER plausible causes that are not already listed, each with why. "
    "If nothing else is plausible, return an empty list. Reply with JSON only."
)
EXTRA_EXAMPLES = [
    Example("Listed: congestion on the node's switch path; OVN fault; etcd slow disk; CPU starvation; API server overload.\n"
            "Findings: drops on Gi1/0/11 rising; CRC flat; timeouts only from master-2; no pressure events.",
            ExtraHypotheses(hypotheses=[ExtraHypothesis(
                statement="A backup or replication job on master-2 saturates its NIC at night",
                why="Drops with flat CRC on one port fit a bulk transfer; check top talkers")])),
    Example("Listed: bursty traffic overflows the egress queue; link fault; port saturated.\nFindings: CRC rising; "
            "link flaps in syslog.", ExtraHypotheses(hypotheses=[])),
]


async def extra_hypotheses(llm: LLMClient, text: str) -> ExtraHypotheses | None:
    return await llm.complete("extra_hypotheses", EXTRA_SYSTEM, EXTRA_EXAMPLES, text, ExtraHypotheses)


# ---------------------------------------------------------------- 3 interpret evidence

INTERPRET_SYSTEM = (
    "Decide if a piece of evidence supports, refutes, or is neutral to an expectation. Use only the "
    "excerpt. If it does not speak to the expectation, answer neutral. Reply with JSON only."
)
INTERPRET_EXAMPLES = [
    Example("Expectation: calls to the local API server endpoint also time out.\nEvidence: 'dial tcp 10.0.0.12:6443: "
            "i/o timeout' x14 from master-2 to its own address.",
            Interpretation(verdict="supports", reason="The node's calls to its own API server time out too")),
    Example("Expectation: 'slow fdatasync' warnings from etcd.\nEvidence: no etcd warnings on the node in 30 min.",
            Interpretation(verdict="refutes", reason="etcd logged no fsync or latency warnings in the window")),
]


async def interpret(llm: LLMClient, expectation: str, excerpt: str) -> Interpretation | None:
    return await llm.complete("interpret", INTERPRET_SYSTEM, INTERPRET_EXAMPLES,
                              f"Expectation: {expectation}\nEvidence: {excerpt[:900]}", Interpretation)


# ---------------------------------------------------------------- 4 explain

EXPLAIN_SYSTEM = (
    "You write the conclusion of an incident investigation for an SRE, from the facts given only. "
    "likely_cause: one sentence. what_happened: 2-4 sentences covering how long the node was NotReady "
    "(it is marked NotReady after 40 s without a lease renewal; pods are evicted after 300 s NotReady), "
    "whether the node rebooted (only if a Rebooted event "
    "exists), whether control-plane pods restarted, and evictions. Say 'transient NotReady (node flap)', "
    "never 'went down', unless a Rebooted event exists. adjustment: change the top confidence by -0.1 "
    "to 0.1 only with a reason from the facts; use 0 otherwise. Reply with JSON only."
)
EXPLAIN_EXAMPLES = [
    Example("Top: congestion on master-3's switch path (0.88). Facts: NotReady 02:14:08, Ready 02:14:55 (47 s); no "
            "Rebooted/Starting events; no API server or etcd restart events; no evictions; drops on Gi1/0/12 still "
            "rising; CRC flat; 2 in 3 Service calls failed.",
            Explanation(likely_cause="Congestion on master-3's switch port (Gi1/0/12) delayed its API calls, so it "
                                     "missed lease renewals and was marked NotReady.",
                        what_happened="master-3 had a transient NotReady (node flap): after 40 s without a lease "
                                      "renewal it was marked NotReady at 02:14:08 and was Ready again at 02:14:55, "
                                      "47 s later, far below the 300 s eviction threshold, so no pods were evicted. It did not reboot (no Rebooted or "
                                      "Starting events) and its API server and etcd did not restart.",
                        adjustment=Adjustment(delta=0.0, reason="The evidence count already reflects the facts"))),
    Example("Top: slow disk delays etcd (0.5). Facts: NotReady 10:01:00, Ready 10:03:10 (130 s); Rebooted event at "
            "10:02:30; etcd 'slow fdatasync' x40; no drops on the port.",
            Explanation(likely_cause="A slow disk on the node delayed etcd and the kubelet; the node then rebooted.",
                        what_happened="The node was NotReady for 130 s, under the 300 s eviction threshold. A Rebooted event at 10:02:30 shows it did reboot. "
                                      "etcd logged 40 slow fdatasync warnings before it.",
                        adjustment=Adjustment(delta=0.1, reason="The reboot after fsync warnings fits a disk problem"))),
]


def explanation_ok(e: Explanation, rebooted: bool) -> bool:
    """Reject an explanation that contradicts the reboot evidence."""
    text = f"{e.likely_cause} {e.what_happened}".lower()
    says_rebooted = re.search(r"\b(it|node|the node|\w+-\d) (did )?reboot(ed)?\b", text) and \
        not re.search(r"(not|no|without|never|didn't|did not)\W+(\w+\W+){0,3}reboot", text)
    if not rebooted and says_rebooted:
        return False
    if not rebooted and ("went down" in text or "crashed" in text):
        return False
    return True


async def explain(llm: LLMClient, text: str, rebooted: bool) -> Explanation | None:
    answer = await llm.complete("explain", EXPLAIN_SYSTEM, EXPLAIN_EXAMPLES, text, Explanation)
    return answer if answer and explanation_ok(answer, rebooted) else None


# ---------------------------------------------------------------- 5 next steps + note summary

STEPS_SYSTEM = (
    "Pick 3 to 5 read-only diagnostic steps for the SRE from the catalogue keys given, most useful "
    "first, each with a short why tied to the evidence. Never invent keys. Then write note_summary: "
    "2-3 sentences for the escalation ticket. No remediation. Reply with JSON only."
)
STEPS_EXAMPLES = [
    Example("Top: congestion on master-3's switch path. Catalogue: top_talkers, queue_stats, lease_watch, "
            "check_peer_ports, node_journal, etcd_health. Facts: drops on Gi1/0/12 ongoing; one flap.",
            NextSteps(steps=[Step(key="top_talkers", why="Find what fills Gi1/0/12's egress queue"),
                             Step(key="queue_stats", why="Confirm the drops sit in one queue class"),
                             Step(key="lease_watch", why="See if renewals slip again while drops continue")],
                      note_summary="master-3 flapped NotReady for 47 s because its switch port Gi1/0/12 is "
                                   "congested; drops are still rising. No reboot and no evictions.")),
    Example("Top: slow disk delays etcd. Catalogue: node_journal, etcd_health, top_talkers. Facts: fsync warnings.",
            NextSteps(steps=[Step(key="etcd_health", why="Check member latency and fsync duration"),
                             Step(key="node_journal", why="Read kubelet errors around the flap"),
                             Step(key="top_talkers", why="Rule out the network path as well")],
                      note_summary="The node flapped after etcd logged slow fdatasync warnings; disk latency is "
                                   "the likely cause.")),
]


async def next_steps(llm: LLMClient, text: str, catalogue: list[str]) -> NextSteps | None:
    answer = await llm.complete("next_steps", STEPS_SYSTEM, STEPS_EXAMPLES, text, NextSteps)
    if not answer:
        return None
    keys = [s.key for s in answer.steps]
    if any(k not in catalogue for k in keys) or len(set(keys)) != len(keys):
        return None
    return answer


# ---------------------------------------------------------------- 6 answer the SRE

ANSWER_SYSTEM = (
    "You answer an SRE's question using ONLY the context items given (evidence and watch items, each with "
    "an id). Cite the ids you used. If the context does not answer the question, set has_evidence to "
    f"false and answer exactly: \"{NO_EVIDENCE}\" Reply with JSON only."
)
ANSWER_EXAMPLES = [
    Example("Question: did master-3 reboot?\nContext:\n[ev-2] No Rebooted or Starting events for master-3: no reboot\n"
            "[ev-1] master-3 NotReady 02:14:08, Ready 02:14:55: 47 s",
            Answer(answer="No. There are no Rebooted or Starting events for master-3 (ev-2); it was NotReady for "
                          "47 s and came back on its own (ev-1).", cited=["ev-2", "ev-1"], has_evidence=True)),
    Example("Question: what is the BGP state on the core router?\nContext:\n[ev-1] master-3 NotReady 02:14:08, "
            "Ready 02:14:55: 47 s", Answer(answer=NO_EVIDENCE, cited=[], has_evidence=False)),
]


async def answer(llm: LLMClient, question: str, context: list[tuple[str, str]]) -> Answer | None:
    text = f"Question: {question}\nContext:\n" + "\n".join(f"[{i}] {t}" for i, t in context)
    reply = await llm.complete("answer", ANSWER_SYSTEM, ANSWER_EXAMPLES, text[:3500], Answer)
    if not reply:
        return None
    known = {i for i, _ in context}
    if reply.has_evidence and (not reply.cited or any(c not in known for c in reply.cited)):
        return None                                        # an uncited or made-up citation is not an answer
    if not reply.has_evidence:
        return Answer(answer=NO_EVIDENCE, cited=[], has_evidence=False)
    return reply
