"""Investigator: one playbook-driven investigation, from request to evidence chain.

1. pick the playbook (trigger signal, alarm or watch item);
2. task the domain agents for the facts and for every expect/refute item (reuse first: the
   only probes are items marked needs_live, plus fallbacks for facts while the budget lasts);
3. evaluate predicates in code; items without one go to the LLM "interpret" job;
4. score confidence = clip((supporting - refuting) / expected, 0, 1), LLM may adjust by ±0.10;
5. explain, choose next steps, route the escalation (escalation.yaml), and build the chain
   (contracts/investigation.schema.json). Every LLM job has a rule fallback.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import time
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Awaitable, Callable, Protocol

from wt.agents.base import EvidenceResult, Task
from wt.agents.parent.correlator import WatchItem
from wt.agents.parent.predicates import PREDICATES
from wt.common import jobs
from wt.common.catalog import Escalation, Playbook, Sources, Topology
from wt.common.contracts import (Alarm, Conclusion, Correlation, Escalation as EscalationOut, EvidenceItem, Hypothesis,
                                 Investigation, LlmAdjustment, Observation, RecommendedAction, Reuse, Trigger, dump,
                                 utcnow)
from wt.common.llm import LLMClient
from wt.common.store import Store

log = logging.getLogger("wt.parent")
OnResult = Callable[[Task, EvidenceResult], Awaitable[None]]
GRACE_S, EVICTION_S = 40, 300
AGENT_LABEL = {"openshift-agent": "Infra Agent", "network-agent": "Network Agent"}   # as the Copilot shows them


class Gatherer(Protocol):
    async def gather(self, tasks: list[Task], timeout_s: float, on_result: OnResult) -> None: ...


class DirectGatherer:
    """Calls domain agents in-process (tests, eval, replay without a bus)."""

    def __init__(self, agents: dict[str, Any]):
        self.agents = agents

    async def gather(self, tasks: list[Task], timeout_s: float, on_result: OnResult) -> None:
        for t in tasks:
            await on_result(t, await self.agents[t.to].handle(t))


class FixtureGatherer:
    """Serves recorded EvidenceResults by signal (eval/cases): the parent's logic on recorded evidence."""

    def __init__(self, results: list[dict]):
        self.by_signal = {r["signal"]: r for r in results}

    async def gather(self, tasks: list[Task], timeout_s: float, on_result: OnResult) -> None:
        for t in tasks:
            rec = self.by_signal.get(t.signal)
            if rec is not None:
                await on_result(t, EvidenceResult.model_validate({**rec, "task_id": t.task_id,
                                                                  "investigation_id": t.investigation_id}))


@dataclass
class Request:
    mode: str = "incident_investigation"            # or proactive_watch
    node: str | None = None                          # display name (worker-1)
    switch: str | None = None
    port: str | None = None
    alarm: dict | None = None
    watch_item: WatchItem | None = None
    requested_by: str | None = None
    playbook: str | None = None
    trigger_signal: str | None = None                # rule that triggered it, if any
    signal_ids: list[str] = field(default_factory=list)


@dataclass
class Run:
    """Working state of one investigation."""
    inv_id: str
    playbook: Playbook
    params: dict[str, Any]
    evidence: dict[str, EvidenceItem] = field(default_factory=dict)        # signal -> item
    facts: dict[str, dict[str, Any]] = field(default_factory=dict)         # signal -> facts
    order: list[str] = field(default_factory=list)                          # signals in arrival order
    llm_used: bool = False
    audit_ids: list[str] = field(default_factory=list)


def clip(x: float) -> float:
    return max(0.0, min(1.0, x))


def label(c: float) -> str:
    return "high" if c >= 0.7 else "medium" if c >= 0.4 else "low"


class Investigator:
    def __init__(self, store: Store, topology: Topology, sources: Sources, playbooks: dict[str, Playbook],
                 escalation: Escalation, rules: dict[str, Any], gatherer: Gatherer, llm: LLMClient | None,
                 publish: Callable[[dict], Awaitable[None]] | None = None, clock: Callable[[], float] = time.time):
        self.store, self.topo, self.sources = store, topology, sources
        self.playbooks, self.escalation = playbooks, escalation
        self.cfg = rules["investigate"]
        self.gatherer, self.llm = gatherer, llm
        self.publish = publish
        self.clock = clock
        self.id_lock = asyncio.Lock()
        self.record = os.environ.get("WT_RECORD_EVIDENCE")

    # ------------------------------------------------------------ ids and progress

    async def new_id(self) -> str:
        async with self.id_lock:
            existing = self.store.next_investigation_id()
            n = max(int(existing[4:]), self.store.kv_get("inv_counter", 0) + 1)
            self.store.kv_put("inv_counter", n)
            return f"INV-{n:04d}"

    async def progress(self, inv_id: str, text: str, status: str = "running", publish_only: dict | None = None,
                       **extra: Any) -> None:
        """Record a progress step (kv, for the Copilot) and publish it on wt.investigations.
        publish_only: fields that go on the bus but not into the progress record (evidence items,
        the final document), so a recording holds everything the Copilot showed."""
        p = self.store.kv_get(f"progress:{inv_id}", {"status": "running", "steps": []})
        p["status"] = status
        p["steps"].append({"t": datetime.fromtimestamp(self.clock(), UTC).strftime("%H:%M:%S"), "text": text})
        p.update(extra)
        self.store.kv_put(f"progress:{inv_id}", p)
        if self.publish:
            await self.publish({"id": inv_id, "status": status, "text": text, **extra, **(publish_only or {})})

    # ------------------------------------------------------------ playbook and params

    def choose_playbook(self, req: Request) -> Playbook:
        if req.playbook:
            return self.playbooks[req.playbook]
        if req.node:
            return self.playbooks["ocp-node-flap"]
        for pb in self.playbooks.values():
            if req.trigger_signal and req.trigger_signal in pb.trigger_signals():
                return pb
            alarm = req.alarm or {}
            if any(t.get("alarm", {}).get("name") == alarm.get("name") for t in pb.triggers if "alarm" in t):
                return pb
        return self.playbooks["network-port-congestion"]

    def resolve(self, req: Request) -> dict[str, Any]:
        node = req.node
        if node:
            node = self.topo.node_name(node)
            switch, port = self.topo.port_of(node)
        else:
            switch, port = req.switch, req.port
            node = self.topo.node_on_port(switch, port) if switch and port else None
            node = node if node in self.topo.nodes() else None
        spec = self.topo.nodes().get(node) if node else None
        peers = [p for n, s in self.topo.nodes().items() if n != node and s.switch_port.startswith(f"{switch}:")
                 for p in [s.switch_port.split(":", 1)[1]]]
        return {"node": node, "kind_node": spec.kind_node if spec else None, "role": spec.role if spec else None,
                "switch": switch, "port": port, "peer_ports": peers, "window": self.cfg["window"],
                "minutes": int(self.cfg["window"].rstrip("m"))}

    def tasks_for(self, run: Run) -> list[Task]:
        pb, p = run.playbook, run.params
        wanted: dict[str, dict[str, Any]] = {}

        def want(signal: str | None, ask: str | None, live: bool = False, fact: bool = False) -> None:
            if not signal or not ask:
                return
            w = wanted.setdefault(signal, {"to": ask, "live": False, "fact": False})
            w["live"] |= live
            w["fact"] |= fact

        for f in pb.facts:
            want(f.signal, f.ask, fact=True)
        for h in pb.hypotheses:
            for e in h.expect:
                want(e.signal, e.ask, live=e.needs_live)
            for r in h.refute:
                want(getattr(r, "signal", None) or (r.model_extra or {}).get("signal"),
                     getattr(r, "ask", None) or (r.model_extra or {}).get("ask"))
        for cd in pb.cross_domain:
            if p.get("node") and cd.get("if_downstream_kind") == "ocp_node":
                for s in cd.get("signals", []):
                    want(s, cd["ask"], fact=True)
        budget = self.sources.probe_policy.max_probes_per_investigation - sum(1 for w in wanted.values() if w["live"])
        tasks = []
        for signal, w in wanted.items():
            if w["to"] == "openshift":
                if not p.get("kind_node"):
                    continue
                params = {"node": p["kind_node"], "window": p["window"]}
            else:
                params = {"switch": p["switch"], "port": p["port"], "minutes": p["minutes"], "window": p["window"]}
            tasks.append(Task(investigation_id=run.inv_id, to=w["to"], signal=signal, params=params,
                              needs_live=w["live"], allow_probe=w["fact"] and budget > 0,
                              why="fact" if w["fact"] else "hypothesis evidence"))
        return tasks

    # ------------------------------------------------------------ evidence

    async def collect(self, run: Run, tasks: list[Task]) -> None:
        async def on_result(task: Task, res: EvidenceResult) -> None:
            if self.record:
                Path(self.record).parent.mkdir(parents=True, exist_ok=True)
                with open(self.record, "a") as f:
                    f.write(res.model_dump_json() + "\n")
            if not res.ok:
                await self.progress(run.inv_id, f"{AGENT_LABEL.get(res.agent, res.agent)}: {task.signal} unavailable ({res.error})")
                return
            existing = run.evidence.get(task.signal)
            ev_id = existing.id if existing else f"ev-{len(run.evidence) + 1}"
            item = EvidenceItem(id=ev_id, domain=res.domain, source=res.source, query=res.query[:4000],
                                reason_for_probe=res.reason_for_probe, collected_at=res.collected_at,
                                excerpt=res.excerpt[:2000], finding=res.finding[:300] or task.signal,
                                interpreted_by="rule")
            run.evidence[task.signal] = item
            run.facts[task.signal] = res.facts
            if task.signal not in run.order:
                run.order.append(task.signal)
            if res.audit_id:
                run.audit_ids.append(res.audit_id)
            self.store.add_evidence(run.inv_id, item)
            tag = {"splunk": "Splunk", "solarwinds": "SolarWinds", "probe_cli": "probe · CLI",
                   "probe_k8s_api": "probe · API"}[res.source]
            await self.progress(run.inv_id, f"{ev_id} from {tag} ({AGENT_LABEL.get(res.agent, res.agent)}): {item.finding}",
                                evidence=len(run.evidence), publish_only={"evidence_item": dump(item)})

        await self.gatherer.gather(tasks, self.cfg["evidence_timeout_s"], on_result)

    async def wait_for_ready(self, run: Run) -> None:
        """A node still NotReady: re-check its NotReady/Ready events (and evictions) for a while."""
        deadline = self.clock() + self.cfg["wait_for_ready_s"]
        while run.facts.get("node_ready_events", {}).get("still_not_ready") and self.clock() < deadline:
            await self.progress(run.inv_id, f"{run.params['node']} is still NotReady: checking again in 20 s")
            await asyncio.sleep(20)
            again = [t for t in self.tasks_for(run) if t.signal in ("node_ready_events", "pod_evictions")]
            await self.collect(run, again)

    # ------------------------------------------------------------ scoring

    async def score(self, run: Run) -> list[Hypothesis]:
        out = []
        f = run.facts
        for h in run.playbook.hypotheses:
            total = sum(e.weight for e in h.expect)
            sup_w = ref_w = 0
            sup, ref, missing = [], [], []
            for e in h.expect:
                ev = run.evidence.get(e.signal) if e.signal else None
                verdict = PREDICATES[e.ev](f) if e.ev in PREDICATES else None
                if e.ev not in PREDICATES and ev is not None and self.llm:
                    interp = await jobs.interpret(self.llm, e.predicate, f"{ev.finding}\n{ev.excerpt}")
                    if interp:
                        run.llm_used = True
                        ev.interpreted_by = "llm"
                        verdict = {"supports": True, "refutes": False, "neutral": None}[interp.verdict]
                if verdict is True and ev:
                    sup_w += e.weight
                    sup.append(ev.id)
                elif verdict is None and ev is None:
                    missing.append(e.ev)
            for r in h.refute:
                sig = (r.model_extra or {}).get("signal")
                ev = run.evidence.get(sig) if sig else None
                verdict = PREDICATES[r.ev](f) if r.ev in PREDICATES else None
                if verdict is True and ev:
                    ref_w += r.weight
                    ref.append(ev.id)
            rule = round(clip((sup_w - ref_w) / total), 3) if total else 0.0
            out.append(Hypothesis(id=h.id, statement=self.fill(h.statement, run), proposed_by="playbook",
                                  supporting=sorted(set(sup), key=sup.index), refuting=sorted(set(ref), key=ref.index),
                                  missing=missing, confidence=rule, confidence_rule=rule))
        return out

    def fill(self, text: str, run: Run) -> str:
        p = run.params
        node = p.get("node") or "the node"
        return (text.replace("{node}", node).replace("the node's", f"{node}'s")
                .replace("on the node", f"on {node}"))

    # ------------------------------------------------------------ the whole investigation

    async def investigate(self, req: Request, inv_id: str | None = None) -> Investigation:
        inv_id = inv_id or await self.new_id()
        opened = datetime.fromtimestamp(self.clock(), UTC)
        pb = self.choose_playbook(req)
        params = self.resolve(req)
        run = Run(inv_id=inv_id, playbook=pb, params=params)
        what = params["node"] or f"{params['switch']} {params['port']}"
        await self.progress(inv_id, f"Opened for {what} with playbook {pb.id} "
                                    f"({req.requested_by or 'WatchTower'})", playbook=pb.id, node=params["node"],
                            opened_at=opened.isoformat(), mode=req.mode)
        tasks = self.tasks_for(run)
        await self.progress(inv_id, f"Tasked the domain agents for {len(tasks)} evidence items "
                                    f"({sum(1 for t in tasks if t.to == 'openshift')} infra, "
                                    f"{sum(1 for t in tasks if t.to == 'network')} network; "
                                    f"{sum(1 for t in tasks if t.needs_live)} live probe)", tasks=len(tasks))
        await self.collect(run, tasks)
        if pb.id == "ocp-node-flap" and req.mode == "incident_investigation":
            await self.wait_for_ready(run)
        await self.progress(inv_id, "Scoring hypotheses from the evidence")
        hyps = await self.score(run)
        hyps += await self.extra_hypotheses(run, hyps)
        hyps.sort(key=lambda h: (-h.confidence, h.id))
        conclusion, adj = await self.conclude(run, hyps)
        if adj and hyps and adj.delta:
            top = hyps[0]
            cap = pb.confidence.llm_adjust_max if pb.confidence else 0.10
            delta = max(-cap, min(cap, adj.delta))
            top.llm_adjustment = LlmAdjustment(delta=round(delta, 2), reason=adj.reason)
            top.confidence = round(clip(top.confidence + delta), 3)
            hyps.sort(key=lambda h: (-h.confidence, h.id))
            conclusion.confidence_label = label(hyps[0].confidence)
        actions, note_summary = await self.actions(run, hyps)
        escalation = self.route(run, hyps, conclusion, actions, note_summary, req)
        evidence = [run.evidence[s] for s in run.order]
        inv = Investigation(
            id=inv_id, opened_at=opened, trigger=self.trigger(req), playbook=pb.id,
            observations=self.observations(run, req), evidence=evidence,
            correlations=self.correlations(run), hypotheses=hyps, conclusion=conclusion,
            recommended_actions=actions, escalation=escalation, llm_used=run.llm_used,
            audit_ids=run.audit_ids[-50:] or None, reuse=Reuse.from_evidence(evidence),
        )
        self.store.save_investigation(inv)
        self.store.kv_put(f"timeline:{inv_id}", self.timeline(run))
        await self.progress(inv_id, f"Done: {hyps[0].statement} ({conclusion.confidence_label}, "
                                    f"{hyps[0].confidence:.2f}); escalate to {escalation.team} {escalation.priority}",
                            status="done", publish_only={"investigation": dump(inv), "timeline": self.timeline(run)})
        return inv

    # ------------------------------------------------------------ LLM jobs with rule fallbacks

    async def extra_hypotheses(self, run: Run, hyps: list[Hypothesis]) -> list[Hypothesis]:
        if not self.llm:
            return []
        text = ("Listed: " + "; ".join(h.statement for h in hyps) + ".\nFindings: "
                + "; ".join(e.finding for e in list(run.evidence.values())[:10]))
        res = await jobs.extra_hypotheses(self.llm, text[:1800])
        if not res:
            return []
        run.llm_used = True
        known = {h.statement.lower() for h in hyps}
        out = []
        for i, x in enumerate(res.hypotheses[:2]):
            if x.statement.lower() in known:
                continue
            out.append(Hypothesis(id=f"H{len(hyps) + i + 1}", statement=x.statement, proposed_by="llm",
                                  supporting=[], refuting=[], missing=[f"evidence for: {x.why}"[:200]],
                                  confidence=0.0, confidence_rule=0.0))
        return out

    def fact_summary(self, run: Run) -> dict[str, Any]:
        f, p = run.facts, run.params
        flap = f.get("node_ready_events", {})
        reboot = f.get("node_reboot_check", {})
        pods = f.get("static_pod_restarts", {})
        evict = f.get("pod_evictions", {})
        disc = f.get("interface_discards", {})
        calls = f.get("api_svc_calls", {})
        return {
            "node": p.get("node"), "role": p.get("role"), "switch": p.get("switch"), "port": p.get("port"),
            "gap_s": flap.get("gap_s"), "not_ready_at": flap.get("not_ready_at"), "ready_at": flap.get("ready_at"),
            "flaps": flap.get("flaps", 0), "still_not_ready": flap.get("still_not_ready", False),
            "rebooted": bool(reboot.get("rebooted")), "kubelet_restarted": bool(reboot.get("kubelet_restarted")),
            "reboot_checked": "node_reboot_check" in f,
            "pod_restarts": pods.get("restarts", 0), "evictions": evict.get("evictions", 0),
            "cancelled": evict.get("cancelled", 0), "drops_ongoing": bool(disc.get("latest_two_rising")),
            "drops_total": disc.get("total_out_discards"), "crc_flat": disc.get("errors_flat"),
            "failed_calls": calls.get("failed"), "calls": calls.get("calls"),
            "success_affected": calls.get("success_rate_affected"),
        }

    def rule_conclusion(self, run: Run, top: Hypothesis, key: str) -> tuple[str, str]:
        s = self.fact_summary(run)
        node, switch, port = s["node"] or "the node", s["switch"], s["port"]
        conf = f"{label(top.confidence)}, {top.confidence:.2f}"
        if top.confidence < 0.4:
            cause = f"Inconclusive: no hypothesis has enough evidence (best: {top.statement}, {conf})"
        elif key == "network_path_congestion":
            cause = (f"Congestion on {node}'s switch port ({switch} {port}) delayed its API calls"
                     + (f", so it missed lease renewals for {GRACE_S} s and was marked NotReady for {s['gap_s']} s"
                        if s["gap_s"] is not None
                        else " and lease renewals"))
        elif key == "microburst_congestion":
            cause = f"Bursty traffic overflows the egress queue on {switch} {port} while average utilisation looks normal"
        else:
            cause = f"{top.statement} ({conf})"
        parts = []
        if s["gap_s"] is not None:
            parts.append(f"{node} had a transient NotReady (node flap): it missed lease renewals for the {GRACE_S} s "
                         f"grace period, was marked NotReady at {s['not_ready_at']} UTC and was Ready again at "
                         f"{s['ready_at']} UTC, {s['gap_s']} s later.")
        elif s["still_not_ready"]:
            parts.append(f"{node} went NotReady at {s['not_ready_at']} UTC and had not reported Ready yet.")
        if s["reboot_checked"]:
            parts.append(f"It {'did reboot (Rebooted event)' if s['rebooted'] else 'recovered without a reboot'}"
                         + ("" if s["rebooted"] else f" (no Rebooted or Starting events for {node})")
                         + ("; the kubelet restarted" if s["kubelet_restarted"] and not s["rebooted"] else "") + ".")
        if "static_pod_restarts" in run.facts:
            what = "its API server and etcd" if s["role"] == "master" else "no control-plane pods"
            parts.append(f"{what[0].upper() + what[1:]} {'did not restart' if s['role'] == 'master' else 'restarted'}"
                         f" (no Killing/Started events)." if not s["pod_restarts"] else
                         f"{s['pod_restarts']} control-plane pod restart event(s).")
        if "pod_evictions" in run.facts:
            if s["evictions"]:
                parts.append(f"{s['evictions']} pod(s) were evicted.")
            elif s["gap_s"] is not None:
                parts.append(f"Pods were not evicted: NotReady lasted {s['gap_s']} s, and eviction starts at "
                             f"{EVICTION_S} s.")
            else:
                parts.append("No pods were evicted.")
        if s["drops_total"]:
            parts.append(f"Output drops on {port}: {s['drops_total']:,} in the window"
                         + (", still rising" if s["drops_ongoing"] else ", stopped") + "; CRC "
                         + ("flat" if s["crc_flat"] else "rising") + ".")
        return cause[:300], " ".join(parts)[:800]

    async def conclude(self, run: Run, hyps: list[Hypothesis]) -> tuple[Conclusion, jobs.Adjustment | None]:
        top = hyps[0]
        key = next((h.key for h in run.playbook.hypotheses if h.id == top.id), "")
        cause, happened = self.rule_conclusion(run, top, key)
        adj = None
        if self.llm and top.confidence >= 0.4:
            s = self.fact_summary(run)
            text = (f"Top: {top.statement} ({top.confidence:.2f}). Facts: "
                    + (f"NotReady {s['not_ready_at']}, Ready {s['ready_at']} ({s['gap_s']} s); " if s["gap_s"] else "")
                    + f"{'Rebooted event' if s['rebooted'] else 'no Rebooted/Starting events'}; "
                    + f"{s['pod_restarts']} control-plane restart events; {s['evictions']} evictions; "
                    + (f"drops on {s['port']} {'still rising' if s['drops_ongoing'] else 'stopped'}; " if s["drops_total"] else "")
                    + f"CRC {'flat' if s['crc_flat'] else 'rising'}; "
                    + (f"{s['failed_calls']} of {s['calls']} Service calls failed." if s["calls"] else "")
                    + f"\nRule text (keep its facts): {happened}")
            res = await jobs.explain(self.llm, text[:1800], rebooted=s["rebooted"])
            if res:
                run.llm_used = True
                adj = res.adjustment                # capped at ±llm_adjust_max by the caller
                if self.covers_facts(f"{res.likely_cause} {res.what_happened}", run, s):
                    cause, happened = res.likely_cause, res.what_happened
                # else: the model dropped a fact, so the rule wording stays
        refs = list(dict.fromkeys(top.supporting + [run.evidence[s].id for s in
                                                    ("node_ready_events", "node_reboot_check", "pod_evictions")
                                                    if s in run.evidence]))
        sources = {e.source for e in run.evidence.values() if e.id in refs}
        if len(sources) < 2:                       # a conclusion cites at least two sources
            refs += [e.id for e in run.evidence.values() if e.source not in sources][:1]
        unknowns = [f"{m} (no evidence)" for m in top.missing]
        if key in ("network_path_congestion", "microburst_congestion"):
            unknowns.append(f"what traffic is filling {run.params.get('port')}")
        return Conclusion(likely_cause=cause, what_happened=happened or "No evidence came back from the domain agents.",
                          confidence_label=label(top.confidence), unknowns=unknowns or None,
                          refs=refs or ([next(iter(run.evidence.values())).id] if run.evidence else ["obs-1"])), adj

    @staticmethod
    def covers_facts(text: str, run: Run, s: dict[str, Any]) -> bool:
        """The model's explanation must keep every key fact the rules established."""
        t = text.lower()
        need = []
        if s["gap_s"] is not None:
            need.append(f"{s['gap_s']} s" if f"{s['gap_s']} s" in t else f"{s['gap_s']} seconds")
        if s["reboot_checked"]:
            need.append("reboot")
        if "static_pod_restarts" in run.facts:
            need.append("restart")
        if "pod_evictions" in run.facts:
            need.append("evict")
        return all(n in t for n in need)

    def render(self, run: Run, template: str | None) -> str | None:
        if not template:
            return None
        p = run.params
        return (template.replace("{node}", p.get("kind_node") or p.get("node") or "")
                .replace("{port}", p.get("port") or "")
                .replace("{peer_ports}", ",".join(p.get("peer_ports") or []) or p.get("port") or ""))

    async def actions(self, run: Run, hyps: list[Hypothesis]) -> tuple[list[RecommendedAction], str | None]:
        pb = run.playbook
        top_spec = next((h for h in pb.hypotheses if h.id == hyps[0].id), None)
        order = list(top_spec.actions_catalogue) if top_spec and top_spec.actions_catalogue else []
        general = ["node_journal"] if run.params.get("node") and "node_journal" in pb.actions else []
        catalogue = order + [k for k in general if k not in order]           # what the model may pick
        rest = [k for k in pb.actions if k not in catalogue]                  # rule fallback tops up from here
        if len(catalogue) < 3:
            catalogue += rest[: 3 - len(catalogue)]
        why_rule = {
            "top_talkers": f"Find what fills {run.params.get('port')}'s egress queue",
            "queue_stats": "Confirm the drops sit in one queue class (bursts, not errors)",
            "lease_watch": f"See whether {run.params.get('node')}'s renewals slip again while drops continue",
            "check_peer_ports": "Compare with the other nodes' ports: is it one port or the whole switch?",
            "node_journal": "Read the kubelet's own account of the failed renewals",
            "etcd_health": "Rule out etcd latency on the control plane",
            "policy_check": "See whether the egress QoS policy leaves enough buffer for bursts",
            "error_counters": f"Confirm the CRC errors on {run.params.get('port')} are still rising",
            "transceiver": "Low or unstable light levels point at the optic or the fibre",
            "link_log": "See how often the link went down and when it started",
            "node_top": f"Find the process starving {run.params.get('node')}",
        }
        chosen: list[tuple[str, str]] = [(k, why_rule.get(k, pb.actions[k].step)) for k in order[:4]]
        for k in catalogue:
            if len(chosen) >= 3:
                break
            if k not in dict(chosen):
                chosen.append((k, why_rule.get(k, pb.actions[k].step)))
        note_summary = None
        if self.llm and hyps[0].confidence >= 0.4:
            s = self.fact_summary(run)
            text = (f"Top: {hyps[0].statement}. Catalogue: {', '.join(catalogue)}. Facts: "
                    + (f"drops on {s['port']} {'ongoing' if s['drops_ongoing'] else 'stopped'}; " if s["drops_total"] else "")
                    + (f"one flap, NotReady {s['gap_s']} s; " if s["gap_s"] else "") + f"node {s['node']} ({s['role']}).")
            res = await jobs.next_steps(self.llm, text, catalogue)
            if res:
                run.llm_used = True
                chosen = [(st.key, st.why) for st in res.steps]
                note_summary = res.note_summary
        acts = [RecommendedAction(step=self.render(run, pb.actions[k].step) or pb.actions[k].step,
                                  command=self.render(run, pb.actions[k].command), why=why, kind="diagnostic",
                                  from_catalogue=k) for k, why in chosen]
        return acts, note_summary

    # ------------------------------------------------------------ escalation

    def route(self, run: Run, hyps: list[Hypothesis], conclusion: Conclusion, actions: list[RecommendedAction],
              note_summary: str | None, req: Request) -> EscalationOut:
        top = hyps[0]
        spec = next((h for h in run.playbook.hypotheses if h.id == top.id), None)
        team_key = self.escalation.team_for(spec.key, spec.escalate_to) if spec else "sre_lead"
        team = self.escalation.teams[team_key].name if team_key in self.escalation.teams else team_key
        s = self.fact_summary(run)
        node = s["node"] or "the node"
        if s["still_not_ready"] or (s["gap_s"] or 0) > EVICTION_S:
            priority, reason = "P2", f"{node} NotReady over {EVICTION_S} s (eviction risk)" if (s["gap_s"] or 0) > EVICTION_S \
                else f"{node} still NotReady"
        elif s["role"] == "master" and s["flaps"] > 1:
            priority, reason = "P2", f"{node} (a master) flapped {s['flaps']} times"
        elif s["flaps"] >= 1:
            flaps = "one flap" if s["flaps"] == 1 else f"{s['flaps']} flaps of a worker in {run.params['window']}"
            priority, reason = (("P3", f"drops ongoing, {flaps}, no outage") if s["drops_ongoing"]
                                else ("P4", f"cause cleared, {flaps}, no outage"))
        elif req.mode == "proactive_watch":
            priority, reason = "heads_up", "proactive watch item, no impact yet"
        else:
            priority, reason = "P4", "no impact found"
        if top.confidence < 0.4:
            team, reason = self.escalation.teams.get("sre_lead").name, "inconclusive: " + reason
        raise_if = f"Raise to P2 if {node} flaps again within 60 min or stays NotReady over {EVICTION_S} s"
        note = self.note(run, hyps, conclusion, actions, team, priority, reason, raise_if, note_summary)
        return EscalationOut(team=team, priority=priority, reason=reason, raise_if=raise_if, note_markdown=note)

    def note(self, run: Run, hyps: list[Hypothesis], c: Conclusion, actions: list[RecommendedAction], team: str,
             priority: str, reason: str, raise_if: str, summary: str | None) -> str:
        tag = {"splunk": "Splunk SPL", "solarwinds": "SolarWinds SWQL", "probe_cli": "Probe (CLI, read-only)",
               "probe_k8s_api": "Probe (Kubernetes API, read-only)"}
        lines = [f"**WatchTower {run.inv_id} → {team} · {priority}** ({reason})", ""]
        if summary:
            lines += [summary, ""]
        lines += [f"**Likely cause:** {c.likely_cause} (confidence {c.confidence_label}, {hyps[0].confidence:.2f})",
                  f"**What happened:** {c.what_happened}", ""]
        ruled = [h for h in hyps[1:] if h.proposed_by == "playbook"]
        if ruled:
            lines.append("**Ruled out / unlikely:** " + "; ".join(f"{h.statement} ({h.confidence:.2f})" for h in ruled))
        tl = self.timeline(run)
        if tl:
            lines += ["", "**Timeline (UTC):**"] + [f"- {t['t']} {t['text']}" for t in tl]
        lines += ["", "**Evidence:**"]
        for sig in run.order:
            e = run.evidence[sig]
            lines.append(f"- [{e.id}] {tag[e.source]}: {e.finding}\n  `{e.query.splitlines()[0][:220]}`")
        lines += ["", "**Recommended next steps (read-only):**"]
        lines += [f"{i}. {a.step}" + (f": `{a.command}`" if a.command else "") for i, a in enumerate(actions, 1)]
        lines += ["", raise_if + ".", f"Investigation {run.inv_id} in WatchTower. WatchTower changed nothing."]
        return "\n".join(lines)

    # ------------------------------------------------------------ chain pieces

    def trigger(self, req: Request) -> Trigger:
        alarm = Alarm(**req.alarm) if req.alarm else None
        return Trigger(mode=req.mode, watch_item_id=req.watch_item.id if req.watch_item else None, alarm=alarm,
                       requested_by=req.requested_by)

    def observations(self, run: Run, req: Request) -> list[Observation]:
        obs: list[Observation] = []
        node = run.params.get("node")
        if req.alarm:
            obs.append(Observation(id="obs-1", text=f"{(req.alarm.get('tool') or '').title()} alert "
                                                    f"{req.alarm.get('name')} fired for {req.alarm.get('entity')}",
                                   signal_ids=[]))
        since = datetime.fromtimestamp(self.clock() - 1800, UTC).isoformat()
        entities = {node, f"{run.params.get('switch')}:{run.params.get('port')}"}
        latest: dict[str, Any] = {}
        for sig in self.store.list_signals(since=since, limit=500):
            if sig.entity.name in entities and sig.rule not in latest:
                latest[sig.rule] = sig
        if req.watch_item:
            for sig in req.watch_item.signals:
                latest.setdefault(sig.rule, sig)
        for sig in sorted(latest.values(), key=lambda s: s.ts):
            obs.append(Observation(id=f"obs-{len(obs) + 1}", text=sig.summary, signal_ids=[sig.id]))
        if not obs:
            obs.append(Observation(id="obs-1", text=f"Investigation requested for {node or run.params.get('port')}",
                                   signal_ids=list(req.signal_ids)))
        return obs

    def correlations(self, run: Run) -> list[Correlation]:
        p, f, ev = run.params, run.facts, run.evidence
        out: list[Correlation] = []
        node, switch, port = p.get("node"), p.get("switch"), p.get("port")
        disc, calls = f.get("interface_discards", {}), f.get("api_svc_calls", {})
        if node and "interface_discards" in ev and "api_svc_calls" in ev:
            text = f"{port} on {switch} feeds {node} (topology.yaml)."
            first_drop, first_fail = disc.get("first_drop_at"), calls.get("first_fail")
            if first_drop and first_fail:
                d = (datetime.fromisoformat(first_fail.replace("Z", "+00:00")) -
                     datetime.fromisoformat(first_drop.replace("Z", "+00:00"))).total_seconds() / 60
                text += (f" Output drops started {abs(d):.0f} min {'before' if d >= 0 else 'after'} the first Service "
                         "timeout." if abs(d) >= 1 else " Output drops and the first Service timeouts started in the same minute.")
            out.append(Correlation(id="cor-1", kind="topology+time", text=text,
                                   refs=[ev["interface_discards"].id, ev["api_svc_calls"].id]))
        if node and "api_svc_calls" in ev:
            masters = [n for n, s in self.topo.nodes().items() if s.role == "master"]
            refs = [ev["api_svc_calls"].id] + ([ev["api_svc_timeouts_by_node"].id] if "api_svc_timeouts_by_node" in ev else [])
            if p.get("role") == "master" and len(masters) > 1:
                text = (f"The kubernetes Service balances across all {len(masters)} API servers, so calls to the other "
                        f"{len(masters) - 1} masters leave {node} and cross the congested port; only the local one "
                        f"stays on the node (about {len(masters) - 1} in {len(masters)} calls can fail).")
            else:
                text = (f"{node} has no API server of its own, so every call to the kubernetes Service leaves the node "
                        f"through {port}; calls failed only while the port was congested.")
            if f.get("api_svc_timeouts_by_node", {}).get("only_node"):
                text += f" Only {node} sees timeouts, so the API servers themselves are fine."
            out.append(Correlation(id=f"cor-{len(out) + 1}", kind="topology", text=text, refs=refs))
        lease, flap = f.get("lease_update_failures", {}), f.get("node_ready_events", {})
        if "lease_update_failures" in ev and "node_ready_events" in ev and lease.get("count") and flap.get("not_ready_at"):
            out.append(Correlation(id=f"cor-{len(out) + 1}", kind="time",
                                   text=f"The kubelet logged {lease['count']} lease update failures from "
                                        f"{_hm(lease.get('first'))}; the node was marked NotReady at {flap['not_ready_at']} "
                                        f"after {GRACE_S} s without a renewal.",
                                   refs=[ev["lease_update_failures"].id, ev["node_ready_events"].id]))
        return out

    def timeline(self, run: Run) -> list[dict[str, Any]]:
        f, ev = run.facts, run.evidence
        items: list[tuple[str, str, str | None]] = []
        disc, calls, lease, flap = (f.get(k, {}) for k in ("interface_discards", "api_svc_calls",
                                                            "lease_update_failures", "node_ready_events"))
        port, node = run.params.get("port"), run.params.get("node")
        if disc.get("first_drop_at"):
            items.append((_hm(disc["first_drop_at"]), f"drops start on {port}", ev.get("interface_discards") and ev["interface_discards"].id))
        if calls.get("first_fail"):
            items.append((_hm(calls["first_fail"]), "first Service timeouts", ev["api_svc_calls"].id))
        if lease.get("first"):
            items.append((_hm(lease["first"]), "lease update failures", ev["lease_update_failures"].id))
        if flap.get("not_ready_at"):
            items.append((flap["not_ready_at"], f"{node} NotReady", ev["node_ready_events"].id))
        if flap.get("ready_at"):
            items.append((flap["ready_at"], f"{node} Ready", ev["node_ready_events"].id))
        if disc.get("latest_two_rising") and disc.get("newest_poll"):
            items.append((_hm(disc["newest_poll"]), "drops continue", ev["interface_discards"].id))
        return [{"t": t, "text": text, "ev": e} for t, text, e in sorted(items, key=lambda x: x[0])]


def _hm(iso: str | None) -> str:
    if not iso:
        return "?"
    return datetime.fromisoformat(str(iso).replace("Z", "+00:00")).astimezone(UTC).strftime("%H:%M:%S")
