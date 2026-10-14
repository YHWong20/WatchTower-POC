"""Correlator (deterministic): groups signals by topology and time into watch items.

A signal is anchored to an OpenShift node when it can be: a node signal is its node; an
interface signal is the node that port feeds (topology.yaml). Signals with the same anchor
inside a 10-minute window form one watch item. One domain stays low; two domains linked by
topology score medium; a state change (node NotReady) scores high and opens an investigation.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import Any, Literal

from pydantic import BaseModel, Field

from wt.common.catalog import Escalation, Playbook, Topology
from wt.common.contracts import Domain, Signal

Status = Literal["watching", "emerging", "investigating", "closed", "dismissed"]


class Suggestion(BaseModel):
    step: str
    command: str | None = None


class WatchItem(BaseModel):
    id: str
    opened_at: datetime
    updated_at: datetime
    status: Status = "watching"
    scope: Literal["single_domain", "cross_domain"] = "single_domain"
    anchor: str                                   # node display name, or switch:port when no node
    node: str | None = None                       # display name (master-2)
    kind_node: str | None = None                  # kind node name (ocp-sim-control-plane2)
    switch: str | None = None
    port: str | None = None
    title: str
    impact: str = ""
    domains: list[Domain] = []
    signals: list[Signal] = []                    # latest signal per (rule, entity) in the window
    signal_ids: list[str] = []                    # every signal ever grouped here
    confidence: float = 0.0
    confidence_label: Literal["low", "medium", "high"] = "low"
    correlation: str = ""
    reuse: dict[str, Any] = Field(default_factory=lambda: {"reused": 0, "probed": 0, "rate": 1.0})
    suggestions: list[Suggestion] = []
    investigation_id: str | None = None
    summary: str | None = None                    # "Explain in plain words" (LLM job 1), cached
    summary_by: Literal["llm", "rule"] | None = None


def label(conf: float) -> Literal["low", "medium", "high"]:
    return "high" if conf >= 0.7 else "medium" if conf >= 0.4 else "low"


class Correlator:
    def __init__(self, topology: Topology, rules: dict[str, Any], playbooks: dict[str, Playbook],
                 escalation: Escalation, new_id):
        self.topo = topology
        self.cfg = rules["correlation"]
        self.playbooks = playbooks
        self.escalation = escalation
        self.new_id = new_id
        self.items: dict[str, WatchItem] = {}

    # ------------------------------------------------------------ anchoring

    def anchor(self, sig: Signal) -> dict[str, str | None]:
        name = sig.entity.name
        if sig.entity.kind == "node":
            try:
                node = self.topo.node_name(name)
            except KeyError:
                return {"anchor": name, "node": name}
            switch, port = self.topo.port_of(node)
            return {"anchor": node, "node": node, "kind_node": self.topo.nodes()[node].kind_node,
                    "switch": switch, "port": port}
        if sig.entity.kind == "interface" and ":" in name:
            switch, port = name.split(":", 1)
            node = self.topo.node_on_port(switch, port)
            if node and node in self.topo.nodes():
                return {"anchor": node, "node": node, "kind_node": self.topo.nodes()[node].kind_node,
                        "switch": switch, "port": port}
            return {"anchor": name, "switch": switch, "port": port}
        return {"anchor": name}

    # ------------------------------------------------------------ grouping

    def add(self, sig: Signal) -> WatchItem:
        a = self.anchor(sig)
        now = sig.ts
        item = next((i for i in self.items.values() if i.anchor == a["anchor"]
                     and i.status not in ("closed", "dismissed")), None)
        if item is None:
            item = WatchItem(id=self.new_id(), opened_at=now, updated_at=now, title="", **a)
            self.items[item.id] = item
        item.signals = [s for s in item.signals if (s.rule, s.entity.name) != (sig.rule, sig.entity.name)] + [sig]
        if sig.id not in item.signal_ids:
            item.signal_ids.append(sig.id)
        for k in ("node", "kind_node", "switch", "port"):
            if a.get(k) and not getattr(item, k):
                setattr(item, k, a[k])
        item.updated_at = max(item.updated_at, now)
        self.score(item, now)
        return item

    def expire(self, now: datetime) -> list[WatchItem]:
        """Close items whose signals all aged out; returns the items that changed."""
        changed = []
        for item in self.items.values():
            if item.status in ("closed", "dismissed"):
                continue
            if now - item.updated_at > timedelta(seconds=self.cfg["close_after_s"]):
                item.status = "closed"
                changed.append(item)
            else:
                before = item.confidence
                self.score(item, now)
                if item.confidence != before:
                    changed.append(item)
        return changed

    # ------------------------------------------------------------ scoring and wording

    def score(self, item: WatchItem, now: datetime) -> None:
        window = timedelta(seconds=self.cfg["window_s"])
        live = [s for s in item.signals if now - s.ts <= window]
        domains = sorted({s.domain for s in live})
        rules = {s.rule for s in live}
        item.domains = domains or item.domains
        state_change = any(s.severity == "state_change" for s in live)
        if state_change:
            conf = self.cfg["state_change"]
        elif len(domains) >= 2 and item.node:
            c = self.cfg["cross_domain"]
            conf = c["base"] + c["per_extra_rule"] * max(len(rules) - 2, 0)
            per_domain_first = {d: min(s.ts for s in live if s.domain == d) for d in domains}
            if all(now - t >= timedelta(seconds=c["persistence_after_s"]) for t in per_domain_first.values()):
                conf += c["persistence"]
            conf = min(conf, c["max"])
        elif live:
            c = self.cfg["single_domain"]
            conf = min(c["base"] + c["per_extra_rule"] * max(len(rules) - 1, 0), c["max"])
        else:
            conf = item.confidence
        item.scope = "cross_domain" if len(domains) >= 2 and item.node else "single_domain"
        item.confidence = round(conf, 2)
        item.confidence_label = label(item.confidence)
        if item.status in ("watching", "emerging"):
            item.status = "emerging" if item.scope == "cross_domain" or state_change else "watching"
        tools = [s.source.tool for s in item.signals]
        probed = sum(1 for t in tools if t.startswith("probe"))
        item.reuse = {"reused": len(tools) - probed, "probed": probed,
                      "rate": round((len(tools) - probed) / len(tools), 3) if tools else 1.0}
        self.words(item, live, state_change)

    def words(self, item: WatchItem, live: list[Signal], state_change: bool) -> None:
        rules = {s.rule for s in live}
        node, switch, port = item.node, item.switch, item.port
        timeouts = "ocp.api_svc_timeouts" in rules
        drops = "net.output_drops_rising" in rules
        if state_change:
            item.title = f"{node} went NotReady" + (f" while its switch port {port} is dropping packets" if drops else "")
            item.impact = "Node state changed: WatchTower opens an investigation."
        elif item.scope == "cross_domain":
            first = ("API calls from " + node + " are timing out") if timeouts else f"{node} shows control-plane symptoms"
            item.title = f"{first} while its switch port starts dropping packets" if drops else \
                f"{first} while its switch port {port} shows trouble"
            item.impact = (f"No service impact yet. If this grows, {node} may miss its 40 s heartbeat window "
                           "and be marked NotReady.")
        elif timeouts:
            item.title = f"API calls from {node} to the kubernetes Service are timing out"
            item.impact = "Only infra signals so far; no network signal linked yet."
        elif drops:
            item.title = f"Output drops rising on {switch} {port}" + (f" (feeds {node})" if node else "")
            item.impact = "Only network signals so far; nothing on the infra side yet." if node else \
                "Not linked to any infra node."
        else:
            item.title = live[-1].summary if live else item.title
            item.impact = ""
        parts = []
        if node and switch:
            parts.append(f"Topology: {port} on {switch} feeds {node}.")
        if len({s.domain for s in live}) >= 2:
            firsts = sorted(min(s.ts for s in live if s.domain == d) for d in {s.domain for s in live})
            gap = (firsts[-1] - firsts[0]).total_seconds()
            parts.append(f"The {'infra and network' if len(firsts) == 2 else ''} signals started within "
                         f"{max(1, round(gap / 60))} min of each other.")
        item.correlation = " ".join(parts)
        item.suggestions = self.suggest(item, drops, timeouts)

    def suggest(self, item: WatchItem, drops: bool, timeouts: bool) -> list[Suggestion]:
        pb = self.playbooks.get("ocp-node-flap") if item.node else self.playbooks.get("network-port-congestion")
        out = []
        params = {"port": item.port or "", "node": item.kind_node or item.node or ""}
        if pb and item.port and "queue_stats" in pb.actions:
            a = pb.actions["queue_stats"]
            out.append(Suggestion(step=f"Check queue drops on {item.port}", command=a.command.format(**params)))
        if pb and item.node and "lease_watch" in pb.actions:
            out.append(Suggestion(step=f"Check {item.node} lease renewals",
                                  command=f"kubectl get lease {params['node']} -n kube-node-lease -o yaml"))
        team = self.escalation.teams.get("network" if drops else "platform")
        if item.status != "investigating":
            out.append(Suggestion(step=f"No escalation yet. Give the {team.name if team else 'owning team'} a "
                                       "heads-up if the signals keep rising for 10 more minutes."))
        return out


def watch_item_text(item: WatchItem) -> str:
    """Compact input for LLM job 1 and the Ask context."""
    lines = [f"Watch item: {item.scope.replace('_', '-')}, {('node ' + item.node) if item.node else item.anchor}, "
             f"confidence {item.confidence}."]
    lines.append("Signals:")
    lines += [f"- {s.summary}" for s in item.signals[-6:]]
    if item.correlation:
        lines.append(f"Link: {item.correlation}")
    return "\n".join(lines)


def utc(ts: float) -> datetime:
    return datetime.fromtimestamp(ts, UTC)
