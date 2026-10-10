"""Loaders for the YAML contracts: sources.yaml, topology.yaml, escalation.yaml, playbooks/*.yaml.

These files are the rulebook, so the models are strict about what the code relies on and
lenient (extra="allow") about descriptive keys such as `meaning`, `reading` or `note`.
"""

from __future__ import annotations

import re
from pathlib import Path
from typing import Literal

import yaml
from pydantic import BaseModel, ConfigDict, Field

from wt.common.config import Profile, settings


class _Loose(BaseModel):
    model_config = ConfigDict(extra="allow")


# ---------------------------------------------------------------- sources.yaml


class ProbeFallback(_Loose):
    tool: Literal["probe_cli", "probe_k8s_api"]
    call: str
    when: str


class SourceSignal(_Loose):
    primary: str | list[str]
    query: str | None = None
    prod_query: str | None = None
    command: str | None = None
    call: str | None = None
    when: str | None = None
    query_splunk: str | None = None
    query_solarwinds: str | None = None
    probe_fallback: ProbeFallback | None = None

    def template_for(self, tool: str) -> str:
        """The query, command or call this signal uses for `tool`."""
        specific = getattr(self, f"query_{tool}", None)
        if specific:
            return specific
        primaries = self.primary if isinstance(self.primary, list) else [self.primary]
        if tool in primaries:
            text = self.query or self.command or self.call
            if text:
                return text
        if self.probe_fallback and self.probe_fallback.tool == tool:
            return self.probe_fallback.call
        raise KeyError(f"signal has no template for tool {tool!r}")


class ProbePolicy(_Loose):
    target_reuse_rate: float
    max_probes_per_investigation: int
    min_interval_per_device_s: int
    record: list[str]


class Sources(_Loose):
    tools: dict[str, dict]
    signals: dict[str, SourceSignal]
    probe_policy: ProbePolicy

    @property
    def cli_allow(self) -> str:
        return self.tools["probe_cli"]["allow"]

    @property
    def k8s_verbs(self) -> list[str]:
        return list(self.tools["probe_k8s_api"]["verbs"])

    def template(self, key: str, tool: str | None = None) -> str:
        sig = self.signals[key]
        if tool is None:
            tool = sig.primary if isinstance(sig.primary, str) else sig.primary[0]
        return sig.template_for(tool)


# ---------------------------------------------------------------- topology.yaml


class Port(_Loose):
    connects_to: str
    kind: Literal["ocp_node", "uplink", "downlink"]


class Switch(_Loose):
    role: str
    mgmt: dict[str, str]
    ports: dict[str, Port]


class Node(_Loose):
    role: Literal["master", "worker"]
    kind_node: str
    switch_port: str


class Cluster(_Loose):
    platform: str
    api_service_vip: str
    nodes: dict[str, Node]


class Topology(_Loose):
    switches: dict[str, Switch]
    clusters: dict[str, Cluster]

    # Nodes are named by display name (master-2); kind node names (ocp-sim-control-plane2)
    # are accepted wherever a node is expected, since Splunk host= fields carry those.

    def nodes(self) -> dict[str, Node]:
        return {name: n for c in self.clusters.values() for name, n in c.nodes.items()}

    def node_name(self, name_or_kind: str) -> str:
        nodes = self.nodes()
        if name_or_kind in nodes:
            return name_or_kind
        for name, n in nodes.items():
            if n.kind_node == name_or_kind:
                return name
        raise KeyError(f"unknown node {name_or_kind!r}")

    def port_of(self, node: str) -> tuple[str, str]:
        """(switch, port) feeding a node, e.g. ('cisco-sim-1', 'Gi1/0/11')."""
        switch, port = self.nodes()[self.node_name(node)].switch_port.split(":", 1)
        return switch, port

    def node_on_port(self, switch: str, port: str) -> str | None:
        sw = self.switches.get(switch)
        p = sw.ports.get(port) if sw else None
        return p.connects_to if p and p.kind == "ocp_node" else None

    def peer_masters(self, node: str) -> list[str]:
        me = self.node_name(node)
        return [n for n, spec in self.nodes().items() if spec.role == "master" and n != me]

    def for_profile(self, profile: Profile) -> Topology:
        """Topology limited to what the profile's lab actually runs (lite: master-1, worker-1, cisco-sim-1)."""
        keep_nodes = set(profile.nodes) if profile.nodes is not None else set(self.nodes())
        keep_switches = set(profile.switches) if profile.switches is not None else set(self.switches)
        all_nodes = set(self.nodes())

        def keep_port(p: Port) -> bool:
            if p.connects_to in all_nodes:
                return p.connects_to in keep_nodes
            peer = p.connects_to.split(":", 1)[0]
            return peer not in self.switches or peer in keep_switches

        switches = {
            name: sw.model_copy(update={"ports": {k: p for k, p in sw.ports.items() if keep_port(p)}})
            for name, sw in self.switches.items()
            if name in keep_switches
        }
        clusters = {
            name: c.model_copy(update={"nodes": {k: n for k, n in c.nodes.items() if k in keep_nodes}})
            for name, c in self.clusters.items()
        }
        return Topology(switches=switches, clusters=clusters)


# ---------------------------------------------------------------- escalation.yaml


class PriorityRule(BaseModel):
    model_config = ConfigDict(extra="forbid", populate_by_name=True)
    condition: str = Field(alias="if")
    priority: Literal["none", "heads_up", "P4", "P3", "P2", "P1"]
    teams: list[str] | None = None


class Team(_Loose):
    name: str
    owns: list[str]


class Escalation(_Loose):
    teams: dict[str, Team]
    priority_rules: list[PriorityRule]
    route_by_top_hypothesis: dict[str, str]
    evidence_pack: list[str]

    def team_for(self, hypothesis_key: str, fallback: str | None = None) -> str:
        return self.route_by_top_hypothesis.get(hypothesis_key) or fallback or "sre_lead"


# ---------------------------------------------------------------- playbooks/*.yaml


class Fact(_Loose):
    id: str
    record: str
    ask: Literal["network", "openshift"] | None = None
    signal: str | None = None
    from_: str | None = Field(default=None, alias="from")


class Expect(_Loose):
    ev: str
    predicate: str
    weight: int = Field(ge=1)
    ask: Literal["network", "openshift"] | None = None
    signal: str | None = None
    on: str | None = None
    needs_live: bool = False


class Refute(_Loose):
    ev: str
    predicate: str
    weight: int = Field(ge=1)
    points_to: str | None = None


class PlaybookHypothesis(_Loose):
    id: str = Field(pattern=r"^H[0-9]+$")
    key: str
    statement: str
    expect: list[Expect] = Field(min_length=1)
    refute: list[Refute] = []
    actions_catalogue: list[str] = []
    escalate_to: str
    optional: bool = False


class Action(_Loose):
    step: str
    command: str | None = None


class ConfidenceSpec(_Loose):
    formula: str
    llm_adjust_max: float = Field(le=0.10)
    labels: dict[str, str]


class Playbook(_Loose):
    id: str
    title: str
    triggers: list[dict]
    scope: dict
    facts: list[Fact] = []
    hypotheses: list[PlaybookHypothesis] = Field(min_length=1)
    actions: dict[str, Action]
    confidence: ConfidenceSpec | None = None
    cross_domain: list[dict] = []
    background: str | None = None
    explain_template: str | None = None

    def trigger_signals(self) -> list[str]:
        return [t["signal"] for t in self.triggers if "signal" in t]


# ---------------------------------------------------------------- loading


class _Yaml12Loader(yaml.SafeLoader):
    """SafeLoader with YAML 1.2 booleans: only true/false. Under YAML 1.1 the playbook key
    `on:` (and values like yes/no/off) would silently load as booleans."""


_Yaml12Loader.yaml_implicit_resolvers = {
    ch: [(tag, rx) for tag, rx in resolvers if tag != "tag:yaml.org,2002:bool"]
    for ch, resolvers in yaml.SafeLoader.yaml_implicit_resolvers.items()
}
_Yaml12Loader.add_implicit_resolver(
    "tag:yaml.org,2002:bool", re.compile(r"^(?:true|True|TRUE|false|False|FALSE)$"), list("tTfF")
)


def _read(path: Path) -> dict:
    with path.open() as f:
        return yaml.load(f, Loader=_Yaml12Loader)  # noqa: S506 - SafeLoader subclass


def load_sources(contracts_dir: Path | None = None) -> Sources:
    return Sources.model_validate(_read((contracts_dir or settings().contracts_dir) / "sources.yaml"))


def load_topology(contracts_dir: Path | None = None, profile: Profile | None = None) -> Topology:
    """Topology for the running profile (WT_PROFILE), unless another profile is passed."""
    topo = Topology.model_validate(_read((contracts_dir or settings().contracts_dir) / "topology.yaml"))
    return topo.for_profile(profile or settings().profile)


def load_escalation(contracts_dir: Path | None = None) -> Escalation:
    return Escalation.model_validate(_read((contracts_dir or settings().contracts_dir) / "escalation.yaml"))


def load_playbooks(playbooks_dir: Path | None = None) -> dict[str, Playbook]:
    books = {}
    for path in sorted((playbooks_dir or settings().playbooks_dir).glob("*.yaml")):
        pb = Playbook.model_validate(_read(path))
        if pb.id != path.stem:
            raise ValueError(f"{path.name}: id {pb.id!r} does not match the file name")
        books[pb.id] = pb
    return books
