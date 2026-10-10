"""python -m wt.common.selfcheck

Loads every contract and playbook, cross-checks the references between them, and prints what
the running profile (WT_PROFILE) will use. Exits 1 on any error. Needs no lab and no network.
"""

from __future__ import annotations

import json
import re
import sys
import typing

from pydantic import BaseModel

from wt.common.catalog import (
    Escalation,
    Playbook,
    Sources,
    Topology,
    load_escalation,
    load_playbooks,
    load_sources,
    load_topology,
)
from wt.common.config import PROFILES, Settings, settings
from wt.common.contracts import Investigation, Signal
from wt.common.probes.base import ProbeRefused
from wt.common.probes.ssh_cli import check_command
from wt.common.sources.base import placeholders, render_template

SCHEMA_MODELS = {"signal.schema.json": Signal, "investigation.schema.json": Investigation}


# ---------------------------------------------------------------- schema drift


def _submodel(annotation: object) -> type[BaseModel] | None:
    if isinstance(annotation, type) and issubclass(annotation, BaseModel):
        return annotation
    for arg in typing.get_args(annotation):
        if found := _submodel(arg):
            return found
    return None


def schema_drift(schema: dict, model: type[BaseModel], path: str = "") -> list[str]:
    """Differences between a JSON-schema object and a Pydantic model, recursively."""
    problems = []
    props = schema.get("properties", {})
    fields = model.model_fields
    where = path or model.__name__
    for name in sorted(set(props) - set(fields)):
        problems.append(f"{where}: schema property {name!r} missing from model")
    for name in sorted(set(fields) - set(props)):
        problems.append(f"{where}: model field {name!r} not in schema")
    for name in schema.get("required", []):
        if name not in fields:
            problems.append(f"{where}: required {name!r} missing from model")
    for name, field in fields.items():
        if field.is_required() and name not in schema.get("required", []):
            problems.append(f"{where}: model requires {name!r}, schema does not")
    for name, sub in props.items():
        if name not in fields:
            continue
        if sub.get("type") == "array":
            sub = sub.get("items", {})
        if "properties" in sub and (child := _submodel(fields[name].annotation)):
            problems += schema_drift(sub, child, f"{where}.{name}")
    return problems


# ---------------------------------------------------------------- cross-references


def check_sources(sources: Sources) -> list[str]:
    errors = []
    for key, sig in sources.signals.items():
        for tool in ("splunk", "solarwinds"):
            try:
                template = sig.template_for(tool)
            except KeyError:
                continue
            try:
                render_template(template, **{p: "x" for p in placeholders(template)})
            except (KeyError, ValueError) as exc:
                errors.append(f"sources.{key}: {tool} template does not render: {exc}")
            if re.search(r"\{[^a-z_}]|\{\}", template):
                errors.append(f"sources.{key}: stray brace in {tool} template")
    try:
        check_command("conf t", sources.cli_allow)
        errors.append("sources.tools.probe_cli.allow lets 'conf t' through")
    except ProbeRefused:
        pass
    if set(sources.k8s_verbs) - {"get", "list", "watch"}:
        errors.append(f"sources.tools.probe_k8s_api.verbs is not read-only: {sources.k8s_verbs}")
    return errors


def check_topology(topo: Topology) -> list[str]:
    errors = []
    for name, node in topo.nodes().items():
        switch, port = node.switch_port.split(":", 1)
        if topo.node_on_port(switch, port) != name:
            errors.append(f"topology: {name}.switch_port {node.switch_port} does not connect back to {name}")
    return errors


def check_playbook(pb: Playbook, sources: Sources, esc: Escalation) -> tuple[list[str], list[str]]:
    errors, notes = [], []
    signal_refs = [f.signal for f in pb.facts if f.signal]
    for h in pb.hypotheses:
        signal_refs += [e.signal for e in h.expect if e.signal]
        if h.escalate_to not in esc.teams:
            errors.append(f"{pb.id}.{h.id}: escalate_to {h.escalate_to!r} is not a team in escalation.yaml")
        for action in h.actions_catalogue:
            if action not in pb.actions:
                errors.append(f"{pb.id}.{h.id}: action {action!r} not in the playbook's actions")
        if h.key not in esc.route_by_top_hypothesis:
            notes.append(f"{pb.id}.{h.id} ({h.key}) not in route_by_top_hypothesis; routes to escalate_to={h.escalate_to}")
        if not h.refute:
            notes.append(f"{pb.id}.{h.id} has no refute items")
        unsourced = [e.ev for e in h.expect if not e.signal]
        if unsourced:
            notes.append(f"{pb.id}.{h.id}: {', '.join(unsourced)} have no source signal (LLM interprets or stays missing)")
    for ref in signal_refs:
        if ref not in sources.signals:
            errors.append(f"{pb.id}: signal {ref!r} not defined in sources.yaml")
    for cd in pb.cross_domain:
        for ref in cd.get("signals", []):
            if ref not in sources.signals:
                errors.append(f"{pb.id}.cross_domain: signal {ref!r} not defined in sources.yaml")
    return errors, notes


def check_escalation(esc: Escalation) -> list[str]:
    errors = [
        f"escalation.route_by_top_hypothesis.{k}: team {v!r} unknown"
        for k, v in esc.route_by_top_hypothesis.items()
        if v not in esc.teams
    ]
    for rule in esc.priority_rules:
        errors += [f"escalation.priority_rules {rule.priority}: team {t!r} unknown" for t in rule.teams or [] if t not in esc.teams]
    return errors


# ---------------------------------------------------------------- main


def run(cfg: Settings) -> int:
    errors: list[str] = []
    notes: list[str] = []

    for filename, model in SCHEMA_MODELS.items():
        schema = json.loads((cfg.contracts_dir / filename).read_text())
        errors += [f"{filename}: {p}" for p in schema_drift(schema, model)]

    sources = load_sources(cfg.contracts_dir)
    full_topo = load_topology(cfg.contracts_dir, PROFILES["full"])
    topo = load_topology(cfg.contracts_dir, cfg.profile)
    esc = load_escalation(cfg.contracts_dir)
    playbooks = load_playbooks(cfg.playbooks_dir)

    errors += check_sources(sources)
    errors += check_topology(full_topo) + check_topology(topo)
    errors += check_escalation(esc)
    for pb in playbooks.values():
        e, n = check_playbook(pb, sources, esc)
        errors += e
        notes += n

    p = cfg.profile
    fault_node = full_topo.node_name(p.fault_node)
    fault_switch, fault_port = full_topo.port_of(fault_node)
    print(f"WatchTower selfcheck  (profile: {p.name})")
    print(f"  contracts   {cfg.contracts_dir}")
    print(f"    schemas     {', '.join(f'{f} -> {m.__name__}' for f, m in SCHEMA_MODELS.items())}")
    print(f"    sources     {len(sources.signals)} signals, CLI allow {sources.cli_allow!r}, "
          f"k8s verbs {sources.k8s_verbs}, target reuse {sources.probe_policy.target_reuse_rate}")
    print(f"    escalation  teams {sorted(esc.teams)}, {len(esc.priority_rules)} priority rules")
    print(f"  playbooks   {cfg.playbooks_dir}")
    for pb in playbooks.values():
        print(f"    {pb.id:<24} {len(pb.hypotheses)} hypotheses, {len(pb.facts)} facts, {len(pb.actions)} actions")
    print(f"  topology    ({p.kind_config})")
    for name, n in topo.nodes().items():
        print(f"    {name:<10} {n.role:<7} kind node {n.kind_node:<24} <- {n.switch_port}")
    for name, sw in topo.switches.items():
        print(f"    {name:<10} {sw.role:<12} ports {', '.join(sw.ports)}")
    print(f"  fault       {p.fault_node} ({fault_node}) on {fault_switch} {fault_port}")
    print(f"  llm         {'on' if cfg.llm_enabled else 'off (WT_LLM=off)'}, model {cfg.llm_model} at {cfg.llm_base_url}")

    for n in notes:
        print(f"  note   {n}")
    for e in errors:
        print(f"  ERROR  {e}")
    print("selfcheck " + ("FAILED" if errors else "OK"))
    return 1 if errors else 0


if __name__ == "__main__":
    sys.exit(run(settings()))
