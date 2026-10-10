"""Pydantic v2 models for contracts/signal.schema.json and contracts/investigation.schema.json.

Written from the JSON schemas, field for field. tests/test_contracts.py checks they have not
drifted (same properties, same required set, dumps validate against the schema).
"""

from __future__ import annotations

import secrets
from datetime import UTC, datetime
from typing import Annotated, Literal

from pydantic import AwareDatetime, BaseModel, ConfigDict, Field

Domain = Literal["network", "openshift"]
Tool = Literal["splunk", "solarwinds", "probe_cli", "probe_k8s_api"]
REUSE_TOOLS: frozenset[str] = frozenset({"splunk", "solarwinds"})
PROBE_TOOLS: frozenset[str] = frozenset({"probe_cli", "probe_k8s_api"})

Ref = Annotated[str, Field(pattern=r"^(obs|ev|cor)-[0-9]+$")]
Refs = Annotated[list[Ref], Field(min_length=1)]


def utcnow() -> datetime:
    return datetime.now(UTC)


class _Model(BaseModel):
    model_config = ConfigDict(extra="forbid")


# ---------------------------------------------------------------- Signal


class Entity(_Model):
    kind: Literal["switch", "interface", "cluster", "node", "pod", "service"]
    name: str


class SourceRef(_Model):
    tool: Tool
    query: str
    reason_for_probe: str | None = None


def new_signal_id() -> str:
    return f"sig-{secrets.token_hex(4)}"


class Signal(_Model):
    """A deterministic watch-rule hit raised by a domain agent. No LLM involvement."""

    id: str = Field(default_factory=new_signal_id, pattern=r"^sig-[0-9a-f]{8}$")
    ts: AwareDatetime = Field(default_factory=utcnow)
    domain: Domain
    agent: Literal["network-agent", "openshift-agent"]
    rule: str
    entity: Entity
    severity: Literal["weak", "notable", "state_change"]
    summary: str = Field(max_length=200)
    value: float
    baseline: float
    window_s: int | None = None
    source: SourceRef


# ---------------------------------------------------------------- Investigation


class Alarm(_Model):
    tool: Literal["splunk", "solarwinds"] | None = None
    name: str | None = None
    entity: str | None = None
    fired_at: str | None = None


class Trigger(_Model):
    mode: Literal["proactive_watch", "incident_investigation"]
    watch_item_id: str | None = None
    alarm: Alarm | None = None
    requested_by: str | None = None


class Observation(_Model):
    id: str = Field(pattern=r"^obs-[0-9]+$")
    text: str
    signal_ids: list[str]


class EvidenceItem(_Model):
    id: str = Field(pattern=r"^ev-[0-9]+$")
    domain: Domain
    source: Tool
    query: str
    reason_for_probe: str | None = None
    collected_at: AwareDatetime
    excerpt: str = Field(max_length=2000)
    finding: str = Field(max_length=300)
    interpreted_by: Literal["rule", "llm"] | None = None

    @property
    def is_probe(self) -> bool:
        return self.source in PROBE_TOOLS


class Correlation(_Model):
    id: str = Field(pattern=r"^cor-[0-9]+$")
    kind: Literal["topology", "time", "topology+time"]
    text: str
    refs: Refs


class LlmAdjustment(_Model):
    delta: float | None = Field(default=None, ge=-0.1, le=0.1)
    reason: str | None = None


class Hypothesis(_Model):
    id: str = Field(pattern=r"^H[0-9]+$")
    statement: str
    proposed_by: Literal["playbook", "llm"]
    supporting: list[str]
    refuting: list[str]
    missing: list[str]
    confidence: float = Field(ge=0, le=1)
    confidence_rule: float | None = Field(default=None, ge=0, le=1)
    llm_adjustment: LlmAdjustment | None = None


class Conclusion(_Model):
    likely_cause: str = Field(max_length=300)
    what_happened: str = Field(max_length=800)
    confidence_label: Literal["low", "medium", "high"]
    unknowns: list[str] | None = None
    refs: Refs


class RecommendedAction(_Model):
    step: str
    command: str | None = None
    why: str
    kind: Literal["diagnostic"] = "diagnostic"
    from_catalogue: str | None = None


class Escalation(_Model):
    team: str
    priority: Literal["none", "heads_up", "P4", "P3", "P2", "P1"]
    reason: str
    raise_if: str | None = None
    note_markdown: str | None = None


class Reuse(_Model):
    reused: int
    probed: int
    rate: float = Field(ge=0, le=1)
    probe_reasons: list[str] | None = None

    @classmethod
    def from_evidence(cls, evidence: list[EvidenceItem]) -> Reuse:
        reused = sum(1 for e in evidence if e.source in REUSE_TOOLS)
        probes = [e for e in evidence if e.is_probe]
        total = reused + len(probes)
        return cls(
            reused=reused,
            probed=len(probes),
            rate=round(reused / total, 3) if total else 1.0,
            probe_reasons=[e.reason_for_probe for e in probes if e.reason_for_probe] or None,
        )


class Investigation(_Model):
    """The evidence chain: observation -> evidence -> correlation -> hypothesis -> confidence -> actions."""

    id: str = Field(pattern=r"^INV-[0-9]{4}$")
    opened_at: AwareDatetime
    trigger: Trigger
    playbook: str
    observations: list[Observation]
    evidence: list[EvidenceItem]
    correlations: list[Correlation]
    hypotheses: Annotated[list[Hypothesis], Field(min_length=1)]
    conclusion: Conclusion
    recommended_actions: list[RecommendedAction]
    escalation: Escalation
    llm_used: bool
    audit_ids: list[str] | None = None
    reuse: Reuse


def dump(model: BaseModel) -> dict:
    """JSON-ready dict without unset optional keys (the schemas do not allow nulls)."""
    return model.model_dump(mode="json", exclude_none=True)
