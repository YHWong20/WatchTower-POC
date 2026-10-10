"""Correlator scoring (single domain low, cross-domain medium, state change high) and the Parent
Agent's triggers (watch item to investigation, dedupe, stale history ignored)."""

from datetime import UTC, datetime, timedelta

import pytest

from wt.agents.base import load_rules
from wt.agents.parent.agent import HERE, ParentAgent
from wt.agents.parent.correlator import Correlator
from wt.common.catalog import load_escalation, load_playbooks, load_topology
from wt.common.config import PROFILES, load_settings
from wt.common.contracts import Signal

RULES = load_rules(HERE / "rules.yaml")
TOPO = load_topology(profile=PROFILES["lite"])


def sig(rule, kind, name, domain, ts, severity="weak", tool="splunk"):
    return Signal(ts=ts, domain=domain, agent=f"{domain}-agent" if domain == "network" else "openshift-agent",
                  rule=rule, entity={"kind": kind, "name": name}, severity=severity, summary=f"{rule} on {name}",
                  value=5, baseline=0, source={"tool": tool, "query": "q"})


def corr():
    n = iter(range(1, 100))
    return Correlator(TOPO, RULES, load_playbooks(), load_escalation(), new_id=lambda: f"WI-{next(n):04d}")


T0 = datetime(2026, 10, 10, 13, 0, tzinfo=UTC)


def test_single_domain_stays_low():
    c = corr()
    item = c.add(sig("ocp.api_svc_timeouts", "node", "worker-1", "openshift", T0))
    assert (item.scope, item.confidence, item.confidence_label, item.status) == ("single_domain", 0.2, "low", "watching")
    item = c.add(sig("ocp.lease_update_failures", "node", "worker-1", "openshift", T0 + timedelta(seconds=30)))
    assert item.confidence == 0.25
    assert item.title == "API calls from worker-1 to the kubernetes Service are timing out"


def test_cross_domain_pair_linked_by_topology_scores_medium():
    c = corr()
    c.add(sig("ocp.api_svc_timeouts", "node", "worker-1", "openshift", T0))
    item = c.add(sig("net.output_drops_rising", "interface", "cisco-sim-1:Gi1/0/13", "network",
                     T0 + timedelta(seconds=90), tool="solarwinds"))
    assert item.scope == "cross_domain" and item.status == "emerging" and item.confidence == 0.5
    assert (item.node, item.kind_node, item.switch, item.port) == ("worker-1", "ocp-sim-worker", "cisco-sim-1", "Gi1/0/13")
    assert "Gi1/0/13 on cisco-sim-1 feeds worker-1" in item.correlation
    assert item.reuse == {"reused": 2, "probed": 0, "rate": 1.0}
    assert item.suggestions[0].command.endswith("queue stats interface Gi1/0/13")
    item = c.add(sig("ocp.lease_update_failures", "node", "worker-1", "openshift", T0 + timedelta(minutes=5)))
    assert item.confidence == 0.6                            # 0.5 + one extra rule + persistence
    assert len(c.items) == 1


def test_unlinked_port_is_its_own_item_and_old_signals_age_out():
    c = corr()
    c.add(sig("ocp.api_svc_timeouts", "node", "worker-1", "openshift", T0))
    other = c.add(sig("net.output_drops_rising", "interface", "cisco-sim-1:Gi1/0/20", "network", T0, tool="solarwinds"))
    assert other.scope == "single_domain" and other.node is None and len(c.items) == 2
    c.add(sig("net.output_drops_rising", "interface", "cisco-sim-1:Gi1/0/13", "network",
              T0 + timedelta(minutes=12), tool="solarwinds"))
    item = next(i for i in c.items.values() if i.anchor == "worker-1")
    assert item.scope == "single_domain"                     # the timeout signal is outside the 10-min window
    assert [i.status for i in c.expire(T0 + timedelta(minutes=40))] == ["closed", "closed"]


def test_state_change_scores_high():
    c = corr()
    c.add(sig("ocp.api_svc_timeouts", "node", "worker-1", "openshift", T0))
    item = c.add(sig("ocp.node_not_ready", "node", "worker-1", "openshift", T0 + timedelta(minutes=8), "state_change"))
    assert item.confidence == 0.9 and item.confidence_label == "high"


class FakeInvestigator:
    def __init__(self):
        self.requests = []
        self.topo = TOPO
        self.n = 0

    async def new_id(self):
        self.n += 1
        return f"INV-{self.n:04d}"

    async def investigate(self, req, inv_id):
        self.requests.append((inv_id, req))


@pytest.fixture
def parent(store):
    c = corr()
    return ParentAgent(load_settings(), store, None, FakeInvestigator(), c, RULES)


async def test_parent_opens_one_investigation_on_node_state_change(parent):
    now = datetime.now(UTC)
    await parent.on_signal(sig("ocp.api_svc_timeouts", "node", "worker-1", "openshift", now))
    assert parent.inv.requests == []
    item = await parent.on_signal(sig("ocp.node_not_ready", "node", "worker-1", "openshift", now, "state_change"))
    for t in list(parent.running.values()):
        await t
    assert item.status == "investigating" and item.investigation_id == "INV-0001"
    inv_id, req = parent.inv.requests[0]
    assert req.mode == "incident_investigation" and req.node == "worker-1" and req.trigger_signal == "ocp.node_not_ready"
    # an SRE pressing Investigate on the Splunk alert a minute later gets the same investigation
    await parent.on_sre({"type": "investigate", "request_id": "r1",
                         "alarm": {"tool": "splunk", "name": "NodeNotReady", "entity": "ocp-sim-worker"}})
    assert parent.store.kv_get("request:r1") == {"investigation_id": "INV-0001"} and len(parent.inv.requests) == 1
    await parent.on_sre({"type": "investigate", "request_id": "r2", "node": "worker-1", "force": True})
    assert parent.store.kv_get("request:r2") == {"investigation_id": "INV-0002"}
    for t in list(parent.running.values()):
        await t
    # the node flaps again later: a new state change opens a new investigation
    await parent.on_signal(sig("ocp.node_not_ready", "node", "worker-1", "openshift", datetime.now(UTC), "state_change"))
    for t in list(parent.running.values()):
        await t
    assert len(parent.inv.requests) == 3


async def test_replayed_history_is_ignored(parent):
    old = datetime.now(UTC) - timedelta(hours=1)
    assert await parent.on_signal(sig("ocp.node_not_ready", "node", "worker-1", "openshift", old, "state_change")) is None
    assert parent.running == {} and parent.corr.items == {}


async def test_dismiss(parent):
    item = await parent.on_signal(sig("ocp.api_svc_timeouts", "node", "worker-1", "openshift", datetime.now(UTC)))
    await parent.on_sre({"type": "dismiss", "watch_item_id": item.id})
    assert parent.store.get_watch_item(item.id)["status"] == "dismissed" and item.id not in parent.corr.items
