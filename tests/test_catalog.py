from wt.common.catalog import load_escalation, load_playbooks, load_sources, load_topology
from wt.common.config import PROFILES

from .conftest import CONTRACTS, PLAYBOOKS


def test_playbooks_load_and_yaml_on_key_stays_a_string():
    books = load_playbooks(PLAYBOOKS)
    assert set(books) == {"network-port-congestion", "ocp-node-flap"}
    h1 = books["ocp-node-flap"].hypotheses[0]
    assert h1.key == "network_path_congestion"
    port_drops = next(e for e in h1.expect if e.ev == "port_drops_rising")
    assert port_drops.on == "topology.port_of({node})"          # YAML 1.1 would make this key True
    assert [e.ev for e in h1.expect if e.needs_live] == ["queue_drops"]


def test_sources_templates_and_probe_rules():
    s = load_sources(CONTRACTS)
    assert s.cli_allow == "^show "
    assert s.k8s_verbs == ["get", "list", "watch"]
    assert s.template("interface_discards").startswith("SELECT DateTime, InDiscards")
    assert s.template("active_alerts", "splunk") == "/services/alerts/fired_alerts"
    assert s.template("node_reboot_check", "probe_k8s_api").startswith("GET /api/v1/nodes/")
    assert s.template("queue_drops").startswith("show platform hardware fed")


def test_lite_topology_keeps_only_the_lite_lab():
    topo = load_topology(CONTRACTS, PROFILES["lite"])
    assert set(topo.nodes()) == {"master-1", "worker-1"}
    assert set(topo.switches) == {"cisco-sim-1"}
    assert set(topo.switches["cisco-sim-1"].ports) == {"Gi1/0/10", "Gi1/0/13"}
    assert topo.port_of("ocp-sim-worker") == ("cisco-sim-1", "Gi1/0/13")   # kind name accepted
    assert topo.node_on_port("cisco-sim-1", "Gi1/0/13") == "worker-1"
    assert topo.peer_masters("worker-1") == ["master-1"]


def test_full_topology():
    topo = load_topology(CONTRACTS, PROFILES["full"])
    assert topo.port_of("master-2") == ("cisco-sim-1", "Gi1/0/11")
    assert topo.peer_masters("master-2") == ["master-1", "master-3"]
    assert topo.node_on_port("cisco-sim-1", "Gi1/0/48") is None          # uplink, not a node


def test_escalation_routing():
    esc = load_escalation(CONTRACTS)
    assert esc.team_for("network_path_congestion") == "network"
    assert esc.team_for("etcd_slow_disk") == "platform"
    assert esc.team_for("microburst_congestion", fallback="network") == "network"
    assert esc.priority_rules[0].condition.startswith("service outage")
