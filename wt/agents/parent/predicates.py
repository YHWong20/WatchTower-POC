"""The playbooks' predicates, in code. Keyed by the playbook item's `ev` name; each reads the
structured facts the domain agents attached to their evidence (EvidenceResult.facts, by
sources.yaml signal) and returns True / False, or None when it cannot be evaluated (no
evidence, or not applicable to this node). Items without a function here go to the LLM
"interpret evidence" job, or stay missing.
"""

from __future__ import annotations

from typing import Any, Callable

Facts = dict[str, dict[str, Any]]          # sources.yaml signal -> facts
Predicate = Callable[[Facts], bool | None]

# "Partial failure": some calls in the affected minutes fail and some succeed. The playbook's
# 0.2-0.8 band is the master's "2 in 3" pattern; a worker has no local API server, so every call
# crosses the switch and fewer fail per minute outside the burst. 0.95 keeps "most minutes mostly
# fine" out while accepting the worker's pattern (measured on the lite lab, see eval/cases).
PARTIAL_LOW, PARTIAL_HIGH = 0.2, 0.95


def _get(f: Facts, signal: str) -> dict[str, Any] | None:
    return f.get(signal)


def api_timeouts_partial(f: Facts) -> bool | None:
    c = _get(f, "api_svc_calls")
    if c is None or c.get("calls", 0) == 0:
        return None
    rate = c.get("success_rate_affected")
    return c["failed"] > 0 and rate is not None and PARTIAL_LOW <= rate <= PARTIAL_HIGH


def lease_failures(f: Facts) -> bool | None:
    c = _get(f, "lease_update_failures")
    return None if c is None else c["count"] >= 2


def port_drops_rising(f: Facts) -> bool | None:
    c = _get(f, "interface_discards")
    return None if c is None or not c.get("polls") else c["rising_polls"] >= 3


def drops_ongoing(f: Facts) -> bool | None:
    c = _get(f, "interface_discards")
    return None if c is None or not c.get("polls") else bool(c["latest_two_rising"])


def queue_drops(f: Facts) -> bool | None:
    c = _get(f, "queue_drops")
    return None if c is None else c["total"] > 0 and c["top_share"] >= 0.9


def no_crc(f: Facts) -> bool | None:
    c = _get(f, "interface_discards")
    return None if c is None or not c.get("polls") else bool(c["errors_flat"])


def crc_rising(f: Facts) -> bool | None:
    c = _get(f, "interface_discards")
    return None if c is None or not c.get("polls") else c["in_errors_polls"] >= 2


def ovn_errors(f: Facts) -> bool | None:
    c = _get(f, "ovn_errors")
    return None if c is None else c["count"] > 10


def etcd_fsync(f: Facts) -> bool | None:
    c = _get(f, "etcd_latency")
    return None if c is None else c["fsync"] > 0


def etcd_peer_only(f: Facts) -> bool | None:
    c = _get(f, "etcd_latency")
    if c is None or not c.get("etcd_on_node"):
        return None
    return c["fsync"] == 0 and c["total"] > 0


def node_pressure(f: Facts) -> bool | None:
    c = _get(f, "node_pressure")
    return None if c is None else c["count"] > 0


def node_cpu_high(f: Facts) -> bool | None:
    c = _get(f, "node_cpu_memory")
    if c is None or not c.get("monitored"):
        return None                                  # not a SolarWinds node: unknown, not "fine"
    return c["avg_load"] > 90


def node_healthy(f: Facts) -> bool | None:
    p, cpu = _get(f, "node_pressure"), _get(f, "node_cpu_memory")
    if p is None:
        return None
    cpu_ok = cpu is None or not cpu.get("monitored") or cpu["avg_load"] < 60
    return p["count"] == 0 and cpu_ok


# "All nodes affected" and "only this node" are judged on proportions, not on "any timeout at all":
# a few incidental timeouts on another node (the lite lab has a single master) must not make an
# API-server problem look as likely as the one congested path.
SHARE_ONLY_NODE = 0.8        # this node has at least 80 % of all timeouts
SHARE_EVERY_NODE = 0.2       # every node has at least 20 % of the worst node's count ...
MIN_PER_NODE = 5             # ... and at least 5


def _by_node(c: dict[str, Any]) -> tuple[dict[str, int], int, str | None]:
    by = {k: int(v) for k, v in (c.get("by_node") or {}).items()}
    node = c.get("node") or (max(by, key=by.get) if by else None)
    return by, sum(by.values()), node


def all_nodes_timeouts(f: Facts) -> bool | None:
    c = _get(f, "api_svc_timeouts_by_node")
    if c is None:
        return None
    by, total, _ = _by_node(c)
    nodes = c.get("nodes") or (sorted(by) if c.get("all_masters") else [])
    if not total or len(nodes) < 2:
        return False
    peak = max(by.values())
    return all(by.get(n, 0) >= max(MIN_PER_NODE, SHARE_EVERY_NODE * peak) for n in nodes)


def one_node_only(f: Facts) -> bool | None:
    c = _get(f, "api_svc_timeouts_by_node")
    if c is None:
        return None
    by, total, node = _by_node(c)
    return bool(total) and node in by and by[node] >= SHARE_ONLY_NODE * total


def util_normal(f: Facts) -> bool | None:
    c = _get(f, "interface_utilisation")
    return None if c is None or c.get("avg_util") is None else c["avg_util"] < 70


def util_high(f: Facts) -> bool | None:
    c = _get(f, "interface_utilisation")
    return None if c is None or c.get("avg_util") is None else c["avg_util"] > 85


def link_flaps(f: Facts) -> bool | None:
    c = _get(f, "switch_syslog")
    return None if c is None else c["link_down"] > 0


PREDICATES: dict[str, Predicate] = {
    # ocp-node-flap
    "api_timeouts_partial": api_timeouts_partial, "lease_failures": lease_failures,
    "port_drops_rising": port_drops_rising, "drops_ongoing": drops_ongoing, "queue_drops": queue_drops,
    "no_crc": no_crc, "crc_rising": crc_rising, "ovn_errors": ovn_errors, "etcd_fsync": etcd_fsync,
    "etcd_peer_only": etcd_peer_only, "node_pressure": node_pressure, "node_cpu_high": node_cpu_high,
    "node_healthy": node_healthy, "all_nodes_timeouts": all_nodes_timeouts, "one_node_only": one_node_only,
    # network-port-congestion
    "drops_rising": port_drops_rising, "util_normal": util_normal, "util_high": util_high, "link_flaps": link_flaps,
}
