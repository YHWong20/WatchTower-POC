"""OpenShift Agent: one agent for the cluster (domain agent, not device agent).

OpenShift events, kubelet journal and control-plane logs are in Splunk, so every question is
answered from Splunk first; the Kubernetes API (read-only) only fills a gap.

Watch (every 30 s, deterministic), rules in rules.yaml:
  ocp.api_svc_timeouts       kubernetes Service timeouts per node per 5 min > baseline + 2
  ocp.lease_update_failures  any "failed to update lease" in 5 min
  ocp.etcd_latency           etcd slowness warnings above baseline
  ocp.node_not_ready         a NodeNotReady event (state_change)
Evidence on request: NotReady/Ready times, reboot or kubelet restart (Rebooted/Starting events),
static pod restarts (Killing/Started), evictions, pressure, etcd and OVN logs, Service call
results, from Splunk; node CPU/memory from SolarWinds where the node is monitored there.
"""

from __future__ import annotations

import json
from collections import defaultdict
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from wt.agents.base import AgentContext, DomainAgent, EvidenceResult, Task, parse_window
from wt.common.contracts import Signal
from wt.common.sources.base import render_template

HERE = Path(__file__).resolve().parent
GRACE_S = 40          # node-monitor-grace-period (OpenShift default; pinned in the lab)
EVICTION_S = 300      # default toleration for not-ready / unreachable
STATIC_PODS = ("kube-apiserver", "etcd", "kube-controller-manager", "kube-scheduler")


def ts(value: str) -> float:
    return datetime.fromisoformat(value.replace("Z", "+00:00")).timestamp()


def hhmmss(t: float) -> str:
    return datetime.fromtimestamp(t, UTC).strftime("%H:%M:%S")


def rows_excerpt(rows: list[dict], keep: tuple[str, ...] | None = None, limit: int = 2000) -> str:
    lines, size = [], 0
    for r in rows:
        item = {k: r[k] for k in keep if k in r} if keep else r
        line = json.dumps(item, separators=(",", ":"), default=str)
        if size + len(line) + 1 > limit:
            lines.append("…(more rows)")
            break
        lines.append(line)
        size += len(line) + 1
    return "\n".join(lines)


class OpenShiftAgent(DomainAgent):
    name = "openshift-agent"
    domain = "openshift"
    rules_file = HERE / "rules.yaml"

    def __init__(self, ctx: AgentContext, rules: dict[str, Any] | None = None, k8s=None):
        super().__init__(ctx, rules)
        self._k8s = k8s

    def describe(self) -> str:
        return f"{next(iter(self.ctx.topology.clusters), 'cluster')} · Splunk"

    def display(self, kind_node: str) -> str:
        """ocp-sim-worker -> worker-1 (topology display names); unknown hosts stay as they are."""
        try:
            return self.ctx.topology.node_name(kind_node)
        except KeyError:
            return kind_node

    def k8s(self):
        if self._k8s is None:
            from wt.common.probes.k8s import K8sProbe
            self._k8s = K8sProbe.from_kubeconfig(self.ctx.audit, self.ctx.settings.kubeconfig, actor=self.name)
        return self._k8s

    # ------------------------------------------------------------ watch

    async def watch_once(self, now: float) -> list[Signal]:
        window = self.rules.get("window", "5m")
        window_s = parse_window(window)
        cfg = self.rules["rules"]
        out: list[Signal] = []
        samples: list[tuple[str, str, float, float]] = []

        calls = await self.splunk("watch_api_svc_calls", window=window)
        per_node: dict[str, dict[str, int]] = defaultdict(lambda: {"ok": 0, "failed": 0})
        for r in calls.rows:
            per_node[r["host"]]["ok" if str(r.get("ok")).lower() == "true" else "failed"] += int(r["count"])
        known = {n.kind_node for n in self.ctx.topology.nodes().values()}
        stale = [self.display(n) for n in sorted(known) if n not in per_node]
        self.status["stale"] = [f"Splunk: no svc-prober lines from {', '.join(stale)} in {window}"] if stale else []
        rule = cfg["ocp.api_svc_timeouts"]
        for host, c in sorted(per_node.items()):
            node = self.display(host)
            key = f"svc_timeouts:{host}"
            base = self.baselines.mean(key, now)
            samples.append(("svc_timeouts", node, now, c["failed"]))
            if c["failed"] > base + rule["above_baseline"]:
                total = c["ok"] + c["failed"]
                out.append(self.signal(
                    "ocp.api_svc_timeouts", "node", node, "weak",
                    f"{c['failed']} × i/o timeout on calls from {node} to the kubernetes Service in {window}: "
                    f"{c['failed']} of {total} failed (baseline {base:g})",
                    value=c["failed"], baseline=base, tool="splunk", query=calls.query, window_s=window_s, ts=now))
            else:
                self.baselines.add(key, now, c["failed"])

        lease = await self.splunk("watch_lease_failures", window=window)
        counts = {r["host"]: int(r["count"]) for r in lease.rows}
        for host in known | set(counts):
            n = counts.get(host, 0)
            samples.append(("lease_failures", self.display(host), now, n))
            if n >= cfg["ocp.lease_update_failures"]["min_count"]:
                out.append(self.signal(
                    "ocp.lease_update_failures", "node", self.display(host), "weak",
                    f"kubelet on {self.display(host)}: {n} × failed to update lease / node status in {window}",
                    value=n, baseline=0, tool="splunk", query=lease.query, window_s=window_s, ts=now))

        etcd = await self.splunk("watch_etcd_warnings", window=window)
        rule = cfg["ocp.etcd_latency"]
        for r in etcd.rows:
            host, n = r["host"], int(r["count"])
            key = f"etcd_warnings:{host}"
            base = self.baselines.mean(key, now)
            samples.append(("etcd_warnings", self.display(host), now, n))
            if n > max(base * rule["factor"], base + rule["above_baseline"]):
                out.append(self.signal(
                    "ocp.etcd_latency", "node", self.display(host), "weak",
                    f"etcd on {self.display(host)}: {n} slowness warnings in {window} (baseline {base:g})",
                    value=n, baseline=base, tool="splunk", query=etcd.query, window_s=window_s, ts=now))
            else:
                self.baselines.add(key, now, n)

        events = await self.splunk("watch_node_events", window=window)
        for r in sorted(events.rows, key=lambda r: r["_time"]):
            if r.get("reason") != "NodeNotReady":
                continue
            node = self.display(r.get("involvedObject.name", "?"))
            at = ts(r["_time"])
            out.append(self.signal(
                "ocp.node_not_ready", "node", node, "state_change",
                f"{node} NotReady at {hhmmss(at)} UTC (node-controller: {r.get('message', '')})",
                value=1, baseline=0, tool="splunk", query=events.query, window_s=window_s, ts=now))

        self.ctx.store.add_samples(samples)
        return out

    # ------------------------------------------------------------ evidence on request

    async def gather(self, task: Task) -> EvidenceResult:
        handler = {
            "node_ready_events": self.ev_node_ready,
            "node_reboot_check": self.ev_reboot,
            "static_pod_restarts": self.ev_static_pods,
            "pod_evictions": self.ev_evictions,
            "api_svc_calls": self.ev_api_calls,
            "api_svc_timeouts": self.ev_api_timeouts,
            "api_svc_timeouts_by_node": self.ev_timeouts_by_node,
            "lease_update_failures": self.ev_lease,
            "etcd_latency": self.ev_etcd,
            "ovn_errors": self.ev_ovn,
            "node_pressure": self.ev_pressure,
            "node_cpu_memory": self.ev_cpu_memory,
            "lease_renew_live": self.ev_lease_live,
        }.get(task.signal)
        if handler is None:
            raise ValueError(f"openshift-agent has no evidence handler for {task.signal!r}")
        return await handler(task)

    def result(self, task: Task, **kw: Any) -> EvidenceResult:
        return EvidenceResult(task_id=task.task_id, investigation_id=task.investigation_id, signal=task.signal,
                              domain=self.domain, agent=self.name, **kw)

    def node_params(self, task: Task) -> tuple[str, str, str]:
        """(kind node name for queries, display name for text, window)."""
        kind_node = task.params["node"]
        return kind_node, self.display(kind_node), task.params.get("window", "30m")

    async def ev_node_ready(self, task: Task) -> EvidenceResult:
        kind_node, node, window = self.node_params(task)
        res = await self.splunk("node_ready_events", node=kind_node, window=window)
        events = sorted(((ts(r["_time"]), r.get("reason")) for r in res.rows), key=lambda x: x[0])
        flaps = []
        down = None
        for t, reason in events:
            if reason == "NodeNotReady" and down is None:
                down = t
            elif reason == "NodeReady" and down is not None:
                flaps.append((down, t))
                down = None
        facts: dict[str, Any] = {"flaps": len(flaps) + (1 if down else 0), "still_not_ready": down is not None,
                                 "events": [{"t": hhmmss(t), "reason": r} for t, r in events]}
        if flaps:
            nr, rd = flaps[-1]
            gap = round(rd - nr)
            facts.update(not_ready_at=hhmmss(nr), ready_at=hhmmss(rd), not_ready_ts=nr, ready_ts=rd, gap_s=gap,
                         longest_gap_s=max(round(b - a) for a, b in flaps))
            finding = (f"{node} NotReady {hhmmss(nr)}, Ready {hhmmss(rd)} UTC: NotReady for {gap} s (marked after "
                       f"{GRACE_S} s without a lease renewal), {'below' if gap < EVICTION_S else 'above'} the "
                       f"{EVICTION_S} s eviction threshold"
                       + (f"; {len(flaps)} flaps in the window" if len(flaps) > 1 else ""))
        elif down:
            facts.update(not_ready_at=hhmmss(down), not_ready_ts=down)
            finding = f"{node} NotReady since {hhmmss(down)} UTC and not Ready again yet"
        else:
            finding = f"No NotReady/Ready events for {node} in the last {window}"
        return self.result(task, source="splunk", query=res.query,
                           excerpt=rows_excerpt(res.rows, ("_time", "reason", "message")), finding=finding[:300],
                           facts=facts, audit_id=res.audit_id)

    async def ev_reboot(self, task: Task) -> EvidenceResult:
        kind_node, node, window = self.node_params(task)
        try:
            res = await self.splunk("node_reboot_check", node=kind_node, window=window)
        except Exception:
            if task.allow_probe:                              # Splunk gap: fall back to the API (bootID)
                return await self.ev_reboot_probe(task)
            raise
        corroborate = self.ctx.sources.signals["node_reboot_check"].model_extra.get("corroborate")
        kubelet_starts = []
        if corroborate:
            q2 = render_template(corroborate, node=kind_node, window=window)
            kubelet_starts = (await self.ctx.splunk.search(q2)).rows
        reasons = sorted({r.get("reason") for r in res.rows if r.get("reason")})
        facts = {"count": len(res.rows), "reasons": reasons, "kubelet_starts": len(kubelet_starts),
                 "rebooted": "Rebooted" in reasons, "kubelet_restarted": "Starting" in reasons or bool(kubelet_starts)}
        if not res.rows and not kubelet_starts:
            finding = f"No Rebooted or Starting events for {node} and no kubelet start in its journal: no reboot, no kubelet restart"
        else:
            finding = f"{node}: {', '.join(reasons) or 'kubelet start lines'} found in the last {window}"
        query = res.query + (f"\n{render_template(corroborate, node=kind_node, window=window)}" if corroborate else "")
        return self.result(task, source="splunk", query=query,
                           excerpt=rows_excerpt(res.rows + kubelet_starts, ("_time", "reason", "message", "_raw")),
                           finding=finding[:300], facts=facts, audit_id=res.audit_id)

    async def ev_reboot_probe(self, task: Task) -> EvidenceResult:
        kind_node, node, _ = self.node_params(task)
        res = await self.k8s().get_node(kind_node)
        info = (res.data or {}).get("status", {}).get("nodeInfo", {})
        return self.result(task, source="probe_k8s_api", query=res.query, excerpt=res.excerpt(),
                           reason_for_probe="Splunk did not answer for the window (gap), so the reboot check fell back "
                                            "to the read-only Kubernetes API",
                           finding=f"{node} bootID {info.get('bootID', '?')} (compare with the previous boot ID to spot a reboot)",
                           facts={"boot_id": info.get("bootID")}, audit_id=res.audit_id)

    async def ev_static_pods(self, task: Task) -> EvidenceResult:
        kind_node, node, window = self.node_params(task)
        res = await self.splunk("static_pod_restarts", node=kind_node, window=window)
        restarts = [r for r in res.rows if r.get("reason") in ("Killing", "Started")
                    and str(r.get("involvedObject.name", "")).startswith(STATIC_PODS)]
        unhealthy = [r for r in res.rows if r.get("reason") in ("Unhealthy", "BackOff")]
        facts = {"restarts": len(restarts), "unhealthy": sum(int(r.get("count", 1)) for r in unhealthy),
                 "rows": res.rows}
        if restarts:
            finding = "Control-plane pod restart events: " + ", ".join(
                f"{r['involvedObject.name']} {r['reason']}" for r in restarts)
        else:
            finding = (f"No Killing/Started events for the API server or etcd pods on {node}: they did not restart"
                       + (f" ({facts['unhealthy']} probe-failure events for other pods, not restarts)" if unhealthy else ""))
        return self.result(task, source="splunk", query=res.query, excerpt=rows_excerpt(res.rows), finding=finding[:300],
                           facts=facts, audit_id=res.audit_id)

    async def ev_evictions(self, task: Task) -> EvidenceResult:
        _, node, window = self.node_params(task)
        res = await self.splunk("pod_evictions", window=window)
        cancelled = [r for r in res.rows if str(r.get("message", "")).startswith("Cancelling")]
        real = [r for r in res.rows if r not in cancelled]
        facts = {"evictions": len(real), "cancelled": len(cancelled)}
        finding = (f"{len(real)} pod eviction(s) in the last {window}" if real
                   else "No pods evicted" + (f" ({len(cancelled)} pending deletions cancelled when the node came back)"
                                             if cancelled else ""))
        return self.result(task, source="splunk", query=res.query,
                           excerpt=rows_excerpt(res.rows, ("_time", "reason", "message")), finding=finding[:300],
                           facts=facts, audit_id=res.audit_id)

    async def ev_api_calls(self, task: Task) -> EvidenceResult:
        kind_node, node, window = self.node_params(task)
        res = await self.splunk("api_svc_calls", node=kind_node, window=window)
        ok_n = [int(r.get("true") or 0) for r in res.rows]
        bad_n = [int(r.get("false") or 0) for r in res.rows]
        affected = [i for i, b in enumerate(bad_n) if b > 0]
        calls_aff = sum(ok_n[i] + bad_n[i] for i in affected)
        failed_aff = sum(bad_n[i] for i in affected)
        success_aff = round(1 - failed_aff / calls_aff, 3) if calls_aff else None
        total, failed = sum(ok_n) + sum(bad_n), sum(bad_n)
        facts = {"calls": total, "failed": failed, "minutes_affected": len(affected),
                 "calls_in_affected": calls_aff, "failed_in_affected": failed_aff,
                 "success_rate_affected": success_aff,
                 "success_rate": round(1 - failed / total, 3) if total else None,
                 "first_fail": res.rows[affected[0]]["_time"] if affected else None,
                 "last_fail": res.rows[affected[-1]]["_time"] if affected else None,
                 "fully_failed_minutes": sum(1 for i in affected if ok_n[i] == 0)}
        if failed:
            finding = (f"{failed} of {total} calls from {node} to the kubernetes Service timed out in {len(affected)} min; "
                       f"in those minutes {success_aff:.0%} still succeeded (partial failure)")
        else:
            finding = f"All {total} calls from {node} to the kubernetes Service succeeded"
        return self.result(task, source="splunk", query=res.query,
                           excerpt=rows_excerpt([r for r in res.rows if int(r.get("false") or 0)] or res.rows[-5:]),
                           finding=finding[:300], facts=facts, audit_id=res.audit_id)

    async def ev_api_timeouts(self, task: Task) -> EvidenceResult:
        kind_node, node, window = self.node_params(task)
        res = await self.splunk("api_svc_timeouts", node=kind_node, window=window)
        total = sum(int(r.get("count") or 0) for r in res.rows)
        return self.result(task, source="splunk", query=res.query,
                           excerpt=rows_excerpt([r for r in res.rows if int(r.get("count") or 0)]),
                           finding=f"{total} i/o timeouts from {node} to the kubernetes Service in {window}",
                           facts={"timeouts": total}, audit_id=res.audit_id)

    async def ev_timeouts_by_node(self, task: Task) -> EvidenceResult:
        _, node, window = self.node_params(task)
        res = await self.splunk("api_svc_timeouts_by_node", window=window)
        by_host = {self.display(r["host"]): int(r["count"]) for r in res.rows}
        masters = [n for n, spec in self.ctx.topology.nodes().items() if spec.role == "master"]
        facts = {"by_node": by_host, "nodes_with_timeouts": sorted(by_host), "node": node,
                 "nodes": sorted(self.ctx.topology.nodes()), "masters": masters,
                 "all_masters": bool(masters) and all(by_host.get(m, 0) > 0 for m in masters),
                 "only_node": set(by_host) <= {node} and node in by_host}
        finding = ("Timeouts only from " + node if facts["only_node"] else
                   "Timeouts from " + ", ".join(f"{n} ({c})" for n, c in sorted(by_host.items()))) if by_host \
            else "No Service timeouts from any node"
        return self.result(task, source="splunk", query=res.query, excerpt=rows_excerpt(res.rows), finding=finding,
                           facts=facts, audit_id=res.audit_id)

    async def ev_lease(self, task: Task) -> EvidenceResult:
        kind_node, node, window = self.node_params(task)
        res = await self.splunk("lease_update_failures", node=kind_node, window=window)
        times = sorted(r["_time"] for r in res.rows)
        facts = {"count": len(res.rows), "first": times[0] if times else None, "last": times[-1] if times else None}
        finding = (f"kubelet on {node}: {len(res.rows)} × failed to update lease / node status"
                   if res.rows else f"No lease update failures from the kubelet on {node}")
        return self.result(task, source="splunk", query=res.query,
                           excerpt=rows_excerpt(res.rows, ("_time", "_raw")), finding=finding[:300], facts=facts,
                           audit_id=res.audit_id)

    async def ev_etcd(self, task: Task) -> EvidenceResult:
        kind_node, node, window = self.node_params(task)
        res = await self.splunk("etcd_latency", node=kind_node, window=window)
        by_msg = {r.get("msg", "?"): int(r.get("count") or 0) for r in res.rows}
        fsync = sum(c for m, c in by_msg.items() if "fdatasync" in m)
        peer = sum(c for m, c in by_msg.items() if "unhealthy" in m or "peer" in m or "round-trip" in m)
        facts = {"by_msg": by_msg, "fsync": fsync, "peer": peer, "total": sum(by_msg.values()),
                 "etcd_on_node": bool(by_msg) or self.ctx.topology.nodes().get(node, None) is not None
                 and self.ctx.topology.nodes()[node].role == "master"}
        finding = (f"etcd on {node}: " + ", ".join(f"{c} × {m}" for m, c in by_msg.items()) if by_msg
                   else f"No etcd slowness warnings on {node}"
                   + ("" if facts["etcd_on_node"] else " (no etcd member on this node)"))
        return self.result(task, source="splunk", query=res.query, excerpt=rows_excerpt(res.rows), finding=finding[:300],
                           facts=facts, audit_id=res.audit_id)

    async def ev_ovn(self, task: Task) -> EvidenceResult:
        kind_node, node, window = self.node_params(task)
        res = await self.splunk("ovn_errors", node=kind_node, window=window)
        n = int(res.rows[0].get("count") or 0) if res.rows else 0
        return self.result(task, source="splunk", query=res.query, excerpt=rows_excerpt(res.rows),
                           finding=f"{n} OVN error lines on {node}", facts={"count": n}, audit_id=res.audit_id)

    async def ev_pressure(self, task: Task) -> EvidenceResult:
        kind_node, node, window = self.node_params(task)
        res = await self.splunk("node_pressure", node=kind_node, window=window)
        reasons = sorted({r.get("reason") for r in res.rows if r.get("reason")})
        finding = (f"Pressure events on {node}: {', '.join(reasons)}" if res.rows
                   else f"No memory, disk, PID pressure or SystemOOM events for {node}")
        return self.result(task, source="splunk", query=res.query,
                           excerpt=rows_excerpt(res.rows, ("_time", "reason", "message")), finding=finding,
                           facts={"count": len(res.rows), "reasons": reasons}, audit_id=res.audit_id)

    async def ev_cpu_memory(self, task: Task) -> EvidenceResult:
        kind_node, node, _ = self.node_params(task)
        res = await self.swql("node_cpu_memory", node=kind_node)
        if res.rows:
            avg = round(sum(r.get("AvgLoad") or 0 for r in res.rows) / len(res.rows), 1)
            mem = res.rows[0].get("AvgPercentMemoryUsed")
            facts = {"monitored": True, "avg_load": avg, "memory_pct": mem}
            finding = f"{node} CPU {avg}% average, memory {mem}% (SolarWinds)"
        else:
            facts = {"monitored": False}
            finding = f"{node} is not a SolarWinds node, so CPU and memory are not known from SolarWinds"
        return self.result(task, source="solarwinds", query=res.query, excerpt=rows_excerpt(res.rows),
                           finding=finding, facts=facts, audit_id=res.audit_id)

    async def ev_lease_live(self, task: Task) -> EvidenceResult:
        kind_node, node, _ = self.node_params(task)
        res = await self.k8s().get_lease(kind_node)
        renew = (res.data or {}).get("spec", {}).get("renewTime")
        return self.result(task, source="probe_k8s_api", query=res.query, excerpt=res.excerpt(),
                           reason_for_probe="the SRE asked for the live lease renewal time (not held in Splunk)",
                           finding=f"{node} lease renewed at {renew}", facts={"renew_time": renew}, audit_id=res.audit_id)
