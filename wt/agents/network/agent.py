"""Network Agent: one agent for every Cisco switch (domain agent, not device agent).

Watch (every 60 s, deterministic): SolarWinds interface errors and traffic for every port, and
Splunk switch syslog. Rules in rules.yaml:
  net.output_drops_rising   OutDiscards > 0 for 3 consecutive polls on a port
  net.errors_rising         InErrors > 0 for 2 consecutive polls
  net.link_flap             %LINK-3-UPDOWN for a port twice in 10 min
  net.util_vs_baseline      utilisation above the port's p95 (hour of week) + 20 points
Evidence on request: from SolarWinds and Splunk; the switch is probed over SSH (show commands
only) for per-queue drops, which SolarWinds does not poll, or when the last poll is stale.
"""

from __future__ import annotations

import asyncio
import json
import re
from collections import defaultdict
from datetime import datetime
from pathlib import Path
from typing import Any

from wt.agents.base import AgentContext, DomainAgent, EvidenceResult, Task, parse_window
from wt.common.contracts import Signal
from wt.common.probes.base import ProbeThrottled
from wt.common.probes.ssh_cli import SshCli

HERE = Path(__file__).resolve().parent
_LINK = re.compile(r"Interface (\S+), changed state to (up|down)")


def short_port(name: str) -> str:
    """GigabitEthernet1/0/13 -> Gi1/0/13."""
    return re.sub(r"^GigabitEthernet", "Gi", name)


def ts(value: str) -> float:
    return datetime.fromisoformat(value.replace("Z", "+00:00")).timestamp()


def streak(values: list[float]) -> int:
    """How many of the latest values in a row are > 0."""
    n = 0
    for v in reversed(values):
        if v and v > 0:
            n += 1
        else:
            break
    return n


def tail_excerpt(rows: list[dict], limit: int = 2000) -> str:
    """The newest rows that fit, oldest first (evidence is about what is happening now)."""
    lines: list[str] = []
    size = 0
    for r in reversed(rows):
        line = json.dumps(r, separators=(",", ":"), default=str)
        if size + len(line) + 1 > limit:
            break
        lines.append(line)
        size += len(line) + 1
    return "\n".join(reversed(lines))


def parse_queue_stats(output: str) -> dict[int, int]:
    """Drop bytes per egress queue from `show platform hardware fed switch active qos queue stats`."""
    drops: dict[int, int] = {}
    section = output.split("Drop Counters", 1)[1] if "Drop Counters" in output else ""
    for line in section.splitlines():
        parts = line.split()
        if len(parts) >= 7 and parts[0].isdigit():
            drops[int(parts[0])] = sum(int(p) for p in parts[1:7] if p.isdigit())
        elif parts and parts[0] == "AQM":
            break
    return drops


class NetworkAgent(DomainAgent):
    name = "network-agent"
    domain = "network"
    rules_file = HERE / "rules.yaml"

    def __init__(self, ctx: AgentContext, rules: dict[str, Any] | None = None, ssh: SshCli | None = None):
        super().__init__(ctx, rules)
        self.ssh = ssh or SshCli(ctx.sources, ctx.audit, ctx.settings.ssh_user, ctx.settings.ssh_password,
                                 actor=self.name)
        self.if_ids: dict[tuple[str, str], int] = {}
        self.sampled_at: dict[str, float] = {}

    def describe(self) -> str:
        n = len(self.ctx.topology.switches)
        return f"{n} switch{'es' if n != 1 else ''} · SolarWinds + Splunk"

    # ------------------------------------------------------------ watch

    async def watch_once(self, now: float) -> list[Signal]:
        lookback = int(self.rules.get("lookback_min", 10))
        errors = await self.swql("watch_interface_errors", minutes=lookback)
        traffic = await self.swql("watch_interface_traffic", minutes=lookback)
        signals: list[Signal] = []
        newest = max((ts(r["DateTime"]) for r in errors.rows), default=None)
        stale_after = float(self.rules.get("stale_after_s", 180))
        if newest is None or now - newest > stale_after:
            self.status["stale"] = [f"SolarWinds: newest poll {'missing' if newest is None else f'{now - newest:.0f} s old'}"]
        else:
            self.status["stale"] = []
            signals += self.drop_and_error_rules(errors.rows, traffic.rows, errors.query, now)
            signals += self.util_rule(traffic.rows, traffic.query, now)
        signals += await self.link_flap_rule(now)
        return signals

    def drop_and_error_rules(self, rows: list[dict], traffic: list[dict], query: str, now: float) -> list[Signal]:
        by_port: dict[tuple[str, str], list[dict]] = defaultdict(list)
        for r in rows:
            by_port[(r["Switch"], r["Port"])].append(r)
        util = self.latest_util(traffic)
        out, samples = [], []
        cfg = self.rules["rules"]
        for (switch, port), polls in by_port.items():
            polls.sort(key=lambda r: r["DateTime"])
            entity = f"{switch}:{port}"
            drops = [r["OutDiscards"] or 0 for r in polls]
            errs = [r["InErrors"] or 0 for r in polls]
            last_ts = ts(polls[-1]["DateTime"])
            if self.sampled_at.get(entity) != last_ts:            # one sample per SolarWinds poll
                self.sampled_at[entity] = last_ts
                samples += [("out_discards", entity, drops[-1]), ("in_errors", entity, errs[-1])]
            n = streak(drops)
            if n >= cfg["net.output_drops_rising"]["min_polls"]:
                u = util.get(entity)
                feeds = self.ctx.topology.node_on_port(switch, port)
                out.append(self.signal(
                    "net.output_drops_rising", "interface", entity, "weak",
                    f"Output drops on {switch} {port} rising {n} polls in a row ({drops[-1]:,} in the last poll)"
                    + (f"; utilisation average {u:.0f}%" if u is not None else "")
                    + (f"; feeds {feeds}" if feeds else ""),
                    value=n, baseline=0, tool="solarwinds", query=query, window_s=len(polls) * 60, ts=now))
            e = streak(errs)
            if e >= cfg["net.errors_rising"]["min_polls"]:
                out.append(self.signal(
                    "net.errors_rising", "interface", entity, "weak",
                    f"Input errors (CRC) on {switch} {port} rising {e} polls in a row ({errs[-1]:,} in the last poll)",
                    value=e, baseline=0, tool="solarwinds", query=query, window_s=len(polls) * 60, ts=now))
        if samples:
            self.ctx.store.add_samples([(m, ent, now, float(v)) for m, ent, v in samples])
        return out

    @staticmethod
    def latest_util(rows: list[dict]) -> dict[str, float]:
        latest: dict[str, tuple[str, float]] = {}
        for r in rows:
            entity = f"{r['Switch']}:{r['Port']}"
            u = max(r.get("InPercentUtil") or 0, r.get("OutPercentUtil") or 0)
            if entity not in latest or r["DateTime"] > latest[entity][0]:
                latest[entity] = (r["DateTime"], u)
        return {k: v for k, (_, v) in latest.items()}

    def util_rule(self, rows: list[dict], query: str, now: float) -> list[Signal]:
        cfg = self.rules["rules"]["net.util_vs_baseline"]
        out = []
        for entity, u in self.latest_util(rows).items():
            key = f"util:{entity}"
            p95 = self.baselines.p95(key, now, cfg["min_samples"])
            hit = p95 is not None and u > p95 + cfg["points_above_p95"]
            if hit:
                switch, port = entity.split(":", 1)
                out.append(self.signal(
                    "net.util_vs_baseline", "interface", entity, "weak",
                    f"Utilisation on {switch} {port} at {u:.0f}%, baseline p95 {p95:.0f}% for this hour",
                    value=u, baseline=p95, tool="solarwinds", query=query, ts=now))
            else:
                self.baselines.add(key, now, u)
            self.ctx.store.add_samples([("util", entity, now, u)])
        return out

    async def link_flap_rule(self, now: float) -> list[Signal]:
        cfg = self.rules["rules"]["net.link_flap"]
        res = await self.splunk("watch_link_events", window=cfg["window"])
        changes: dict[tuple[str, str], list[str]] = defaultdict(list)
        for r in res.rows:
            m = _LINK.search(r.get("_raw", ""))
            if m:
                changes[(r.get("host", "?"), short_port(m.group(1)))].append(m.group(2))
        out = []
        for (switch, port), states in changes.items():
            if len(states) >= cfg["min_changes"] and "down" in states:
                out.append(self.signal(
                    "net.link_flap", "interface", f"{switch}:{port}", "notable",
                    f"{port} on {switch} changed state {len(states)} times in {cfg['window']} "
                    f"({states.count('down')} down)", value=len(states), baseline=0,
                    tool="splunk", query=res.query, window_s=parse_window(cfg["window"]), ts=now))
        return out

    # ------------------------------------------------------------ evidence on request

    async def gather(self, task: Task) -> EvidenceResult:
        handler = {
            "interface_discards": self.ev_interface_discards,
            "interface_utilisation": self.ev_interface_utilisation,
            "interface_lookup": self.ev_interface_lookup,
            "active_alerts": self.ev_active_alerts,
            "switch_syslog": self.ev_switch_syslog,
            "queue_drops": self.ev_queue_drops,
            "live_interface_counters": self.ev_live_counters,
        }.get(task.signal)
        if handler is None:
            raise ValueError(f"network-agent has no evidence handler for {task.signal!r}")
        return await handler(task)

    def result(self, task: Task, **kw: Any) -> EvidenceResult:
        return EvidenceResult(task_id=task.task_id, investigation_id=task.investigation_id, signal=task.signal,
                              domain=self.domain, agent=self.name, **kw)

    async def if_id(self, switch: str, port: str) -> int:
        if (switch, port) not in self.if_ids:
            res = await self.swql("interface_lookup", switch=switch, port=port)
            if not res.rows:
                raise LookupError(f"SolarWinds does not monitor {switch} {port}")
            self.if_ids[(switch, port)] = int(res.rows[0]["InterfaceID"])
        return self.if_ids[(switch, port)]

    async def ev_interface_discards(self, task: Task) -> EvidenceResult:
        p = task.params
        switch, port, minutes = p["switch"], p["port"], int(p.get("minutes", 60))
        res = await self.swql("interface_discards", if_id=await self.if_id(switch, port), minutes=minutes)
        rows = res.rows
        out_d = [r["OutDiscards"] or 0 for r in rows]
        in_e = [r["InErrors"] or 0 for r in rows]
        out_e = [r["OutErrors"] or 0 for r in rows]
        now = self.ctx.clock()
        newest = ts(rows[-1]["DateTime"]) if rows else None
        stale = newest is None or now - newest > task.max_age_s
        first_drop = next((r["DateTime"] for r in rows if (r["OutDiscards"] or 0) > 0), None)
        # longest run of polls with drops, and whether the latest polls still have them
        best = run = 0
        for v in out_d:
            run = run + 1 if v > 0 else 0
            best = max(best, run)
        facts = {
            "polls": len(rows), "out_discards": out_d, "in_errors": in_e, "out_errors": out_e,
            "rising_polls": best, "latest_streak": streak(out_d), "total_out_discards": sum(out_d),
            "latest_two_rising": len(out_d) >= 2 and out_d[-1] > 0 and out_d[-2] > 0,
            "errors_flat": not any(in_e) and not any(out_e), "in_errors_polls": sum(1 for v in in_e if v > 0),
            "first_drop_at": first_drop, "newest_poll": rows[-1]["DateTime"] if rows else None,
            "stale": stale, "switch": switch, "port": port,
        }
        if not rows:
            finding = f"SolarWinds has no polls for {switch} {port} in the last {minutes} min"
        else:
            drops_part = (f"OutDiscards on {port} > 0 in {best} consecutive polls (total {sum(out_d):,}), "
                          + ("still rising in the latest polls" if facts["latest_two_rising"] else "not in the latest polls")
                          if best else f"no output drops on {port} in {len(rows)} polls")
            err_part = "; InErrors and OutErrors flat" if facts["errors_flat"] else \
                f"; InErrors > 0 in {facts['in_errors_polls']} polls (CRC)"
            finding = drops_part + err_part + (" (SolarWinds data is stale)" if stale else "")
        result = self.result(task, source="solarwinds", query=res.query, excerpt=tail_excerpt(rows),
                             finding=finding[:300], facts=facts, audit_id=res.audit_id)
        if stale and rows and (task.needs_live or task.allow_probe):
            live = await self.ev_live_counters(task)
            result.facts["live"] = live.facts
        return result

    async def ev_interface_utilisation(self, task: Task) -> EvidenceResult:
        p = task.params
        switch, port, minutes = p["switch"], p["port"], int(p.get("minutes", 60))
        res = await self.swql("interface_utilisation", if_id=await self.if_id(switch, port), minutes=minutes)
        util = [max(r.get("InPercentUtil") or 0, r.get("OutPercentUtil") or 0) for r in res.rows]
        avg = round(sum(util) / len(util), 1) if util else None
        peak = round(max(util), 1) if util else None
        finding = (f"Utilisation on {switch} {port}: average {avg}%, peak {peak}% over {len(util)} polls (60 s averages)"
                   if util else f"SolarWinds has no traffic polls for {switch} {port}")
        return self.result(task, source="solarwinds", query=res.query, excerpt=tail_excerpt(res.rows), finding=finding,
                           facts={"avg_util": avg, "peak_util": peak, "polls": len(util)}, audit_id=res.audit_id)

    async def ev_interface_lookup(self, task: Task) -> EvidenceResult:
        res = await self.swql("interface_lookup", switch=task.params["switch"], port=task.params["port"])
        row = res.rows[0] if res.rows else None
        finding = (f"{task.params['switch']} {task.params['port']} is InterfaceID {row['InterfaceID']}, "
                   f"status {'up' if row['Status'] == 1 else 'down'}") if row else "interface not in SolarWinds"
        return self.result(task, source="solarwinds", query=res.query, excerpt=tail_excerpt(res.rows), finding=finding,
                           facts={"row": row}, audit_id=res.audit_id)

    async def ev_active_alerts(self, task: Task) -> EvidenceResult:
        res = await self.ctx.solarwinds.active_alerts()
        finding = (f"{len(res.rows)} active SolarWinds alert(s): " + "; ".join(r["TriggeredMessage"] for r in res.rows))[:300] \
            if res.rows else "No active SolarWinds alerts"
        return self.result(task, source="solarwinds", query=res.query, excerpt=tail_excerpt(res.rows), finding=finding,
                           facts={"count": len(res.rows)}, audit_id=res.audit_id)

    async def ev_switch_syslog(self, task: Task) -> EvidenceResult:
        p = task.params
        res = await self.splunk("switch_syslog", switch=p["switch"], port=p["port"], window=p.get("window", "60m"))
        flaps = [r for r in res.rows if (m := _LINK.search(r.get("_raw", ""))) and short_port(m.group(1)) == p["port"]
                 and m.group(2) == "down"]
        finding = (f"{len(flaps)} link-down message(s) for {p['port']} in Splunk" if flaps
                   else f"No %LINK-3-UPDOWN down messages for {p['port']} in Splunk")
        return self.result(task, source="splunk", query=res.query,
                           excerpt=tail_excerpt([{"_time": r.get("_time"), "_raw": r.get("_raw")} for r in res.rows]),
                           finding=finding, facts={"link_down": len(flaps), "lines": len(res.rows)}, audit_id=res.audit_id)

    def mgmt(self, switch: str) -> str:
        spec = self.ctx.topology.switches[switch]
        return spec.mgmt.get("ssh", f"{switch}:22")

    async def probe(self, switch: str, command: str):
        try:
            return await self.ssh.run(self.mgmt(switch), command)
        except ProbeThrottled:
            await asyncio.sleep(self.ctx.sources.probe_policy.min_interval_per_device_s)
            return await self.ssh.run(self.mgmt(switch), command)

    async def ev_queue_drops(self, task: Task) -> EvidenceResult:
        p = task.params
        command = self.query_text("queue_drops", "probe_cli", port=p["port"])
        res = await self.probe(p["switch"], command)
        drops = parse_queue_stats(res.output)
        total = sum(drops.values())
        top_q, top = max(drops.items(), key=lambda kv: kv[1]) if drops else (None, 0)
        share = round(top / total, 3) if total else 0.0
        finding = (f"Egress drops on {p['port']}: {share:.0%} in queue {top_q} ({top:,} bytes of {total:,}): bursty traffic in one class"
                   if total else f"No egress queue drops on {p['port']}")
        return self.result(task, source="probe_cli", query=command, excerpt=res.excerpt(),
                           reason_for_probe="SolarWinds does not poll per-queue drops (no CBQoS polling); "
                                            "the playbook marks this evidence needs_live",
                           finding=finding, facts={"drops_by_queue": drops, "total": total, "top_queue": top_q,
                                                   "top_share": share}, audit_id=res.audit_id)

    async def ev_live_counters(self, task: Task) -> EvidenceResult:
        p = task.params
        command = self.query_text("live_interface_counters", "probe_cli", port=p["port"])
        res = await self.probe(p["switch"], command)
        m = re.search(r"Total output drops: (\d+)", res.output)
        drops = int(m.group(1)) if m else None
        return self.result(task, source="probe_cli", query=command, excerpt=res.excerpt(),
                           reason_for_probe="the last SolarWinds poll is older than "
                                            f"{task.max_age_s} s and the investigation needs current counters",
                           finding=f"Live counters on {p['port']}: total output drops {drops}",
                           facts={"total_output_drops": drops}, audit_id=res.audit_id)
