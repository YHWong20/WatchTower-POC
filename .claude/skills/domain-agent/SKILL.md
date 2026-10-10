---
name: domain-agent
description: Build or extend a WatchTower domain agent (Network for Cisco, OpenShift for OCP) - watch rules over Splunk/SolarWinds data, evidence gathering on request, and read-only probes. Use when adding a watch rule, an evidence type, or a new domain.
---

# Domain agents

A domain agent covers one technology, never one device. It does two jobs:

1. **Watch** (deterministic). On a timer, run the queries in `contracts/sources.yaml` for its
   domain, compare with a rolling baseline, and publish a `Signal` (`contracts/signal.schema.json`)
   to `wt.signals` when a rule hits. No LLM here.
2. **Investigate on request.** Read `wt.tasks` for tasks addressed to it
   (`{investigation_id, evidence_key, params, needs_live}`), gather the evidence and publish an
   evidence item to `wt.evidence`.

## Reuse first, probe second
For every evidence request:
1. Look up the key in `contracts/sources.yaml`.
2. If the primary source is `splunk` or `solarwinds`, query it. Use the result if it is fresh
   enough (`max_age_s` in the task, default 120).
3. Probe only if the task says `needs_live: true`, or the reused data is missing or stale.
   Write `reason_for_probe` on the evidence item.
4. Respect `probe_policy` (max probes per investigation, minimum interval per device).
5. Never write a collector that duplicates what Splunk or SolarWinds already ingests.

## Evidence item checklist
`id`, `domain`, `source`, exact `query` (SPL, SWQL, CLI command or API path), `collected_at`,
`excerpt` (raw, trimmed to 2000 chars), `finding` (one line, deterministic where a predicate
exists, else `interpreted_by: llm`).

## Watch rules (starting set)
Network Agent, every 60 s:
- `net.output_drops_rising`: OutDiscards delta > 0 for 3 consecutive SolarWinds polls on any port.
- `net.errors_rising`: InErrors/CRC delta > 0 for 2 polls.
- `net.link_flap`: `%LINK-3-UPDOWN` in Splunk syslog for a port twice in 10 min.
- `net.util_vs_baseline`: utilisation > baseline p95 + 20 points (baseline per hour of week).
OpenShift Agent, every 30 s:
- `ocp.api_svc_timeouts`: Service timeouts per node per 5 min > baseline + 2 (svc-prober calls
  every second with a 1 s timeout; phase A of the lab fault gives about 10 per 5 min).
- `ocp.lease_update_failures`: any `failed to update lease` in 5 min.
- `ocp.etcd_latency`: etcd "took too long" / unhealthy-peer warnings > baseline.
- `ocp.node_not_ready`: NodeNotReady event (severity `state_change`).
Thresholds live in `wt/agents/<domain>/rules.yaml`, not in code. Each watch query is a `watch_*`
entry in `contracts/sources.yaml` (one query covers every node or port). Signals repeat every
`repeat_s` while a rule keeps hitting; state changes are sent once.

## Where things are
- `wt/agents/base.py`: Task / EvidenceResult messages, watch loop, task worker, baselines, /health.
- `wt/agents/network/agent.py`, `wt/agents/openshift/agent.py`: rules and evidence handlers
  (`ev_<signal>`), each returning `facts` the Parent Agent's predicates read.
- Record live exchanges with `WT_RECORD_SOURCES=<file>`; replay them in tests with
  `wt.common.sources.recording.ReplayTransport` (see tests/test_domain_agents.py).

## Read-only guarantees
- CLI: the SSH adapter refuses anything not matching `^show ` and logs the attempt.
- Kubernetes: the service account has get/list/watch only; the adapter has no write methods.
- Every query and probe goes to the audit log.

## Adding a domain later (e.g. Palo Alto, Nutanix)
Out of scope for the PoC. The pattern: a new agent folder, its sources in `sources.yaml`
(reuse Splunk/SolarWinds first), its watch rules, its ports/hosts in `topology.yaml`, and at
least one playbook. The Parent Agent does not change.

## Tests
Fixtures in `eval/fixtures/<domain>/`: recorded SPL/SWQL responses, CLI output, Kubernetes
objects. Each watch rule gets a test with a hit, a miss and a stale-data case.
