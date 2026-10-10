# Master prompt: WatchTower, an AI operations copilot (Builders Day PoC)

You are building a proof of concept called **WatchTower** on a **MacBook Air M2** (fanless,
**8 GB RAM**, Docker via OrbStack). Build for the lite profile first; the full profile is for
a 16 GB machine. Read `CLAUDE.md`, `contracts/`, `playbooks/`,
`lab/` and `wireframes/` before writing code. Work in the phases at the end, commit after
each one, and stop after each phase to show me what works and how to see it.

## What WatchTower is

WatchTower is **not** a monitoring dashboard and not a failure-prediction system. It is an
**AI operations copilot** that sits **above existing monitoring tools** (SolarWinds and
Splunk) and helps a 24/7 monitoring team:

1. **detect emerging issues** through weak-signal and cross-domain correlation;
2. do **first-level fault diagnosis** when an alarm fires;
3. **explain the likely cause** with supporting evidence;
4. **recommend next diagnostic steps** and the right **escalation**.

End state: existing monitoring tools observe → domain agents investigate → the parent agent
connects the dots → WatchTower explains and guides the SRE.

Scope is deliberately narrow and deep: **Cisco switching and Red Hat OpenShift only.**

## The real incident this PoC is built around

On OpenShift, a master node made API calls to the `kubernetes` Service (itself, through the
Service VIP) and the calls timed out, probably because of a network bottleneck. The node then
dropped out and came back: it went NotReady and recovered without a physical reboot, and key
services did not fully restart. WatchTower must be able to (a) spot that pattern building up
and (b) explain it after the fact, including "it was a transient NotReady (node flap), not a
reboot", with evidence.

Useful facts for the explanation (verify against the cluster you run):
- The kubelet renews its node Lease every 10 s; OpenShift's default profile marks a node
  NotReady after 40 s without a renewal (`node-monitor-grace-period`) and starts evicting pods
  after 300 s. A short gap gives NotReady → Ready with no reboot and no evictions.
- Calls to the `kubernetes` Service from a master are load-balanced across all API servers,
  so most of them leave the node and cross the switch even though the caller is a master.
  Partial failure (about 2 in 3 with 3 masters) points at the network path, not the local node.

## Design principles (non-negotiable)

- **Domain agents, not device agents.** One Network Agent for all Cisco switches, one
  OpenShift Agent for the cluster.
- **Parent agent investigates across domains**: correlation, hypotheses, evidence, confidence,
  recommendations.
- **Reuse first, probe second.** Splunk is the primary source for logs and events; SolarWinds
  for interface and device metrics. Native APIs and CLI are used only when live state or deeper
  evidence is needed. `contracts/sources.yaml` is the rulebook.
- **Deterministic monitoring, AI-assisted reasoning.** Thresholds, baselines, collection and
  basic correlation are plain code. The AI does hypothesis generation, investigation planning,
  evidence interpretation, explanation and recommended next actions.
- **Evidence before opinion.** Every conclusion is traceable:
  observation → evidence → correlation → hypothesis → confidence → recommended actions
  (`contracts/investigation.schema.json`).
- **Read-only, human in the loop.** Stop at Observe → Detect → Diagnose → Recommend. No
  remediation, no write calls, no config changes, ever.

## Agents (each its own long-running container)

1. **Network Agent** (Cisco domain). Every 60 s it queries the SolarWinds stand-in (SWQL) for
   interface traffic, discards and errors, and the Splunk stand-in (SPL) for switch syslog. It
   runs deterministic watch rules (rising output drops, error growth, flaps, utilisation vs.
   baseline) and publishes `Signal`s. When the Parent Agent tasks it, it gathers named evidence,
   from SolarWinds and Splunk. It probes the switch over SSH (`show` commands only) for one
   thing SolarWinds does not poll, per-queue drops, or when the last poll is over 2 minutes old.
2. **OpenShift Agent** (OCP domain). Every 30 s it queries Splunk for API Service timeouts per
   node, kubelet lease and node-status errors, etcd latency warnings and node events, and runs
   watch rules. **OpenShift events are in Splunk**, so when tasked it answers every question
   from Splunk: NotReady/Ready times (NodeNotReady/NodeReady events), reboot or kubelet restart
   (Rebooted/Starting events), static pod restarts (Killing/Started events in the control-plane
   namespaces), evictions, pressure events, etcd and OVN logs. Node CPU/memory comes from
   SolarWinds if the node is monitored there. The Kubernetes API (read-only) is a fallback only
   when Splunk has a gap for the window.
3. **Parent Agent**. Correlates signals across domains using `contracts/topology.yaml` and a
   time window. Raises **watch items** (proactive watch) and opens **investigations** when a
   watch item passes 0.7 confidence, when a node changes state, or when an SRE asks. For each
   investigation it selects a playbook from `playbooks/`, tasks the domain agents for the
   evidence each hypothesis expects, scores confidence from evidence, and produces
   recommendations and an escalation from `contracts/escalation.yaml`.
4. **Copilot** (web UI on http://localhost:8080) for the SRE, following
   `wireframes/proactive-watch.html` and `wireframes/incident-investigation.html`: watch items,
   alarms from the existing tools with an **Investigate** button, investigation reports with the
   full evidence chain and its reuse rate, an Ask box, and a copyable escalation note. Supports **replay mode**.

## How the reasoning works (built for a modest on-prem model)

Production uses an on-prem **Gemma (~40B)** that may not be fully competent at agentic work;
the PoC uses **gemma3:4b** in Ollama. So:
- Detection, correlation and confidence scoring are deterministic. Confidence for a hypothesis =
  (weights of supporting evidence − weights of refuting evidence) / weights of all expected
  evidence, clipped to 0..1. The LLM may adjust it by at most ±0.10, with a stated reason.
- The LLM does six narrow jobs, each with a short prompt, two few-shot examples and a JSON schema
  passed to Ollama's `format` parameter (no native tool calling):
  1. summarise a watch item in plain words;
  2. propose up to two extra hypotheses not in the playbook (marked `proposed_by: llm`, start at 0);
  3. interpret evidence that has no deterministic predicate (e.g. a log excerpt) as
     supports / refutes / neutral, with a reason;
  4. explain the likely cause and what happened, including "reboot or not";
  5. choose next diagnostic steps from the playbook's action catalogue and draft the escalation note;
  6. answer SRE questions only from stored investigations and watch items, citing evidence ids.
- Validate every response with Pydantic; retry once with the error; else use a rule-based
  fallback and mark `llm_used=false`. Call the LLM only when inputs change (cache by hash).
- One LLM client, OpenAI-compatible, `WT_LLM_BASE_URL` (default
  `http://host.docker.internal:11434/v1`) and `WT_MODEL` (default `gemma3:4b`).
- `make eval` scores the six jobs on recorded fixtures (valid JSON %, citation accuracy,
  hypothesis ranking vs. expected, latency) and can target the on-prem Gemma later.

## Lab (MacBook Air stand-ins)

- **ocp-sim**: kind cluster from `lab/kind-config.yaml`: 3 control-plane nodes (masters) +
  1 worker, so the `kubernetes` Service really balances across 3 API servers. metrics-server,
  a small demo app, and **svc-prober**: a DaemonSet on the masters (hostNetwork, read-only RBAC)
  that calls `https://10.96.0.1:443/readyz` every 5 s and logs latency or `i/o timeout`
  (kind's Service VIP is 10.96.0.1; OpenShift's is 172.30.0.1).
- **cisco-sim-1** (access) and **cisco-sim-2** (distribution): Python containers with an
  IOS-style SSH CLI (`show version`, `show interfaces [port]`, `show interfaces counters errors`,
  `show platform hardware fed switch active qos queue stats interface <port>`, `show logging`,
  `show processes cpu sorted`), SNMP (IF-MIB incl. ifOutDiscards, ifHCOutOctets), syslog out,
  and a fault API (`congestion(port, level)`, `link_flap(port)`, `crc(port, rate)`, `clear`).
  `topology.yaml` maps cisco-sim-1 Gi1/0/10–12 to masters 1–3 and Gi1/0/13 to the worker.
- **splunk-sim**: FastAPI + SQLite. HEC ingest (`/services/collector/event`), search
  (`/services/search/jobs/export` with an SPL subset: `index= sourcetype= host= field=value
  "phrase" earliest= latest=` plus `| stats count by f` and `| timechart span=1m count`), and
  fired alerts (`/services/alerts/fired_alerts`) from saved searches including NodeNotReady.
- **solarwinds-sim**: polls the Cisco sims over SNMP every 60 s like Orion, stores history, and
  serves SWIS (`/SolarWinds/InformationService/v3/Json/Query`, SWQL subset `SELECT … FROM …
  WHERE … ORDER BY`) for Orion.Nodes, Orion.NPM.Interfaces, Orion.NPM.InterfaceTraffic,
  Orion.NPM.InterfaceErrors (InDiscards, OutDiscards, InErrors, OutErrors) and Orion.AlertActive.
  Its own alert (utilisation > 90 % for 5 min) should stay quiet during the creeping phase.
- **forwarder**: the stand-in for Splunk forwarding. It ships Kubernetes events, pod logs from
  kube-system and the lab namespace (API server, etcd, svc-prober), kubelet journal from every
  kind node (`docker exec … journalctl -u kubelet -f -o json`), and switch syslog to splunk-sim.
- **Fault**: `lab/faults/cp-congest.sh <node> [--investigate]` creates the incident on master 2
  in phases: A creep (netem delay 80 ms ± 60 ms, loss 3 %; low congestion on Gi1/0/11), B worse
  (250 ms ± 150 ms, loss 12 %), C burst (loss 70 % for 75 s, enough to miss the 40 s lease
  window), D partial recovery (back to B for 3 min), then clear. `--investigate` shortens A/B.
  `lab/faults/clear-all.sh` resets everything.
- Redis 7 streams: `wt.signals`, `wt.tasks`, `wt.evidence`, `wt.watch`, `wt.investigations`,
  `wt.sre`. SQLite evidence store for investigations, evidence and the audit log.
- Everything except kind and Ollama runs from one `docker-compose.yml` on the `kind` network,
  with a `mem_limit` on every service.
- **Lite profile (`WT_PROFILE=lite`) for the 8 GB MacBook Air the demo actually runs on:**
  `lab/kind-config-lite.yaml` (1 master + 1 worker), a compose profile `lite` that runs all four
  agents in one `wt-agents` container and the simulators in one `wt-sims` container (cisco-sim-1
  only), `gemma3:1b` with a 2k context, fault target `ocp-sim-worker` on Gi1/0/13. Budget: under
  2.5 GB inside Docker. Agent code must not care which profile is running; only compose,
  topology selection and the fault target change.

## Tech stack

Python 3.12, `uv`, FastAPI, Pydantic v2, redis-py (async), `kubernetes` client, asyncssh,
pysnmp, httpx, SQLite, Jinja2 + HTMX + Chart.js served locally (no CDN, no Node build), pytest.
Makefile: `lab-up`, `lab-down`, `up`, `down`, `logs`, `fault`, `clear`, `demo`, `record`,
`replay`, `eval`, `test`.

## Phases (stop after each)

1. Skeleton: contracts as Pydantic models, bus, LLM client, source adapters (Splunk, SolarWinds),
   probe adapters (SSH, Kubernetes), evidence store and audit log.
2. Lab: kind HA cluster, svc-prober, demo app; Cisco simulators; fault scripts.
3. Existing-tool stand-ins: splunk-sim, forwarder, solarwinds-sim.
4. Domain agents: watch rules and evidence gathering, reuse first, probe second.
5. Parent agent: correlation, watch items, playbook-driven investigations, confidence,
   recommendations, escalation.
6. Copilot UI to the wireframes, Investigate flow, Ask box.
7. Demo, record and replay.
8. Eval and polish: `make eval`, LLM-off and Wi-Fi-off runs, README.

## Definition of done

- **Proactive watch:** within 6 minutes of `cp-congest.sh ocp-sim-control-plane2`, a
  cross-domain watch item naming master 2 and cisco-sim-1 Gi1/0/11 appears with confidence
  ≥ 0.5, while splunk-sim and solarwinds-sim have fired no alerts and master 2 is still Ready.
- **Incident investigation:** after the flap, the NodeNotReady alert appears in the Copilot;
  pressing Investigate produces, within 3 minutes, a report whose top hypothesis is network
  congestion on master 2's path with confidence ≥ 0.7, states that the node did not reboot
  (no Rebooted or Starting events), lists at least 3 read-only next steps and recommends
  escalation to the Network team at P3. Every statement links to evidence showing its source
  and the exact query or probe.
- **Reuse:** the proactive watch item uses no probes at all; the investigation takes at least
  85% of its evidence from Splunk and SolarWinds, with at most one probe (per-queue drops),
  and the Copilot shows the reuse rate.
- On the lite profile, the same two outcomes hold with worker-1 and Gi1/0/13, and
  `docker stats` stays under 2.5 GB in total.
- The same top hypothesis comes out with the LLM switched off, and `make replay` reproduces
  both screens with the lab stopped.
