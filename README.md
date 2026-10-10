# WatchTower PoC kit (Builders Day)

Everything needed to vibe-code a small WatchTower proof of concept on a MacBook Air M2 with
Claude Code. WatchTower is an **AI operations copilot that sits above SolarWinds and Splunk**:
domain agents investigate, a parent agent connects the dots, and the copilot explains the
likely cause, shows the evidence and recommends next diagnostic steps and escalation.
Read-only; it stops at Observe → Detect → Diagnose → Recommend. Scope: Cisco + OpenShift.

## What's in the kit

| File | Purpose |
|---|---|
| `MASTER_PROMPT.md` | Paste as the first message to Claude Code in an empty repo. The whole spec. |
| `CLAUDE.md` | Project memory Claude Code reads every session: layout, rules, Mac notes. |
| `prompts/01..08-*.md` | One prompt per build phase, each with a "done when" check. |
| `.claude/skills/lab-control/` | Bring the lab up/down, health check, inject and clear the fault. |
| `.claude/skills/domain-agent/` | Build domain agents: watch rules, reuse first, probe second, read-only. |
| `.claude/skills/investigation-playbooks/` | Hypotheses, evidence, confidence, explanation, escalation. |
| `.claude/skills/demo-scenario/` | Run, record and replay the demo (one fault, both modes). |
| `contracts/signal.schema.json` | Deterministic watch-rule hit from a domain agent, with its source query. |
| `contracts/investigation.schema.json` | The evidence chain: observation → evidence → correlation → hypothesis → confidence → actions. |
| `contracts/sources.yaml` | Reuse-first rulebook: SPL/SWQL per signal, and when a probe is allowed. |
| `contracts/topology.yaml` | Which switch port feeds which OpenShift node. |
| `contracts/escalation.yaml` | Teams, priority rules, evidence pack. |
| `playbooks/` | `ocp-node-flap` and `network-port-congestion` investigation playbooks. |
| `wireframes/` | Copilot screens for both modes (HTML + PNG). |
| `lab/kind-config.yaml` | 3 masters + 1 worker standing in for OpenShift. |
| `lab/faults/` | `cp-congest.sh` (the incident) and `clear-all.sh`. Reference scripts, not yet run on an M2. |
| `lab/clab-topology.yml` | Stretch only: containerlab with FRR. |
| `FEASIBILITY.md` | What runs on an M2 Air, memory budget, alternatives. |

## The lab

| Real system | PoC stand-in |
|---|---|
| OpenShift cluster (3 masters + workers) | kind `ocp-sim`: 3 control-plane nodes + 1 worker, svc-prober |
| Cisco access + distribution switch | `cisco-sim-1`, `cisco-sim-2` (SSH CLI, SNMP, syslog, fault API) |
| Splunk (+ forwarders) | `splunk-sim` (HEC, SPL subset, saved-search alerts) + `forwarder` |
| SolarWinds NPM | `solarwinds-sim` (SNMP poller, SWIS/SWQL subset, alerts) |
| On-prem Gemma (~40B) | `gemma3:4b` in Ollama on the Mac |

## The demo: one fault, both modes

`cp-congest.sh` congests master 2's switch port and network path. Its calls to the kubernetes
Service start timing out while Splunk and SolarWinds stay below every threshold.
1. **Proactive watch:** WatchTower raises a cross-domain watch item (master-2 timeouts +
   Gi1/0/11 drops) before anything breaks.
2. **Incident investigation:** master 2 flaps NotReady for about 45 s and recovers without a
   reboot; Splunk alerts; the SRE clicks Investigate and gets the likely cause, the evidence
   chain, next steps and "escalate to Network team, P3".

## How to use it

1. Before the day (large downloads): on the 8 GB Air use the lite profile (FEASIBILITY.md), install OrbStack,
   `brew install kind kubectl k9s ollama python@3.12 uv jq`, `ollama pull gemma3:4b gemma3:1b`,
   install Claude Code.
2. `mkdir watchtower && cd watchtower && git init`, copy the kit in.
3. Start Claude Code, paste `MASTER_PROMPT.md`, then work through `prompts/02..08`. Commit per phase.
4. The evening before: `make record NAME=builders-day` a clean run so `make replay` can carry
   the walkthrough if the live lab misbehaves.

## Why these choices

- **kind with 3 control-plane nodes.** The incident needs real API Service load-balancing across
  several API servers, and kind nodes are containers whose network can be degraded with `tc`.
  Minikube and MicroK8s (Multipass VM on macOS) are heavier for HA.
- **Simulated Splunk and SolarWinds.** WatchTower's value is reusing what those tools already
  hold, so the PoC must query them through their real API shapes (SPL export search, SWIS/SWQL).
  Pointing at the real tools later is a config change.
- **Cisco simulators, not GNS3.** The M2 has no nested virtualisation, so VM-based IOS images do
  not run usefully; simulators also give reliable, repeatable faults.
- **Deterministic detection and scoring, narrow LLM jobs.** The on-prem Gemma may not be fully
  competent; the demo still ranks the right cause with the LLM off, and `make eval` measures how
  much the model adds.
