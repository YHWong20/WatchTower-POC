# WatchTower: project memory

AI operations copilot above SolarWinds and Splunk. Domain agents (Network, OpenShift)
investigate, the Parent Agent connects the dots, the Copilot explains and guides the SRE.
Scope: Cisco + OpenShift only. See MASTER_PROMPT.md.

## Layout
- `wt/common/` contracts (Pydantic), bus, LLM client + prompts, source adapters
  (splunk.py, solarwinds.py), probe adapters (ssh_cli.py, k8s.py), evidence store, audit log
- `wt/agents/network/`, `wt/agents/openshift/`, `wt/agents/parent/`, `wt/copilot/`
- `sims/cisco_sim/`, `sims/splunk_sim/`, `sims/solarwinds_sim/`, `sims/forwarder/`
- `lab/` kind config, manifests (svc-prober, demo app), fault scripts
- `contracts/` schemas, sources (reuse-first rulebook), topology, escalation
- `playbooks/` investigation playbooks (YAML)
- `wireframes/` the Copilot design
- `eval/` recorded fixtures (`fixtures/`, `fixtures/recorded/`) and investigation cases (`cases/`:
  recorded evidence + expected outcome; `make_case.py` builds one from WT_RECORD_EVIDENCE output)

## Rules
- Read-only. No write calls to switches or the cluster. CLI allowlist: commands starting with
  `show `. Kubernetes: get/list/watch only (RBAC enforces it). No remediation features.
- OpenShift events (and kubelet / control-plane logs) are in Splunk. Answer OpenShift questions
  from Splunk; the Kubernetes API is a fallback for gaps, not a primary source.
- Reuse first, probe second: read `contracts/sources.yaml` before adding any data access. Probe
  only when the playbook marks the evidence `needs_live: true` or reused data is stale/missing,
  and record why in the evidence item.
- Deterministic first: watch rules, baselines, correlation and confidence are plain code.
  The LLM only does the six jobs in MASTER_PROMPT.md, with JSON schemas, two few-shot examples,
  one retry, rule fallback, ±0.10 confidence cap. No native tool calling.
- Evidence before opinion: every finding, hypothesis and recommendation references evidence ids;
  every evidence item stores source, query/probe text, timestamp and raw excerpt.
- Domain agents are per technology, never per device.
- Call the LLM only when inputs change; cache by hash. The M2 Air is fanless.
- No cloud calls. Everything must run with Wi-Fi off once images are pulled.
- Small images (`python:3.12-slim`), `mem_limit` on every service.
- Tests use recorded fixtures (SPL/SWQL responses, CLI output, k8s objects), not the live lab.

## Commands
- `make lab-up` / `make lab-down`, `make up` / `make down`, `make logs`
- `make health`, `make fault [NODE=…] [MODE=investigate]` (NODE defaults by profile), `make clear`,
  `make verify-flap`, `make verify-sources`, `make verify-watch`, `make verify-investigation`
- Copilot: http://localhost:8080. Agents' health: :8101 network, :8102 openshift, :8103 parent.
- Rebuilding the image restarts every service that uses it (sims too, which drops the switch's
  in-memory fault). To update only the agents: `docker compose build wt-agents && docker compose
  up -d --no-deps wt-agents`.
- `make demo`, `make record NAME=…`, `make replay NAME=…`, `make eval`, `make test`

## Mac notes (MacBook Air M2)
- Ollama runs natively; containers use `http://host.docker.internal:11434/v1`.
- Agents join the `kind` network; API via the read-only `data/kubeconfig-agent.yaml` (wt-agent
  token, written by `make lab-up`), never the kind admin kubeconfig.
- Load images into kind with `docker save --platform` + `kind load image-archive` (plain
  `kind load docker-image` fails on multi-platform images).
- kind's kubernetes Service VIP is 10.96.0.1 (OpenShift: 172.30.0.1). Display names
  master-1..3 come from topology.yaml.
- Stand-ins on the Mac: splunk-sim REST 127.0.0.1:8089 (token `wt-poc-token`, or admin/demo),
  HEC :8088; solarwinds-sim SWIS 127.0.0.1:17778 (watchtower/watchtower). The kind network is
  dual-stack: bind or connect IPv4 explicitly where a name could resolve to IPv6 first.
- No nested virtualisation on M2, so no VM-based network images.
