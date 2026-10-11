# PoC feasibility on a MacBook Air M2

YH's Mac is an **8 GB** M2 Air, with OrbStack. The lite profile has now been built and run on it
(phases 1 to 8, October 2026); the measured figures are in the next section, the rest are the
original estimates.

## Measured on the lite lab (8 GB M2 Air, 2026-10-11)

| Container | Memory in use | Limit |
|---|---|---|
| kind `ocp-sim-control-plane` (API server, etcd, controllers) | 0.73 to 0.83 GB | none (kind node) |
| kind `ocp-sim-worker` (svc-prober, demo app, metrics-server) | 0.26 to 0.31 GB | none (kind node) |
| `wt-agents` (Network, OpenShift, Parent agents + Copilot) | 75 to 110 MB | 384 MB |
| `wt-sims` (cisco-sim-1, splunk-sim, solarwinds-sim, forwarder) | 60 to 70 MB | 256 MB |
| `redis` | 5 to 8 MB | 64 MB |
| **Docker total (`make health`)** | **1.2 to 1.5 GB** | budget 2.5 GB |
| gemma3:1b in Ollama (outside Docker) | about 0.8 GB loaded | |

Every compose service has a `mem_limit`; the kind nodes are bounded by the OrbStack VM. The
whole demo (proactive watch, the flap, the investigation with gemma3:1b) passed `make
verify-copilot` with Safari and a terminal open. `make eval` with gemma3:4b runs with the
compose stack stopped (`make stop`), so the model has room.

## Verdict

**The basic demo is feasible on 8 GB with the lite profile, but tight.** The walkthrough with
wireframes and a recorded replay is low risk. A live run of both modes on the lite profile
(1 master + 1 worker, merged containers, `gemma3:1b`) should fit in about 6 to 7.5 GB. The full
3-master lab with `gemma3:4b` needs 16 GB and will not run on this Mac.
Recommended order on the day: slides and wireframes, then the replay of a recorded lite run, then
the live lite run as the finale if the Mac is behaving.

| Stand-in | Mimics | On an M2 Air | Effort | Verdict |
|---|---|---|---|---|
| kind, 1 master + 1 worker (lite) | OpenShift | Native arm64 containers, about 1.5 GB | Low | Recommended on 8 GB |
| kind, 3 control-plane + 1 worker | OpenShift (HA masters) | Native, 4.5 to 5.5 GB | Low | 16 GB only |
| k3d (3 servers) | OpenShift | Native arm64 | Low | Alternative (k3s uses embedded etcd/dqlite differently) |
| Minikube `--ha` (docker driver) | OpenShift | Native arm64, heavier | Medium | Alternative |
| MicroK8s | OpenShift | Multipass VM on macOS | Medium | Avoid |
| OpenShift Local (CRC) | Real OpenShift | Single node, about 10.5 GB RAM itself | High | Not on an Air; no multi-master |
| Cisco simulator (Python SSH + SNMP + syslog) | Cisco IOS-XE CLI/SNMP | Native | Low to medium | Recommended |
| containerlab + FRR | Real routing, link flaps | Native arm64 images | Medium | Stretch |
| GNS3 VM, vrnetlab IOSv/IOL, CML | Real Cisco IOS | x86 VMs; no nested virtualisation on M2 | High | Not feasible on M2 |
| splunk-sim (FastAPI, SPL subset) | Splunk search + alerts | Native, about 100 MB | Medium | Recommended |
| Splunk Enterprise trial container | Real Splunk | amd64 image under emulation, 2-4 GB | High | Only on a bigger machine |
| solarwinds-sim (SNMP poller + SWIS) | SolarWinds NPM | Native, about 80 MB | Medium | Recommended |
| gemma3:1b in Ollama (native) | On-prem Gemma ~40B | Apple GPU, about 1 GB | Low | Recommended on 8 GB |
| gemma3:4b in Ollama (native) | On-prem Gemma ~40B | Apple GPU, about 3.5 GB | Low | 16 GB only |

## Memory budget: lite profile (8 GB M2 Air, the demo machine)

| Component | RAM |
|---|---|
| macOS, Terminal, Safari with the Copilot (nothing else open) | 3 to 3.5 GB |
| OrbStack VM overhead | 0.3 to 0.5 GB |
| kind: 1 master + 1 worker, metrics-server, svc-prober | 1.3 to 1.7 GB |
| `wt-agents` (4 agents in one container) + `wt-sims` (cisco-sim-1, splunk-sim, solarwinds-sim, forwarder) + Redis | 0.5 to 0.7 GB |
| gemma3:1b with a 2k context (or `WT_LLM=off`: 0) | 1 to 1.3 GB |
| **Total** | **about 6 to 7.5 GB** |

Settings: OrbStack memory limit 3.5 GB; `OLLAMA_CONTEXT_LENGTH=2048`; close Claude Code, IDEs and
other browsers before presenting. macOS will compress memory and may swap a little. If the
Copilot gets sluggish, set `WT_LLM=off`: detection, ranking and the evidence chain do not need
the model, only the plain-words explanations do (rule templates take over).

What the lite lab gives up: three API servers, so the "2 in 3 calls" pattern of a master calling
the kubernetes Service. The fault goes on worker-1 instead; the mechanism (API calls time out,
lease missed for more than 40 s, transient NotReady, no reboot) is the same. The master-specific
detail is shown from the slides and wireframes. gemma3:1b explanations will be plainer than
gemma3:4b's.

Building with Claude Code on 8 GB is fine: phases run one part of the lab at a time.

## Memory budget: full profile (16 GB machine)

| Component | RAM |
|---|---|
| kind: 3 control-plane nodes (each API server + etcd) + 1 worker, metrics-server, demo app | 4.5 to 5.5 GB |
| 4 agents + Copilot + Redis + 2 Cisco sims + splunk-sim + solarwinds-sim + forwarder | 1.5 to 2 GB |
| gemma3:4b (q4) with a 4k context | about 3.5 to 4 GB |
| OrbStack VM overhead + macOS + browser | 3 to 4 GB |
| **Total** | **about 12.5 to 15 GB** |

That is tight even on 16 GB. Not possible on the 8 GB demo Mac.

## Reproducing the real incident

What we need to show: a master's calls to the kubernetes Service time out, the node flaps
NotReady and recovers, with no reboot and no full restart of key services.
- With 3 API servers, the kubernetes Service (10.96.0.1 in kind, 172.30.0.1 in OpenShift)
  sends about 2 in 3 calls from a master to the other masters, across the network. Degrading
  master 2's interface with `tc netem` makes those calls time out while local ones succeed.
- The kubelet reaches the API through kind's load balancer, so heavy loss also stops lease
  renewals; after 40 s the node controller marks master 2 NotReady, and it returns to Ready
  when the loss stops. The node container is not restarted, so the kubelet emits no Rebooted or
  Starting event; WatchTower reads that from Splunk, as it would in production. (kind nodes share
  the Docker VM's kernel, so boot IDs cannot tell nodes apart in the lab.)
- Risk: etcd on master 2 may lose quorum participation during the 70% loss burst; with 3
  members the other two keep quorum, which matches the real behaviour (cluster stays up).
- Check `tc` works inside kind nodes first:
  `docker exec ocp-sim-control-plane2 tc qdisc add dev eth0 root netem delay 100ms && docker exec ocp-sim-control-plane2 tc qdisc del dev eth0 root`.

## M2 Air specifics
- Fanless: sustained load throttles. Agents call the LLM only when inputs change; start the lab
  shortly before presenting.
- No nested virtualisation on M2 (Apple added it from M3 with macOS 15), so VM-based network
  operating systems are out.

## The LLM question
The on-prem Gemma (~40B) may not be fully competent at agentic tasks. The design copes:
- Detection, correlation and confidence scoring are deterministic; the demo ranks the right
  cause with the LLM off.
- The LLM does six narrow jobs with JSON schemas and few-shot examples, chooses next steps from
  a fixed catalogue, and may move confidence by at most ±0.10.
- `make eval` scores any OpenAI-compatible model on those jobs. Run it on gemma3:4b in the PoC
  and on the on-prem Gemma in the pilot to measure competence instead of guessing.
- Measured (eval/results, 2026-10-11): gemma3:4b 100% valid JSON, 86% citation accuracy, p50
  17 s; gemma3:1b 100% valid JSON, 57% citation accuracy, p50 4.6 s. Both rank all 7 cases
  correctly because the rules do; the rules alone also get 7 of 7.
- gemma3:4b on the 8 GB Mac: only with the lab stopped. With the OrbStack VM running, macOS
  swapped 10 GB and single calls took minutes; with the kind nodes stopped, 6 to 30 s.

## Known limits
- kind is upstream Kubernetes, not OpenShift: no ClusterOperators, OVN-Kubernetes, Routes or
  OpenShift's own alerting. The agent design carries over; OCP-specific sources come in the pilot.
- splunk-sim and solarwinds-sim implement only the query subsets the agents use. Real SPL and
  SWQL may need adjusting against the production data model (index and sourcetype names).
- Simulator output is only as realistic as its fixtures. Seed from sanitised real `show`
  output if policy allows.
