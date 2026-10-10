---
name: lab-control
description: Bring the WatchTower Mac lab (kind HA "ocp-sim" cluster, svc-prober, Cisco simulators, Splunk/SolarWinds stand-ins, forwarder, Redis) up or down, check its health, and inject or clear the master-2 congestion fault. Use whenever a task needs the lab running or a fault applied.
---

# Lab control

## Bring up
1. `docker info >/dev/null` or ask the user to start OrbStack / Docker Desktop.
2. `kind get clusters | grep -q ocp-sim || kind create cluster --name ocp-sim --config lab/kind-config.yaml`
   (3 control-plane nodes + 1 worker; about 3 minutes on an M2 Air). On an 8 GB Mac use
   `WT_PROFILE=lite` and `lab/kind-config-lite.yaml` instead (see below).
3. `kubectl --context kind-ocp-sim apply -f lab/k8s/` (metrics-server with `--kubelet-insecure-tls`,
   demo app, svc-prober DaemonSet on masters, read-only RBAC for the agents).
4. `kubectl --context kind-ocp-sim wait --for=condition=Ready nodes --all --timeout=240s`
5. `docker compose up -d redis cisco-sim-1 cisco-sim-2 splunk-sim solarwinds-sim forwarder`
6. Confirm the forwarder ships node events: splunk-sim should hold a `Starting` event for each
   node from cluster creation. The reboot check relies on these events, not on the API (kind
   nodes share the Docker VM's kernel, so their boot IDs are identical anyway).

## Health check (run before any demo)
- `kubectl top nodes` returns numbers for all 4 nodes.
- `kubectl -n wt-lab logs ds/svc-prober --tail=3` shows `ok` lines with latency in ms.
- splunk-sim has data from every source:
  `curl -s -u admin:demo 'localhost:8089/services/search/jobs/export' -d search='search index=* earliest=-5m | stats count by sourcetype' -d output_mode=json`
  must list kube:events, kubelet, kube:container:svc-prober, kube:container:etcd, cisco:ios.
- solarwinds-sim has polled: SWQL `SELECT TOP 1 DateTime FROM Orion.NPM.InterfaceErrors ORDER BY DateTime DESC` is under 2 min old.
- No alerts fired in either stand-in.
- `ssh -p 2221 admin@localhost "show interfaces Gi1/0/11"` returns IOS-style text.
- `docker exec ocp-sim-control-plane2 tc qdisc show dev eth0` shows no `netem`.
- `curl -s localhost:11434/api/tags` lists `WT_MODEL`.

## The fault
`lab/faults/cp-congest.sh ocp-sim-control-plane2 [--investigate]` reproduces the real incident on
master 2 and keeps both halves in step:
| Phase | Node (tc netem on eth0) | Switch (cisco-sim-1 Gi1/0/11) | Expected effect |
|---|---|---|---|
| A creep (6 min) | delay 80ms 60ms, loss 3% | `congestion level=low` | some Service timeouts, drops rising, no alarms |
| B worse (4 min) | delay 250ms 150ms, loss 12% | `level=medium` | lease update failures, etcd peer warnings |
| C burst (75 s) | loss 70% | `level=high` | master 2 NotReady (40 s grace), then Ready |
| D partial (3 min) | as B | `level=medium` | node Ready, drops continue |
| clear | remove netem | `clear` | back to normal |
`--investigate` shortens A and B to 1 minute each for the incident-investigation demo.
The switch sim models congestion as rising output drops with **zero CRC**, average utilisation
near 46%, and drops concentrated in one egress queue.

If `tc` fails with "Specified qdisc kind is unknown", the Docker VM kernel lacks `sch_netem`:
use the fallback (a privileged sidecar `nicolaka/netshoot` in the node's network namespace), or,
if that also fails, `make replay` the recorded run and tell the user.

Clear everything: `lab/faults/clear-all.sh`.

## Memory on a MacBook Air M2
`docker stats --no-stream` after bring-up. Budget on 16 GB: kind (4 nodes) about 4.5-5.5 GB,
sims + Redis + agents about 1.5 GB, Ollama gemma3:4b about 3.5 GB, macOS and browser the rest.

### Lite profile (8 GB, YH's Mac): `WT_PROFILE=lite`
- OrbStack: Settings > System > Memory limit 3.5 GB. Quit everything except Terminal and Safari.
- kind from `lab/kind-config-lite.yaml`: 1 master + 1 worker (about 1.3-1.7 GB).
- `docker compose --profile lite up -d`: one `wt-agents` container running all four agents as
  asyncio tasks, one `wt-sims` container running cisco-sim-1, splunk-sim, solarwinds-sim and the
  forwarder, plus Redis (about 0.5-0.7 GB together). cisco-sim-2 is skipped.
- Model: `gemma3:1b` with `OLLAMA_CONTEXT_LENGTH=2048` (about 1-1.3 GB), or `WT_LLM=off`.
- Fault: `make fault NODE=ocp-sim-worker` (port Gi1/0/13). The worker flaps NotReady through the
  same mechanism. The "2 in 3 calls" line does not apply here: every API call from a worker
  crosses the switch, so expect the evidence chain to say that instead.
- Do not run Claude Code, a second browser or an IDE while the demo runs.
- Expect macOS to compress memory and use some swap; if the Copilot lags, switch to `WT_LLM=off`.

## Tear down
`docker compose down -v && kind delete cluster --name ocp-sim`
