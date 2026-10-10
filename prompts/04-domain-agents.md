# Phase 4: domain agents

Use the domain-agent skill. Build `wt/agents/network/` and `wt/agents/openshift/`:
- Watch loops with the starting rules, thresholds in `rules.yaml`, baselines in SQLite.
- Evidence workers that consume `wt.tasks`, follow reuse first, probe second, and publish
  evidence items with source, exact query, excerpt and a deterministic finding where possible.
- The OpenShift Agent answers reboot, restart and eviction questions from Splunk events
  (Rebooted, Starting, Killing, Started, Evicted), using the Kubernetes API only to fill gaps.
- Both agents expose `/health` and their recent signals.
Done when: with `cp-congest.sh` phase A running, `wt.signals` gets `ocp.api_svc_timeouts` for
master-2 and `net.output_drops_rising` for cisco-sim-1 Gi1/0/11 within 5 minutes, both from
Splunk/SolarWinds queries (no probes), and tests pass on recorded fixtures.
