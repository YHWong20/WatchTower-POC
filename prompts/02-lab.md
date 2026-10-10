# Phase 2: lab

Build the lab (use the lab-control skill):
- `lab/kind-config.yaml` is given (3 control-plane + 1 worker). Add `lab/k8s/`: metrics-server
  (`--kubelet-insecure-tls`), namespace `wt-lab`, a small demo app, the read-only ClusterRole for
  agents, and **svc-prober**: a DaemonSet on masters (hostNetwork, tolerate the control-plane
  taint) that every 5 s calls `https://10.96.0.1:443/readyz` with a 2 s timeout using its service
  account token and logs one JSON line per call: `{"node","target","ok","ms","err"}` where err
  keeps the Go-style text `dial tcp 10.96.0.1:443: i/o timeout`.
- `sims/cisco_sim/`: Python container, IOS-style SSH CLI (commands in MASTER_PROMPT.md, output
  copied from real IOS-XE formatting), SNMP agent (IF-MIB incl. ifOutDiscards, ifInErrors,
  ifHCOutOctets), syslog to the forwarder, and a fault API on :9101/:9102
  (`congestion(port, level)`, `link_flap`, `crc`, `clear`). Congestion raises output drops and
  per-queue drops with zero CRC and average utilisation near 46%.
- `lab/faults/cp-congest.sh` and `lab/faults/clear-all.sh` as described in the lab-control skill.
Done when: the health check in the skill passes; running `cp-congest.sh ocp-sim-control-plane2
--investigate` makes `kubectl get nodes -w` show master 2 go NotReady then Ready without its
a Rebooted or Starting event for it in the cluster events, while svc-prober on master 2 logs timeouts for about 2 in 3 calls.
