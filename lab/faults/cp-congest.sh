#!/usr/bin/env bash
# Reproduce the incident: congestion on master 2's network path makes its calls to the
# kubernetes Service time out, it misses lease renewals, flaps NotReady and recovers without
# a reboot. Drives both halves in step: tc netem in the kind node, drops on the switch sim.
# Reference implementation, not yet run on an M2: phase 2 of the build verifies it.
# Usage: lab/faults/cp-congest.sh [node] [--investigate]
#   full profile: ocp-sim-control-plane2 (master-2)   lite profile (8 GB): ocp-sim-worker (worker-1)
set -euo pipefail
NODE="${1:-ocp-sim-control-plane2}"
case "$NODE" in                       # switch port per contracts/topology.yaml
  ocp-sim-control-plane)  PORT="Gi1/0/10" ;;
  ocp-sim-control-plane2) PORT="Gi1/0/11" ;;
  ocp-sim-control-plane3) PORT="Gi1/0/12" ;;
  ocp-sim-worker)         PORT="Gi1/0/13" ;;
  *) echo "unknown node $NODE" >&2; exit 1 ;;
esac
SW="http://localhost:9101"            # cisco-sim-1 fault API
A=360; B=240; C=75; D=180
[[ "${2:-}" == "--investigate" ]] && { A=60; B=60; }

netem() { docker exec "$NODE" tc qdisc replace dev eth0 root netem "$@"; }
switch() { curl -fsS -XPOST "$SW/faults" -H 'content-type: application/json' \
             -d "{\"type\":\"congestion\",\"port\":\"$PORT\",\"level\":\"$1\"}" >/dev/null; }
clear_all() {
  docker exec "$NODE" tc qdisc del dev eth0 root 2>/dev/null || true
  curl -fsS -XDELETE "$SW/faults" >/dev/null || true
  echo "$(date +%T) cleared"
}
trap clear_all EXIT INT TERM

echo "$(date +%T) phase A: creeping congestion (${A}s)"; netem delay 80ms 60ms loss 3%;  switch low;    sleep "$A"
echo "$(date +%T) phase B: getting worse (${B}s)";      netem delay 250ms 150ms loss 12%; switch medium; sleep "$B"
echo "$(date +%T) phase C: burst, expect NotReady (${C}s)"; netem loss 70%;              switch high;   sleep "$C"
echo "$(date +%T) phase D: partial recovery (${D}s)";   netem delay 250ms 150ms loss 12%; switch medium; sleep "$D"
