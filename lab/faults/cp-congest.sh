#!/usr/bin/env bash
# Reproduce the incident: congestion on a node's network path makes its calls to the kubernetes
# Service time out, it misses lease renewals, flaps NotReady and recovers without a reboot.
# Drives both halves in step: tc netem in the kind node, drops on the cisco-sim-1 port.
# Usage: lab/faults/cp-congest.sh [node] [--investigate]
#   full profile: ocp-sim-control-plane2 (master-2)   lite profile (8 GB): ocp-sim-worker (worker-1)
# Phase start times go to data/faults/last.jsonl (lab/verify-flap.sh reads them).
# Phase C uses 90% loss, not the 70% in MASTER_PROMPT.md: measured on the lite lab, 70% after
# phase B let a lease renewal through inside the 40 s window, so the node stayed Ready. 90%
# reliably misses it, and on a master it fails nearly every call that leaves the node, which is
# the "2 in 3" pattern (the third call reaches the local API server). Ready returns ~45 s after
# the burst: the kubelet first drops its dead HTTP/2 connection and reconnects.
set -euo pipefail
cd "$(dirname "$0")/../.."
if [[ "${WT_PROFILE:-lite}" == "lite" ]]; then DEFAULT_NODE=ocp-sim-worker; else DEFAULT_NODE=ocp-sim-control-plane2; fi
NODE="${1:-$DEFAULT_NODE}"
case "$NODE" in                       # switch port per contracts/topology.yaml
  ocp-sim-control-plane)  PORT="Gi1/0/10" ;;
  ocp-sim-control-plane2) PORT="Gi1/0/11" ;;
  ocp-sim-control-plane3) PORT="Gi1/0/12" ;;
  ocp-sim-worker)         PORT="Gi1/0/13" ;;
  *) echo "unknown node $NODE" >&2; exit 1 ;;
esac
docker inspect "$NODE" >/dev/null 2>&1 || { echo "kind node $NODE is not running (make lab-up?)" >&2; exit 1; }
SW="http://127.0.0.1:9101"            # cisco-sim-1 fault API (every node port is on cisco-sim-1)
curl -fsS "$SW/health" >/dev/null || { echo "cisco-sim-1 fault API not reachable on $SW (make up?)" >&2; exit 1; }
A=360; B=240; C=75; D=180
[[ "${2:-}" == "--investigate" ]] && { A=60; B=60; }

# tc inside the kind node; if the kernel lacks sch_netem there, use a netshoot sidecar in the
# node's network namespace (same effect, one extra image).
TC=(docker exec "$NODE" tc)
if ! docker exec "$NODE" tc qdisc replace dev eth0 root netem delay 1ms 2>/dev/null; then
  echo "tc netem failed inside $NODE; using a nicolaka/netshoot sidecar" >&2
  TC=(docker run --rm --net "container:$NODE" --cap-add NET_ADMIN nicolaka/netshoot:v0.13 tc)
fi
netem() { "${TC[@]}" qdisc replace dev eth0 root netem "$@"; }
switch() { curl -fsS -XPOST "$SW/faults" -H 'content-type: application/json' \
             -d "{\"type\":\"congestion\",\"port\":\"$PORT\",\"level\":\"$1\"}" >/dev/null; }
mkdir -p data/faults
LOG=data/faults/last.jsonl
: > "$LOG"
mark() { printf '{"phase":"%s","at":%s,"node":"%s","port":"%s"}\n' "$1" "$(date +%s)" "$NODE" "$PORT" >> "$LOG"; }
clear_all() {
  "${TC[@]}" qdisc del dev eth0 root 2>/dev/null || true
  curl -fsS -XDELETE "$SW/faults" >/dev/null || true
  mark clear
  echo "$(date +%T) cleared"
}
trap clear_all EXIT
trap 'exit 130' INT TERM          # stop for good (the EXIT trap clears); never carry on to the next phase

echo "$(date +%T) fault on $NODE, cisco-sim-1 $PORT"
mark A; echo "$(date +%T) phase A: creeping congestion (${A}s)";    netem delay 80ms 60ms loss 3%;    switch low;    sleep "$A"
mark B; echo "$(date +%T) phase B: getting worse (${B}s)";          netem delay 250ms 150ms loss 12%; switch medium; sleep "$B"
mark C; echo "$(date +%T) phase C: burst, expect NotReady (${C}s)"; netem loss 90%;                   switch high;   sleep "$C"
mark D; echo "$(date +%T) phase D: partial recovery (${D}s)";       netem delay 250ms 150ms loss 12%; switch medium; sleep "$D"
