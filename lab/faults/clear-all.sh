#!/usr/bin/env bash
# Remove every injected fault from the lab.
for n in $(kind get nodes --name ocp-sim); do docker exec "$n" tc qdisc del dev eth0 root 2>/dev/null || true; done
for p in 9101 9102; do curl -fsS -XDELETE "http://localhost:$p/faults" >/dev/null || true; done
echo "all faults cleared"
