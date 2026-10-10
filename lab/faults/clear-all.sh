#!/usr/bin/env bash
# Remove every injected fault from the lab.
for n in $(kind get nodes --name ocp-sim 2>/dev/null); do docker exec "$n" tc qdisc del dev eth0 root 2>/dev/null || true; done
for p in 9101 9102; do curl -fsS -XDELETE "http://127.0.0.1:$p/faults" >/dev/null 2>&1 || true; done
echo "all faults cleared"
