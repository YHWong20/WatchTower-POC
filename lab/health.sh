#!/usr/bin/env bash
# Lab health check (the lab-control skill's list). Read-only. Exit 1 if any check fails.
set -uo pipefail
cd "$(dirname "$0")/.."
PROFILE="${WT_PROFILE:-lite}"
K=(kubectl --context kind-ocp-sim)
fails=0
ok()   { echo "   ok   $*"; }
bad()  { echo "   FAIL $*"; fails=$((fails + 1)); }
warn() { echo "   WARN $*"; }
skip() { echo "   SKIP $*"; }

echo "WatchTower lab health (profile: $PROFILE)"
docker info >/dev/null 2>&1 && ok "Docker is running" || { bad "Docker is not running"; exit 1; }

echo " cluster"
if ! kind get clusters 2>/dev/null | grep -qx ocp-sim; then bad "kind cluster ocp-sim missing (make lab-up)"; exit 1; fi
want=2; [[ "$PROFILE" == "full" ]] && want=4
ready=$("${K[@]}" get nodes --no-headers 2>/dev/null | awk '$2=="Ready"' | wc -l | tr -d ' ')
[[ "$ready" == "$want" ]] && ok "$ready/$want nodes Ready" || bad "$ready/$want nodes Ready"
grace=$(docker exec ocp-sim-control-plane grep -o 'node-monitor-grace-period=[0-9a-z]*' /etc/kubernetes/manifests/kube-controller-manager.yaml 2>/dev/null)
[[ "$grace" == "node-monitor-grace-period=40s" ]] && ok "$grace (OpenShift timing)" || bad "node-monitor-grace-period not pinned to 40s"
top=$("${K[@]}" top nodes --no-headers 2>/dev/null | wc -l | tr -d ' ')
[[ "$top" == "$want" ]] && ok "kubectl top nodes returns $top nodes" || bad "kubectl top nodes returns $top of $want nodes"
for pod in $("${K[@]}" -n wt-lab get pods -l app=svc-prober -o name 2>/dev/null); do
  line=$("${K[@]}" -n wt-lab logs "$pod" --tail=1 2>/dev/null)
  node=$(echo "$line" | jq -r .node 2>/dev/null)
  if [[ "$(echo "$line" | jq -r .ok 2>/dev/null)" == "true" ]]; then ok "svc-prober on $node: ok in $(echo "$line" | jq -r .ms) ms"
  else bad "svc-prober ${node:-$pod}: ${line:-no output}"; fi
done
[[ -n "$("${K[@]}" -n wt-lab get pods -l app=svc-prober -o name 2>/dev/null)" ]] || bad "svc-prober not deployed"
"${K[@]}" -n wt-lab get deploy demo-web -o jsonpath='{.status.readyReplicas}' 2>/dev/null | grep -q 2 \
  && ok "demo-web 2/2 ready" || bad "demo-web not ready"
SA=system:serviceaccount:wt-lab:wt-agent
if [[ "$("${K[@]}" auth can-i list nodes --as=$SA 2>/dev/null)" == yes && "$("${K[@]}" auth can-i get leases -n kube-node-lease --as=$SA 2>/dev/null)" == yes \
      && "$("${K[@]}" auth can-i delete pods --as=$SA 2>/dev/null)" == no && "$("${K[@]}" auth can-i patch nodes --as=$SA 2>/dev/null)" == no \
      && "$("${K[@]}" auth can-i create pods --as=$SA 2>/dev/null)" == no ]]; then
  ok "wt-agent RBAC is read-only (list nodes/leases yes; create, patch, delete no)"
else bad "wt-agent RBAC is not exactly read-only"; fi
[[ -s data/kubeconfig-agent.yaml ]] && ok "agent kubeconfig data/kubeconfig-agent.yaml" || bad "data/kubeconfig-agent.yaml missing (make lab-up)"
for n in $(kind get nodes --name ocp-sim); do
  docker exec "$n" tc qdisc show dev eth0 2>/dev/null | grep -q netem && bad "netem still on $n (make clear)" || ok "no netem on $n"
done

echo " simulators and bus"
sims=(cisco-sim-1); [[ "$PROFILE" == "full" ]] && sims+=(cisco-sim-2)
PYTHONPATH=. uv run --quiet python lab/check_sims.py "${sims[@]}" || fails=$((fails + 1))
docker compose exec -T redis redis-cli ping 2>/dev/null | grep -q PONG && ok "redis PONG" || bad "redis not answering (make up)"

echo " existing-tool stand-ins (splunk-sim, forwarder, solarwinds-sim)"
uv run --quiet python lab/check_tools.py $(kind get nodes --name ocp-sim 2>/dev/null) || fails=$((fails + 1))

echo " model"
model="${WT_MODEL:-$([[ "$PROFILE" == lite ]] && echo gemma3:1b || echo gemma3:4b)}"
if [[ "${WT_LLM:-on}" == "off" ]]; then skip "WT_LLM=off"
elif tags=$(curl -fs -m 3 http://127.0.0.1:11434/api/tags 2>/dev/null); then
  echo "$tags" | jq -e --arg m "$model" '.models[] | select(.name == $m)' >/dev/null && ok "Ollama has $model" \
    || warn "Ollama is running but $model is not pulled (ollama pull $model)"
else warn "Ollama not running on :11434 (fine with WT_LLM=off)"; fi

echo " memory"
total=$(docker stats --no-stream --format '{{.MemUsage}}' | awk '{v=$1; u=v; gsub(/[0-9.]/,"",u); gsub(/[A-Za-z]/,"",v);
  m = (u=="GiB") ? v*1024 : (u=="KiB") ? v/1024 : v; s+=m} END {printf "%d", s}')
budget=2560; [[ "$PROFILE" == "full" ]] && budget=7680
(( total < budget )) && ok "Docker containers use ${total} MiB (budget ${budget} MiB)" || warn "Docker containers use ${total} MiB (budget ${budget} MiB)"

echo
(( fails == 0 )) && echo "health OK" || echo "health: $fails check(s) failed"
exit $(( fails > 0 ))
