#!/usr/bin/env bash
# Bring up the kind "ocp-sim" cluster for WT_PROFILE (lite: 1 master + 1 worker; full: 3 + 1)
# with metrics-server, svc-prober, the demo app and read-only RBAC, then write the agents'
# read-only kubeconfig to data/kubeconfig-agent.yaml. Safe to re-run.
set -euo pipefail
cd "$(dirname "$0")/.."
PROFILE="${WT_PROFILE:-lite}"
CONFIG=lab/kind-config.yaml; [[ "$PROFILE" == "lite" ]] && CONFIG=lab/kind-config-lite.yaml
K=(kubectl --context kind-ocp-sim)
IMAGES=(python:3.12-slim registry.k8s.io/metrics-server/metrics-server:v0.7.2)

docker info >/dev/null 2>&1 || { echo "Docker is not running: start OrbStack" >&2; exit 1; }
if ! kind get clusters 2>/dev/null | grep -qx ocp-sim; then
  echo "creating kind cluster ocp-sim ($PROFILE, $CONFIG)"
  kind create cluster --name ocp-sim --config "$CONFIG"
else
  echo "kind cluster ocp-sim exists"
fi

# Load images from the Mac so the nodes never pull (works with Wi-Fi off once pulled here).
# Save one platform only: `kind load docker-image` fails on multi-platform images with the
# containerd image store ("content digest ... not found").
tmp=$(mktemp -d); trap 'rm -rf "$tmp"' EXIT
for img in "${IMAGES[@]}"; do
  docker image inspect "$img" >/dev/null 2>&1 || docker pull -q "$img"
  docker save --platform "linux/$(docker version -f '{{.Server.Arch}}')" -o "$tmp/img.tar" "$img"
  kind load image-archive --name ocp-sim "$tmp/img.tar" >/dev/null
  echo "loaded $img"
done

"${K[@]}" apply -k lab/k8s/ >/dev/null
"${K[@]}" wait --for=condition=Ready nodes --all --timeout=240s
"${K[@]}" -n kube-system rollout status deploy/metrics-server --timeout=180s
"${K[@]}" -n wt-lab rollout status ds/svc-prober --timeout=180s
"${K[@]}" -n wt-lab rollout status deploy/demo-web --timeout=180s

# Read-only kubeconfig for the agents (wt-agent service account, get/list/watch only).
mkdir -p data
until token=$("${K[@]}" -n wt-lab get secret wt-agent-token -o jsonpath='{.data.token}' 2>/dev/null) && [[ -n "$token" ]]; do sleep 1; done
ca=$("${K[@]}" -n wt-lab get secret wt-agent-token -o jsonpath='{.data.ca\.crt}')
server=$(kind get kubeconfig --internal --name ocp-sim | awk '/server:/ {print $2; exit}')
cat > data/kubeconfig-agent.yaml <<YAML
apiVersion: v1
kind: Config
clusters:
  - name: ocp-sim
    cluster: {server: "$server", certificate-authority-data: "$ca"}
users:
  - name: wt-agent
    user: {token: "$(echo "$token" | base64 -d)"}
contexts:
  - name: wt-agent@ocp-sim
    context: {cluster: ocp-sim, user: wt-agent}
current-context: wt-agent@ocp-sim
YAML
chmod 600 data/kubeconfig-agent.yaml
echo "agent kubeconfig: data/kubeconfig-agent.yaml (server $server, read-only)"
"${K[@]}" get nodes -L wt/name
