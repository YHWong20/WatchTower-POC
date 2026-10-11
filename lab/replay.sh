#!/usr/bin/env bash
# make replay NAME=x [SPEED=10] [PORT=8080]: replay a recording with the lab and the model stopped.
# Starts a fresh, throwaway Redis (wt-replay-redis on 127.0.0.1:6390), then the player + Copilot.
set -euo pipefail
cd "$(dirname "$0")/.."
NAME="${1:?usage: make replay NAME=<recording> (make replay-list shows them)}"; SPEED="${2:-10}"; PORT="${3:-8080}"
docker info >/dev/null 2>&1 || { echo "Docker is not running: start OrbStack (the replay needs only a Redis container)" >&2; exit 1; }
docker rm -f wt-replay-redis >/dev/null 2>&1 || true
docker run -d --rm --name wt-replay-redis -p 127.0.0.1:6390:6379 --memory 64m redis:7-alpine \
  redis-server --save "" --appendonly no >/dev/null
trap 'docker rm -f wt-replay-redis >/dev/null 2>&1 || true' EXIT
for _ in $(seq 1 20); do docker exec wt-replay-redis redis-cli ping >/dev/null 2>&1 && break; sleep 0.3; done
WT_LLM=off uv run python -m wt.replay play "$NAME" --speed "$SPEED" --port "$PORT" --redis redis://127.0.0.1:6390/0
