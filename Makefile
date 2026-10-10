# WatchTower PoC. WT_PROFILE=lite (default, 8 GB Mac) or full (16 GB).
export WT_PROFILE ?= lite
ifeq ($(WT_PROFILE),lite)
NODE ?= ocp-sim-worker
else
NODE ?= ocp-sim-control-plane2
endif
COMPOSE = docker compose --profile $(WT_PROFILE)

.PHONY: sync test selfcheck lab-up lab-down up down logs health fault clear verify-flap verify-sources verify-watch verify-investigation demo record replay eval

sync:
	uv sync

test: sync
	uv run pytest -q

selfcheck:
	uv run python -m wt.common.selfcheck

# ---------------------------------------------------------------- lab (phases 2 and 3)
lab-up:                     ## kind cluster + metrics-server, svc-prober, demo app, read-only RBAC
	lab/up.sh

lab-down:
	kind delete cluster --name ocp-sim

up:                         ## sims, stand-ins, redis and the agents + Copilot (http://localhost:8080)
	$(COMPOSE) up -d --build

down:
	$(COMPOSE) down -v

logs:
	$(COMPOSE) logs -f --tail=50

health:
	lab/health.sh

fault:                      ## make fault [NODE=...] [MODE=investigate]
	lab/faults/cp-congest.sh $(NODE) $(if $(filter investigate,$(MODE)),--investigate)

clear:
	lab/faults/clear-all.sh

verify-flap:                ## fault + NotReady/Ready, no reboot, prober timeouts, no alerts in phase A
	uv run python lab/verify_flap.py $(NODE)

verify-sources:             ## phase 3 "done when": every SPL/SWQL template in sources.yaml against the stand-ins
	PYTHONPATH=. uv run python lab/verify_sources.py

verify-watch:               ## phase 4 "done when": fault phase A -> both watch signals within 5 min, no probes
	uv run python lab/verify_watch.py

verify-investigation:       ## phase 5 "done when": recorded incidents (eval/cases) with the LLM off and on
	uv run pytest -v tests/test_investigation.py

# ---------------------------------------------------------------- later phases (see prompts/)
demo record replay:
	@echo "$@: built in phase 7 (prompts/07-demo-replay.md)"; exit 1
eval:
	@echo "$@: built in phase 8 (prompts/08-eval-polish.md)"; exit 1
