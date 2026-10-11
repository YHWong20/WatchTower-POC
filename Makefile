# WatchTower PoC. WT_PROFILE=lite (default, 8 GB Mac) or full (16 GB).
export WT_PROFILE ?= lite
ifeq ($(WT_PROFILE),lite)
NODE ?= ocp-sim-worker
else
NODE ?= ocp-sim-control-plane2
endif
COMPOSE = docker compose --profile $(WT_PROFILE)

.PHONY: sync test selfcheck lab-up lab-down up down stop logs health fault clear verify-flap verify-sources verify-watch verify-investigation verify-copilot demo record replay replay-list eval offline-check

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

down:                       ## stop and remove everything, including stored data (volumes)
	$(COMPOSE) down -v

stop:                       ## stop the containers, keep their data (make up starts them again)
	$(COMPOSE) stop

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

verify-copilot:             ## phase 6 / definition of done, live through the Copilot (full fault, ~15 min)
	uv run python lab/verify_copilot.py

# ---------------------------------------------------------------- demo, record, replay (phase 7)
NAME ?=
SPEED ?= 10
PORT ?= 8080
demo:                       ## the storyline, narrated and recorded: make demo [MODE=investigate] [NAME=x]
	uv run python lab/demo.py $(if $(filter investigate,$(MODE)),--investigate) $(if $(NAME),--name $(NAME))

record:                     ## record the bus and the stand-ins' alerts until Ctrl-C: make record NAME=x
	@test -n "$(NAME)" || { echo "usage: make record NAME=x"; exit 1; }
	uv run python -m wt.replay record $(NAME)

replay:                     ## replay a recording with the lab and LLM stopped: make replay NAME=x [SPEED=10]
	lab/replay.sh $(NAME) $(SPEED) $(PORT)

replay-list:
	@uv run python -m wt.replay list

# ---------------------------------------------------------------- eval (phase 8)
offline-check:              ## offline readiness: local images/model, no external loads, no internet flows during CMD
	uv run python lab/offline_check.py $(if $(DURATION),--duration $(DURATION)) $(if $(CMD),-- $(CMD))

MODEL ?= gemma3:4b
BASE_URL ?= http://127.0.0.1:11434/v1
eval:                       ## score a model on the six LLM jobs: make eval [MODEL=gemma3:4b] [BASE_URL=...] [API_KEY=...]
	uv run python eval/run_eval.py --model $(MODEL) --base-url $(BASE_URL) $(if $(API_KEY),--api-key $(API_KEY))
