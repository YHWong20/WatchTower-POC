# WatchTower PoC. WT_PROFILE=lite (default, 8 GB Mac) or full (16 GB).
export WT_PROFILE ?= lite

.PHONY: sync test selfcheck lab-up lab-down up down logs fault clear demo record replay eval

sync:
	uv sync

test: sync
	uv run pytest -q

selfcheck:
	uv run python -m wt.common.selfcheck

# Later phases (see prompts/).
lab-up lab-down fault clear:
	@echo "$@: built in phase 2 (prompts/02-lab.md)"; exit 1
up down logs:
	@echo "$@: built in phases 3-4 (compose)"; exit 1
demo record replay:
	@echo "$@: built in phase 7 (prompts/07-demo-replay.md)"; exit 1
eval:
	@echo "$@: built in phase 8 (prompts/08-eval-polish.md)"; exit 1
