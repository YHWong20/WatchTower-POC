# Phase 1: skeleton

Read CLAUDE.md, MASTER_PROMPT.md and everything in contracts/ and playbooks/.
Create the repo layout from CLAUDE.md with `uv`, then build the shared library in `wt/common/`:
- Pydantic v2 models generated from `contracts/*.schema.json` (Signal, Investigation and its parts),
  plus loaders for `sources.yaml`, `topology.yaml`, `escalation.yaml` and `playbooks/*.yaml`.
- `bus.py`: async Redis Streams publish/consume with consumer groups for the six `wt.*` streams.
- `llm.py`: OpenAI-compatible client (`WT_LLM_BASE_URL`, `WT_MODEL`), JSON-schema output via
  Ollama's `format`, validate, retry once with the error, return None on failure; hash cache;
  `WT_LLM=off` switch.
- `sources/splunk.py` (SPL export search, fired alerts) and `sources/solarwinds.py` (SWQL query),
  both taking templates from `sources.yaml`.
- `probes/ssh_cli.py` (asyncssh, refuses anything not matching `^show `) and `probes/k8s.py`
  (read-only methods only).
- `store.py` (SQLite: investigations, evidence, watch items) and `audit.py` (every query, probe
  and LLM call with inputs hash and duration).
Tests with fixtures for each adapter, including the CLI allowlist refusing `conf t`.
Done when: `make test` passes and `python -m wt.common.selfcheck` loads every contract and playbook.
