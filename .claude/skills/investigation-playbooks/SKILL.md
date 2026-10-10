---
name: investigation-playbooks
description: Write or change the Parent Agent's investigation playbooks (hypotheses, expected evidence, confidence weights, diagnostic action catalogue, escalation routing) and the evidence-chain logic. Use when tuning how WatchTower diagnoses an issue or explains it.
---

# Investigation playbooks

Playbooks in `playbooks/*.yaml` are the deterministic backbone of an investigation. The
Parent Agent:
1. Picks the playbook whose `triggers` match the alarm or watch item.
2. Collects `facts` first (what happened), then every `expect` and `refute` item for each
   hypothesis by tasking the domain agents (`wt.tasks`).
3. Evaluates each `predicate` in code. Items without a predicate, or with free text, go to the
   LLM "interpret evidence" job, which returns supports / refutes / neutral with a reason.
4. Scores confidence with the playbook's `formula`. The LLM may adjust by at most
   `llm_adjust_max`, with a reason stored in `llm_adjustment`.
5. Builds the evidence chain (`contracts/investigation.schema.json`): observations, evidence,
   correlations (topology + time), hypotheses with supporting/refuting/missing ids, conclusion.
6. Asks the LLM to explain and to pick 3-5 next steps from `actions` only, then routes the
   escalation with `contracts/escalation.yaml`.

## Writing a hypothesis
- One clear statement an SRE would recognise.
- 2-6 `expect` items, each tied to a source in `sources.yaml`, with a weight (1 weak, 2 strong).
- At least one `refute` item: what would prove it wrong. This is what makes the ranking honest.
- Prefer evidence from Splunk and SolarWinds; the playbook should reach at least 85% reuse.
- `needs_live: true` only when the evidence must be current (counters still rising) or is not
  held by Splunk/SolarWinds.
- `escalate_to` a team key from `escalation.yaml`.

## Explaining what happened (the OCP node flap)
Always state, from evidence:
- NotReady duration vs. the 40 s grace period and the 300 s eviction threshold.
- Whether the node rebooted or the kubelet restarted (Rebooted / Starting events in Splunk), and
  whether the API server or etcd pods restarted (Killing / Started events in Splunk).
- That the kubernetes Service spreads calls across all API servers, so partial failures from
  a master point at the path between masters.
Use the words "transient NotReady (node flap)", not "the node went down", unless a Rebooted event exists.

## Guardrails
- Actions are diagnostic only (`kind: diagnostic`). No remediation steps, no config changes.
- A conclusion must reference at least two evidence ids from two sources.
- If the top hypothesis is below 0.4, say "inconclusive", list what is missing, and recommend
  the evidence to gather next.

## Where things are
- Predicates in code: `wt/agents/parent/predicates.py`, keyed by the playbook item's `ev` name.
  An item without one goes to the LLM "interpret" job (job 3), or stays missing.
- Probe budget: only `needs_live` items probe; facts may fall back to a probe while
  `max_probes_per_investigation` allows; other expectations never probe (missing instead).
- Investigator: `wt/agents/parent/investigator.py`; LLM jobs and guards: `wt/common/jobs.py`.
  The model's wording is used only if it keeps every rule fact (duration, reboot, restarts,
  evictions); its ±0.10 adjustment is kept either way.

## Tests
`eval/cases/<name>/`: `evidence.jsonl` (recorded EvidenceResults, from WT_RECORD_EVIDENCE via
`eval/make_case.py`) and `case.yaml` (request + expected top hypothesis, confidence, escalation,
reuse). `tests/test_investigation.py` runs every case with the LLM off and with a fake LLM that
adjusts by -0.10 (`make verify-investigation`); `make eval` (phase 8) adds the real model.
