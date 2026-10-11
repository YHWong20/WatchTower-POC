---
name: demo-scenario
description: Run, rehearse, record or replay the WatchTower Builders Day demo (proactive watch and incident investigation on the master-2 congestion fault). Use when preparing or running the demo.
---

# Demo scenario: one fault, both modes

Story: master-2's API calls to the kubernetes Service start timing out because its switch
port is congested. Splunk and SolarWinds raise nothing. WatchTower spots it (Mode 1), then the
node flaps NotReady, Splunk alerts, and the SRE asks WatchTower to investigate (Mode 2).

## Before
1. lab-control health check passes. Clear faults. Copilot at http://localhost:8080 is empty.
2. Warm the model: `curl localhost:11434/api/generate -d '{"model":"gemma3:4b","prompt":"ok"}'`.
3. Wi-Fi off once everything is pulled (shows air-gap readiness).
4. Have the replay ready: `make replay NAME=builders-day` works on its own.

## Run (live, about 15 minutes)
| t | Action | What the audience sees |
|---|---|---|
| 0:00 | `make fault NODE=ocp-sim-control-plane2` | Copilot: tools quiet, agents green |
| ~3:00 | | OpenShift and Network signals appear (weak, below thresholds) |
| ~5:00 | | Cross-domain watch item: master-2 timeouts + Gi1/0/11 drops, medium confidence |
| 5:30 | Click "Explain in plain words" | LLM summary with evidence links |
| ~10:30 | | master-2 NotReady about 45 s, then Ready; Splunk NodeNotReady alert appears |
| 11:00 | SRE clicks Investigate on the alert | Investigation runs: tasks, evidence arrives live |
| ~13:00 | | Report: likely cause, no reboot, evidence chain, hypotheses, next steps, Network P3 |
| 13:30 | Ask "why only 2 in 3 calls?" | Answer citing the Service load-balancing evidence |
| 14:00 | Copy escalation note | Markdown note with the evidence pack |
On the 8 GB Mac (lite profile) inject `make fault NODE=ocp-sim-worker`; the story is the same,
told on worker-1, and the master-specific "2 in 3" detail is shown from the slides.
Short version for a tight slot: `make fault NODE=ocp-sim-control-plane2 MODE=investigate`
(flap at about 3 minutes), show Mode 2 only.

## What the commands do
- `make demo-reset`: empty Copilot (watch items, investigations, signals) and no splunk-sim fired
  alerts; keeps baselines. `make demo RESET=1` runs it first.
- `lab/demo_screenshots.py` (run next to `make demo`): a screenshot per stage into docs/demo/.
- `make demo [MODE=investigate] [NAME=x]`: preflight (`make health`, waits for open watch items
  to close), records to `demo/recordings/<NAME>`, runs the fault through the Copilot with every
  definition-of-done check printed (lab/verify_copilot.py), keeps recording 30 s after the report.
- `make record NAME=x`: records until Ctrl-C (all six wt.* streams, the stand-ins' alerts and
  the agents' status every 10 s, sparkline samples).
- `make stop && make replay NAME=x [SPEED=10]`: fresh Redis (wt-replay-redis on :6390), player +
  projector + Copilot in replay mode on :8080 (recording clock, no LLM, no Splunk/SolarWinds).
  `SPEED=0` jumps to the end state. `make up` brings the live lab back.
- On the 8 GB Mac the storyline runs on worker-1 / Gi1/0/13 (lite profile).

## Fallbacks
- Lab not behaving: `make stop && make replay NAME=rehearsal` (the recorded rehearsal at 10x).
- LLM slow or wrong: toggle `WT_LLM=off`; the rule-based explanation and ranking still show.
- No laptop: walk through `wireframes/*.png` in the deck.

## Record
`make record NAME=builders-day` during a good rehearsal; keep the recording in `demo/recordings/`.
