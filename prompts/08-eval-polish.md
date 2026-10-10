# Phase 8: eval and polish

- `eval/`: cases for the six LLM jobs built from the recorded run (plus 3 variants: CRC link
  fault, etcd slow disk, node CPU starvation). `make eval MODEL=x` reports valid-JSON rate,
  citation accuracy, top-hypothesis accuracy, confidence-band accuracy and p50/p95 latency.
  It must also run against any OpenAI-compatible endpoint so the on-prem Gemma can be scored.
- Run the demo with Wi-Fi off and with `WT_LLM=off`; fix anything that breaks.
- Check `docker stats` against the budget in FEASIBILITY.md; add `mem_limit`s.
- README: setup, demo steps, what is simulated vs. real, how to point at real Splunk/SolarWinds.
Done when: `make eval` prints a table for gemma3:4b, and both offline runs pass.
