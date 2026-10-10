# Phase 5: parent agent

Use the investigation-playbooks skill. Build `wt/agents/parent/`:
- **Correlator** (deterministic): group signals by topology (`topology.yaml`) and a 10-minute
  window into watch items; single-domain items stay low; a cross-domain pair linked by topology
  scores medium. Publish to `wt.watch`.
- **Investigator**: opens an investigation when a watch item passes 0.7, a `state_change` signal
  arrives, or an SRE asks via `wt.sre`. Selects a playbook, collects facts, tasks domain agents
  for expected and refuting evidence, evaluates predicates, scores confidence, builds the evidence
  chain, then calls the LLM jobs (interpret, extra hypotheses, explain, choose next steps, draft
  escalation note), each with a rule fallback.
- Routes escalation with `escalation.yaml`.
Done when: on the recorded fixtures for the master-2 incident, the investigation validates
against `investigation.schema.json`, H1 network congestion ranks first at >= 0.7 with
`WT_LLM=off` and on, the conclusion says no reboot and no eviction, and escalation is
Network team P3, and the reuse rate is at least 0.85 with at most one probe.
