# Phase 6: Copilot UI

Build `wt/copilot/` (FastAPI + Jinja2 + HTMX + Chart.js served locally) to match
`wireframes/proactive-watch.html` and `wireframes/incident-investigation.html`:
- Proactive watch: alarms from SolarWinds/Splunk stand-ins, domain agent status, watch items
  with evidence cards and sparklines, "Explain in plain words", "Open investigation".
- Investigate: an Investigate button on every alarm, live progress while evidence arrives,
  then the report: likely cause, what happened, evidence chain with source tags and expandable
  raw query/output, hypotheses with bars and for/against, timeline, next steps, escalation with
  "Copy escalation note".
- Ask box: answers only from stored investigations and watch items, citing evidence ids; says
  "I don't have evidence for that" otherwise.
- Read-only badge; no buttons that change anything.
Done when: both screens work against the live lab and look like the wireframes.
