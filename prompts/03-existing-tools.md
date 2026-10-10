# Phase 3: existing-tool stand-ins

WatchTower sits above SolarWinds and Splunk, so the lab needs believable stand-ins.
- `sims/splunk_sim/`: FastAPI + SQLite. HEC `/services/collector/event` (token auth), export
  search `/services/search/jobs/export` with the SPL subset in MASTER_PROMPT.md, saved searches
  in YAML with a 60 s scheduler, `/services/alerts/fired_alerts`. Saved search `NodeNotReady`
  fires on a NodeNotReady event.
- `sims/forwarder/`: ships Kubernetes events (watch), pod logs from kube-system and wt-lab,
  kubelet journal from each kind node (`docker exec <node> journalctl -u kubelet -f -o json`),
  and switch syslog, to HEC with sensible `index`, `sourcetype`, `host`.
- `sims/solarwinds_sim/`: polls both Cisco sims over SNMP every 60 s, stores history in SQLite,
  serves SWIS `/SolarWinds/InformationService/v3/Json/Query` with the SWQL subset and tables in
  MASTER_PROMPT.md, and one alert (utilisation > 90% for 5 min) in Orion.AlertActive.
Done when: every SPL and SWQL template in `contracts/sources.yaml` returns data against the
stand-ins, and during phase A of the fault neither stand-in has fired an alert.
