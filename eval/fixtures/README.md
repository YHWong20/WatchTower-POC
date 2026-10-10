# Fixtures

Responses in the shapes the real APIs return, used by `make test` (no lab needed).
Phase 1 fixtures are hand-written to the API formats; phase 7 (`make record`) replaces them with
recordings from the lite lab.

| Dir | What |
|---|---|
| `contracts/` | A sample Signal and Investigation (lite profile: worker-1 flap on cisco-sim-1 Gi1/0/13) |
| `splunk/` | `/services/search/jobs/export` output (JSON lines) and `/services/alerts/fired_alerts` |
| `solarwinds/` | SWIS `Json/Query` responses |
| `cli/` | IOS `show` command output |
| `k8s/` | Kubernetes API objects |
