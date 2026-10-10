"""Health check for the existing-tool stand-ins from the Mac: splunk-sim has fresh data from every
source the forwarder ships, solarwinds-sim is polling, and neither has alerts. Used by lab/health.sh.

    uv run python lab/check_tools.py [node ...]
"""

import calendar
import json
import sys
import time

import httpx

SPLUNK = "http://127.0.0.1:8089"
SWIS = "http://127.0.0.1:17778/SolarWinds/InformationService/v3/Json/Query"
# sourcetype -> (how far back to look, why it should be there)
STREAMS = {
    "kube:container:svc-prober": ("-2m", "svc-prober logs every 5 s"),
    "kube:container:kube-apiserver": ("-30m", "API server logs"),
    "kube:container:etcd": ("-30m", "etcd logs"),
    "kubelet": ("-24h", "kubelet journal"),
    "kube:events": ("-24h", "Kubernetes events (watch)"),
}


def line(ok: bool | None, text: str) -> bool:
    print(("   ok   " if ok else "   FAIL " if ok is False else "   WARN ") + text)
    return ok is not False


def search(http: httpx.Client, spl: str) -> list[dict]:
    resp = http.post(f"{SPLUNK}/services/search/jobs/export", data={"search": f"search {spl}", "output_mode": "json"})
    resp.raise_for_status()
    return [json.loads(x)["result"] for x in resp.text.splitlines() if '"result"' in x]


def main(nodes: list[str]) -> bool:
    good = True
    with httpx.Client(timeout=20, auth=("admin", "demo")) as http:
        try:
            http.get(f"{SPLUNK}/services/server/info").raise_for_status()
        except httpx.HTTPError as exc:
            line(False, f"splunk-sim not answering on :8089 ({type(exc).__name__}); make up?")
            return False
        for st, (window, what) in STREAMS.items():
            rows = search(http, f"index=ocp sourcetype={st} earliest={window} | stats count by host")
            hosts = {r["host"]: r["count"] for r in rows}
            need = nodes if st in ("kubelet", "kube:container:svc-prober") else []
            missing = [n for n in need if n not in hosts]
            ok = bool(hosts) and not missing
            detail = ", ".join(f"{h} {c}" for h, c in sorted(hosts.items())) or "nothing"
            good &= line(ok, f"splunk-sim {st} ({what}, {window}): {detail}"
                             + (f"; missing {', '.join(missing)}" if missing else ""))
        rows = search(http, "index=network sourcetype=cisco:ios earliest=-3m | stats count by host")
        good &= line(bool(rows), "splunk-sim cisco:ios switch syslog (EEM heartbeat every 60 s, -3m): "
                                 + (", ".join(f"{r['host']} {r['count']}" for r in rows) or "nothing"))
        fired = http.get(f"{SPLUNK}/services/alerts/fired_alerts", params={"output_mode": "json"}).json()["entry"]
        recent = [e for e in fired if e["content"].get("trigger_time", 0) > time.time() - 3600]
        line(True if not recent else None, "splunk-sim fired alerts in the last hour: "
             + (", ".join(f"{e['name']} ({e['content'].get('entity')})" for e in recent) or "none"))

    with httpx.Client(timeout=20, auth=("watchtower", "watchtower")) as http:
        try:
            nodes_rows = http.post(SWIS, json={"query": "SELECT Caption, Status, LastSystemUpTimePollUtc FROM Orion.Nodes"}
                                   ).raise_for_status().json()["results"]
        except httpx.HTTPError as exc:
            line(False, f"solarwinds-sim not answering on :17778 ({type(exc).__name__}); make up?")
            return False
        if not nodes_rows:
            good &= line(False, "solarwinds-sim has not polled any switch yet (first poll within 60 s of start)")
        for n in nodes_rows:
            last = n["LastSystemUpTimePollUtc"]
            age = time.time() - calendar.timegm(time.strptime(last[:19], "%Y-%m-%dT%H:%M:%S")) if last else 1e9
            ok = n["Status"] == 1 and age < 150
            good &= line(ok, f"solarwinds-sim polls {n['Caption']}: status {'up' if n['Status'] == 1 else 'DOWN'}, "
                             f"last poll {age:.0f} s ago")
        n_if = http.post(SWIS, json={"query": "SELECT COUNT(InterfaceID) AS n FROM Orion.NPM.Interfaces"}).json()["results"]
        good &= line(n_if and n_if[0]["n"] >= 48, f"solarwinds-sim monitors {n_if[0]['n'] if n_if else 0} interfaces")
        active = http.post(SWIS, json={"query": "SELECT TriggeredMessage FROM Orion.AlertActive"}).json()["results"]
        line(True if not active else None, "solarwinds-sim active alerts: "
             + ("; ".join(a["TriggeredMessage"] for a in active) or "none"))
    return good


if __name__ == "__main__":
    sys.exit(0 if main(sys.argv[1:]) else 1)
