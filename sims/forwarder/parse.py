"""Turn raw lines and objects into HEC events: the forwarder's index-time configuration.

    Kubernetes events      index=ocp      sourcetype=kube:events            fields namespace, source_host
    container logs         index=ocp      sourcetype=kube:container:<name>  host=<node>, fields namespace, pod, container_name
    kubelet journal        index=ocp      sourcetype=kubelet                host=<node>
    switch syslog          index=network  sourcetype=cisco:ios              host=<switch>
These match what Splunk Connect for Kubernetes / the Splunk OTel Collector and a syslog input
with the Cisco add-on produce, so the SPL in contracts/sources.yaml carries over to production.
"""

from __future__ import annotations

import re
from datetime import UTC, datetime
from typing import Any

CLUSTER = "ocp-sim"

# ---------------------------------------------------------------- container logs (CRI format)

_LOG_FILE = re.compile(r"^(?P<pod>[^_]+)_(?P<namespace>[^_]+)_(?P<container>.+)-(?P<id>[0-9a-f]{64})\.log$")


def container_log_meta(filename: str) -> dict[str, str] | None:
    """/var/log/containers/<pod>_<namespace>_<container>-<id>.log -> its parts."""
    m = _LOG_FILE.match(filename.rsplit("/", 1)[-1])
    return m.groupdict() if m else None


def cri_ns(stamp: str) -> int:
    """RFC 3339 with up to 9 fractional digits (containerd trims trailing zeros) -> epoch ns."""
    main, _, frac = stamp.rstrip("Z").partition(".")
    tz = ""
    for sign in ("+", "-"):
        if sign in frac:
            frac, _, rest = frac.partition(sign)
            tz = sign + rest
    secs = int(datetime.fromisoformat(main + (tz or "+00:00")).timestamp())
    return secs * 1_000_000_000 + int((frac or "0").ljust(9, "0")[:9])


def parse_cri(line: str) -> tuple[int, str, bool, str] | None:
    """'2026-10-10T13:01:47.2698Z stdout F {...}' -> (ns, stream, full_line, message)."""
    parts = line.split(" ", 3)
    if len(parts) < 3 or parts[1] not in ("stdout", "stderr"):
        return None
    try:
        ns = cri_ns(parts[0])
    except ValueError:
        return None
    return ns, parts[1], parts[2] != "P", parts[3] if len(parts) > 3 else ""


def container_event(node: str, filename: str, meta: dict[str, str], ns: int, message: str) -> dict[str, Any]:
    return {
        "time": round(ns / 1e9, 6), "host": node, "index": "ocp",
        "source": f"/var/log/containers/{filename}", "sourcetype": f"kube:container:{meta['container']}",
        "event": message,
        "fields": {"namespace": meta["namespace"], "pod": meta["pod"], "container_name": meta["container"],
                   "cluster": CLUSTER},
    }


# ---------------------------------------------------------------- kubelet journal


def journal_event(entry: dict[str, Any], fallback_host: str) -> dict[str, Any] | None:
    """One `journalctl -o json` entry -> HEC event."""
    msg = entry.get("MESSAGE")
    if isinstance(msg, list):                           # journald sends non-UTF-8 messages as bytes
        msg = bytes(msg).decode("utf-8", "replace")
    if not msg:
        return None
    try:
        t = int(entry["__REALTIME_TIMESTAMP"]) / 1e6
    except (KeyError, ValueError):
        return None
    return {
        "time": round(t, 6), "host": entry.get("_HOSTNAME") or fallback_host, "index": "ocp",
        "source": "journald:kubelet.service", "sourcetype": "kubelet", "event": msg,
        "fields": {"unit": entry.get("_SYSTEMD_UNIT") or entry.get("UNIT") or "kubelet.service",
                   "priority": entry.get("PRIORITY"), "cluster": CLUSTER},
    }


# ---------------------------------------------------------------- Kubernetes events


def _ts(value: Any) -> float | None:
    if not value:
        return None
    try:
        return datetime.fromisoformat(str(value).replace("Z", "+00:00")).timestamp()
    except ValueError:
        return None


def event_time(e: dict[str, Any]) -> float:
    """When the event last happened: lastTimestamp, eventTime or series.lastObservedTime."""
    times = [_ts(e.get(k)) for k in ("lastTimestamp", "eventTime", "firstTimestamp")]
    times.append(_ts((e.get("series") or {}).get("lastObservedTime")))
    times.append(_ts((e.get("metadata") or {}).get("creationTimestamp")))
    return max((t for t in times if t), default=datetime.now(UTC).timestamp())


def kube_event(e: dict[str, Any]) -> dict[str, Any]:
    """A core/v1 Event (from list or watch) -> HEC event; the object is the event body, as the
    Splunk OTel k8sobjects receiver ships it, without managedFields."""
    obj = dict(e)
    meta = dict(obj.get("metadata") or {})
    meta.pop("managedFields", None)
    obj["metadata"] = meta
    involved = obj.get("involvedObject") or {}
    source_host = (obj.get("source") or {}).get("host") or obj.get("reportingInstance") or ""
    return {
        "time": round(event_time(obj), 3), "host": source_host or CLUSTER, "index": "ocp",
        "source": "kube:events", "sourcetype": "kube:events", "event": obj,
        "fields": {"namespace": involved.get("namespace") or meta.get("namespace") or "",
                   "source_host": source_host, "cluster": CLUSTER},
    }


# ---------------------------------------------------------------- switch syslog

_SYSLOG = re.compile(r"^<(?P<pri>\d{1,3})>(?P<body>.*)$", re.S)
_HOST = re.compile(r"^\d+: (?P<host>[A-Za-z0-9_.\-]+): ")
_STAMP = re.compile(r"\*(?P<mon>[A-Z][a-z]{2}) +(?P<day>\d{1,2}) (?P<hms>\d\d:\d\d:\d\d)(?:\.(?P<ms>\d{3}))?")


def syslog_event(packet: bytes, sender: str, received: float) -> dict[str, Any] | None:
    """'<190>000123: cisco-sim-1: *Oct 10 09:14:00.123 UTC: %LINK-3-UPDOWN: ...' -> HEC event.
    The timestamp comes from the message (the Cisco add-on's TIME_FORMAT), else receive time."""
    text = packet.decode("utf-8", "replace").strip()
    m = _SYSLOG.match(text)
    if not m:
        return None
    body = m.group("body").strip()
    hm = _HOST.match(body)
    host = hm.group("host") if hm else sender
    t = received
    sm = _STAMP.search(body)
    if sm:
        year = datetime.fromtimestamp(received, UTC).year
        try:
            dt = datetime.strptime(f"{year} {sm['mon']} {sm['day']} {sm['hms']}", "%Y %b %d %H:%M:%S").replace(tzinfo=UTC)
            parsed = dt.timestamp() + int(sm["ms"] or 0) / 1000
            if abs(parsed - received) < 86400:
                t = parsed
        except ValueError:
            pass
    return {"time": round(t, 3), "host": host, "index": "network", "source": "udp:5514",
            "sourcetype": "cisco:ios", "event": body}
