"""solarwinds-sim poller: SNMP-polls each switch every 60 s like Orion, turns counters into the
per-interval rows Orion keeps (InterfaceTraffic, InterfaceErrors, CPULoad) and evaluates the
alert definitions into Orion.AlertActive.

Alerts (Orion's out-of-the-box style; thresholds are what NOCs typically leave in place):
- High interface utilisation: In or Out utilisation above 90 % for 5 minutes.
- Interface down: a cabled port (it has an alias) is operationally down.
- Node down: the switch stopped answering SNMP (two polls in a row).
Average utilisation hides microbursts, so creeping congestion fires none of these.
"""

from __future__ import annotations

import asyncio
import logging
import socket
import sqlite3
import threading
import time
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

from sims.common import snmp
from sims.solarwinds_sim.schema import ddl
from sims.solarwinds_sim.swql import Compiled, to_db

log = logging.getLogger("solarwinds-sim")

SYS = (1, 3, 6, 1, 2, 1, 1)
IF = (1, 3, 6, 1, 2, 1, 2, 2, 1)
IFX = (1, 3, 6, 1, 2, 1, 31, 1, 1, 1)
CPU_5MIN = (1, 3, 6, 1, 4, 1, 9, 9, 109, 1, 1, 1, 1, 8)     # CISCO-PROCESS-MIB cpmCPUTotal5minRev
CPU_1MIN = (1, 3, 6, 1, 4, 1, 9, 9, 109, 1, 1, 1, 1, 7)
MEM_USED = (1, 3, 6, 1, 4, 1, 9, 9, 48, 1, 1, 1, 5)         # CISCO-MEMORY-POOL-MIB ciscoMemoryPoolUsed
MEM_FREE = (1, 3, 6, 1, 4, 1, 9, 9, 48, 1, 1, 1, 6)

IF_COLS = {"descr": IF + (2,), "oper": IF + (8,), "admin": IF + (7,), "mtu": IF + (4,), "last_change": IF + (9,),
           "in_discards": IF + (13,), "in_errors": IF + (14,), "out_discards": IF + (19,), "out_errors": IF + (20,),
           "name": IFX + (1,), "in_octets": IFX + (6,), "in_pkts": IFX + (7,), "out_octets": IFX + (10,),
           "out_pkts": IFX + (11,), "high_speed": IFX + (15,), "alias": IFX + (18,)}

UTIL_THRESHOLD = 90.0
UTIL_FOR_S = 300


def node_id(name: str) -> int:
    """Stable ids across profiles and restarts: cisco-sim-1 -> 1, cisco-sim-2 -> 2."""
    tail = name.rsplit("-", 1)[-1]
    return int(tail) if tail.isdigit() else sum(map(ord, name)) % 1000 + 100


def interface_id(node: int, if_index: int) -> int:
    """cisco-sim-1 Gi1/0/13 -> 113."""
    return node * 100 + if_index


@dataclass
class Target:
    name: str
    host: str
    port: int = 161
    community: str = "public"


class OrionDb:
    def __init__(self, path: str | Path):
        if str(path) != ":memory:":
            Path(path).parent.mkdir(parents=True, exist_ok=True)
        self.db = sqlite3.connect(str(path), check_same_thread=False)
        self.db.execute("PRAGMA journal_mode=WAL")
        self.db.executescript(ddl())
        self.lock = threading.Lock()

    def query(self, c: Compiled) -> list[tuple]:
        with self.lock:
            return self.db.execute(c.sql, c.params).fetchall()

    def execute(self, sql: str, args: tuple | list = ()) -> list[tuple]:
        with self.lock, self.db:
            return self.db.execute(sql, args).fetchall()

    def executemany(self, sql: str, rows: list) -> None:
        with self.lock, self.db:
            self.db.executemany(sql, rows)

    def upsert(self, table: str, row: dict[str, Any]) -> None:
        cols = ", ".join(f'"{c}"' for c in row)
        marks = ", ".join("?" for _ in row)
        self.execute(f"INSERT OR REPLACE INTO {table} ({cols}) VALUES ({marks})", list(row.values()))


def _delta(cur: int, prev: int, bits: int) -> int:
    return (cur - prev) % (2 ** bits)


class Poller:
    def __init__(self, db: OrionDb, targets: list[Target], interval_s: float = 60.0, retention_h: float = 48,
                 snmp_timeout_s: float = 2.0):
        self.db, self.targets, self.interval_s, self.retention_h = db, targets, interval_s, retention_h
        self.snmp_timeout_s = snmp_timeout_s
        self.prev: dict[str, dict[str, Any]] = {}
        self.failures: dict[str, int] = {}

    # ------------------------------------------------------------ SNMP

    async def fetch(self, t: Target) -> dict[str, Any]:
        async with snmp.SnmpClient(t.host, t.port, t.community, timeout_s=self.snmp_timeout_s, retries=1) as c:
            sysvals = dict(await c.get(*(SYS + (i, 0) for i in (1, 2, 3, 5, 6, 4))))
            cols = {}
            for key, base in IF_COLS.items():
                cols[key] = {o[-1]: v[1] for o, v in await c.walk(base, max_rep=50)}
            extra = {}
            for key, base in (("cpu5", CPU_5MIN), ("cpu1", CPU_1MIN), ("mem_used", MEM_USED), ("mem_free", MEM_FREE)):
                extra[key] = [v[1] for _, v in await c.walk(base, max_rep=10)]

        def text(v: Any) -> str:
            return v.decode(errors="replace") if isinstance(v, (bytes, bytearray)) else str(v)

        return {
            "time": time.time(),
            "descr": text(sysvals.get(SYS + (1, 0), (0, b""))[1]),
            "object_id": ".".join(map(str, sysvals.get(SYS + (2, 0), (0, ()))[1] or ())),
            "uptime_ticks": int(sysvals.get(SYS + (3, 0), (0, 0))[1] or 0),
            "sys_name": text(sysvals.get(SYS + (5, 0), (0, b""))[1]),
            "location": text(sysvals.get(SYS + (6, 0), (0, b""))[1]),
            "contact": text(sysvals.get(SYS + (4, 0), (0, b""))[1]),
            "ifs": {i: {k: (text(cols[k][i]) if k in ("descr", "name", "alias") else int(cols[k][i]))
                        for k in IF_COLS if i in cols[k]} for i in cols["oper"]},
            **extra,
        }

    # ------------------------------------------------------------ one poll

    async def poll_once(self) -> None:
        for t in self.targets:
            try:
                data = await self.fetch(t)
            except (OSError, TimeoutError, RuntimeError) as exc:
                self.failures[t.name] = self.failures.get(t.name, 0) + 1
                log.warning("poll %s failed (%d in a row): %r", t.name, self.failures[t.name], exc)
                await asyncio.to_thread(self.mark_unreachable, t)
                continue
            self.failures[t.name] = 0
            await asyncio.to_thread(self.store, t, data)
        await asyncio.to_thread(self.evaluate_alerts)

    def mark_unreachable(self, t: Target) -> None:
        if self.failures.get(t.name, 0) >= 2:
            self.db.execute("UPDATE Nodes SET Status = 2, StatusDescription = ? WHERE NodeID = ?",
                            ("Node status is Down.", node_id(t.name)))

    def store(self, t: Target, d: dict[str, Any]) -> None:
        nid = node_id(t.name)
        now = datetime.fromtimestamp(d["time"], UTC).replace(microsecond=0)
        stamp = to_db(now)
        uptime_s = d["uptime_ticks"] / 100
        try:
            ip = socket.gethostbyname(t.host)
        except OSError:
            ip = t.host
        cpu = int(d["cpu5"][0]) if d.get("cpu5") else None
        used, free = (d.get("mem_used") or [0])[0], (d.get("mem_free") or [0])[0]
        mem_pct = round(100 * used / (used + free), 1) if used + free else None
        version = d["descr"].split("Version ", 1)[-1].split(",", 1)[0] if "Version " in d["descr"] else ""
        self.db.upsert("Nodes", {
            "NodeID": nid, "Caption": d["sys_name"] or t.name, "DNS": t.host, "SysName": d["sys_name"],
            "IPAddress": ip, "Vendor": "Cisco", "MachineType": "Catalyst 9300-48T", "IOSVersion": version,
            "IOSImage": "CAT9K_IOSXE", "Location": d["location"], "Contact": d["contact"],
            "SysObjectID": d["object_id"], "Status": 1, "StatusDescription": "Node status is Up.",
            "CPULoad": cpu, "PercentMemoryUsed": mem_pct, "LastSystemUpTimePollUtc": stamp,
            "LastBoot": to_db(now - timedelta(seconds=uptime_s)), "SystemUpTime": uptime_s, "Unmanaged": 0,
        })
        if cpu is not None:
            cpu1 = int(d["cpu1"][0]) if d.get("cpu1") else cpu
            self.db.execute(
                'INSERT INTO CPULoad ("NodeID","DateTime","MinLoad","MaxLoad","AvgLoad","TotalMemory",'
                '"MinMemoryUsed","MaxMemoryUsed","AvgMemoryUsed","AvgPercentMemoryUsed") VALUES (?,?,?,?,?,?,?,?,?,?)',
                (nid, stamp, min(cpu, cpu1), max(cpu, cpu1), cpu, used + free, used, used, used, mem_pct))

        prev = self.prev.get(t.name)
        restarted = prev is not None and d["uptime_ticks"] < prev["uptime_ticks"]
        dt = d["time"] - prev["time"] if prev else 0
        traffic, errors = [], []
        day = now.date()
        for idx, f in sorted(d["ifs"].items()):
            iid = interface_id(nid, idx)
            speed = float(f.get("high_speed", 1000)) * 1_000_000
            caption = f["name"] + (f" · {f['alias']}" if f.get("alias") else "")
            row = {
                "InterfaceID": iid, "NodeID": nid, "Index": idx, "Name": f["name"], "Caption": caption,
                "FullName": f"{d['sys_name'] or t.name} - {caption}", "IfName": f["name"],
                "InterfaceName": f["descr"], "Alias": f.get("alias", ""), "TypeName": "ethernetCsmacd",
                "Speed": speed, "MTU": f.get("mtu"), "Status": f["oper"], "OperStatus": f["oper"],
                "AdminStatus": f.get("admin", 1), "StatusDescription": "Up" if f["oper"] == 1 else "Down",
                "LastChange": to_db(now - timedelta(seconds=max(uptime_s - f.get("last_change", 0) / 100, 0))),
                "LastSync": stamp,
            }
            old = self.db.execute('SELECT "InBps","OutBps","InPercentUtil","OutPercentUtil","InDiscardsToday",'
                                  '"OutDiscardsToday","InErrorsToday","OutErrorsToday","LastSync" FROM Interfaces '
                                  'WHERE InterfaceID = ?', (iid,))
            keep = dict(zip(("InBps", "OutBps", "InPercentUtil", "OutPercentUtil", "InDiscardsToday",
                             "OutDiscardsToday", "InErrorsToday", "OutErrorsToday"), old[0][:8])) if old else {}
            same_day = bool(old) and old[0][8] is not None and old[0][8][:10] == stamp[:10]
            if prev and not restarted and dt > 0 and idx in prev["ifs"]:
                p = prev["ifs"][idx]
                in_b = _delta(f["in_octets"], p["in_octets"], 64)
                out_b = _delta(f["out_octets"], p["out_octets"], 64)
                in_bps, out_bps = in_b * 8 / dt, out_b * 8 / dt
                in_pct, out_pct = round(100 * in_bps / speed, 2), round(100 * out_bps / speed, 2)
                d_in = _delta(f["in_discards"], p["in_discards"], 32)
                d_out = _delta(f["out_discards"], p["out_discards"], 32)
                e_in = _delta(f["in_errors"], p["in_errors"], 32)
                e_out = _delta(f["out_errors"], p["out_errors"], 32)
                traffic.append((nid, iid, stamp, in_bps, out_bps, in_bps, out_bps, in_b, out_b,
                                _delta(f["in_pkts"], p["in_pkts"], 64), _delta(f["out_pkts"], p["out_pkts"], 64),
                                in_pct, out_pct))
                errors.append((nid, iid, stamp, e_in, e_out, d_in, d_out))
                base = keep if same_day else {}
                row.update({"InBps": in_bps, "OutBps": out_bps, "InPercentUtil": in_pct, "OutPercentUtil": out_pct,
                            "InDiscardsToday": (base.get("InDiscardsToday") or 0) + d_in,
                            "OutDiscardsToday": (base.get("OutDiscardsToday") or 0) + d_out,
                            "InErrorsToday": (base.get("InErrorsToday") or 0) + e_in,
                            "OutErrorsToday": (base.get("OutErrorsToday") or 0) + e_out})
            else:
                row.update({k: (v if same_day or not k.endswith("Today") else 0) for k, v in keep.items()})
            self.db.upsert("Interfaces", row)
        if traffic:
            self.db.executemany(
                'INSERT INTO InterfaceTraffic ("NodeID","InterfaceID","DateTime","InAveragebps","OutAveragebps",'
                '"InMaxbps","OutMaxbps","InTotalBytes","OutTotalBytes","InTotalPkts","OutTotalPkts",'
                '"InPercentUtil","OutPercentUtil") VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)', traffic)
            self.db.executemany(
                'INSERT INTO InterfaceErrors ("NodeID","InterfaceID","DateTime","InErrors","OutErrors",'
                '"InDiscards","OutDiscards") VALUES (?,?,?,?,?,?,?)', errors)
        if restarted:
            log.info("%s restarted (sysUpTime went back): counters re-baselined", t.name)
        self.prev[t.name] = d

    # ------------------------------------------------------------ alerts

    def evaluate_alerts(self, now: datetime | None = None) -> None:
        now = now or datetime.now(UTC)
        want: dict[int, tuple[str, str, str, str]] = {}      # AlertObjectID -> (alert, entity, node, message)
        samples = max(1, round(UTIL_FOR_S / self.interval_s))
        for iid, name, node, alias in self.db.execute(
                "SELECT i.InterfaceID, i.Name, n.Caption, i.Alias FROM Interfaces i JOIN Nodes n ON n.NodeID = i.NodeID"):
            rows = self.db.execute(
                'SELECT "InPercentUtil","OutPercentUtil" FROM InterfaceTraffic WHERE InterfaceID = ? '
                'AND DateTime > ? ORDER BY DateTime DESC LIMIT ?',
                (iid, to_db(now - timedelta(seconds=UTIL_FOR_S + 2 * self.interval_s)), samples))
            if len(rows) >= samples and all(max(a or 0, b or 0) > UTIL_THRESHOLD for a, b in rows):
                peak = max(max(a or 0, b or 0) for a, b in rows)
                want[1_000_000 + iid] = ("High Interface Utilization", name, node,
                                         f"Interface {name} on {node} utilization is {peak:.0f} % "
                                         f"(above {UTIL_THRESHOLD:.0f} % for {UTIL_FOR_S // 60} minutes)")
        for iid, name, node, alias in self.db.execute(
                "SELECT i.InterfaceID, i.Name, n.Caption, i.Alias FROM Interfaces i JOIN Nodes n ON n.NodeID = i.NodeID "
                "WHERE i.OperStatus = 2 AND i.Alias != '' AND n.Status = 1"):
            want[2_000_000 + iid] = ("Interface Down", name, node, f"Interface {name} ({alias}) on {node} is Down")
        for nid, node in self.db.execute("SELECT NodeID, Caption FROM Nodes WHERE Status = 2"):
            want[3_000_000 + nid] = ("Node Down", node, node, f"Node {node} is Down (no SNMP response)")

        active = {r[0] for r in self.db.execute("SELECT AlertObjectID FROM AlertActive")}
        for obj in active - set(want):
            aid, msg = self.db.execute("SELECT AlertActiveID, TriggeredMessage FROM AlertActive WHERE AlertObjectID = ?",
                                       (obj,))[0]
            self.db.execute("DELETE FROM AlertActive WHERE AlertObjectID = ?", (obj,))
            self.history(aid, obj, 1, f"Reset: {msg}", now)
            log.info("alert reset: %s", msg)
        for obj in set(want) - active:
            alert, entity, node, message = want[obj]
            nxt = self.db.execute("SELECT COALESCE(MAX(AlertActiveID), 0) + 1 FROM AlertActive")[0][0]
            self.db.upsert("AlertActive", {
                "AlertActiveID": nxt, "AlertObjectID": obj, "Acknowledged": 0, "TriggeredDateTime": to_db(now),
                "TriggeredMessage": message, "NumberOfNotes": 0, "LastExecutedEscalationLevel": 0})
            self.db.upsert("sim_alert_state", {"AlertObjectID": obj, "AlertName": alert, "EntityCaption": entity,
                                               "RelatedNodeCaption": node})
            self.history(nxt, obj, 0, message, now)
            log.info("alert triggered: %s", message)

    def history(self, active_id: int, obj: int, event_type: int, message: str, at: datetime) -> None:
        nxt = self.db.execute("SELECT COALESCE(MAX(AlertHistoryID), 0) + 1 FROM AlertHistory")[0][0]
        self.db.upsert("AlertHistory", {"AlertHistoryID": nxt, "AlertActiveID": active_id, "AlertObjectID": obj,
                                        "EventType": event_type, "Message": message, "TimeStamp": to_db(at)})

    # ------------------------------------------------------------ loop

    def trim(self) -> None:
        cutoff = to_db(datetime.now(UTC) - timedelta(hours=self.retention_h))
        for table in ("InterfaceTraffic", "InterfaceErrors", "CPULoad"):
            self.db.execute(f"DELETE FROM {table} WHERE DateTime < ?", (cutoff,))
        self.db.execute("DELETE FROM AlertHistory WHERE TimeStamp < ?", (cutoff,))

    async def loop(self) -> None:
        polls = 0
        while True:
            started = time.monotonic()
            try:
                await self.poll_once()
            except Exception:                            # never stop polling
                log.exception("poll failed")
            polls += 1
            if polls % 60 == 0:
                await asyncio.to_thread(self.trim)
            await asyncio.sleep(max(self.interval_s - (time.monotonic() - started), 1))
