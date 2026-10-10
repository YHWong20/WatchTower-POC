"""The SWIS entities solarwinds-sim serves, with Orion's names, stored in SQLite.

DateTime columns are stored as 'YYYY-MM-DD HH:MM:SS' (UTC) so SQLite compares and shifts them,
and are returned as SWIS does: 2026-10-10T09:15:00.0000000Z.
"""

from __future__ import annotations

from dataclasses import dataclass, field

INT, REAL, TEXT, BOOL, DATETIME = "INT", "REAL", "TEXT", "BOOL", "DATETIME"


@dataclass
class Entity:
    swis: str                                   # Orion.NPM.Interfaces
    table: str                                  # SQLite table
    columns: dict[str, str]                     # Orion column name -> type
    nav: dict[str, tuple[str, str]] = field(default_factory=dict)   # Node -> (Orion.Nodes, NodeID)
    key: tuple[str, ...] = ()

    def column(self, name: str) -> str | None:
        """Case-insensitive lookup (SWQL identifiers are case-insensitive); returns the real name."""
        return next((c for c in self.columns if c.lower() == name.lower()), None)

    def navigation(self, name: str) -> tuple[str, str] | None:
        return next((v for k, v in self.nav.items() if k.lower() == name.lower()), None)


ENTITIES = {e.swis.lower(): e for e in [
    Entity("Orion.Nodes", "Nodes", {
        "NodeID": INT, "Caption": TEXT, "DNS": TEXT, "SysName": TEXT, "IPAddress": TEXT, "Vendor": TEXT,
        "MachineType": TEXT, "IOSVersion": TEXT, "IOSImage": TEXT, "Location": TEXT, "Contact": TEXT,
        "SysObjectID": TEXT, "Status": INT, "StatusDescription": TEXT, "CPULoad": INT, "PercentMemoryUsed": INT,
        "LastSystemUpTimePollUtc": DATETIME, "LastBoot": DATETIME, "SystemUpTime": REAL, "Unmanaged": BOOL,
    }, key=("NodeID",)),
    Entity("Orion.NPM.Interfaces", "Interfaces", {
        "InterfaceID": INT, "NodeID": INT, "Index": INT, "Name": TEXT, "Caption": TEXT, "FullName": TEXT,
        "IfName": TEXT, "InterfaceName": TEXT, "Alias": TEXT, "TypeName": TEXT, "Speed": REAL, "MTU": INT,
        "Status": INT, "StatusDescription": TEXT, "OperStatus": INT, "AdminStatus": INT,
        "InBps": REAL, "OutBps": REAL, "InPercentUtil": REAL, "OutPercentUtil": REAL,
        "InDiscardsToday": INT, "OutDiscardsToday": INT, "InErrorsToday": INT, "OutErrorsToday": INT,
        "LastChange": DATETIME, "LastSync": DATETIME,
    }, nav={"Node": ("orion.nodes", "NodeID")}, key=("InterfaceID",)),
    Entity("Orion.NPM.InterfaceTraffic", "InterfaceTraffic", {
        "NodeID": INT, "InterfaceID": INT, "DateTime": DATETIME, "InAveragebps": REAL, "OutAveragebps": REAL,
        "InMaxbps": REAL, "OutMaxbps": REAL, "InTotalBytes": REAL, "OutTotalBytes": REAL,
        "InTotalPkts": REAL, "OutTotalPkts": REAL, "InPercentUtil": REAL, "OutPercentUtil": REAL,
    }, nav={"Node": ("orion.nodes", "NodeID"), "Interface": ("orion.npm.interfaces", "InterfaceID")}),
    Entity("Orion.NPM.InterfaceErrors", "InterfaceErrors", {
        "NodeID": INT, "InterfaceID": INT, "DateTime": DATETIME, "InErrors": INT, "OutErrors": INT,
        "InDiscards": INT, "OutDiscards": INT,
    }, nav={"Node": ("orion.nodes", "NodeID"), "Interface": ("orion.npm.interfaces", "InterfaceID")}),
    Entity("Orion.CPULoad", "CPULoad", {
        "NodeID": INT, "DateTime": DATETIME, "MinLoad": INT, "MaxLoad": INT, "AvgLoad": INT,
        "TotalMemory": REAL, "MinMemoryUsed": REAL, "MaxMemoryUsed": REAL, "AvgMemoryUsed": REAL,
        "AvgPercentMemoryUsed": REAL,
    }, nav={"Node": ("orion.nodes", "NodeID")}),
    Entity("Orion.AlertActive", "AlertActive", {
        "AlertActiveID": INT, "AlertObjectID": INT, "Acknowledged": BOOL, "AcknowledgedBy": TEXT,
        "AcknowledgedDateTime": DATETIME, "AcknowledgedNote": TEXT, "TriggeredDateTime": DATETIME,
        "TriggeredMessage": TEXT, "NumberOfNotes": INT, "LastExecutedEscalationLevel": INT,
    }, key=("AlertActiveID",)),
    # EventType 0 = triggered, 1 = reset (Orion uses more types; the sim records these two).
    Entity("Orion.AlertHistory", "AlertHistory", {
        "AlertHistoryID": INT, "AlertActiveID": INT, "AlertObjectID": INT, "EventType": INT, "Message": TEXT,
        "TimeStamp": DATETIME,
    }, key=("AlertHistoryID",)),
]}

_SQL_TYPE = {INT: "INTEGER", REAL: "REAL", TEXT: "TEXT", BOOL: "INTEGER", DATETIME: "TEXT"}


def ddl() -> str:
    stmts = []
    for e in ENTITIES.values():
        cols = ", ".join(f'"{c}" {_SQL_TYPE[t]}' for c, t in e.columns.items())
        if e.key:
            cols += f", PRIMARY KEY ({', '.join(e.key)})"
        stmts.append(f"CREATE TABLE IF NOT EXISTS {e.table} ({cols});")
        if "DateTime" in e.columns:
            ref = "InterfaceID" if "InterfaceID" in e.columns else "NodeID"
            stmts.append(f"CREATE INDEX IF NOT EXISTS {e.table}_dt ON {e.table}({ref}, DateTime);")
    # Internal state that is not part of SWIS: the alert definitions' bookkeeping.
    stmts.append("CREATE TABLE IF NOT EXISTS sim_alert_state (AlertObjectID INTEGER PRIMARY KEY, "
                 "AlertName TEXT, EntityCaption TEXT, RelatedNodeCaption TEXT)")
    return "\n".join(stmts)
