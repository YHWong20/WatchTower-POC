"""cisco-sim: one simulated switch with SSH CLI, SNMP agent, syslog out and a fault API.

Everything runs in one asyncio loop, so the lite profile can host it in the shared wt-sims
container next to the other stand-ins.

Environment:
  SIM_NAME        cisco-sim-1                      switch name; ports come from contracts/topology.yaml
  SIM_SSH_PORT    22         SIM_SNMP_PORT 161     SIM_API_PORT 9101
  SIM_USERS       admin:demo,watchtower:watchtower
  SIM_COMMUNITY   public
  SIM_SYSLOG      forwarder:5514                   comma-separated host:port (UDP); empty = off
  SIM_HOST_KEY    /etc/wt-sim/ssh_host_ed25519_key generated if missing
"""

from __future__ import annotations

import asyncio
import logging
import os
import socket
from datetime import UTC, datetime
from pathlib import Path

import asyncssh
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

from sims.cisco_sim.cli import format_log_line, run_command
from sims.cisco_sim.model import Switch
from sims.common import snmp
from sims.common.state import http_server
from wt.common.catalog import load_topology

log = logging.getLogger("cisco-sim")

IF_TABLE = (1, 3, 6, 1, 2, 1, 2, 2, 1)
IF_X_TABLE = (1, 3, 6, 1, 2, 1, 31, 1, 1, 1)
SYSTEM = (1, 3, 6, 1, 2, 1, 1)
CAT9300_48T = (1, 3, 6, 1, 4, 1, 9, 1, 2494)
CPM_CPU = (1, 3, 6, 1, 4, 1, 9, 9, 109, 1, 1, 1, 1)
MEM_POOL = (1, 3, 6, 1, 4, 1, 9, 9, 48, 1, 1, 1)


def build_switch(name: str) -> Switch:
    topo = load_topology()
    spec = topo.switches.get(name)
    if spec is None:
        raise SystemExit(f"{name} is not in topology.yaml for this profile")
    ports = {}
    for port, p in spec.ports.items():
        desc = p.connects_to if p.kind == "ocp_node" else f"to {p.connects_to.replace(':', ' ')}"
        ports[port] = (desc, p.kind)
    return Switch(name, spec.role, ports, seed=sum(map(ord, name)))


# ---------------------------------------------------------------- SNMP


def mib(sw: Switch) -> dict[snmp.OID, snmp.Value]:
    sw.advance()
    ticks = int(sw.uptime_s() * 100)
    v: dict[snmp.OID, snmp.Value] = {
        SYSTEM + (1, 0): (snmp.OCTET_STRING, "Cisco IOS Software [Cupertino], Catalyst L3 Switch Software "
                                             "(CAT9K_IOSXE), Version 17.9.4a, RELEASE SOFTWARE (fc3)"),
        SYSTEM + (2, 0): (snmp.OBJECT_ID, CAT9300_48T),
        SYSTEM + (3, 0): (snmp.TIMETICKS, ticks),
        SYSTEM + (4, 0): (snmp.OCTET_STRING, "noc@example.net"),
        SYSTEM + (5, 0): (snmp.OCTET_STRING, sw.name),
        SYSTEM + (6, 0): (snmp.OCTET_STRING, "Lab rack 1"),
        (1, 3, 6, 1, 2, 1, 2, 1, 0): (snmp.INTEGER, len(sw.ports)),
    }
    # CISCO-PROCESS-MIB (CPU, one entry) and CISCO-MEMORY-POOL-MIB (Processor pool): what Orion polls
    # for a Cisco node's CPU load and memory.
    five_s, one_m, five_m = sw.cpu()
    v[CPM_CPU + (6, 1)] = (snmp.GAUGE32, five_s)
    v[CPM_CPU + (7, 1)] = (snmp.GAUGE32, one_m)
    v[CPM_CPU + (8, 1)] = (snmp.GAUGE32, five_m)
    used = 412_000_000 + 2_000_000 * len(sw.congestion)
    v[MEM_POOL + (2, 1)] = (snmp.OCTET_STRING, "Processor")
    v[MEM_POOL + (5, 1)] = (snmp.GAUGE32, used)
    v[MEM_POOL + (6, 1)] = (snmp.GAUGE32, 1_598_000_000 - used)
    for i, p in sw.ports.items():
        up = 1 if p.oper_up else 2
        last_change = int((p.last_change - sw.booted) * 100)
        mac = bytes.fromhex(f"00a3d1f4b{2 + sw.name.endswith('2'):x}{p.index:02x}")
        cols = {
            1: (snmp.INTEGER, i), 2: (snmp.OCTET_STRING, p.name), 3: (snmp.INTEGER, 6),
            4: (snmp.INTEGER, 1500), 5: (snmp.GAUGE32, 1_000_000_000), 6: (snmp.OCTET_STRING, mac),
            7: (snmp.INTEGER, 1), 8: (snmp.INTEGER, up), 9: (snmp.TIMETICKS, max(last_change, 0)),
            10: (snmp.COUNTER32, int(p.in_octets)), 11: (snmp.COUNTER32, int(p.in_pkts)),
            13: (snmp.COUNTER32, 0), 14: (snmp.COUNTER32, int(p.in_errors)),
            16: (snmp.COUNTER32, int(p.out_octets)), 17: (snmp.COUNTER32, int(p.out_pkts)),
            19: (snmp.COUNTER32, int(p.out_drops)), 20: (snmp.COUNTER32, 0),
        }
        for col, val in cols.items():
            v[IF_TABLE + (col, i)] = val
        xcols = {
            1: (snmp.OCTET_STRING, p.short), 6: (snmp.COUNTER64, int(p.in_octets)),
            7: (snmp.COUNTER64, int(p.in_pkts)), 10: (snmp.COUNTER64, int(p.out_octets)),
            11: (snmp.COUNTER64, int(p.out_pkts)), 15: (snmp.GAUGE32, 1000),
            18: (snmp.OCTET_STRING, p.description),
        }
        for col, val in xcols.items():
            v[IF_X_TABLE + (col, i)] = val
    return v


# ---------------------------------------------------------------- syslog out


class SyslogOut:
    """Sends each log line over UDP in IOS wire format: <PRI>seq: host: *timestamp: %MSG.
    Lines logged before the collector's name resolves (the boot messages) are replayed once it
    does, like a switch sending its logging buffer when the syslog host becomes reachable."""

    def __init__(self, sw: Switch, targets: list[tuple[str, int]]):
        self.sw = sw
        self.targets = targets
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.sock.setblocking(False)
        self.resolved: dict[tuple[str, int], tuple[str, int]] = {}
        first = sw.log_seq - len(sw.log) + 1
        self.pending = [(line, first + i) for i, line in enumerate(sw.log)] if targets else []
        sw.listeners.append(self.send)

    async def resolve_forever(self) -> None:
        loop = asyncio.get_running_loop()
        await asyncio.sleep(5)          # UDP has no retry: give a collector starting alongside time to listen
        while True:
            for host, port in self.targets:
                try:
                    info = await loop.getaddrinfo(host, port, family=socket.AF_INET, type=socket.SOCK_DGRAM)
                    self.resolved[(host, port)] = info[0][4]
                except OSError:
                    self.resolved.pop((host, port), None)   # forwarder not up yet
            if self.resolved and self.pending:
                backlog, self.pending = self.pending, []
                for line, seq in backlog:
                    self.send(line, seq)
            await asyncio.sleep(30 if self.resolved else 2)

    def send(self, line, seq: int) -> None:
        if not self.resolved:
            if self.targets and len(self.pending) < 500:
                self.pending.append((line, seq))
            return
        pri = 23 * 8 + line.severity                            # facility local7
        msg = f"<{pri}>{format_log_line(line, seq).replace(': *', f': {self.sw.name}: *', 1)}"
        for addr in self.resolved.values():
            try:
                self.sock.sendto(msg.encode(), addr)
            except OSError:
                pass


async def heartbeat(sw: Switch, every_s: float = 60) -> None:
    """An EEM applet that logs every minute, as many NOCs configure to prove the syslog path."""
    while True:
        await asyncio.sleep(every_s)
        sw.syslog("HA_EM", 6, "LOG", "WT_HEARTBEAT: syslog path check")


# ---------------------------------------------------------------- SSH


class _SshServer(asyncssh.SSHServer):
    def __init__(self, sw: Switch, users: dict[str, str]):
        self.sw, self.users, self.peer = sw, users, "unknown"

    def connection_made(self, conn: asyncssh.SSHServerConnection) -> None:
        self.peer = (conn.get_extra_info("peername") or ("unknown",))[0]

    def begin_auth(self, username: str) -> bool:
        return True

    def password_auth_supported(self) -> bool:
        return True

    def validate_password(self, username: str, password: str) -> bool:
        ok = self.users.get(username) == password
        now = datetime.now(UTC).strftime("%H:%M:%S UTC %a %b %d %Y")
        if ok:
            self.sw.syslog("SEC_LOGIN", 5, "LOGIN_SUCCESS",
                           f"Login Success [user: {username}] [Source: {self.peer}] [localport: 22] at {now}")
        else:
            self.sw.syslog("SEC_LOGIN", 4, "LOGIN_FAILED",
                           f"Login failed [user: {username}] [Source: {self.peer}] [localport: 22] "
                           f"[Reason: Login Authentication Failed] at {now}")
        return ok


def _session(sw: Switch):
    async def handle(process: asyncssh.SSHServerProcess) -> None:
        if process.command is not None:                     # ssh host "show ..."
            process.stdout.write(run_command(sw, process.command))
            process.exit(0)
            return
        prompt = f"{sw.name}#"
        process.stdout.write(f"\n{prompt}")
        while True:
            try:
                line = await process.stdin.readline()
            except (asyncssh.BreakReceived, asyncssh.TerminalSizeChanged, asyncssh.SignalReceived):
                continue
            except asyncssh.DisconnectError:
                break
            if not line:
                break
            cmd = line.strip()
            if cmd.lower() in ("exit", "quit", "logout"):
                break
            if not cmd.lower().startswith("ter"):           # "terminal length 0" is a no-op here
                process.stdout.write(run_command(sw, cmd))
            process.stdout.write(prompt)
        process.exit(0)

    return handle


def _host_key(path: str) -> asyncssh.SSHKey:
    p = Path(path)
    if p.exists():
        return asyncssh.read_private_key(p)
    key = asyncssh.generate_private_key("ssh-ed25519")
    try:
        p.parent.mkdir(parents=True, exist_ok=True)
        key.write_private_key(p)
    except OSError:
        pass
    return key


# ---------------------------------------------------------------- fault API


class Fault(BaseModel):
    type: str                       # congestion | link_flap | crc
    port: str
    level: str | None = None        # congestion: low | medium | high
    rate: float | None = None       # crc: errors per second
    down_s: float = 5.0             # link_flap
    count: int = 1                  # link_flap repetitions


def fault_api(sw: Switch) -> FastAPI:
    app = FastAPI(title=f"{sw.name} fault API", docs_url=None, redoc_url=None)
    tasks: set[asyncio.Task] = set()

    async def flap(port: str, down_s: float, count: int) -> None:
        p = sw.port(port)
        for _ in range(count):
            sw.set_oper(p, False)
            await asyncio.sleep(down_s)
            sw.set_oper(p, True)
            await asyncio.sleep(max(down_s, 5))

    @app.get("/health")
    def health():
        return {"switch": sw.name, "uptime_s": int(sw.uptime_s()), "faults": sw.faults()}

    @app.get("/faults")
    def list_faults():
        return sw.faults()

    @app.post("/faults")
    async def add_fault(f: Fault):
        try:
            if f.type == "congestion":
                sw.set_congestion(f.port, f.level or "medium")
            elif f.type == "crc":
                sw.set_crc(f.port, f.rate if f.rate is not None else 5.0)
            elif f.type == "link_flap":
                sw.port(f.port)
                t = asyncio.create_task(flap(f.port, f.down_s, f.count))
                tasks.add(t)
                t.add_done_callback(tasks.discard)
            else:
                raise HTTPException(422, f"unknown fault type {f.type!r}")
        except KeyError:
            raise HTTPException(404, f"no such port {f.port!r}")
        except ValueError as exc:
            raise HTTPException(422, str(exc))
        log.info("fault %s", f.model_dump(exclude_none=True))
        return sw.faults()

    @app.delete("/faults")
    def clear_faults():
        for t in list(tasks):
            t.cancel()
        sw.clear()
        log.info("faults cleared")
        return sw.faults()

    @app.get("/ports/{port:path}")
    def port_state(port: str):
        sw.advance()
        try:
            p = sw.port(port)
        except KeyError:
            raise HTTPException(404, f"no such port {port!r}")
        in_u, out_u = sw.util(p)
        return {"port": p.short, "description": p.description, "up": p.oper_up,
                "out_discards": int(p.out_drops), "in_errors": int(p.in_errors), "crc": int(p.crc),
                "in_util": round(in_u, 3), "out_util": round(out_u, 3),
                "queue_drops_bytes": [int(x) for x in p.queue_drops]}

    return app


# ---------------------------------------------------------------- main


def _targets(spec: str) -> list[tuple[str, int]]:
    out = []
    for item in filter(None, (s.strip() for s in spec.split(","))):
        host, _, port = item.rpartition(":")
        out.append((host, int(port)))
    return out


async def serve(name: str | None = None) -> None:
    name = name or os.environ.get("SIM_NAME", "cisco-sim-1")
    sw = build_switch(name)
    users = dict(u.split(":", 1) for u in os.environ.get("SIM_USERS", "admin:demo,watchtower:watchtower").split(","))
    ssh_port = int(os.environ.get("SIM_SSH_PORT", "22"))
    snmp_port = int(os.environ.get("SIM_SNMP_PORT", "161"))
    api_port = int(os.environ.get("SIM_API_PORT", "9101"))
    out = SyslogOut(sw, _targets(os.environ.get("SIM_SYSLOG", "forwarder:5514")))

    await asyncssh.create_server(
        lambda: _SshServer(sw, users), "", ssh_port,
        server_host_keys=[_host_key(os.environ.get("SIM_HOST_KEY", "/etc/wt-sim/ssh_host_ed25519_key"))],
        process_factory=_session(sw),
    )
    await snmp.serve_agent(snmp.MibView(lambda: mib(sw)), port=snmp_port,
                           community=os.environ.get("SIM_COMMUNITY", "public"))
    api = http_server(fault_api(sw), api_port)
    log.info("%s up: ssh :%d, snmp :%d/udp, fault api :%d, syslog -> %s, connected ports %s",
             name, ssh_port, snmp_port, api_port, out.targets,
             ", ".join(p.short for p in sw.ports.values() if p.connected))
    await asyncio.gather(api.serve(), out.resolve_forever(), heartbeat(sw))
