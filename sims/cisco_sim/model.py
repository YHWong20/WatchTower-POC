"""State of one simulated Catalyst 9300: 48 ports whose counters advance with wall-clock time.

Counters are computed from rates, so SNMP, the CLI and the SolarWinds poller all see the
same monotonically rising numbers whenever they look. Faults change the rates:

- congestion(port, level): output drops rise, all in egress queue 1; average output
  utilisation sits near 46 %; CRC and input errors stay at zero. That is microburst
  congestion: the average looks fine, the queue overflows.
- link_flap(port, down_s): the port goes down and up, with %LINK/%LINEPROTO syslog.
- crc(port, rate): input errors and CRC rise at `rate` per second.
"""

from __future__ import annotations

import math
import random
import time
from collections import deque
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Callable

SPEED_BPS = 1_000_000_000
LOAD_INTERVAL_S = 300.0                    # IOS "5 minute" rates

# level -> (output utilisation, output drops per second, input utilisation)
CONGESTION = {
    None:     (0.18, 0.0, 0.12),
    "low":    (0.40, 6.0, 0.14),
    "medium": (0.46, 45.0, 0.15),
    "high":   (0.52, 380.0, 0.15),
}
UPLINK = (0.30, 0.0, 0.26)
AVG_OUT_PKT = 900                          # bytes; drops are counted in packets, queue drops in bytes
AVG_IN_PKT = 600
DROP_PKT = 1300                            # dropped packets are mostly big bursts


def short_name(name: str) -> str:
    """GigabitEthernet1/0/11, gi1/0/11, Gig1/0/11 -> Gi1/0/11. Unknown shapes are returned as is."""
    n = name.strip()
    low = n.lower()
    for prefix in ("gigabitethernet", "gigabit", "gig", "gi"):
        if low.startswith(prefix) and low[len(prefix):][:1].isdigit():
            return "Gi" + n[len(prefix):]
    return n


@dataclass
class Port:
    index: int                              # ifIndex; also the port number Gi1/0/<index>
    description: str = ""
    connected: bool = False
    kind: str = ""                          # ocp_node / uplink / downlink
    # cumulative counters
    in_octets: float = 0.0
    out_octets: float = 0.0
    in_pkts: float = 0.0
    out_pkts: float = 0.0
    in_bcast: float = 0.0
    out_drops: float = 0.0
    in_errors: float = 0.0
    crc: float = 0.0
    resets: int = 0
    queue_enq: list[float] = field(default_factory=lambda: [0.0, 0.0, 0.0])   # bytes per egress queue
    queue_drops: list[float] = field(default_factory=lambda: [0.0, 0.0, 0.0])
    # 5-minute exponentially decayed rates
    in_bps: float = 0.0
    out_bps: float = 0.0
    in_pps: float = 0.0
    out_pps: float = 0.0
    oper_up: bool = False
    last_change: float = 0.0                # monotonic time of the last up/down change

    @property
    def name(self) -> str:
        return f"GigabitEthernet1/0/{self.index}"

    @property
    def short(self) -> str:
        return f"Gi1/0/{self.index}"


@dataclass
class LogLine:
    ts: datetime
    facility: str
    severity: int
    mnemonic: str
    text: str


class Switch:
    def __init__(self, name: str, role: str, ports: dict[str, tuple[str, str]], seed: int = 7,
                 clock: Callable[[], float] = time.monotonic):
        """ports: {"Gi1/0/11": ("master-2", "ocp_node"), ...} for the cabled ports."""
        self.name = name
        self.role = role
        self.clock = clock
        self.booted = clock() - 3 * 86400 - 4 * 3600 - 17 * 60     # "uptime is 3 days, 4 hours"
        self.rng = random.Random(seed)
        self.ports: dict[int, Port] = {}
        for i in range(1, 49):
            desc, kind = ports.get(f"Gi1/0/{i}", ("", ""))
            p = Port(index=i, description=desc, connected=bool(kind), kind=kind, oper_up=bool(kind))
            p.last_change = self.booted + 60
            self.ports[i] = p
        self.congestion: dict[int, str] = {}
        self.crc_rate: dict[int, float] = {}
        self.log: deque[LogLine] = deque(maxlen=500)
        self.log_seq = 0
        self.listeners: list[Callable[[LogLine, int], None]] = []
        self._last = self.booted
        self.advance()                      # three days of history so counters look lived-in
        self.syslog("SYS", 5, "RESTART", "System restarted --")
        for p in self.ports.values():               # cabled ports come up after boot, as on a real switch
            if p.connected:
                self.syslog("LINK", 3, "UPDOWN", f"Interface {p.name}, changed state to up")
                self.syslog("LINEPROTO", 5, "UPDOWN", f"Line protocol on Interface {p.name}, changed state to up")

    # ------------------------------------------------------------ time

    def uptime_s(self) -> float:
        return self.clock() - self.booted

    def advance(self) -> None:
        now = self.clock()
        dt = now - self._last
        if dt <= 0:
            return
        self._last = now
        decay = 1 - math.exp(-dt / LOAD_INTERVAL_S)
        for p in self.ports.values():
            if not (p.connected and p.oper_up):
                p.in_bps -= p.in_bps * decay
                p.out_bps -= p.out_bps * decay
                p.in_pps -= p.in_pps * decay
                p.out_pps -= p.out_pps * decay
                continue
            out_u, drops_ps, in_u = UPLINK if p.kind in ("uplink", "downlink") else CONGESTION[self.congestion.get(p.index)]
            jitter = 1 + self.rng.uniform(-0.06, 0.06) if dt < 3600 else 1
            out_bps = SPEED_BPS * out_u * jitter
            in_bps = SPEED_BPS * in_u * jitter
            out_bytes = out_bps / 8 * dt
            in_bytes = in_bps / 8 * dt
            drops = drops_ps * dt * (self.rng.uniform(0.5, 1.5) if drops_ps and dt < 3600 else 1)
            p.out_octets += out_bytes
            p.in_octets += in_bytes
            p.out_pkts += out_bytes / AVG_OUT_PKT
            p.in_pkts += in_bytes / AVG_IN_PKT
            p.in_bcast += dt * 0.4
            p.out_drops += drops
            p.queue_enq[0] += out_bytes * 0.02
            p.queue_enq[1] += out_bytes * 0.98
            p.queue_drops[1] += drops * DROP_PKT
            crc = self.crc_rate.get(p.index, 0.0) * dt
            p.crc += crc
            p.in_errors += crc
            p.in_bps += (in_bps - p.in_bps) * decay
            p.out_bps += (out_bps - p.out_bps) * decay
            p.in_pps += (in_bps / 8 / AVG_IN_PKT - p.in_pps) * decay
            p.out_pps += (out_bps / 8 / AVG_OUT_PKT - p.out_pps) * decay

    # ------------------------------------------------------------ lookups

    def port(self, name: str) -> Port:
        short = short_name(name)
        if short.startswith("Gi1/0/"):
            try:
                return self.ports[int(short.split("/")[-1])]
            except (ValueError, KeyError):
                pass
        raise KeyError(name)

    # ------------------------------------------------------------ syslog

    def syslog(self, facility: str, severity: int, mnemonic: str, text: str) -> LogLine:
        self.log_seq += 1
        line = LogLine(datetime.now(UTC), facility, severity, mnemonic, text)
        self.log.append(line)
        for listener in self.listeners:
            listener(line, self.log_seq)
        return line

    def set_oper(self, p: Port, up: bool) -> None:
        self.advance()
        if p.oper_up == up:
            return
        p.oper_up = up
        p.last_change = self.clock()
        state = "up" if up else "down"
        if not up:
            p.resets += 1
        self.syslog("LINK", 3, "UPDOWN", f"Interface {p.name}, changed state to {state}")
        self.syslog("LINEPROTO", 5, "UPDOWN", f"Line protocol on Interface {p.name}, changed state to {state}")

    # ------------------------------------------------------------ faults

    def set_congestion(self, name: str, level: str | None) -> None:
        if level not in CONGESTION:
            raise ValueError(f"level must be one of low, medium, high, got {level!r}")
        self.advance()
        p = self.port(name)
        if level is None:
            self.congestion.pop(p.index, None)
        else:
            self.congestion[p.index] = level

    def set_crc(self, name: str, rate: float) -> None:
        self.advance()
        p = self.port(name)
        if rate > 0:
            self.crc_rate[p.index] = rate
        else:
            self.crc_rate.pop(p.index, None)

    def clear(self) -> None:
        """Remove every fault. Counters are not reset, as on a real switch."""
        self.advance()
        self.congestion.clear()
        self.crc_rate.clear()
        for p in self.ports.values():
            if p.connected and not p.oper_up:
                self.set_oper(p, True)

    def faults(self) -> list[dict]:
        out = [{"type": "congestion", "port": f"Gi1/0/{i}", "level": lvl} for i, lvl in sorted(self.congestion.items())]
        out += [{"type": "crc", "port": f"Gi1/0/{i}", "rate": r} for i, r in sorted(self.crc_rate.items())]
        out += [{"type": "link_down", "port": p.short} for p in self.ports.values() if p.connected and not p.oper_up]
        return out

    # ------------------------------------------------------------ derived values

    def util(self, p: Port) -> tuple[float, float]:
        """(input, output) utilisation 0..1 from the 5-minute rates."""
        return p.in_bps / SPEED_BPS, p.out_bps / SPEED_BPS

    def cpu(self) -> tuple[int, int, int]:
        busy = 4 + len(self.congestion) + (2 if any(lvl == "high" for lvl in self.congestion.values()) else 0)
        return busy + self.rng.randint(0, 2), busy + 1, busy
