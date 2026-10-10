"""IOS-XE style CLI: parses a command line and renders output in Catalyst 9300 formatting.

Only `show` commands exist. Anything else gets the IOS error a read-only user would see.
Output filters `| include`, `| exclude`, `| begin`, `| section` and `| count` work as on IOS.
"""

from __future__ import annotations

import re
from datetime import UTC, datetime

from sims.cisco_sim.model import SPEED_BPS, Port, Switch, short_name  # noqa: F401 (re-exported)

INVALID = "% Invalid input detected at '^' marker.\n"
INCOMPLETE = "% Incomplete command.\n"
NOT_AUTHORIZED = "% Authorization failed.\n"


def run_command(sw: Switch, line: str) -> str:
    line = line.strip()
    if not line:
        return ""
    cmd, *pipes = line.split("|")
    try:
        out = _dispatch(sw, cmd.split())
    except KeyError:
        out = INVALID
    for pipe in pipes:
        out = _filter(out, pipe.strip())
    return out


def _dispatch(sw: Switch, words: list[str]) -> str:
    w = [x.lower() for x in words]
    if not w:
        return ""
    if not "show".startswith(w[0]) or len(w[0]) < 2:
        if w[0] in ("conf", "config", "configure", "reload", "write", "copy", "clear", "debug", "delete", "erase"):
            return NOT_AUTHORIZED
        return INVALID
    sw.advance()
    rest, args = w[1:], words[1:]
    if not rest:
        return INCOMPLETE
    if _is(rest[0], "version", 3):
        return show_version(sw)
    if _is(rest[0], "clock", 2):
        return datetime.now(UTC).strftime("*%H:%M:%S.%f")[:-3] + datetime.now(UTC).strftime(" UTC %a %b %d %Y\n")
    if _is(rest[0], "logging", 3):
        return show_logging(sw)
    if _is(rest[0], "processes", 4):
        return show_processes_cpu(sw)
    if _is(rest[0], "interfaces", 3):
        tail = rest[1:]
        if tail and _is(tail[0], "counters", 4):
            return show_counters_errors(sw)
        if tail and _is(tail[0], "status", 2):
            return show_status(sw)
        if tail and _is(tail[0], "description", 4):
            return show_description(sw)
        names = [n for n in re.split(r"[,\s]+", " ".join(args[1:])) if n]
        ports = [sw.port(n) for n in names] if names else list(sw.ports.values())
        return "".join(show_interface(sw, p) for p in ports)
    if _is(rest[0], "platform", 4):
        # show platform hardware fed switch active qos queue stats interface <port>
        if [x.lower() for x in args[1:8]] == ["hardware", "fed", "switch", "active", "qos", "queue", "stats"] \
                and len(rest) > 8 and _is(rest[8], "interface", 3):
            if len(args) < 10:
                return INCOMPLETE
            return show_queue_stats(sw, sw.port(args[9]))
        return INVALID
    return INVALID


def _is(word: str, keyword: str, min_len: int) -> bool:
    return len(word) >= min_len and keyword.startswith(word)


def _filter(text: str, pipe: str) -> str:
    verb, _, pattern = pipe.partition(" ")
    verb, pattern = verb.lower(), pattern.strip()
    lines = text.splitlines()
    try:
        rx = re.compile(pattern) if pattern else None
    except re.error:
        rx = re.compile(re.escape(pattern))
    if _is(verb, "include", 1) and rx:
        lines = [ln for ln in lines if rx.search(ln)]
    elif _is(verb, "exclude", 1) and rx:
        lines = [ln for ln in lines if not rx.search(ln)]
    elif _is(verb, "begin", 1) and rx:
        start = next((i for i, ln in enumerate(lines) if rx.search(ln)), len(lines))
        lines = lines[start:]
    elif _is(verb, "section", 1) and rx:
        out, keep = [], False
        for ln in lines:
            if not ln.startswith(" "):
                keep = bool(rx.search(ln))
            if keep:
                out.append(ln)
        lines = out
    elif _is(verb, "count", 1):
        n = sum(1 for ln in lines if rx is None or rx.search(ln))
        return f"Number of lines which match regexp = {n}\n"
    else:
        return INVALID
    return "\n".join(lines) + ("\n" if lines else "")


# ---------------------------------------------------------------- renderers


def _uptime(seconds: float) -> str:
    s = int(seconds)
    d, s = divmod(s, 86400)
    h, s = divmod(s, 3600)
    m = s // 60
    parts = [f"{d} day{'s' * (d != 1)}"] if d else []
    parts += [f"{h} hour{'s' * (h != 1)}"] if h else []
    parts.append(f"{m} minute{'s' * (m != 1)}")
    return ", ".join(parts)


def _mac(sw: Switch, p: Port) -> str:
    base = 0x00A3D1F4B200 + (0x100 if sw.name.endswith("2") else 0) + p.index
    h = f"{base:012x}"
    return f"{h[0:4]}.{h[4:8]}.{h[8:12]}"


def show_version(sw: Switch) -> str:
    return f"""Cisco IOS XE Software, Version 17.09.04a
Cisco IOS Software [Cupertino], Catalyst L3 Switch Software (CAT9K_IOSXE), Version 17.9.4a, RELEASE SOFTWARE (fc3)
Technical Support: http://www.cisco.com/techsupport
Copyright (c) 1986-2023 by Cisco Systems, Inc.
Compiled Fri 20-Oct-23 10:44 by mcpre


Cisco IOS-XE software, Copyright (c) 2005-2023 by cisco Systems, Inc.
All rights reserved.  Certain components of Cisco IOS-XE software are
licensed under the GNU General Public License ("GPL") Version 2.0.

ROM: IOS-XE ROMMON
BOOTLDR: System Bootstrap, Version 17.9.1r[FC1], RELEASE SOFTWARE (P)

{sw.name} uptime is {_uptime(sw.uptime_s())}
Uptime for this control processor is {_uptime(sw.uptime_s() - 90)}
System returned to ROM by Reload Command
System image file is "flash:packages.conf"
Last reload reason: Reload Command

cisco C9300-48T (X86) processor with 1331521K/6147K bytes of memory.
Processor board ID FOC2422{sw.name[-1]}0XYZ
1 Virtual Ethernet interface
48 Gigabit Ethernet interfaces
2048K bytes of non-volatile configuration memory.
8388608K bytes of physical memory.
1638400K bytes of Crash Files at crashinfo:.
11264000K bytes of Flash at flash:.

Base Ethernet MAC Address          : 00:a3:d1:f4:b2:00
Motherboard Assembly Number        : 73-18271-03
Model Revision Number              : A0
Model Number                       : C9300-48T
System Serial Number               : FOC2422{sw.name[-1]}0XYZ

Switch Ports Model              SW Version        SW Image              Mode
------ ----- -----              ----------        ----------            ----
*    1 49    C9300-48T          17.09.04a         CAT9K_IOSXE           INSTALL


Configuration register is 0x102
"""


def show_interface(sw: Switch, p: Port) -> str:
    in_u, out_u = sw.util(p)
    if p.oper_up:
        head = f"{p.name} is up, line protocol is up (connected) \n"
    else:
        head = f"{p.name} is down, line protocol is down (notconnect) \n"
    desc = f"  Description: {p.description}\n" if p.description else ""
    duplex = "Full-duplex, 1000Mb/s" if p.oper_up else "Auto-duplex, Auto-speed"
    last_io = "00:00:00" if p.oper_up else "never"
    return (
        head
        + f"  Hardware is Gigabit Ethernet, address is {_mac(sw, p)} (bia {_mac(sw, p)})\n"
        + desc
        + f"  MTU 1500 bytes, BW {SPEED_BPS // 1000} Kbit/sec, DLY 10 usec, \n"
        + f"     reliability 255/255, txload {max(1, round(out_u * 255))}/255, rxload {max(1, round(in_u * 255))}/255\n"
        + "  Encapsulation ARPA, loopback not set\n"
        + "  Keepalive set (10 sec)\n"
        + f"  {duplex}, media type is 10/100/1000BaseTX\n"
        + "  input flow-control is on, output flow-control is unsupported \n"
        + "  ARP type: ARPA, ARP Timeout 04:00:00\n"
        + f"  Last input {last_io}, output {last_io}, output hang never\n"
        + f"  Last clearing of \"show interface\" counters never\n"
        + f"  Input queue: 0/2000/0/0 (size/max/drops/flushes); Total output drops: {int(p.out_drops)}\n"
        + "  Queueing strategy: fifo\n"
        + "  Output queue: 0/40 (size/max)\n"
        + f"  5 minute input rate {int(p.in_bps)} bits/sec, {int(p.in_pps)} packets/sec\n"
        + f"  5 minute output rate {int(p.out_bps)} bits/sec, {int(p.out_pps)} packets/sec\n"
        + f"     {int(p.in_pkts)} packets input, {int(p.in_octets)} bytes, 0 no buffer\n"
        + f"     Received {int(p.in_bcast)} broadcasts ({int(p.in_bcast * 0.9)} multicasts)\n"
        + "     0 runts, 0 giants, 0 throttles \n"
        + f"     {int(p.in_errors)} input errors, {int(p.crc)} CRC, 0 frame, 0 overrun, 0 ignored\n"
        + f"     0 watchdog, {int(p.in_bcast * 0.9)} multicast, 0 pause input\n"
        + "     0 input packets with dribble condition detected\n"
        + f"     {int(p.out_pkts)} packets output, {int(p.out_octets)} bytes, 0 underruns\n"
        + "     Output 0 broadcasts (0 multicasts)\n"
        + f"     0 output errors, 0 collisions, {p.resets} interface resets\n"
        + "     0 unknown protocol drops\n"
        + "     0 babbles, 0 late collision, 0 deferred\n"
        + f"     {p.resets} lost carrier, 0 no carrier, 0 pause output\n"
        + "     0 output buffer failures, 0 output buffers swapped out\n"
    )


def show_counters_errors(sw: Switch) -> str:
    out = ["", "Port        Align-Err     FCS-Err    Xmit-Err     Rcv-Err  UnderSize  OutDiscards"]
    for p in sw.ports.values():
        out.append(f"{p.short:<10}{0:>11}{int(p.crc):>12}{0:>12}{int(p.in_errors):>12}{0:>11}{int(p.out_drops):>13}")
    out += ["", "Port      Single-Col  Multi-Col   Late-Col  Excess-Col  Carri-Sen      Runts     Giants"]
    for p in sw.ports.values():
        out.append(f"{p.short:<10}{0:>10}{0:>11}{0:>11}{0:>12}{p.resets:>11}{0:>11}{0:>11}")
    return "\n".join(out) + "\n\n"


def show_status(sw: Switch) -> str:
    out = ["", "Port         Name               Status       Vlan       Duplex  Speed Type"]
    for p in sw.ports.values():
        status = "connected" if p.oper_up else "notconnect"
        duplex, speed = ("a-full", "a-1000") if p.oper_up else ("auto", "auto")
        vlan = "trunk" if p.kind in ("uplink", "downlink") else "10"
        out.append(f"{p.short:<13}{p.description[:18]:<19}{status:<13}{vlan:<11}{duplex:>6}{speed:>7} 10/100/1000BaseTX")
    return "\n".join(out) + "\n"


def show_description(sw: Switch) -> str:
    out = ["Interface                      Status         Protocol Description"]
    for p in sw.ports.values():
        st = "up" if p.oper_up else "down"
        out.append(f"{p.short:<31}{st:<15}{st:<9}{p.description}")
    return "\n".join(out) + "\n"


def show_queue_stats(sw: Switch, p: Port) -> str:
    asic_port = p.index - 1
    enq = "\n".join(f"{q:>5} {0:>7} {0:>11} {0:>11} {int(p.queue_enq[q]):>11} {0:>9}" for q in range(3))
    drops = "\n".join(f"{q:>5} {0:>11} {0:>11} {int(p.queue_drops[q]):>11} {0:>11} {0:>11} {0:>11}" for q in range(3))
    return f"""DATA Port:{asic_port} Enqueue Counters
-------------------------------
Queue Buffers Enqueue-TH0 Enqueue-TH1 Enqueue-TH2 Qpolicer
      (Count)     (Bytes)     (Bytes)     (Bytes)   (Bytes)
----- ------- ----------- ----------- ----------- ---------
{enq}

DATA Port:{asic_port} Drop Counters
-------------------------------
Queue Drop-TH0    Drop-TH1    Drop-TH2    SBufDrop    QebDrop     QpolicerDrop
      (Bytes)     (Bytes)     (Bytes)     (Bytes)     (Bytes)     (Bytes)
----- ----------- ----------- ----------- ----------- ----------- -----------
{drops}

 AQM Broadcast Early WTD COUNTERS(In terms of Bytes)
--------------------------------------------------
  PORT TYPE          ENQUEUE             DROP
--------------------------------------------------
  UPLINK PORT-0                 N/A               0
"""


def format_log_line(line, seq: int) -> str:
    """IOS buffer format: 000123: *Oct 10 09:14:00.123 UTC: %LINK-3-UPDOWN: ..."""
    ts = line.ts.strftime("%b %d %H:%M:%S.") + f"{line.ts.microsecond // 1000:03d}"
    ts = ts.replace(" 0", "  ", 1) if ts[4] == "0" else ts
    return f"{seq:06d}: *{ts} UTC: %{line.facility}-{line.severity}-{line.mnemonic}: {line.text}"


def show_logging(sw: Switch) -> str:
    n = len(sw.log)
    first = sw.log_seq - n + 1
    lines = [format_log_line(ln, first + i) for i, ln in enumerate(sw.log)]
    return f"""Syslog logging: enabled (0 messages dropped, 0 messages rate-limited, 0 flushes, 0 overruns, xml disabled, filtering disabled)

No Active Message Discriminator.



No Inactive Message Discriminator.


    Console logging: disabled
    Monitor logging: level debugging, 0 messages logged, xml disabled,
                     filtering disabled
    Buffer logging:  level debugging, {sw.log_seq} messages logged, xml disabled,
                    filtering disabled
    Exception Logging: size (4096 bytes)
    Count and timestamp logging messages: disabled
    File logging: disabled
    Persistent logging: disabled

No active filter modules.

    Trap logging: level informational, {sw.log_seq} message lines logged
        Logging to forwarder  (udp port 514, audit disabled,
              link up),
              {sw.log_seq} message lines logged,
              0 message lines rate-limited,
              0 message lines dropped-by-MD,
              xml disabled, sequence number disabled
              filtering disabled
        Logging Source-Interface:       VRF Name:

Log Buffer (102400 bytes):

""" + "\n".join(lines) + "\n"


def show_processes_cpu(sw: Switch) -> str:
    five_s, one_m, five_m = sw.cpu()
    procs = [
        (523, 8121331, 61234411, 132, "0.95%", "0.88%", "0.87%", "fed main event"),
        (88, 2210333, 9192331, 240, "0.31%", "0.33%", "0.33%", "IOSD ipc task"),
        (421, 1203221, 5512331, 218, "0.15%", "0.17%", "0.16%", "SNMP ENGINE"),
        (190, 801221, 4402212, 182, "0.07%", "0.08%", "0.08%", "IP Input"),
        (233, 512231, 2210331, 231, "0.04%", "0.05%", "0.05%", "SSH Process"),
        (12, 301221, 1203311, 250, "0.00%", "0.01%", "0.01%", "ARP Input"),
    ]
    head = (f"CPU utilization for five seconds: {five_s}%/0%; one minute: {one_m}%; five minutes: {five_m}%\n"
            " PID Runtime(ms)     Invoked      uSecs   5Sec   1Min   5Min TTY Process \n")
    return head + "".join(f"{pid:>4}{rt:>12}{inv:>12}{us:>11}{a:>7}{b:>7}{c:>7}   0 {name}\n"
                          for pid, rt, inv, us, a, b, c, name in procs)


__all__ = ["run_command", "format_log_line", "short_name"]
