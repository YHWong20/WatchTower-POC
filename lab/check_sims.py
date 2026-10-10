"""Health check for the Cisco simulators from the Mac: fault API, SSH CLI, SNMP. Used by lab/health.sh.

    uv run python lab/check_sims.py cisco-sim-1 [cisco-sim-2]
"""

import asyncio
import json
import sys
import urllib.request

import asyncssh

from sims.common.snmp import SnmpClient, oid

HOST_PORTS = {"cisco-sim-1": (2221, 1611, 9101, "Gi1/0/13"), "cisco-sim-2": (2222, 1612, 9102, "Gi1/0/48")}


async def check(name: str) -> bool:
    ssh_port, snmp_port, api_port, port = HOST_PORTS[name]
    ok = True
    try:
        health = json.load(urllib.request.urlopen(f"http://127.0.0.1:{api_port}/health", timeout=3))
        faults = health["faults"]
        print(f"   {'ok  ' if not faults else 'WARN'} {name} fault API: {'no faults' if not faults else faults}")
    except OSError as exc:
        print(f"   FAIL {name} fault API on :{api_port}: {exc}")
        return False
    try:
        async with asyncssh.connect("127.0.0.1", port=ssh_port, username="admin", password="demo",
                                    known_hosts=None, connect_timeout=5) as conn:
            out = (await conn.run(f"show interfaces {port}", timeout=5)).stdout
        first = out.splitlines()[0] if out else "(empty)"
        good = "line protocol is" in first
        print(f"   {'ok  ' if good else 'FAIL'} {name} ssh -p {ssh_port} admin@localhost \"show interfaces {port}\": {first.strip()}")
        ok &= good
    except (OSError, asyncssh.Error) as exc:
        print(f"   FAIL {name} SSH on :{ssh_port}: {exc}")
        ok = False
    try:
        idx = int(port.split("/")[-1])
        async with SnmpClient("127.0.0.1", snmp_port) as c:
            (_, (_, name_v)), (_, (_, drops)), (_, (_, octets)) = await c.get(
                oid("1.3.6.1.2.1.1.5.0"), oid(f"1.3.6.1.2.1.2.2.1.19.{idx}"), oid(f"1.3.6.1.2.1.31.1.1.1.10.{idx}"))
        print(f"   ok   {name} SNMP :{snmp_port}/udp sysName={name_v.decode()} {port} ifOutDiscards={drops} ifHCOutOctets={octets}")
    except (OSError, TimeoutError, RuntimeError) as exc:
        print(f"   FAIL {name} SNMP on :{snmp_port}/udp: {exc!r}")
        ok = False
    return ok


async def main(names):
    results = [await check(n) for n in names]
    return all(results)


if __name__ == "__main__":
    sys.exit(0 if asyncio.run(main(sys.argv[1:] or ["cisco-sim-1"])) else 1)
