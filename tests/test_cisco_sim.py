import asyncio

import asyncssh
import pytest
import httpx

from sims.cisco_sim.cli import run_command
from sims.cisco_sim.model import Switch, short_name
from sims.cisco_sim.server import _session, _SshServer, build_switch, fault_api, mib
from sims.common import snmp
from wt.common.config import settings
from wt.common.probes.base import ProbeRefused
from wt.common.probes.ssh_cli import SshCli

QUEUE_CMD = "show platform hardware fed switch active qos queue stats interface Gi1/0/13"


class Clock:
    def __init__(self):
        self.t = 1_000_000.0

    def __call__(self):
        return self.t


@pytest.fixture
def clock():
    return Clock()


@pytest.fixture
def sw(clock):
    return Switch("cisco-sim-1", "access", {"Gi1/0/10": ("master-1", "ocp_node"),
                                            "Gi1/0/13": ("worker-1", "ocp_node")}, clock=clock)


# ---------------------------------------------------------------- SNMP codec and agent

def test_ber_round_trip():
    vbs = [
        (snmp.oid("1.3.6.1.2.1.1.5.0"), (snmp.OCTET_STRING, b"cisco-sim-1")),
        (snmp.oid("1.3.6.1.2.1.2.2.1.19.13"), (snmp.COUNTER32, 2**32 - 1)),
        (snmp.oid("1.3.6.1.2.1.31.1.1.1.10.13"), (snmp.COUNTER64, 2**63 + 5)),
        (snmp.oid("1.3.6.1.2.1.1.2.0"), (snmp.OBJECT_ID, snmp.oid("1.3.6.1.4.1.9.1.2494"))),
        (snmp.oid("1.3.6.1.2.1.2.2.1.8.11"), (snmp.INTEGER, -2)),
        (snmp.oid("1.3.6.1.2.1.1.3.0"), (snmp.TIMETICKS, 27462600)),
    ]
    raw = snmp.encode_message("public", snmp.RESPONSE, 4242, 0, 0, vbs)
    ver, comm, pdu, req_id, a, b, out = snmp.decode_message(raw)
    assert (ver, comm, pdu, req_id, a, b) == (1, "public", snmp.RESPONSE, 4242, 0, 0)
    assert out == vbs


@pytest.fixture
async def agent(sw):
    transport = await snmp.serve_agent(snmp.MibView(lambda: mib(sw)), host="127.0.0.1", port=0)
    yield transport.get_extra_info("sockname")[1]
    transport.close()


async def test_agent_get_walk_and_read_only(sw, agent):
    sw.set_congestion("Gi1/0/13", "high")
    async with snmp.SnmpClient("127.0.0.1", agent) as c:
        (_, name), (_, oper11), (_, missing) = await c.get(
            snmp.oid("1.3.6.1.2.1.1.5.0"), snmp.oid("1.3.6.1.2.1.2.2.1.8.11"), snmp.oid("1.3.6.1.2.1.2.2.1.8.99"))
        assert name == (snmp.OCTET_STRING, b"cisco-sim-1")
        assert oper11 == (snmp.INTEGER, 2)                         # Gi1/0/11 not cabled in lite
        assert missing[0] == snmp.NO_SUCH_INSTANCE
        names = await c.walk(snmp.oid("1.3.6.1.2.1.31.1.1.1.1"))
        assert len(names) == 48 and names[12][1] == (snmp.OCTET_STRING, b"Gi1/0/13")
        with pytest.raises(RuntimeError, match="error-status 17"):
            await c._request(snmp.SET, [snmp.oid("1.3.6.1.2.1.1.5.0")])
    async with snmp.SnmpClient("127.0.0.1", agent, community="private", timeout_s=0.3, retries=0) as c:
        with pytest.raises(TimeoutError):
            await c.get(snmp.oid("1.3.6.1.2.1.1.5.0"))


# ---------------------------------------------------------------- model

def test_congestion_is_drops_without_crc_near_46_percent(sw, clock):
    p = sw.port("Gi1/0/13")
    base_drops = p.out_drops
    sw.set_congestion("Gi1/0/13", "medium")
    for _ in range(30):                                            # 30 minutes of 60 s polls
        clock.t += 60
        sw.advance()
    in_u, out_u = sw.util(p)
    assert 0.43 < out_u < 0.49                                     # average looks unremarkable
    assert p.out_drops - base_drops > 30 * 60 * 20                 # but the queue overflows
    assert p.crc == 0 and p.in_errors == 0
    assert p.queue_drops[1] > 0 and p.queue_drops[0] == p.queue_drops[2] == 0
    assert sw.port("Gi1/0/10").out_drops == 0                      # neighbours unaffected


def test_crc_and_flap_and_clear(sw, clock):
    sw.set_crc("Gi1/0/10", 5)
    clock.t += 10
    sw.advance()
    assert sw.port("Gi1/0/10").crc == pytest.approx(50)
    sw.set_oper(sw.port("Gi1/0/10"), False)
    assert [ln.mnemonic for ln in list(sw.log)[-2:]] == ["UPDOWN", "UPDOWN"]
    assert {f["type"] for f in sw.faults()} == {"crc", "link_down"}
    sw.clear()
    assert sw.faults() == [] and sw.port("Gi1/0/10").oper_up
    assert sw.port("Gi1/0/10").crc == pytest.approx(50)            # counters are never reset


def test_short_names():
    assert short_name("GigabitEthernet1/0/11") == short_name("gi1/0/11") == short_name("Gig1/0/11") == "Gi1/0/11"


@pytest.mark.parametrize("profile, cabled", [("lite", ["Gi1/0/10", "Gi1/0/13"]),
                                             ("full", ["Gi1/0/10", "Gi1/0/11", "Gi1/0/12", "Gi1/0/13", "Gi1/0/48"])])
def test_switch_ports_follow_the_profile(monkeypatch, profile, cabled):
    monkeypatch.setenv("WT_PROFILE", profile)
    settings.cache_clear()
    try:
        s = build_switch("cisco-sim-1")
    finally:
        settings.cache_clear()
    assert [p.short for p in s.ports.values() if p.connected] == cabled
    assert s.port("Gi1/0/13").description == "worker-1"


# ---------------------------------------------------------------- CLI

def test_cli_show_commands(sw, clock):
    sw.set_congestion("Gi1/0/13", "high")
    clock.t += 120
    out = run_command(sw, "sh int gi1/0/13")
    assert out.startswith("GigabitEthernet1/0/13 is up, line protocol is up (connected)")
    assert "Description: worker-1" in out and "0 input errors, 0 CRC" in out
    drops = int(out.split("Total output drops: ")[1].split()[0])
    assert drops > 0
    q = run_command(sw, QUEUE_CMD)
    drop_rows = q.split("Drop Counters")[1].splitlines()[5:8]
    assert [int(r.split()[3]) > 0 for r in drop_rows] == [False, True, False]   # Drop-TH2, queue 1 only
    assert run_command(sw, "show interfaces counters errors | include Gi1/0/13").splitlines()[0].split()[-1] == str(drops)
    assert run_command(sw, "show interfaces Gi1/0/10 Gi1/0/13 | count drops") == "Number of lines which match regexp = 4\n"
    assert "uptime is 3 days" in run_command(sw, "show version")
    assert run_command(sw, "show processes cpu sorted").startswith("CPU utilization for five seconds")


def test_cli_refuses_and_rejects(sw):
    assert run_command(sw, "conf t") == "% Authorization failed.\n"
    assert run_command(sw, "reload") == "% Authorization failed.\n"
    assert run_command(sw, "show bogus").startswith("% Invalid input")
    assert run_command(sw, "show interfaces Gi9/9/9").startswith("% Invalid input")
    assert run_command(sw, "show platform hardware fed switch active qos queue stats interface") == "% Incomplete command.\n"


# ---------------------------------------------------------------- fault API

async def test_fault_api(sw):
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=fault_api(sw)), base_url="http://sim") as client:
        r = await client.post("/faults", json={"type": "congestion", "port": "Gi1/0/13", "level": "low"})
        assert r.status_code == 200 and r.json() == [{"type": "congestion", "port": "Gi1/0/13", "level": "low"}]
        bad_level = await client.post("/faults", json={"type": "congestion", "port": "Gi1/0/13", "level": "extreme"})
        assert bad_level.status_code == 422
        assert (await client.post("/faults", json={"type": "crc", "port": "Gi7/0/1"})).status_code == 404
        assert (await client.get("/ports/Gi1/0/13")).json()["crc"] == 0
        assert (await client.delete("/faults")).json() == []


# ---------------------------------------------------------------- phase 1 SSH probe against the sim

async def test_wt_ssh_probe_against_the_sim(sw, sources, audit, store):
    sw.set_congestion("Gi1/0/13", "medium")
    server = await asyncssh.create_server(
        lambda: _SshServer(sw, {"watchtower": "pw"}), "127.0.0.1", 0,
        server_host_keys=[asyncssh.generate_private_key("ssh-ed25519")], process_factory=_session(sw))
    port = server.sockets[0].getsockname()[1]
    try:
        cli = SshCli(sources, audit, "watchtower", "pw")
        res = await cli.run(f"127.0.0.1:{port}", QUEUE_CMD)
        assert "DATA Port:12 Drop Counters" in res.output
        with pytest.raises(ProbeRefused):
            await cli.run(f"127.0.0.1:{port}", "conf t")
    finally:
        server.close()
        await server.wait_closed()
    assert any(ln.mnemonic == "LOGIN_SUCCESS" and "watchtower" in ln.text for ln in sw.log)
    assert [r["kind"] for r in store.audit_rows()] == ["refused", "probe"]
