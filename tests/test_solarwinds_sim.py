"""solarwinds-sim: SWQL compilation, the SNMP poller against an in-process cisco-sim, alert
rules, and SWIS through the phase-1 SolarWindsClient."""

from datetime import UTC, datetime, timedelta

import httpx
import pytest

from sims.cisco_sim.model import Switch
from sims.cisco_sim.server import mib
from sims.common import snmp
from sims.solarwinds_sim import swql
from sims.solarwinds_sim.poller import OrionDb, Poller, Target, interface_id
from sims.solarwinds_sim.server import create_app
from wt.common.sources.base import placeholders, render_template
from wt.common.sources.solarwinds import SolarWindsClient

T0 = datetime(2026, 10, 10, 9, 0, tzinfo=UTC)


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


@pytest.fixture
async def agent(sw):
    transport = await snmp.serve_agent(snmp.MibView(lambda: mib(sw), ttl_s=0), host="127.0.0.1", port=0)
    yield transport.get_extra_info("sockname")[1]
    transport.close()


@pytest.fixture
def db():
    d = OrionDb(":memory:")
    yield d
    d.db.close()


async def poll(poller: Poller, clock: Clock, at: datetime) -> None:
    """One poll with the switch clock and the sample timestamp moved in step."""
    t = poller.targets[0]
    data = await poller.fetch(t)
    data["time"] = at.timestamp()
    poller.store(t, data)
    poller.evaluate_alerts(at)


async def polled(db, agent, sw, clock, minutes: int, level: str | None = None) -> Poller:
    p = Poller(db, [Target("cisco-sim-1", "127.0.0.1", agent)], interval_s=60)
    if level:
        sw.set_congestion("Gi1/0/13", level)
    for i in range(minutes + 1):
        await poll(p, clock, T0 + timedelta(minutes=i))
        clock.t += 60
    return p


def q(db, text, now=None, **params):
    c = swql.compile_query(text, params, now=now or T0 + timedelta(minutes=30))
    return swql.shape(db.query(c), c.columns)


# ---------------------------------------------------------------- SWQL


def test_every_swql_template_in_sources_compiles(sources):
    params = {"node": "cisco-sim-1", "switch": "cisco-sim-1", "port": "Gi1/0/13", "if_id": 113, "minutes": 60}
    seen = 0
    for key, sig in sources.signals.items():
        for attr in ("query", "query_solarwinds"):
            text = getattr(sig, attr, None)
            if text and text.startswith("SELECT"):
                swql.compile_query(render_template(text, **{p: params[p] for p in placeholders(text)}))
                seen += 1
    assert seen == 7                     # 5 evidence queries + 2 Network Agent watch queries


def test_compiles_navigation_alias_top_and_date_functions():
    c = swql.compile_query("SELECT TOP 5 I.InterfaceID, I.Node.Caption AS Switch FROM Orion.NPM.Interfaces I "
                           "WHERE I.Node.Caption = 'cisco-sim-1' AND I.Name = 'Gi1/0/13' ORDER BY I.InterfaceID DESC")
    assert "LEFT JOIN Nodes j1" in c.sql and c.sql.endswith("LIMIT 5") and c.params == ["cisco-sim-1", "Gi1/0/13"]
    assert [n for n, _ in c.columns] == ["InterfaceID", "Switch"]
    now = datetime(2026, 10, 10, 9, 30, tzinfo=UTC)
    c = swql.compile_query("SELECT DateTime FROM Orion.NPM.InterfaceErrors WHERE DateTime > "
                           "ADDDATE('minute', -60, GETUTCDATE())", now=now)
    assert c.params == ["2026-10-10 08:30:00"]
    c = swql.compile_query("SELECT InterfaceID FROM Orion.NPM.InterfaceErrors WHERE DateTime >= '2026-10-10T09:00:00Z' "
                           "AND InterfaceID IN (@a, 114)", {"a": 113})
    assert c.params == ["2026-10-10 09:00:00", 113, 114]


@pytest.mark.parametrize("bad", [
    "SELECT * FROM Orion.Nodes",
    "SELECT Caption FROM Orion.Nope",
    "SELECT Bogus FROM Orion.Nodes",
    "SELECT Caption FROM Orion.Nodes; DELETE FROM Nodes",
    "DELETE FROM Orion.Nodes",
    "SELECT Caption FROM Orion.Nodes WHERE Caption = @missing",
    "SELECT I.Foo.Caption FROM Orion.NPM.Interfaces I",
])
def test_bad_or_writing_swql_is_rejected(bad):
    with pytest.raises(swql.SwqlError):
        swql.compile_query(bad)


# ---------------------------------------------------------------- poller


async def test_poller_turns_counters_into_orion_rows(db, agent, sw, clock):
    await polled(db, agent, sw, clock, minutes=3)
    node = q(db, "SELECT NodeID, Caption, Vendor, Status, CPULoad, PercentMemoryUsed, IOSVersion FROM Orion.Nodes")[0]
    assert node["Caption"] == "cisco-sim-1" and node["Status"] == 1 and node["IOSVersion"] == "17.9.4a"
    assert 0 < node["CPULoad"] < 20 and 20 < node["PercentMemoryUsed"] < 40
    iface = q(db, "SELECT I.InterfaceID, I.Name, I.Status FROM Orion.NPM.Interfaces I "
                  "WHERE I.Node.Caption = 'cisco-sim-1' AND I.Name = 'Gi1/0/13'")
    assert iface == [{"InterfaceID": interface_id(1, 13), "Name": "Gi1/0/13", "Status": 1}]
    traffic = q(db, "SELECT DateTime, OutPercentUtil FROM Orion.NPM.InterfaceTraffic WHERE InterfaceID = 113 ORDER BY DateTime")
    assert len(traffic) == 3 and traffic[0]["DateTime"] == "2026-10-10T09:01:00.0000000Z"
    assert all(15 < r["OutPercentUtil"] < 21 for r in traffic)
    cpu = q(db, "SELECT TOP 1 AvgLoad, AvgPercentMemoryUsed FROM Orion.CPULoad WHERE Node.Caption = 'cisco-sim-1' "
                "ORDER BY DateTime DESC")
    assert cpu and cpu[0]["AvgLoad"] is not None
    assert q(db, "SELECT COUNT(*) AS n FROM Orion.NPM.Interfaces") == [{"n": 48}]


async def test_congestion_shows_as_discards_not_utilisation_and_fires_nothing(db, agent, sw, clock):
    await polled(db, agent, sw, clock, minutes=8, level="high")
    rows = q(db, "SELECT DateTime, InDiscards, OutDiscards, InErrors, OutErrors FROM Orion.NPM.InterfaceErrors "
                 "WHERE InterfaceID = 113 AND DateTime > ADDDATE('minute', -60, GETUTCDATE()) ORDER BY DateTime",
             now=T0 + timedelta(minutes=9))
    assert len(rows) == 8
    assert all(10_000 < r["OutDiscards"] < 40_000 for r in rows)        # ~380 drops/s x 60 s
    assert all(r["InErrors"] == 0 and r["InDiscards"] == 0 for r in rows)
    util = q(db, "SELECT MAX(OutPercentUtil) AS peak FROM Orion.NPM.InterfaceTraffic WHERE InterfaceID = 113")
    assert util[0]["peak"] < 60
    today = q(db, "SELECT OutDiscardsToday FROM Orion.NPM.Interfaces WHERE InterfaceID = 113")[0]["OutDiscardsToday"]
    assert today == sum(r["OutDiscards"] for r in rows)
    assert q(db, "SELECT AlertActiveID FROM Orion.AlertActive") == []


async def test_alert_rules_trigger_and_reset(db, agent, sw, clock):
    p = await polled(db, agent, sw, clock, minutes=1)
    # utilisation above 90 % for 5 minutes (written directly: the switch model never gets there)
    for i in range(5):
        stamp = swql.to_db(T0 + timedelta(minutes=2 + i))
        db.execute('INSERT INTO InterfaceTraffic ("NodeID","InterfaceID","DateTime","InPercentUtil","OutPercentUtil") '
                   "VALUES (1, 110, ?, 20, 95)", (stamp,))
        p.evaluate_alerts(T0 + timedelta(minutes=2 + i, seconds=1))
        active = q(db, "SELECT TriggeredMessage FROM Orion.AlertActive")
        assert (len(active) == 1) == (i == 4), f"after {i + 1} samples"
    assert "Gi1/0/10 on cisco-sim-1 utilization is 95 %" in active[0]["TriggeredMessage"]
    # a cabled port going down
    sw.set_oper(sw.port("Gi1/0/13"), False)
    clock.t += 60
    await poll(p, clock, T0 + timedelta(minutes=7, seconds=2))
    msgs = [r["TriggeredMessage"] for r in q(db, "SELECT TriggeredMessage FROM Orion.AlertActive")]
    assert any("Gi1/0/13 (worker-1) on cisco-sim-1 is Down" in m for m in msgs)
    sw.clear()
    clock.t += 60
    await poll(p, clock, T0 + timedelta(minutes=8, seconds=2))
    assert not any("is Down" in r["TriggeredMessage"] for r in q(db, "SELECT TriggeredMessage FROM Orion.AlertActive"))
    history = q(db, "SELECT EventType, Message FROM Orion.AlertHistory WHERE Message LIKE '%Gi1/0/13%' ORDER BY TimeStamp")
    assert [h["EventType"] for h in history] == [0, 1]           # triggered, then reset


async def test_switch_restart_rebaselines_instead_of_a_negative_spike(db, agent, sw, clock):
    p = await polled(db, agent, sw, clock, minutes=1)
    prev = p.prev["cisco-sim-1"]
    prev["uptime_ticks"] += 10**9                            # pretend the switch rebooted since
    await poll(p, clock, T0 + timedelta(minutes=2))
    assert len(q(db, "SELECT DateTime FROM Orion.NPM.InterfaceTraffic WHERE InterfaceID = 113")) == 1


async def test_unreachable_switch_goes_down_after_two_polls(db):
    p = Poller(db, [Target("cisco-sim-1", "127.0.0.1", 9)], interval_s=60, snmp_timeout_s=0.1)
    db.upsert("Nodes", {"NodeID": 1, "Caption": "cisco-sim-1", "Status": 1})
    for _ in range(2):
        await p.poll_once()
    assert q(db, "SELECT Status FROM Orion.Nodes") == [{"Status": 2}]
    assert "cisco-sim-1 is Down" in q(db, "SELECT TriggeredMessage FROM Orion.AlertActive")[0]["TriggeredMessage"]


# ---------------------------------------------------------------- SWIS API


async def test_swis_through_the_wt_adapter(db, agent, sw, clock, sources, audit):
    await polled(db, agent, sw, clock, minutes=2, level="medium")
    transport = httpx.ASGITransport(app=create_app(db, {"watchtower": "watchtower"}))
    client = SolarWindsClient("http://solarwinds", "watchtower", "watchtower", sources, audit, transport=transport)
    lookup = await client.query_signal("interface_lookup", switch="cisco-sim-1", port="Gi1/0/13")
    assert lookup.rows == [{"InterfaceID": 113, "Name": "Gi1/0/13", "Status": 1}]
    assert (await client.active_alerts()).rows == []
    with pytest.raises(httpx.HTTPStatusError) as exc:
        await client.query("SELECT * FROM Orion.Nodes")
    assert exc.value.response.status_code == 400 and "SELECT *" in exc.value.response.json()["Message"]
    await client.close()
    bad = SolarWindsClient("http://solarwinds", "watchtower", "nope", sources, audit, transport=transport)
    with pytest.raises(httpx.HTTPStatusError) as exc:
        await bad.query("SELECT Caption FROM Orion.Nodes")
    assert exc.value.response.status_code == 401
    await bad.close()
