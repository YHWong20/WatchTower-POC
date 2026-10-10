import fakeredis
import pytest

from wt.common.bus import EVIDENCE, SIGNALS, STREAMS, TASKS, Bus
from wt.common.contracts import Signal

from .conftest import fixture_json


@pytest.fixture
async def bus():
    b = Bus(fakeredis.FakeAsyncRedis(decode_responses=True))
    yield b
    await b.close()


def test_six_streams():
    assert STREAMS == ("wt.signals", "wt.tasks", "wt.evidence", "wt.watch", "wt.investigations", "wt.sre")


async def test_publish_model_and_consume_in_group(bus):
    sig = Signal.model_validate(fixture_json("contracts/signal.json"))
    await bus.ensure_group(SIGNALS, "parent")
    await bus.publish(SIGNALS, sig)

    msgs = await bus.read([SIGNALS], "parent", "parent-1", block_ms=None)
    assert len(msgs) == 1 and msgs[0].stream == SIGNALS
    assert Signal.model_validate(msgs[0].data) == sig
    await bus.ack("parent", msgs[0])
    assert await bus.read([SIGNALS], "parent", "parent-1", block_ms=None) == []


async def test_consumers_in_a_group_share_work(bus):
    await bus.ensure_group(TASKS, "openshift")
    for i in range(4):
        await bus.publish(TASKS, {"investigation_id": "INV-0001", "evidence_key": f"k{i}"})
    a = await bus.read([TASKS], "openshift", "a", count=2, block_ms=None)
    b = await bus.read([TASKS], "openshift", "b", count=2, block_ms=None)
    assert {m.data["evidence_key"] for m in a + b} == {"k0", "k1", "k2", "k3"}


async def test_restarted_consumer_gets_unacked_messages_first(bus):
    await bus.ensure_group(EVIDENCE, "parent")
    await bus.publish(EVIDENCE, {"n": 1})
    await bus.publish(EVIDENCE, {"n": 2})
    first = await bus.read([EVIDENCE], "parent", "p1", block_ms=None)
    await bus.ack("parent", first[0])                     # n=1 handled, n=2 not: "crash" here
    await bus.publish(EVIDENCE, {"n": 3})

    got = []
    async for msg in bus.consume([EVIDENCE], "parent", "p1", block_ms=10):
        got.append(msg.data["n"])
        await bus.ack("parent", msg)
        if len(got) == 2:
            break
    assert got == [2, 3]


async def test_pending_without_ack_does_not_loop_forever(bus):
    await bus.ensure_group(EVIDENCE, "parent")
    await bus.publish(EVIDENCE, {"n": 1})
    await bus.read([EVIDENCE], "parent", "p1", block_ms=None)
    await bus.publish(EVIDENCE, {"n": 2})
    got = []
    async for msg in bus.consume([EVIDENCE], "parent", "p1", block_ms=10):
        got.append(msg.data["n"])                         # never acked
        if len(got) == 2:
            break
    assert got == [1, 2]


async def test_unknown_stream_refused(bus):
    with pytest.raises(ValueError):
        await bus.publish("wt.remediation", {"x": 1})
