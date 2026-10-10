"""forwarder: index-time parsing, HEC batching into splunk-sim, checkpoints, event de-duplication,
and the Docker exec stream (against a fake Docker socket)."""

import asyncio
import json
import struct
import tempfile

import httpx
import pytest

from sims.forwarder import parse
from sims.forwarder.docker_api import DockerClient
from sims.forwarder.hec import Checkpoints, HecSender
from sims.forwarder.server import KubeEvents
from sims.splunk_sim import spl
from sims.splunk_sim.server import create_app
from sims.splunk_sim.store import EventStore

FILE = "svc-prober-7xk2p_wt-lab_svc-prober-" + "0123456789abcdef" * 4 + ".log"


def test_container_log_names_and_cri_lines():
    meta = parse.container_log_meta("/var/log/containers/" + FILE)
    assert meta["pod"] == "svc-prober-7xk2p" and meta["namespace"] == "wt-lab" and meta["container"] == "svc-prober"
    assert parse.container_log_meta("junk.log") is None
    ns, stream, full, msg = parse.parse_cri('2026-10-10T13:01:47.26988586Z stdout F {"ok": true}')
    assert ns == 1791637307_269885860 and stream == "stdout" and full and msg == '{"ok": true}'
    assert parse.parse_cri("2026-10-10T13:01:47Z stderr P part")[2] is False
    assert parse.parse_cri("not a cri line") is None
    ev = parse.container_event("ocp-sim-worker", FILE, meta, ns, msg)
    assert ev["sourcetype"] == "kube:container:svc-prober" and ev["host"] == "ocp-sim-worker"
    assert ev["fields"]["namespace"] == "wt-lab" and ev["time"] == pytest.approx(1791637307.269886)


def test_journal_entries():
    entry = {"MESSAGE": [72, 105], "__REALTIME_TIMESTAMP": "1791629521082638", "_HOSTNAME": "ocp-sim-worker",
             "_SYSTEMD_UNIT": "kubelet.service", "PRIORITY": "6"}
    ev = parse.journal_event(entry, "fallback")
    assert ev["event"] == "Hi" and ev["sourcetype"] == "kubelet" and ev["host"] == "ocp-sim-worker"
    assert parse.journal_event({"MESSAGE": "", "__REALTIME_TIMESTAMP": "1"}, "x") is None


def test_kube_events_get_namespace_and_source_host_fields():
    e = {"metadata": {"name": "x", "namespace": "kube-system", "managedFields": [{}], "uid": "u", "resourceVersion": "5"},
         "involvedObject": {"kind": "Pod", "name": "etcd-ocp-sim-control-plane", "namespace": "kube-system"},
         "reason": "Killing", "source": {"component": "kubelet", "host": "ocp-sim-control-plane"},
         "lastTimestamp": "2026-10-10T13:00:00Z", "eventTime": None}
    ev = parse.kube_event(e)
    assert ev["fields"] == {"namespace": "kube-system", "source_host": "ocp-sim-control-plane", "cluster": "ocp-sim"}
    assert "managedFields" not in ev["event"]["metadata"] and ev["time"] == 1791637200
    node = parse.kube_event({"metadata": {}, "involvedObject": {"kind": "Node", "name": "ocp-sim-worker"},
                             "reason": "NodeNotReady", "source": {"component": "node-controller"},
                             "series": {"lastObservedTime": "2026-10-10T13:00:05.000000Z"}})
    assert node["host"] == "ocp-sim" and node["time"] == 1791637205


def test_switch_syslog():
    pkt = b"<187>000042: cisco-sim-1: *Oct 10 13:00:01.250 UTC: %LINK-3-UPDOWN: Interface GigabitEthernet1/0/13, changed state to down"
    ev = parse.syslog_event(pkt, "172.18.0.5", 1791637210.0)
    assert ev["host"] == "cisco-sim-1" and ev["index"] == "network" and ev["sourcetype"] == "cisco:ios"
    assert ev["time"] == pytest.approx(1791637201.25) and ev["event"].startswith("000042: cisco-sim-1:")
    assert parse.syslog_event(b"no pri", "h", 0) is None


# ---------------------------------------------------------------- HEC to splunk-sim


async def test_hec_batches_reach_splunk_sim_and_checkpoints_move_on_ack(tmp_path):
    store = EventStore(":memory:")
    app = create_app(store, [], tokens={"t"}, users={}, hec_tokens={"hec"})
    hec = HecSender("http://splunk:8088", "hec", batch=2, transport=httpx.ASGITransport(app=app))
    cp = Checkpoints(tmp_path / "cp.json")
    for i in range(5):
        hec.send({"time": 1791637200 + i, "host": "h", "index": "ocp", "sourcetype": "kubelet", "event": f"line {i}"},
                 lambda i=i: cp.set("k", i))
    assert cp.get("k") is None                                 # nothing acknowledged yet
    while hec.queue:
        assert await hec.flush()
    assert cp.get("k") == 4 and hec.shipped["kubelet"] == 5
    cp.save()
    assert Checkpoints(tmp_path / "cp.json").get("k") == 4
    s = spl.parse("search index=ocp sourcetype=kubelet | stats count")
    assert spl.run(s, store.candidates(None, None, s.prefilter), now=1791637300) == [{"count": "5"}]

    refused = HecSender("http://splunk:8088", "wrong", transport=httpx.ASGITransport(app=app))
    refused.send({"event": "x"}, lambda: cp.set("k", -1))
    assert not await refused.flush() and len(refused.queue) == 1 and cp.get("k") == 4   # kept for retry
    for s_ in (hec, refused):
        await s_.close()
    store.close()


def test_kube_events_dedupe_and_skip_what_was_shipped_before_a_restart(tmp_path):
    sent = []

    class FakeHec:
        def send(self, ev, on_ack=None):
            sent.append(ev)
            on_ack and on_ack()

    cp = Checkpoints(tmp_path / "cp.json")
    ke = KubeEvents(FakeHec(), cp, tmp_path / "kubeconfig")
    ev = lambda uid, rv, ts: {"metadata": {"uid": uid, "resourceVersion": rv}, "reason": "X",
                              "involvedObject": {}, "lastTimestamp": ts}
    ke.ship(ev("a", "1", "2026-10-10T13:00:00Z"), listed=True)
    ke.ship(ev("a", "1", "2026-10-10T13:00:00Z"))               # same object again (relist): skipped
    ke.ship(ev("a", "2", "2026-10-10T13:00:30Z"))               # count went up: a new Splunk event
    assert len(sent) == 2 and cp.get("kube_events_since") == 1791637230
    restarted = KubeEvents(FakeHec(), cp, tmp_path / "kubeconfig")
    restarted.ship(ev("a", "2", "2026-10-10T13:00:30Z"), listed=True)    # at the checkpoint: kept (at-least-once)
    restarted.ship(ev("b", "7", "2026-10-10T12:00:00Z"), listed=True)    # older than the checkpoint: skipped
    restarted.ship(ev("c", "8", "2026-10-10T12:00:00Z"))                 # late, but from the watch: kept
    assert [e["event"]["metadata"]["uid"] for e in sent[2:]] == ["a", "c"]


# ---------------------------------------------------------------- Docker exec stream


async def fake_docker(path, frames):
    async def handle(reader, writer):
        request = await reader.readuntil(b"\r\n\r\n")
        length = int(next((l.split(b":")[1] for l in request.split(b"\r\n") if l.lower().startswith(b"content-length")), 0))
        await reader.readexactly(length)
        if b"/exec HTTP" in request.split(b"\r\n")[0]:
            body = json.dumps({"Id": "abc"}).encode()
            writer.write(b"HTTP/1.1 201 Created\r\nContent-Type: application/json\r\nContent-Length: %d\r\n\r\n%s"
                         % (len(body), body))
        else:
            writer.write(b"HTTP/1.1 101 UPGRADED\r\nConnection: Upgrade\r\nUpgrade: tcp\r\n\r\n")
            for stream, data in frames:
                writer.write(bytes([stream, 0, 0, 0]) + struct.pack(">I", len(data)) + data)
        await writer.drain()
        writer.close()

    return await asyncio.start_unix_server(handle, path)


async def test_exec_stream_demultiplexes_stdout_and_stderr():
    short = tempfile.TemporaryDirectory(dir="/tmp")        # macOS caps unix socket paths at 104 bytes
    sock = f"{short.name}/d.sock"
    server = await fake_docker(sock, [(1, b"pid 42\nfirst li"), (2, b"tail: warning\n"), (1, b"ne\nlast")])
    client = DockerClient(sock)
    lines = [x async for x in client.exec_lines("ocp-sim-worker", ["tail", "-F", "x"])]
    assert lines == [(1, "pid 42"), (2, "tail: warning"), (1, "first line"), (1, "last")]
    await client.close()
    server.close()
    short.cleanup()
