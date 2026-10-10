import asyncio
import inspect
import json

import asyncssh
import httpx
import pytest

from wt.common.probes.base import ProbeRefused, ProbeThrottled, Throttle
from wt.common.probes.k8s import K8sProbe
from wt.common.probes.ssh_cli import SshCli, check_command

from .conftest import fixture_json, fixture_text

QUEUE_CMD = "show platform hardware fed switch active qos queue stats interface Gi1/0/13"


# ---------------------------------------------------------------- CLI allowlist

@pytest.mark.parametrize("cmd", [
    "show version",
    "show interfaces Gi1/0/13",
    QUEUE_CMD,
    "show interfaces Gi1/0/10, Gi1/0/12 | include drops",
    "show logging | begin LINK",
])
def test_allowlist_accepts_read_only_show(cmd):
    check_command(cmd, "^show ")


@pytest.mark.parametrize("cmd", [
    "conf t",
    "configure terminal",
    "reload",
    "clear counters",
    "debug all",
    "Show version",                         # IOS would accept it; the allowlist is exact
    " show version",                         # leading space is stripped by SshCli.run, not here
    "show version\nconf t",                  # second command smuggled on a new line
    "show version\r\nreload",
    "show running-config | redirect flash:x.txt",
    "show running-config | tee flash:x.txt",
    "show tech | append tftp://h/x",
    "show version |",
])
def test_allowlist_refuses_everything_else(cmd):
    with pytest.raises(ProbeRefused):
        check_command(cmd, "^show ")


async def test_refused_command_is_audited_and_never_sent(sources, audit, store, monkeypatch):
    async def boom(*a, **k):
        raise AssertionError("must not connect")

    monkeypatch.setattr(asyncssh, "connect", boom)
    cli = SshCli(sources, audit, "wt", "pw")
    with pytest.raises(ProbeRefused):
        await cli.run("cisco-sim-1", "conf t")
    row = store.audit_rows()[0]
    assert (row["kind"], row["target"], row["input"], row["ok"]) == ("refused", "cisco-sim-1", "conf t", 0)


# ---------------------------------------------------------------- CLI over a real SSH session (localhost)

class _FakeSwitch(asyncssh.SSHServer):
    def begin_auth(self, username):
        return True

    def password_auth_supported(self):
        return True

    def validate_password(self, username, password):
        return (username, password) == ("wt", "pw")


@pytest.fixture
async def fake_switch():
    """An SSH server on 127.0.0.1 that answers show commands from fixtures, like cisco-sim will."""
    received = []
    outputs = {QUEUE_CMD: fixture_text("cli/show_qos_queue_stats_gi1-0-13.txt")}

    async def handle(process: asyncssh.SSHServerProcess):
        received.append(process.command)
        process.stdout.write(outputs.get(process.command, "% Invalid input detected at '^' marker.\n"))
        process.exit(0)

    server = await asyncssh.create_server(
        _FakeSwitch, "127.0.0.1", 0,
        server_host_keys=[asyncssh.generate_private_key("ssh-ed25519")],
        process_factory=handle,
    )
    port = server.sockets[0].getsockname()[1]
    yield port, received
    server.close()
    await server.wait_closed()


async def test_ssh_probe_runs_show_command(sources, audit, store, fake_switch):
    port, received = fake_switch
    cli = SshCli(sources, audit, "wt", "pw")
    res = await cli.run(f"127.0.0.1:{port}", QUEUE_CMD)
    assert received == [QUEUE_CMD]
    assert res.tool == "probe_cli" and res.query == QUEUE_CMD
    assert "    1           0           0       18422" in res.output
    row = store.audit_rows("probe")[0]
    assert (row["target"], row["input"], row["ok"]) == ("127.0.0.1", QUEUE_CMD, 1)


async def test_ssh_probe_throttles_per_device(sources, audit, fake_switch):
    port, received = fake_switch
    cli = SshCli(sources, audit, "wt", "pw")             # 30 s minimum interval from sources.yaml
    await cli.run(f"127.0.0.1:{port}", QUEUE_CMD)
    with pytest.raises(ProbeThrottled):
        await cli.run(f"127.0.0.1:{port}", QUEUE_CMD)
    assert len(received) == 1


def test_throttle_is_per_device():
    t = Throttle(30)
    t.check("cisco-sim-1")
    t.check("cisco-sim-2")
    with pytest.raises(ProbeThrottled):
        t.check("cisco-sim-1")


# ---------------------------------------------------------------- Kubernetes (read-only)

def _k8s(handler, audit) -> K8sProbe:
    def only_get(req: httpx.Request) -> httpx.Response:
        assert req.method == "GET", f"k8s probe sent {req.method}"
        return handler(req)

    return K8sProbe(httpx.AsyncClient(base_url="https://ocp-sim:6443", transport=httpx.MockTransport(only_get)), audit)


async def test_k8s_get_node_boot_id(audit, store):
    probe = _k8s(lambda r: httpx.Response(200, json=fixture_json("k8s/node_ocp-sim-worker.json")), audit)
    res = await probe.get_node("ocp-sim-worker")
    await probe.close()
    assert res.query == "GET /api/v1/nodes/ocp-sim-worker"
    assert res.data["status"]["nodeInfo"]["bootID"].startswith("4c1f6a2e")
    assert store.audit_rows("probe")[0]["input"] == "GET /api/v1/nodes/ocp-sim-worker"


async def test_k8s_lease_and_pods(audit):
    seen = []

    def handler(req):
        seen.append(str(req.url))
        if "leases" in req.url.path:
            return httpx.Response(200, json=fixture_json("k8s/lease_ocp-sim-worker.json"))
        return httpx.Response(200, json={"kind": "PodList", "items": []})

    probe = _k8s(handler, audit)
    lease = await probe.get_lease("ocp-sim-worker")
    pods = await probe.list_pods(node="ocp-sim-worker", namespace="kube-system")
    await probe.close()
    assert lease.data["spec"]["renewTime"].startswith("2026-10-10T09:22:05")
    assert pods.query == "GET /api/v1/namespaces/kube-system/pods?fieldSelector=spec.nodeName=ocp-sim-worker"
    assert seen[1].endswith("fieldSelector=spec.nodeName%3Docp-sim-worker")


async def test_k8s_refuses_bad_names(audit):
    probe = _k8s(lambda r: httpx.Response(200, json={}), audit)
    for bad in ["../../apis", "ocp-sim-worker/proxy", "Node1", ""]:
        with pytest.raises(ProbeRefused):
            await probe.get_node(bad)
    with pytest.raises(ProbeRefused):
        await probe.list_events(field_selector="a b")
    await probe.close()


def test_k8s_probe_has_no_write_methods():
    public = [n for n, _ in inspect.getmembers(K8sProbe, inspect.isfunction) if not n.startswith("_")]
    assert all(n.startswith(("get_", "list_")) or n in ("from_kubeconfig", "close") for n in public), public
    src = inspect.getsource(K8sProbe)
    for verb in ("post(", "put(", "patch(", "delete(", ".request("):
        assert verb not in src
