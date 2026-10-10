"""forwarder: the stand-in for Splunk forwarding. Ships to splunk-sim over HEC:

- Kubernetes events: list + watch on the API with its own read-only token (events only).
- Container logs from kube-system and wt-lab (API server, etcd, svc-prober, ...): `tail -F` of
  /var/log/containers/*.log inside each kind node, the way a node log agent reads them.
- kubelet journal from each kind node: `journalctl -u kubelet -f -o json`.
- Switch syslog: UDP :5514.
Reading inside the nodes goes through `docker exec`, not the node network, so the fault's netem
does not delay the logs (a real node agent would share the congested link; the lab keeps the
evidence path clean so the demo is about the incident, not about lost logs).

Environment:
  FWD_HEC_URL http://splunk:8088   FWD_HEC_TOKEN wt-hec-token   FWD_SYSLOG_PORT 5514
  FWD_KUBECONFIG /lab-data/kubeconfig-forwarder.yaml   FWD_KIND_CLUSTER ocp-sim
  FWD_NAMESPACES kube-system,wt-lab   FWD_DOCKER_SOCK /var/run/docker.sock
"""

from __future__ import annotations

import asyncio
import base64
import json
import logging
import os
import ssl
import time
from collections import OrderedDict
from pathlib import Path
from typing import Any

import httpx
import yaml

from sims.common.state import state_dir
from sims.forwarder import parse
from sims.forwarder.docker_api import DockerClient
from sims.forwarder.hec import Checkpoints, HecSender

log = logging.getLogger("forwarder")


# ---------------------------------------------------------------- Kubernetes events


class KubeEvents:
    def __init__(self, hec: HecSender, cp: Checkpoints, kubeconfig: Path):
        self.hec, self.cp, self.kubeconfig = hec, cp, kubeconfig
        self.seen: OrderedDict[str, str] = OrderedDict()   # uid -> resourceVersion already shipped

    def client(self) -> httpx.AsyncClient:
        cfg = yaml.safe_load(self.kubeconfig.read_text())
        cluster = cfg["clusters"][0]["cluster"]
        user = cfg["users"][0]["user"]
        ctx = ssl.create_default_context(cadata=base64.b64decode(cluster["certificate-authority-data"]).decode())
        return httpx.AsyncClient(base_url=cluster["server"], verify=ctx,
                                 headers={"Authorization": f"Bearer {user['token']}"},
                                 timeout=httpx.Timeout(10, read=330))

    def ship(self, obj: dict[str, Any], listed: bool = False) -> None:
        """Send one event. After a restart the initial list skips what was shipped before
        (older than the checkpoint); within a run, uid + resourceVersion removes repeats."""
        meta = obj.get("metadata") or {}
        uid, rv = meta.get("uid", ""), meta.get("resourceVersion", "")
        if self.seen.get(uid) == rv:
            return
        self.seen[uid] = rv
        self.seen.move_to_end(uid)
        while len(self.seen) > 20000:
            self.seen.popitem(last=False)
        ev = parse.kube_event(obj)
        t = ev["time"]
        if listed and t < self.cp.get("kube_events_since", 0) - 1:
            return
        self.hec.send(ev, lambda: self.cp.set("kube_events_since", max(t, self.cp.get("kube_events_since", 0))))

    async def run(self) -> None:
        while not self.kubeconfig.exists():
            log.info("waiting for %s (make lab-up writes it)", self.kubeconfig)
            await asyncio.sleep(15)
        while True:
            try:
                async with self.client() as http:
                    rv = await self.list(http)
                    await self.watch(http, rv)
            except (httpx.HTTPError, OSError, KeyError, ValueError) as exc:
                log.warning("kube events: %s: %s; retrying in 10 s", type(exc).__name__, exc)
                await asyncio.sleep(10)

    async def list(self, http: httpx.AsyncClient) -> str:
        cont, n = None, 0
        while True:
            resp = await http.get("/api/v1/events", params={"limit": 500, **({"continue": cont} if cont else {})})
            resp.raise_for_status()
            body = resp.json()
            for item in body.get("items", []):
                self.ship(item, listed=True)
                n += 1
            cont = body["metadata"].get("continue")
            if not cont:
                rv = body["metadata"]["resourceVersion"]
                self.cp.set("kube_events_rv", rv)
                log.info("kube events: listed %d, watching from resourceVersion %s", n, rv)
                return rv

    async def watch(self, http: httpx.AsyncClient, rv: str) -> None:
        while True:
            params = {"watch": "1", "resourceVersion": rv, "allowWatchBookmarks": "true", "timeoutSeconds": "300"}
            async with http.stream("GET", "/api/v1/events", params=params) as resp:
                resp.raise_for_status()
                async for line in resp.aiter_lines():
                    if not line.strip():
                        continue
                    msg = json.loads(line)
                    obj = msg.get("object") or {}
                    if msg.get("type") == "ERROR":
                        if obj.get("code") == 410:                  # too old: relist
                            log.info("kube events: watch expired, relisting")
                            return
                        raise ValueError(f"watch error: {obj.get('message')}")
                    rv = (obj.get("metadata") or {}).get("resourceVersion", rv)
                    if msg.get("type") in ("ADDED", "MODIFIED"):
                        self.ship(obj)
            self.cp.set("kube_events_rv", rv)


# ---------------------------------------------------------------- node logs (docker exec)


class NodeLogs:
    """Per kind node: kubelet journal, and one `tail -F` per container log file in the namespaces."""

    def __init__(self, hec: HecSender, cp: Checkpoints, docker: DockerClient, cluster: str, namespaces: set[str]):
        self.hec, self.cp, self.docker = hec, cp, docker
        self.label = f"io.x-k8s.kind.cluster={cluster}"
        self.namespaces = namespaces
        self.nodes: dict[str, list[asyncio.Task]] = {}           # container id -> its tasks

    async def run(self) -> None:
        while True:
            try:
                found = {c["Id"]: c["Names"][0].lstrip("/") for c in await self.docker.containers(self.label)}
            except (httpx.HTTPError, OSError) as exc:
                log.warning("docker: %s (is /var/run/docker.sock mounted?)", exc)
                found = None
            if found is not None:
                for cid in set(self.nodes) - set(found):
                    for t in self.nodes.pop(cid):
                        t.cancel()
                for cid, name in found.items():
                    if cid not in self.nodes:
                        log.info("node %s: shipping kubelet journal and container logs", name)
                        self.nodes[cid] = [asyncio.create_task(self.journal(cid, name)),
                                           asyncio.create_task(self.files(cid, name))]
            await asyncio.sleep(30)

    async def journal(self, cid: str, node: str) -> None:
        key = f"journal:{cid[:12]}"
        while True:
            cursor = self.cp.get(key)
            cmd = ["journalctl", "-u", "kubelet", "-f", "-o", "json", "--no-pager", "-n", "all"]
            if cursor:
                cmd.append(f"--after-cursor={cursor}")
            try:
                async for stream, line in self.docker.exec_lines(cid, cmd):
                    if stream != 1 or not line.startswith("{"):
                        if line.strip():
                            log.debug("journal %s: %s", node, line)
                        if stream == 2 and "cursor" in line.lower():
                            log.warning("journal %s: %s; reading from the start instead", node, line.strip())
                            self.cp.set(key, None)
                        continue
                    entry = json.loads(line)
                    ev = parse.journal_event(entry, node)
                    if ev:
                        cur = entry.get("__CURSOR")
                        self.hec.send(ev, (lambda c=cur: self.cp.set(key, c)) if cur else None)
            except (httpx.HTTPError, OSError, RuntimeError, ValueError) as exc:
                log.warning("journal %s: %s", node, exc)
            await asyncio.sleep(5)

    async def files(self, cid: str, node: str) -> None:
        tails: dict[str, asyncio.Task] = {}
        try:
            while True:
                try:
                    listing = await self.docker.exec_output(cid, ["ls", "-1", "/var/log/containers"])
                except (httpx.HTTPError, OSError, RuntimeError, TimeoutError) as exc:
                    log.warning("ls in %s: %s", node, exc)
                    listing = None
                if listing is not None:
                    wanted = {}
                    for name in listing.split():
                        meta = parse.container_log_meta(name)
                        if meta and meta["namespace"] in self.namespaces:
                            wanted[name] = meta
                    for name in set(tails) - set(wanted):
                        tails.pop(name).cancel()
                    for name, meta in wanted.items():
                        if name not in tails or tails[name].done():
                            tails[name] = asyncio.create_task(self.tail(cid, node, name, meta))
                await asyncio.sleep(15)
        finally:
            for t in tails.values():
                t.cancel()

    async def tail(self, cid: str, node: str, filename: str, meta: dict[str, str]) -> None:
        """Ship one container log from the start, skipping lines at or before the checkpoint."""
        key = f"log:{cid[:12]}:{filename}"
        path = f"/var/log/containers/{filename}"
        pid = None
        partial: list[str] = []
        try:
            cmd = ["sh", "-c", 'echo "pid $$"; exec tail -n +1 -F "$1"', "sh", path]
            async for stream, line in self.docker.exec_lines(cid, cmd):
                if stream != 1:
                    continue
                if pid is None and line.startswith("pid "):
                    pid = line[4:].strip()
                    continue
                rec = parse.parse_cri(line)
                if rec is None:
                    continue
                ns, _stream, full, message = rec
                if ns <= self.cp.get(key, 0):
                    continue
                if not full:
                    partial.append(message)
                    continue
                if partial:
                    message, partial = "".join(partial) + message, []
                self.hec.send(parse.container_event(node, filename, meta, ns, message),
                              lambda n=ns: self.cp.set(key, max(n, self.cp.get(key, 0))))
        except asyncio.CancelledError:
            if pid:                                       # the file is gone; stop its tail in the node
                try:
                    await self.docker.exec_output(cid, ["kill", pid], timeout_s=5)
                except Exception:
                    pass
            raise
        except (httpx.HTTPError, OSError, RuntimeError) as exc:
            log.warning("tail %s on %s: %s", filename, node, exc)
            await asyncio.sleep(5)


# ---------------------------------------------------------------- syslog


class _Syslog(asyncio.DatagramProtocol):
    def __init__(self, hec: HecSender):
        self.hec = hec

    def datagram_received(self, data: bytes, addr) -> None:
        ev = parse.syslog_event(data, addr[0], time.time())
        if ev:
            self.hec.send(ev)


async def syslog(hec: HecSender, port: int) -> None:
    loop = asyncio.get_running_loop()
    await loop.create_datagram_endpoint(lambda: _Syslog(hec), local_addr=("0.0.0.0", port))
    log.info("syslog: listening on :%d/udp", port)
    await asyncio.Event().wait()


# ---------------------------------------------------------------- main


async def serve() -> None:
    cp = Checkpoints(state_dir() / "forwarder-checkpoints.json")
    hec = HecSender(os.environ.get("FWD_HEC_URL", "http://splunk:8088"), os.environ.get("FWD_HEC_TOKEN", "wt-hec-token"))
    docker = DockerClient(os.environ.get("FWD_DOCKER_SOCK", "/var/run/docker.sock"))
    namespaces = {n.strip() for n in os.environ.get("FWD_NAMESPACES", "kube-system,wt-lab").split(",") if n.strip()}
    events = KubeEvents(hec, cp, Path(os.environ.get("FWD_KUBECONFIG", "/lab-data/kubeconfig-forwarder.yaml")))
    nodes = NodeLogs(hec, cp, docker, os.environ.get("FWD_KIND_CLUSTER", "ocp-sim"), namespaces)
    log.info("forwarder up: HEC %s, namespaces %s", hec.http.base_url, ", ".join(sorted(namespaces)))
    await asyncio.gather(hec.run(), hec.report(), cp.loop(), events.run(), nodes.run(),
                         syslog(hec, int(os.environ.get("FWD_SYSLOG_PORT", "5514"))))
