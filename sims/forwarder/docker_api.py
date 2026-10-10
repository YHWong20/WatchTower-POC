"""Just enough of the Docker Engine API (over the unix socket) to read logs inside the kind nodes:
list the node containers and stream the output of `docker exec`. No docker CLI in the image.

The forwarder only ever runs read commands this way (ls, tail, journalctl): it stands in for a
log agent on each node. WatchTower itself never gets the Docker socket.
"""

from __future__ import annotations

import asyncio
import json
import struct
from typing import AsyncIterator
from urllib.parse import quote

import httpx


class DockerClient:
    def __init__(self, socket_path: str = "/var/run/docker.sock"):
        self.socket_path = socket_path
        self.http = httpx.AsyncClient(transport=httpx.AsyncHTTPTransport(uds=socket_path),
                                      base_url="http://docker", timeout=10)

    async def close(self) -> None:
        await self.http.aclose()

    async def containers(self, label: str) -> list[dict]:
        """Running containers with a label, e.g. io.x-k8s.kind.cluster=ocp-sim."""
        resp = await self.http.get("/containers/json", params={"filters": json.dumps({"label": [label]})})
        resp.raise_for_status()
        return resp.json()

    async def exec_stream(self, container: str, cmd: list[str]) -> AsyncIterator[tuple[int, bytes]]:
        """Run cmd in the container and yield (stream, chunk): 1 = stdout, 2 = stderr.
        Ends when the command exits; closing the generator closes the connection, and the
        command dies on its next write (SIGPIPE)."""
        resp = await self.http.post(f"/containers/{quote(container)}/exec",
                                    json={"AttachStdout": True, "AttachStderr": True, "Tty": False, "Cmd": cmd})
        resp.raise_for_status()
        exec_id = resp.json()["Id"]
        reader, writer = await asyncio.open_unix_connection(self.socket_path, limit=2**20)
        try:
            body = json.dumps({"Detach": False, "Tty": False}).encode()
            writer.write(f"POST /exec/{exec_id}/start HTTP/1.1\r\nHost: docker\r\nContent-Type: application/json\r\n"
                         f"Connection: Upgrade\r\nUpgrade: tcp\r\nContent-Length: {len(body)}\r\n\r\n".encode() + body)
            await writer.drain()
            status = await reader.readline()
            if not (status.startswith(b"HTTP/1.1 101") or status.startswith(b"HTTP/1.1 200")):
                raise RuntimeError(f"docker exec start failed: {status.decode(errors='replace').strip()}")
            while (await reader.readline()) not in (b"\r\n", b"\n", b""):
                pass
            while True:
                try:
                    header = await reader.readexactly(8)
                except asyncio.IncompleteReadError:
                    return
                stream, size = header[0], struct.unpack(">I", header[4:])[0]
                yield stream, await reader.readexactly(size)
        finally:
            writer.close()

    async def exec_lines(self, container: str, cmd: list[str]) -> AsyncIterator[tuple[int, str]]:
        """exec_stream split into lines."""
        buffers = {1: b"", 2: b""}
        async for stream, chunk in self.exec_stream(container, cmd):
            buffers[stream] = buffers.get(stream, b"") + chunk
            *lines, buffers[stream] = buffers[stream].split(b"\n")
            for line in lines:
                yield stream, line.decode("utf-8", "replace")
        for stream, rest in buffers.items():
            if rest:
                yield stream, rest.decode("utf-8", "replace")

    async def exec_output(self, container: str, cmd: list[str], timeout_s: float = 10) -> str:
        async def collect() -> str:
            return "\n".join([line async for stream, line in self.exec_lines(container, cmd) if stream == 1])
        return await asyncio.wait_for(collect(), timeout_s)
