"""uvicorn servers that share one asyncio loop (several agents or sims in one process)."""

from __future__ import annotations

import contextlib

import uvicorn


class _Server(uvicorn.Server):
    """uvicorn without its own signal handling: the process runner (wt/run.py, sims/run.py)
    stops every server together on SIGTERM."""

    @contextlib.contextmanager
    def capture_signals(self):
        yield


SERVERS: list[uvicorn.Server] = []          # every server in this process, for stop_on_signals


def http_server(app, port: int, host: str = "0.0.0.0") -> uvicorn.Server:
    server = _Server(uvicorn.Config(app, host=host, port=port, log_level="warning"))
    SERVERS.append(server)
    return server


def stop_on_signals(task, servers: list[uvicorn.Server] | None = None, grace_s: float = 2.0) -> None:
    """On SIGTERM/SIGINT ask every server to exit (clean lifespan shutdown), then cancel the rest."""
    import asyncio
    import signal

    loop = asyncio.get_running_loop()

    def stop() -> None:
        for s in servers if servers is not None else SERVERS:
            s.should_exit = True
        loop.call_later(grace_s, task.cancel)

    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, stop)
