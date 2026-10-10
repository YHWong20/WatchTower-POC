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


def http_server(app, port: int, host: str = "0.0.0.0") -> uvicorn.Server:
    return _Server(uvicorn.Config(app, host=host, port=port, log_level="warning"))
