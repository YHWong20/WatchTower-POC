"""Shared plumbing for the simulators: where they keep state, and HTTP servers that share a loop."""

from __future__ import annotations

import contextlib
import os
from pathlib import Path

import uvicorn

from wt.common.config import REPO_ROOT


def state_dir() -> Path:
    """SIM_STATE_DIR (a named volume in compose), or data/sims when run from the repo."""
    path = Path(os.environ.get("SIM_STATE_DIR", str(REPO_ROOT / "data" / "sims")))
    path.mkdir(parents=True, exist_ok=True)
    return path


class _Server(uvicorn.Server):
    """uvicorn without its own signal handling: several servers share one loop in wt-sims, and
    sims/run.py stops them all together on SIGTERM."""

    @contextlib.contextmanager
    def capture_signals(self):
        yield


def http_server(app, port: int) -> uvicorn.Server:
    return _Server(uvicorn.Config(app, host="0.0.0.0", port=port, log_level="warning"))
