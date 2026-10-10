"""Shared plumbing for the simulators: where they keep state (and the shared HTTP server helper)."""

from __future__ import annotations

import os
from pathlib import Path

from wt.common.config import REPO_ROOT
from wt.common.web import http_server  # noqa: F401  (re-exported for the sims)


def state_dir() -> Path:
    """SIM_STATE_DIR (a named volume in compose), or data/sims when run from the repo."""
    path = Path(os.environ.get("SIM_STATE_DIR", str(REPO_ROOT / "data" / "sims")))
    path.mkdir(parents=True, exist_ok=True)
    return path
