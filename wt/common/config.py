"""Runtime settings from the environment, and the lab profile (lite or full).

Agent code must not care which profile is running: only compose, topology selection and the
fault target change. So the profile is resolved here once and everything else asks
`settings()` for the topology filter and the model, never for the profile name.
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from functools import cache
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]


@dataclass(frozen=True)
class Profile:
    name: str
    kind_config: str
    nodes: tuple[str, ...] | None      # display names kept from topology.yaml; None = all
    switches: tuple[str, ...] | None   # switches kept from topology.yaml; None = all
    model: str                         # default WT_MODEL
    fault_node: str                    # kind node the demo fault targets


PROFILES = {
    # 8 GB MacBook Air: 1 master + 1 worker, cisco-sim-1 only, gemma3:1b (2k context).
    "lite": Profile(
        name="lite",
        kind_config="lab/kind-config-lite.yaml",
        nodes=("master-1", "worker-1"),
        switches=("cisco-sim-1",),
        model="gemma3:1b",
        fault_node="ocp-sim-worker",
    ),
    # 16 GB machine: 3 masters + 1 worker, both switches, gemma3:4b.
    "full": Profile(
        name="full",
        kind_config="lab/kind-config.yaml",
        nodes=None,
        switches=None,
        model="gemma3:4b",
        fault_node="ocp-sim-control-plane2",
    ),
}


@dataclass(frozen=True)
class Settings:
    profile: Profile
    contracts_dir: Path
    playbooks_dir: Path
    db_path: Path
    redis_url: str
    llm_enabled: bool
    llm_base_url: str
    llm_model: str
    llm_timeout_s: float
    splunk_url: str
    splunk_token: str
    solarwinds_url: str
    solarwinds_user: str
    solarwinds_password: str
    verify_tls: bool
    ssh_user: str
    ssh_password: str
    kubeconfig: str | None


def _env(name: str, default: str) -> str:
    return os.environ.get(name, default)


def _flag(name: str, default: str) -> bool:
    return _env(name, default).strip().lower() not in ("0", "off", "false", "no", "")


def load_settings() -> Settings:
    profile_name = _env("WT_PROFILE", "lite")
    if profile_name not in PROFILES:
        raise ValueError(f"WT_PROFILE must be one of {sorted(PROFILES)}, got {profile_name!r}")
    profile = PROFILES[profile_name]
    return Settings(
        profile=profile,
        contracts_dir=Path(_env("WT_CONTRACTS_DIR", str(REPO_ROOT / "contracts"))),
        playbooks_dir=Path(_env("WT_PLAYBOOKS_DIR", str(REPO_ROOT / "playbooks"))),
        db_path=Path(_env("WT_DB_PATH", str(REPO_ROOT / "data" / "watchtower.db"))),
        redis_url=_env("WT_REDIS_URL", "redis://redis:6379/0"),
        llm_enabled=_flag("WT_LLM", "on"),
        llm_base_url=_env("WT_LLM_BASE_URL", "http://host.docker.internal:11434/v1"),
        llm_model=_env("WT_MODEL", profile.model),
        llm_timeout_s=float(_env("WT_LLM_TIMEOUT_S", "60")),
        splunk_url=_env("WT_SPLUNK_URL", "http://splunk:8089"),
        splunk_token=_env("WT_SPLUNK_TOKEN", "wt-poc-token"),
        solarwinds_url=_env("WT_SOLARWINDS_URL", "http://solarwinds:17778"),
        solarwinds_user=_env("WT_SOLARWINDS_USER", "watchtower"),
        solarwinds_password=_env("WT_SOLARWINDS_PASSWORD", "watchtower"),
        verify_tls=_flag("WT_VERIFY_TLS", "off"),
        ssh_user=_env("WT_SSH_USER", "watchtower"),
        ssh_password=_env("WT_SSH_PASSWORD", "watchtower"),
        kubeconfig=os.environ.get("WT_KUBECONFIG"),
    )


@cache
def settings() -> Settings:
    return load_settings()
