"""Read-only Cisco CLI over SSH (asyncssh).

Only commands matching the sources.yaml allowlist (`^show `) are sent. On top of that, a
command may not contain line breaks or control characters (which would smuggle a second
command), and may only pipe into IOS output filters: `| redirect`, `| tee` and `| append`
write files, so they are refused too. Every refusal is written to the audit log.
"""

from __future__ import annotations

import re

import asyncssh

from wt.common.audit import Audit
from wt.common.catalog import Sources
from wt.common.probes.base import ProbeRefused, ProbeResult, Throttle

_SAFE_PIPES = {"include", "exclude", "begin", "section", "count", "i", "e", "b", "s"}
_CONTROL = re.compile(r"[\x00-\x1f\x7f]")


def check_command(command: str, allow: str) -> None:
    """Raise ProbeRefused unless the command is a single read-only show command."""
    if _CONTROL.search(command):
        raise ProbeRefused(f"refused: control characters in {command!r}")
    if not re.match(allow, command):
        raise ProbeRefused(f"refused: {command!r} does not match the allowlist {allow!r}")
    for segment in command.split("|")[1:]:
        words = segment.split()
        if not words or words[0].lower() not in _SAFE_PIPES:
            raise ProbeRefused(f"refused: pipe {segment.strip()!r} is not an output filter")


class SshCli:
    """One instance serves every switch in the domain (domain agents, not device agents)."""

    def __init__(
        self,
        sources: Sources,
        audit: Audit,
        username: str,
        password: str,
        actor: str = "network-agent",
        connect_timeout_s: float = 10.0,
        command_timeout_s: float = 20.0,
        known_hosts: str | None = None,
        throttle: Throttle | None = None,
    ):
        self.allow = sources.cli_allow
        self.audit = audit
        self.username = username
        self.password = password
        self.actor = actor
        self.connect_timeout_s = connect_timeout_s
        self.command_timeout_s = command_timeout_s
        self.known_hosts = known_hosts
        self.throttle = throttle or Throttle(sources.probe_policy.min_interval_per_device_s)

    async def run(self, host: str, command: str, port: int = 22) -> ProbeResult:
        """Run one show command on `host` ("cisco-sim-1" or "cisco-sim-1:22")."""
        if ":" in host:
            host, port_s = host.rsplit(":", 1)
            port = int(port_s)
        command = command.strip()
        try:
            check_command(command, self.allow)
        except ProbeRefused as exc:
            self.audit.record("refused", self.actor, host, command, ok=False, detail=str(exc))
            raise
        self.throttle.check(host)

        async with self.audit.timed("probe", self.actor, host, command) as rec:
            async with asyncssh.connect(
                host,
                port=port,
                username=self.username,
                password=self.password,
                known_hosts=self.known_hosts,  # PoC sims regenerate host keys; pin them in production
                connect_timeout=self.connect_timeout_s,
            ) as conn:
                proc = await conn.run(command, check=False, timeout=self.command_timeout_s)
            output = str(proc.stdout or "")
            rec.detail = output[:500]
        return ProbeResult(
            tool="probe_cli",
            target=host,
            query=command,
            output=output,
            duration_ms=rec.duration_ms,
            audit_id=rec.audit_id,
        )
