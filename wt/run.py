"""Run one or more WatchTower agents in one process (one asyncio loop).

    python -m wt.run network-agent                      # full profile: one container per agent
    python -m wt.run network-agent openshift-agent parent-agent copilot   # lite: all in wt-agents

Ports (each agent's /health and /signals): network-agent 8101, openshift-agent 8102,
parent-agent 8103, copilot 8080.
"""

from __future__ import annotations

import asyncio
import logging
import sys

from wt.common.config import settings

PORTS = {"network-agent": 8101, "openshift-agent": 8102, "parent-agent": 8103, "copilot": 8080}


async def main(names: list[str]) -> None:
    from wt.common.bus import Bus
    from wt.common.web import http_server, stop_on_signals

    cfg = settings()
    bus = Bus.from_url(cfg.redis_url)
    jobs = []
    servers = []

    def serve(app, port):
        servers.append(http_server(app, port))
        return servers[-1].serve()
    for name in names:
        if name == "network-agent":
            from wt.agents.base import AgentContext
            from wt.agents.network.agent import NetworkAgent

            agent = NetworkAgent(AgentContext.build(cfg, name, bus))
            jobs += [agent.watch_loop(), agent.task_loop(), serve(agent.app(), PORTS[name])]
        elif name == "openshift-agent":
            from wt.agents.base import AgentContext
            from wt.agents.openshift.agent import OpenShiftAgent

            agent = OpenShiftAgent(AgentContext.build(cfg, name, bus))
            jobs += [agent.watch_loop(), agent.task_loop(), serve(agent.app(), PORTS[name])]
        elif name == "parent-agent":
            from wt.agents.parent.agent import ParentAgent

            parent = ParentAgent.build(cfg, bus)
            jobs += [parent.run(), serve(parent.app(), PORTS[name])]
        elif name == "copilot":
            from wt.copilot.app import create_app

            jobs.append(serve(create_app(cfg, bus), PORTS[name]))
        else:
            raise SystemExit(f"unknown agent {name!r}: {', '.join(PORTS)}")
    logging.getLogger("wt").info("WatchTower %s up (profile %s, LLM %s)", ", ".join(names), cfg.profile.name,
                                 f"{cfg.llm_model} at {cfg.llm_base_url}" if cfg.llm_enabled else "off")
    task = asyncio.ensure_future(asyncio.gather(*jobs))
    stop_on_signals(task, servers)
    try:
        await task
    except asyncio.CancelledError:
        logging.getLogger("wt").info("stopped")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(message)s")
    for noisy in ("httpx", "asyncssh"):
        logging.getLogger(noisy).setLevel(logging.WARNING)
    asyncio.run(main(sys.argv[1:] or list(PORTS)))
