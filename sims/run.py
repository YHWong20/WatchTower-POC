"""Run one or more simulators in one process (one asyncio loop).

    python -m sims.run cisco-sim-1                 # full profile: one container per sim
    python -m sims.run cisco-sim-1 splunk-sim ...  # lite profile: everything in wt-sims

Each sim is started by name; cisco-sim-<n> is any switch in topology.yaml.
"""

from __future__ import annotations

import asyncio
import logging
import signal
import sys


async def main(names: list[str]) -> None:
    jobs = []
    for name in names:
        if name.startswith("cisco-sim-"):
            from sims.cisco_sim.server import serve

            jobs.append(serve(name))
        elif name == "splunk-sim":
            from sims.splunk_sim.server import serve as splunk

            jobs.append(splunk())
        elif name == "solarwinds-sim":
            from sims.solarwinds_sim.server import serve as solarwinds

            jobs.append(solarwinds())
        elif name == "forwarder":
            from sims.forwarder.server import serve as forwarder

            jobs.append(forwarder())
        else:
            raise SystemExit(f"unknown sim {name!r}: cisco-sim-<n>, splunk-sim, solarwinds-sim or forwarder")
    task = asyncio.ensure_future(asyncio.gather(*jobs))
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, task.cancel)       # one stop for every server in the process
    try:
        await task
    except asyncio.CancelledError:
        logging.getLogger("sims").info("stopped")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(message)s")
    logging.getLogger("httpx").setLevel(logging.WARNING)
    asyncio.run(main(sys.argv[1:] or ["cisco-sim-1"]))
