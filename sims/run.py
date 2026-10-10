"""Run one or more simulators in one process (one asyncio loop).

    python -m sims.run cisco-sim-1                 # full profile: one container per sim
    python -m sims.run cisco-sim-1 splunk-sim ...  # lite profile: everything in wt-sims

Each sim is started by name; cisco-sim-<n> is any switch in topology.yaml.
"""

from __future__ import annotations

import asyncio
import logging
import sys


async def main(names: list[str]) -> None:
    jobs = []
    for name in names:
        if name.startswith("cisco-sim-"):
            from sims.cisco_sim.server import serve

            jobs.append(serve(name))
        else:
            raise SystemExit(f"unknown sim {name!r} (splunk-sim, solarwinds-sim and forwarder arrive in phase 3)")
    await asyncio.gather(*jobs)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(message)s")
    asyncio.run(main(sys.argv[1:] or ["cisco-sim-1"]))
