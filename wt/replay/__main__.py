"""python -m wt.replay record NAME [--duration S]
python -m wt.replay play NAME [--speed 10] [--port 8080] [--redis redis://127.0.0.1:6390/0]
python -m wt.replay list"""

from __future__ import annotations

import argparse
import asyncio
import dataclasses
import json
import logging
import signal
import socket

from wt.replay.recorder import RECORDINGS, Recorder


async def play(args) -> None:
    from wt.common.bus import Bus
    from wt.common.config import load_settings
    from wt.common.store import Store
    from wt.common.web import http_server
    from wt.copilot.app import Copilot, create_app
    from wt.replay.player import Player, recorded_label

    with socket.socket() as s:
        if s.connect_ex(("127.0.0.1", args.port)) == 0:
            raise SystemExit(f"port {args.port} is in use: stop the lab first (make down), or PORT=8090 make replay")
    db = RECORDINGS.parent.parent / "data" / "replay" / f"{args.name}.db"
    db.parent.mkdir(parents=True, exist_ok=True)
    for suffix in ("", "-wal", "-shm"):
        db.with_name(db.name + suffix).unlink(missing_ok=True)
    store = Store(db)
    bus = Bus.from_url(args.redis)
    try:
        await bus.r.ping()
    except Exception as exc:
        raise SystemExit(f"replay Redis not reachable at {args.redis} ({exc}); make replay starts one") from None
    player = Player(args.name, store, bus, speed=args.speed)
    await player.fresh()
    store.kv_put("replay:now", player.start)
    settings = dataclasses.replace(load_settings(), llm_enabled=False, db_path=db)
    replay = {"name": args.name, "speed": args.speed or "end state", "recorded": recorded_label(player.meta)}
    app = create_app(settings, bus, Copilot(settings, bus, store=store, llm=None, agent_urls=[], replay=replay))
    print(f"replaying {args.name}: {len(player.events)} events, {recorded_label(player.meta)}, "
          f"{(player.end - player.start) / 60:.1f} min at {args.speed or 'instant'}x -> http://localhost:{args.port}",
          flush=True)
    server = http_server(app, args.port, host="127.0.0.1")
    jobs = [asyncio.create_task(player.play()), asyncio.create_task(player.project()),
            asyncio.create_task(server.serve())]

    async def announce() -> None:
        await player.done.wait()
        print("replay finished: the Copilot shows the end state (Ctrl-C to stop)", flush=True)
    jobs.append(asyncio.create_task(announce()))
    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, stop.set)
    await stop.wait()
    server.should_exit = True                       # a clean uvicorn shutdown, then the rest
    await asyncio.wait([jobs[2]], timeout=3)
    for j in jobs:
        j.cancel()
    await asyncio.gather(*jobs, return_exceptions=True)
    await bus.close()


def main() -> None:
    logging.basicConfig(level=logging.WARNING, format="%(asctime)s %(name)s %(message)s")
    ap = argparse.ArgumentParser(prog="python -m wt.replay")
    sub = ap.add_subparsers(dest="cmd", required=True)
    r = sub.add_parser("record")
    r.add_argument("name")
    r.add_argument("--duration", type=float, default=None)
    p = sub.add_parser("play")
    p.add_argument("name")
    p.add_argument("--speed", type=float, default=10.0)
    p.add_argument("--port", type=int, default=8080)
    p.add_argument("--redis", default="redis://127.0.0.1:6390/0")
    sub.add_parser("list")
    args = ap.parse_args()
    if args.cmd == "record":
        asyncio.run(Recorder(args.name).run(args.duration))
    elif args.cmd == "play":
        asyncio.run(play(args))
    else:
        for d in sorted(RECORDINGS.glob("*/meta.json")):
            m = json.loads(d.read_text())
            print(f"{m['name']:<24} {sum(m.get('events', {}).values()):>6} events  {m.get('profile', '')}")


if __name__ == "__main__":
    main()
