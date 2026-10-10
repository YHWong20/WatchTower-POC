"""solarwinds-sim: the SolarWinds Orion stand-in. Polls the Cisco sims over SNMP every 60 s and
serves SWIS so wt/common/sources/solarwinds.py works unchanged against Orion later.

    POST /SolarWinds/InformationService/v3/Json/Query   {"query": "SELECT ...", "parameters": {...}}
    GET  /SolarWinds/InformationService/v3/Json/Query?query=SELECT...
    Basic auth. Read-only: SWQL is SELECT only, and the CRUD endpoints do not exist.

Entities: Orion.Nodes, Orion.NPM.Interfaces, Orion.NPM.InterfaceTraffic, Orion.NPM.InterfaceErrors,
Orion.CPULoad, Orion.AlertActive, Orion.AlertHistory (see schema.py).

Environment:
  SW_PORT 17778   SW_USERS watchtower:watchtower,admin:demo   SW_POLL_S 60   SW_RETENTION_H 48
  SW_DB $SIM_STATE_DIR/solarwinds.db   SW_COMMUNITY public
  SW_TARGETS      override the topology's SNMP targets, e.g. cisco-sim-1=127.0.0.1:1611
"""

from __future__ import annotations

import asyncio
import logging
import os
import secrets
import time
from typing import Any

from fastapi import Depends, FastAPI, HTTPException, Request
from fastapi.responses import JSONResponse
from fastapi.security import HTTPBasic, HTTPBasicCredentials

from sims.common.state import http_server, state_dir
from sims.solarwinds_sim import swql
from sims.solarwinds_sim.poller import OrionDb, Poller, Target
from wt.common.catalog import load_topology

log = logging.getLogger("solarwinds-sim")
QUERY_PATH = "/SolarWinds/InformationService/v3/Json/Query"


def create_app(db: OrionDb, users: dict[str, str]) -> FastAPI:
    app = FastAPI(title="solarwinds-sim", docs_url=None, redoc_url=None, openapi_url=None)
    basic = HTTPBasic(auto_error=False)

    def auth(cred: HTTPBasicCredentials | None = Depends(basic)) -> str:
        if cred and cred.username in users and secrets.compare_digest(users[cred.username], cred.password):
            return cred.username
        raise HTTPException(401, detail="Authentication failed")

    @app.exception_handler(HTTPException)
    async def error(_request: Request, exc: HTTPException):
        return JSONResponse({"Message": exc.detail, "ExceptionType": "SolarWinds.InformationService.Contract2.InfoServiceFaultContract",
                             "FullException": exc.detail}, status_code=exc.status_code,
                            headers={"WWW-Authenticate": "Basic"} if exc.status_code == 401 else None)

    async def run(query: str, parameters: dict[str, Any] | None) -> dict:
        if not query.strip():
            raise HTTPException(400, detail="query is required")
        try:
            compiled = swql.compile_query(query, parameters or {})
            rows = await asyncio.to_thread(db.query, compiled)
        except swql.SwqlError as exc:
            raise HTTPException(400, detail=f"Query error: {exc}") from None
        return {"results": swql.shape(rows, compiled.columns)}

    @app.post(QUERY_PATH)
    async def query_post(request: Request, _user: str = Depends(auth)):
        try:
            body = await request.json()
        except ValueError:
            raise HTTPException(400, detail="Body must be JSON: {\"query\": ..., \"parameters\": {...}}") from None
        return await run(str(body.get("query", "")), body.get("parameters"))

    @app.get(QUERY_PATH)
    async def query_get(query: str = "", _user: str = Depends(auth)):
        return await run(query, None)

    @app.get("/sim/health")
    def health():
        """Not a SWIS endpoint: last poll time per node, for lab/health.sh."""
        rows = db.execute("SELECT Caption, Status, LastSystemUpTimePollUtc FROM Nodes ORDER BY NodeID")
        return {"now": time.time(), "nodes": [{"caption": c, "status": s, "last_poll": swql.from_db(p)}
                                              for c, s, p in rows]}

    return app


def targets_from_env() -> list[Target]:
    community = os.environ.get("SW_COMMUNITY", "public")
    overrides = dict(item.split("=", 1) for item in os.environ.get("SW_TARGETS", "").split(",") if "=" in item)
    out = []
    for name, spec in load_topology().switches.items():
        addr = overrides.get(name) or spec.mgmt.get("snmp", f"{name}:161")
        host, _, port = addr.rpartition(":")
        out.append(Target(name, host, int(port), community))
    return out


async def serve() -> None:
    db = OrionDb(os.environ.get("SW_DB", str(state_dir() / "solarwinds.db")))
    users = dict(u.split(":", 1) for u in os.environ.get("SW_USERS", "watchtower:watchtower,admin:demo").split(","))
    poller = Poller(db, targets_from_env(), float(os.environ.get("SW_POLL_S", "60")),
                    float(os.environ.get("SW_RETENTION_H", "48")))
    port = int(os.environ.get("SW_PORT", "17778"))
    log.info("solarwinds-sim up: SWIS :%d, polling %s every %.0f s", port,
             ", ".join(f"{t.name} ({t.host}:{t.port})" for t in poller.targets), poller.interval_s)
    await asyncio.gather(http_server(create_app(db, users), port).serve(), poller.loop())
