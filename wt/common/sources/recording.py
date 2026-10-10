"""Record and replay the agents' Splunk and SolarWinds traffic (httpx transports).

Record (live lab): WT_RECORD_SOURCES=data/recordings/run.jsonl appends one line per request:
    {"at": epoch, "host": "splunk", "path": ..., "query": <SPL or SWQL>, "status": 200, "body": <response text>}
Replay (tests, eval): ReplayTransport serves the recorded responses by (path, query). When the
same query was recorded several times (a watch query every cycle), replies come back in order,
and the last one repeats; `at` lets a test set its clock to the recording time.
"""

from __future__ import annotations

import json
import threading
import time
from collections import defaultdict
from pathlib import Path
from urllib.parse import parse_qs

import httpx


def request_query(request: httpx.Request) -> str:
    """The SPL (form field `search`) or SWQL (JSON field `query`) a request carries."""
    body = request.content.decode("utf-8", "replace") if request.content else ""
    if request.url.path.endswith("/jobs/export"):
        return parse_qs(body).get("search", [""])[-1].removeprefix("search ").strip()
    if request.url.path.endswith("/Json/Query"):
        try:
            return json.loads(body).get("query", "").strip()
        except ValueError:
            return body
    return request.url.path + (f"?{request.url.query.decode()}" if request.url.query else "")


class RecordingTransport(httpx.AsyncBaseTransport):
    def __init__(self, path: str | Path, inner: httpx.AsyncBaseTransport | None = None, tool: str = ""):
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.inner = inner or httpx.AsyncHTTPTransport()
        self.tool = tool
        self.lock = threading.Lock()

    async def handle_async_request(self, request: httpx.Request) -> httpx.Response:
        response = await self.inner.handle_async_request(request)
        body = await response.aread()
        line = {"at": round(time.time(), 3), "tool": self.tool, "path": request.url.path,
                "query": request_query(request), "status": response.status_code,
                "body": body.decode("utf-8", "replace")}
        with self.lock, self.path.open("a") as f:
            f.write(json.dumps(line) + "\n")
        return httpx.Response(response.status_code, headers=response.headers, content=body, request=request)

    async def aclose(self) -> None:
        await self.inner.aclose()


class ReplayTransport(httpx.AsyncBaseTransport):
    def __init__(self, records: list[dict], strict: bool = True):
        self.by_key: dict[tuple[str, str], list[dict]] = defaultdict(list)
        for r in records:
            self.by_key[(r["path"], r["query"])].append(r)
        self.cursor: dict[tuple[str, str], int] = defaultdict(int)
        self.strict = strict
        self.misses: list[str] = []

    @classmethod
    def from_file(cls, path: str | Path, tool: str | None = None, **kw) -> ReplayTransport:
        records = [json.loads(line) for line in Path(path).read_text().splitlines() if line.strip()]
        return cls([r for r in records if tool is None or r.get("tool") == tool], **kw)

    async def handle_async_request(self, request: httpx.Request) -> httpx.Response:
        key = (request.url.path, request_query(request))
        recs = self.by_key.get(key)
        if not recs:
            self.misses.append(key[1])
            if self.strict:
                return httpx.Response(599, text=f"no recording for {key[1]}", request=request)
            empty = '{"results": []}' if key[0].endswith("/Json/Query") else '{"preview":false,"lastrow":true}'
            return httpx.Response(200, text=empty, request=request)
        i = min(self.cursor[key], len(recs) - 1)
        self.cursor[key] += 1
        return httpx.Response(recs[i]["status"], text=recs[i]["body"], request=request)
