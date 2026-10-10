"""Async Redis Streams bus with consumer groups for the six wt.* streams.

Each message carries one JSON document in the field "data". Consumers in a group share the
work; a restarted consumer first re-reads its own unacknowledged messages, then new ones.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any, AsyncIterator

import redis.asyncio as aioredis
from pydantic import BaseModel
from redis.exceptions import ResponseError

from wt.common.contracts import dump

SIGNALS = "wt.signals"
TASKS = "wt.tasks"
EVIDENCE = "wt.evidence"
WATCH = "wt.watch"
INVESTIGATIONS = "wt.investigations"
SRE = "wt.sre"
STREAMS = (SIGNALS, TASKS, EVIDENCE, WATCH, INVESTIGATIONS, SRE)

MAXLEN = 10_000  # approximate cap per stream; the lab runs for hours, not weeks


@dataclass
class Message:
    stream: str
    id: str
    data: dict[str, Any]


class Bus:
    def __init__(self, client: aioredis.Redis):
        self.r = client

    @classmethod
    def from_url(cls, url: str) -> Bus:
        return cls(aioredis.from_url(url, decode_responses=True))

    async def close(self) -> None:
        await self.r.aclose()

    async def publish(self, stream: str, payload: BaseModel | dict[str, Any]) -> str:
        _check(stream)
        body = dump(payload) if isinstance(payload, BaseModel) else payload
        return await self.r.xadd(stream, {"data": json.dumps(body, default=str)}, maxlen=MAXLEN, approximate=True)

    async def ensure_group(self, stream: str, group: str, start: str = "0") -> None:
        _check(stream)
        try:
            await self.r.xgroup_create(stream, group, id=start, mkstream=True)
        except ResponseError as exc:
            if "BUSYGROUP" not in str(exc):
                raise

    async def read(
        self,
        streams: list[str] | tuple[str, ...],
        group: str,
        consumer: str,
        count: int = 10,
        block_ms: int | None = 5000,
        after: dict[str, str] | None = None,
    ) -> list[Message]:
        """One XREADGROUP for new messages, or with after={stream: id} for this consumer's
        unacknowledged messages with ids greater than id."""
        ids = {s: (after or {}).get(s, ">") for s in streams}
        reply = await self.r.xreadgroup(group, consumer, ids, count=count, block=None if after else block_ms)
        return [
            Message(stream=stream, id=msg_id, data=json.loads(fields["data"]))
            for stream, entries in reply or []
            for msg_id, fields in entries
            if fields  # pending entries already trimmed from the stream come back empty
        ]

    async def consume(
        self,
        streams: list[str] | tuple[str, ...],
        group: str,
        consumer: str,
        count: int = 10,
        block_ms: int = 5000,
    ) -> AsyncIterator[Message]:
        """Yield messages forever (pending ones first). Call ack() once a message is handled."""
        for s in streams:
            await self.ensure_group(s, group)
        for s in streams:
            cursor = "0"
            while batch := await self.read([s], group, consumer, count, after={s: cursor}):
                for msg in batch:
                    yield msg
                cursor = batch[-1].id
        while True:
            for msg in await self.read(streams, group, consumer, count, block_ms):
                yield msg

    async def ack(self, group: str, msg: Message) -> None:
        await self.r.xack(msg.stream, group, msg.id)


def _check(stream: str) -> None:
    if stream not in STREAMS:
        raise ValueError(f"unknown stream {stream!r}; expected one of {STREAMS}")
