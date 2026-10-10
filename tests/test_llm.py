import json

import httpx
import pytest
from pydantic import BaseModel, Field

from wt.common.llm import Example, LLMClient


class Summary(BaseModel):
    summary: str = Field(max_length=120)
    refs: list[str] = Field(min_length=1)


EXAMPLES = [
    Example("signals: sig-1 timeouts on worker-1", Summary(summary="API timeouts on worker-1", refs=["sig-1"])),
    Example("signals: sig-2 drops on Gi1/0/13", Summary(summary="Drops on Gi1/0/13", refs=["sig-2"])),
]
GOOD = '{"summary": "worker-1 API timeouts with drops on its switch port", "refs": ["sig-1", "sig-2"]}'


class FakeOllama:
    """Replies with the given contents in order; records request bodies."""

    def __init__(self, *replies):
        self.replies = list(replies)
        self.bodies = []

    def __call__(self, req: httpx.Request) -> httpx.Response:
        self.bodies.append(json.loads(req.content))
        reply = self.replies.pop(0)
        if isinstance(reply, int):
            return httpx.Response(reply)
        return httpx.Response(200, json={"choices": [{"message": {"role": "assistant", "content": reply}}]})


def make(fake, audit, store, enabled=True):
    return LLMClient("http://ollama:11434/v1", "gemma3:1b", audit, store, enabled=enabled,
                     transport=httpx.MockTransport(fake))


async def ask(llm):
    return await llm.complete("summarise_watch_item", "Summarise in plain words.", EXAMPLES,
                              "signals: sig-1, sig-2", Summary)


async def test_valid_answer_and_request_shape(audit, store):
    fake = FakeOllama(GOOD)
    llm = make(fake, audit, store)
    out = await ask(llm)
    await llm.close()
    assert out == Summary.model_validate_json(GOOD)
    body = fake.bodies[0]
    assert body["model"] == "gemma3:1b" and body["temperature"] == 0
    assert body["response_format"]["type"] == "json_schema"
    assert body["response_format"]["json_schema"]["schema"]["required"] == ["summary", "refs"]
    roles = [m["role"] for m in body["messages"]]
    assert roles == ["system", "user", "assistant", "user", "assistant", "user"]   # two few-shot pairs
    assert "tools" not in body                                                    # no native tool calling
    assert store.audit_rows("llm")[0]["ok"] == 1


async def test_invalid_then_valid_retries_once_with_the_error(audit, store):
    fake = FakeOllama('{"summary": "x"}', "```json\n" + GOOD + "\n```")
    llm = make(fake, audit, store)
    out = await ask(llm)
    await llm.close()
    assert out is not None and out.refs == ["sig-1", "sig-2"]                     # fences stripped
    retry_msgs = fake.bodies[1]["messages"]
    assert retry_msgs[-2] == {"role": "assistant", "content": '{"summary": "x"}'}
    assert "refs" in retry_msgs[-1]["content"] and "invalid" in retry_msgs[-1]["content"]


async def test_invalid_twice_returns_none(audit, store):
    fake = FakeOllama("not json", '{"summary": "' + "x" * 200 + '", "refs": ["a"]}')
    llm = make(fake, audit, store)
    assert await ask(llm) is None
    await llm.close()
    assert len(fake.bodies) == 2
    assert sum(1 for r in store.audit_rows("llm") if not r["ok"]) == 2


async def test_unreachable_returns_none_after_one_retry(audit, store):
    fake = FakeOllama(500, 500)
    llm = make(fake, audit, store)
    assert await ask(llm) is None
    await llm.close()
    assert len(fake.bodies) == 2


async def test_same_inputs_hit_the_cache(audit, store):
    fake = FakeOllama(GOOD)
    llm = make(fake, audit, store)
    first = await ask(llm)
    second = await ask(llm)                       # FakeOllama has no reply left: would raise if called
    await llm.close()
    assert first == second and len(fake.bodies) == 1

    other = make(FakeOllama(), audit, store)      # cache survives a restart (SQLite)
    assert await ask(other) == first
    await other.close()


async def test_changed_inputs_miss_the_cache(audit, store):
    fake = FakeOllama(GOOD, GOOD)
    llm = make(fake, audit, store)
    await ask(llm)
    await llm.complete("summarise_watch_item", "Summarise in plain words.", EXAMPLES, "signals: sig-3", Summary)
    await llm.close()
    assert len(fake.bodies) == 2


async def test_llm_off_never_calls(audit, store):
    fake = FakeOllama()
    llm = make(fake, audit, store, enabled=False)
    assert await ask(llm) is None
    await llm.close()
    assert fake.bodies == [] and store.audit_rows("llm") == []
