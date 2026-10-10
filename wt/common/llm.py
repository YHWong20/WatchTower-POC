"""The one LLM client: OpenAI-compatible chat completions (Ollama in the PoC, on-prem Gemma later).

Each of the six jobs calls `complete()` with a short system prompt, two few-shot examples, the
input, and a Pydantic model for the answer. The model's JSON schema goes in `response_format`,
which Ollama's OpenAI endpoint passes to its `format` parameter (constrained decoding). No
native tool calling.

Contract with callers:
- returns a validated model instance, or None (LLM off, unreachable, or invalid twice).
  On None the caller uses its rule-based fallback and sets llm_used=false.
- invalid output is retried once, with the validation error shown to the model.
- identical inputs are never sent twice: answers are cached by hash in SQLite (the M2 Air is
  fanless, and a replay should not need the model at all).
"""

from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass
from typing import TypeVar

import httpx
from pydantic import BaseModel, ValidationError

from wt.common.audit import Audit
from wt.common.store import Store

T = TypeVar("T", bound=BaseModel)

_FENCE = re.compile(r"^\s*```(?:json)?\s*|\s*```\s*$")


@dataclass(frozen=True)
class Example:
    """One few-shot pair: an input and the exact JSON answer we want for it."""

    input: str
    output: BaseModel


class LLMClient:
    def __init__(
        self,
        base_url: str,
        model: str,
        audit: Audit,
        store: Store,
        enabled: bool = True,
        timeout_s: float = 60.0,
        max_tokens: int = 512,
        actor: str = "watchtower",
        transport: httpx.AsyncBaseTransport | None = None,
    ):
        self.model = model
        self.audit = audit
        self.store = store
        self.enabled = enabled
        self.max_tokens = max_tokens
        self.actor = actor
        self.http = httpx.AsyncClient(base_url=base_url.rstrip("/"), timeout=timeout_s, transport=transport)

    async def close(self) -> None:
        await self.http.aclose()

    def cache_key(self, job: str, system: str, examples: list[Example], user: str, schema: type[BaseModel]) -> str:
        payload = json.dumps(
            {
                "model": self.model,
                "job": job,
                "system": system,
                "examples": [[e.input, e.output.model_dump(mode="json")] for e in examples],
                "user": user,
                "schema": schema.model_json_schema(),
            },
            sort_keys=True,
        )
        return hashlib.sha256(payload.encode()).hexdigest()

    async def complete(
        self, job: str, system: str, examples: list[Example], user: str, schema: type[T]
    ) -> T | None:
        if not self.enabled:
            return None
        key = self.cache_key(job, system, examples, user, schema)
        if (cached := self.store.cache_get(key)) is not None:
            return schema.model_validate_json(cached)

        messages = [{"role": "system", "content": system}]
        for ex in examples:
            messages.append({"role": "user", "content": ex.input})
            messages.append({"role": "assistant", "content": ex.output.model_dump_json()})
        messages.append({"role": "user", "content": user})

        for attempt in (1, 2):
            text, failed = await self._call(job, key, attempt, messages, schema)
            if failed:  # transport error, already in the audit log; retry as is
                continue
            try:
                answer = schema.model_validate_json(_strip_fences(text))
            except ValidationError as exc:
                error = _short_error(exc)
                self.audit.record("llm", self.actor, self.model, f"{job} attempt {attempt} invalid", ok=False,
                                  detail=error)
                messages += [
                    {"role": "assistant", "content": text},
                    {"role": "user", "content": f"That reply was invalid: {error}. "
                                                "Reply again with only JSON that matches the schema."},
                ]
                continue
            self.store.cache_put(key, job, self.model, answer.model_dump_json())
            return answer
        return None

    async def _call(
        self, job: str, key: str, attempt: int, messages: list[dict], schema: type[BaseModel]
    ) -> tuple[str, bool]:
        """One chat completion. Returns (content, failed); failures are audited by `timed`."""
        body = {
            "model": self.model,
            "messages": messages,
            "temperature": 0,
            "seed": 7,
            "max_tokens": self.max_tokens,
            "response_format": {
                "type": "json_schema",
                "json_schema": {"name": job, "schema": schema.model_json_schema(), "strict": True},
            },
        }
        try:
            async with self.audit.timed("llm", self.actor, self.model, f"{job} attempt {attempt} key {key[:16]}") as rec:
                resp = await self.http.post("/chat/completions", json=body)
                resp.raise_for_status()
                text = resp.json()["choices"][0]["message"]["content"] or ""
                rec.detail = text[:500]
            return text, False
        except (httpx.HTTPError, KeyError, IndexError, ValueError):
            return "", True


def _strip_fences(text: str) -> str:
    """Small models sometimes wrap JSON in ``` fences even with constrained output."""
    return _FENCE.sub("", text).strip()


def _short_error(exc: ValidationError) -> str:
    return "; ".join(f"{'.'.join(map(str, e['loc'])) or 'root'}: {e['msg']}" for e in exc.errors()[:5])
