"""What every reuse-source call returns, and safe rendering of sources.yaml templates."""

from __future__ import annotations

import json
import re
from datetime import datetime
from typing import Any, Literal

from pydantic import BaseModel, Field

from wt.common.contracts import utcnow

# Template values end up inside SPL and SWQL strings, so only plain identifiers are allowed:
# node and switch names, ports like Gi1/0/11, windows like 30m, numeric ids.
_SAFE_VALUE = re.compile(r"^[A-Za-z0-9_.:/\-]+$")
_PLACEHOLDER = re.compile(r"\{([a-z_]+)\}")


def placeholders(template: str) -> set[str]:
    return set(_PLACEHOLDER.findall(template))


def render_template(template: str, **params: Any) -> str:
    """Fill {placeholders}. Every placeholder must be given and every value must be a plain identifier."""

    def sub(m: re.Match) -> str:
        name = m.group(1)
        if name not in params:
            raise KeyError(f"template needs {{{name}}}: {template}")
        value = str(params[name])
        if not _SAFE_VALUE.match(value):
            raise ValueError(f"unsafe value for {{{name}}}: {value!r}")
        return value

    return _PLACEHOLDER.sub(sub, template)


class QueryResult(BaseModel):
    """Rows from Splunk or SolarWinds plus everything an evidence item needs to cite them."""

    tool: Literal["splunk", "solarwinds"]
    query: str
    rows: list[dict[str, Any]]
    collected_at: datetime = Field(default_factory=utcnow)
    duration_ms: int = 0
    audit_id: str | None = None

    def excerpt(self, limit: int = 2000) -> str:
        """Raw rows as JSON lines, trimmed to the evidence excerpt limit."""
        text = "\n".join(json.dumps(r, separators=(",", ":"), default=str) for r in self.rows)
        return text if len(text) <= limit else text[: limit - 15] + "\n…(truncated)"
