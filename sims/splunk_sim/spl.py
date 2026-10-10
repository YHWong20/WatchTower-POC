"""The SPL subset splunk-sim understands, parsed into a small tree and run over stored events.

Search terms (implicit AND, plus OR, NOT and parentheses):
    index=ocp  sourcetype=kube:container:ovnkube*  host!=x  count>3     field comparisons
    reason IN (NodeNotReady, NodeReady)                                 IN lists
    "i/o timeout"                                                       phrases (case-insensitive)
    error  Gi1/0/13  fail*                                              terms (whole tokens)
    earliest=-30m  latest=now  earliest=-1h@h  _index_earliest=<epoch>  time modifiers
Commands after `|`:
    stats count [, dc(f), min(f), max(f), avg(f), sum(f), latest(f), earliest(f), values(f)] [by f1, f2]
    timechart span=1m count [by f]      head N      table f1 f2      fields f1, f2
    sort [-]f      dedup f
Anything else is rejected with a Splunk-style error, so a query that works here works on Splunk.
"""

from __future__ import annotations

import json
import math
import re
import time
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any, Callable, Iterable

# ---------------------------------------------------------------- events and field extraction

META = ("index", "sourcetype", "source", "host")
_KV = re.compile(r'(?<![\w.])([A-Za-z_][\w.]*)=("(?:[^"\\]|\\.)*"|[^\s,"]+)')
_CISCO = re.compile(r"%(?P<facility>[A-Z0-9_]+)-(?P<severity_id>\d)-(?P<mnemonic>[A-Z0-9_]+):\s*(?P<message_text>.*)")


@dataclass
class Event:
    time: float
    index: str
    sourcetype: str
    source: str
    host: str
    raw: str
    indexed: dict[str, Any] = field(default_factory=dict)
    indextime: float = 0.0
    _fields: dict[str, Any] | None = None

    @property
    def fields(self) -> dict[str, Any]:
        """Metadata, indexed fields (HEC `fields`) and search-time fields: JSON keys flattened
        with dots (involvedObject.name), key=value pairs, and the Cisco %FAC-SEV-MNEMONIC parts."""
        if self._fields is None:
            f: dict[str, Any] = {}
            raw = self.raw.lstrip()
            if raw.startswith("{"):
                try:
                    _flatten(json.loads(raw), "", f)
                except ValueError:
                    pass
            elif self.sourcetype == "cisco:ios":
                if m := _CISCO.search(self.raw):
                    f.update(m.groupdict())
            else:
                for k, v in _KV.findall(self.raw):
                    f.setdefault(k, v[1:-1].replace('\\"', '"') if v.startswith('"') else v)
            f.update(self.indexed)
            f.update(index=self.index, sourcetype=self.sourcetype, source=self.source, host=self.host)
            self._fields = f
        return self._fields

    def get(self, name: str) -> Any:
        if name == "_time":
            return self.time
        if name == "_indextime":
            return self.indextime
        if name == "_raw":
            return self.raw
        return self.fields.get(name)


def _flatten(obj: Any, prefix: str, out: dict[str, Any]) -> None:
    if isinstance(obj, dict):
        for k, v in obj.items():
            _flatten(v, f"{prefix}.{k}" if prefix else str(k), out)
    elif isinstance(obj, list):
        for v in obj:
            _flatten(v, f"{prefix}{{}}", out)
    else:
        value = "true" if obj is True else "false" if obj is False else "" if obj is None else str(obj)
        if prefix in out:
            prev = out[prefix]
            out[prefix] = (prev if isinstance(prev, list) else [prev]) + [value]
        else:
            out[prefix] = value


# ---------------------------------------------------------------- errors and time


class SplError(ValueError):
    """Raised for unsupported or malformed SPL; the API answers 400 with the message."""


_REL = re.compile(r"^([+-])(\d+)([a-z]+)(?:@([a-z]+))?$")
_UNITS = {"s": 1, "sec": 1, "secs": 1, "second": 1, "seconds": 1, "m": 60, "min": 60, "mins": 60,
          "minute": 60, "minutes": 60, "h": 3600, "hr": 3600, "hrs": 3600, "hour": 3600, "hours": 3600,
          "d": 86400, "day": 86400, "days": 86400, "w": 604800, "week": 604800, "weeks": 604800}


def parse_time(value: str, now: float) -> float:
    """Splunk time modifiers: -30m, -1h@h, @d, now, epoch seconds, or %m/%d/%Y:%H:%M:%S (UTC)."""
    v = value.strip().strip('"').lower()
    if v in ("now", ""):
        return now
    if v == "0":
        return 0.0
    try:
        return float(v)
    except ValueError:
        pass
    if v.startswith("@"):
        return _snap(now, v[1:])
    m = _REL.match(v)
    if m:
        sign, n, unit, snap = m.groups()
        if unit not in _UNITS:
            raise SplError(f"Invalid time unit '{unit}' in '{value}'")
        t = now + (1 if sign == "+" else -1) * int(n) * _UNITS[unit]
        return _snap(t, snap) if snap else t
    for fmt in ("%m/%d/%Y:%H:%M:%S", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%d %H:%M:%S"):
        try:
            return datetime.strptime(value.strip('"'), fmt).replace(tzinfo=UTC).timestamp()
        except ValueError:
            continue
    raise SplError(f"Invalid value \"{value}\" for time term")


def _snap(t: float, unit: str) -> float:
    if unit not in _UNITS:
        raise SplError(f"Invalid snap unit '{unit}'")
    step = _UNITS[unit]
    return math.floor(t / step) * step


def iso(t: float) -> str:
    """Splunk's _time rendering: 2026-10-10T09:20:41.000+00:00 (the sim runs in UTC)."""
    return datetime.fromtimestamp(t, UTC).strftime("%Y-%m-%dT%H:%M:%S.") + f"{int(t * 1000) % 1000:03d}+00:00"


def span_seconds(span: str) -> int:
    m = re.fullmatch(r"(\d+)([a-z]+)", span.strip().lower())
    if not m or m.group(2) not in _UNITS:
        raise SplError(f"Invalid span '{span}'")
    return int(m.group(1)) * _UNITS[m.group(2)]


# ---------------------------------------------------------------- tokenizer


@dataclass
class Tok:
    kind: str          # WORD, STRING, LP, RP, COMMA
    text: str


def _tokenize(s: str) -> list[Tok]:
    out, i, n = [], 0, len(s)
    while i < n:
        c = s[i]
        if c.isspace():
            i += 1
        elif c == "(":
            out.append(Tok("LP", c)); i += 1
        elif c == ")":
            out.append(Tok("RP", c)); i += 1
        elif c == ",":
            out.append(Tok("COMMA", c)); i += 1
        elif c == '"':
            j, buf = i + 1, []
            while j < n and s[j] != '"':
                if s[j] == "\\" and j + 1 < n:
                    j += 1
                buf.append(s[j]); j += 1
            if j >= n:
                raise SplError("Unbalanced quotes.")
            out.append(Tok("STRING", "".join(buf))); i = j + 1
        else:
            j = i
            while j < n and not s[j].isspace() and s[j] not in '(),"':
                j += 1
            word = s[i:j]
            # field="quoted value": keep the value with the word
            if j < n and s[j] == '"' and re.search(r"(=|!=|<|>|<=|>=)$", word):
                k, buf = j + 1, []
                while k < n and s[k] != '"':
                    if s[k] == "\\" and k + 1 < n:
                        k += 1
                    buf.append(s[k]); k += 1
                if k >= n:
                    raise SplError("Unbalanced quotes.")
                out.append(Tok("WORD", word + "\x00" + "".join(buf))); i = k + 1
                continue
            out.append(Tok("WORD", word)); i = j
    return out


def split_pipeline(spl: str) -> list[str]:
    """Split on | outside quotes."""
    parts, buf, quoted = [], [], False
    for i, c in enumerate(spl):
        if c == '"' and (i == 0 or spl[i - 1] != "\\"):
            quoted = not quoted
        if c == "|" and not quoted:
            parts.append("".join(buf)); buf = []
        else:
            buf.append(c)
    parts.append("".join(buf))
    return [p.strip() for p in parts]


# ---------------------------------------------------------------- search tree

Pred = Callable[[Event], bool]


def _wild(pattern: str) -> re.Pattern:
    return re.compile("^" + ".*".join(re.escape(p) for p in pattern.split("*")) + "$", re.I | re.S)


def _values(ev: Event, name: str) -> list[str]:
    v = ev.get(name)
    if v is None:
        return []
    return [str(x) for x in v] if isinstance(v, list) else [str(v)]


@dataclass
class Cmp:
    field: str
    op: str
    value: str

    def __post_init__(self):
        self._rx = _wild(self.value)

    def __call__(self, ev: Event) -> bool:
        vals = _values(ev, self.field)
        if self.op == "=":
            return any(self._rx.match(v) for v in vals)
        if self.op == "!=":
            return bool(vals) and not any(self._rx.match(v) for v in vals)
        for v in vals:
            try:
                a, b = float(v), float(self.value)
            except ValueError:
                a, b = v.lower(), self.value.lower()
            if {"<": a < b, "<=": a <= b, ">": a > b, ">=": a >= b}[self.op]:
                return True
        return False


@dataclass
class In:
    field: str
    values: list[str]

    def __post_init__(self):
        self._rx = [_wild(v) for v in self.values]

    def __call__(self, ev: Event) -> bool:
        return any(rx.match(v) for v in _values(ev, self.field) for rx in self._rx)


@dataclass
class Term:
    text: str
    phrase: bool

    def __post_init__(self):
        parts = [re.escape(p) for p in self.text.split("*")]
        if self.phrase:
            self._rx = re.compile(".*?".join(parts), re.I)
        else:
            body = r"\S*".join(parts)
            self._rx = re.compile(rf"(?<![A-Za-z0-9_]){body}(?![A-Za-z0-9_])", re.I)

    def __call__(self, ev: Event) -> bool:
        return bool(self._rx.search(ev.raw))


@dataclass
class And:
    items: list[Pred]

    def __call__(self, ev: Event) -> bool:
        return all(p(ev) for p in self.items)


@dataclass
class Or:
    items: list[Pred]

    def __call__(self, ev: Event) -> bool:
        return any(p(ev) for p in self.items)


@dataclass
class Not:
    item: Pred

    def __call__(self, ev: Event) -> bool:
        return not self.item(ev)


_CMP = re.compile(r"^([A-Za-z_][\w.:{}\-]*?)(!=|<=|>=|=|<|>)(.*)$", re.S)
TIME_FIELDS = ("earliest", "latest", "_index_earliest", "_index_latest")


class _SearchParser:
    def __init__(self, text: str):
        self.toks = _tokenize(text)
        self.i = 0
        self.times: dict[str, str] = {}

    def peek(self) -> Tok | None:
        return self.toks[self.i] if self.i < len(self.toks) else None

    def take(self) -> Tok:
        tok = self.peek()
        if tok is None:
            raise SplError("Unexpected end of search.")
        self.i += 1
        return tok

    def parse(self) -> Pred:
        node = self.or_expr()
        if self.peek() is not None:
            raise SplError(f"Unexpected '{self.peek().text}' in search.")
        return node

    def or_expr(self) -> Pred:
        items = [self.and_expr()]
        while (t := self.peek()) and t.kind == "WORD" and t.text == "OR":
            self.take()
            items.append(self.and_expr())
        return items[0] if len(items) == 1 else Or(items)

    def and_expr(self) -> Pred:
        items = []
        while (t := self.peek()) and not (t.kind == "RP" or (t.kind == "WORD" and t.text == "OR")):
            if t.kind == "WORD" and t.text == "AND":
                self.take()
                continue
            items.append(self.not_expr())
        if not items:
            raise SplError("Empty search clause.")
        return items[0] if len(items) == 1 else And(items)

    def not_expr(self) -> Pred:
        t = self.peek()
        if t and t.kind == "WORD" and t.text == "NOT":
            self.take()
            return Not(self.not_expr())
        return self.primary()

    def primary(self) -> Pred:
        t = self.take()
        if t.kind == "LP":
            node = self.or_expr()
            if self.take().kind != "RP":
                raise SplError("Mismatched parentheses.")
            return node
        if t.kind == "STRING":
            return Term(t.text, phrase=True)
        if t.kind in ("RP", "COMMA"):
            raise SplError(f"Unexpected '{t.text}' in search.")
        nxt = self.peek()
        if nxt and nxt.kind == "WORD" and nxt.text == "IN":
            self.take()
            return In(t.text, self.in_list())
        if "\x00" in t.text:                          # field="quoted"
            head, value = t.text.split("\x00", 1)
            m = _CMP.match(head)
            return self.cmp(m.group(1), m.group(2), value)
        m = _CMP.match(t.text)
        if m and m.group(3) != "":
            return self.cmp(m.group(1), m.group(2), m.group(3))
        return Term(t.text, phrase=False)

    def cmp(self, name: str, op: str, value: str) -> Pred:
        if name in TIME_FIELDS:
            if op != "=":
                raise SplError(f"{name} only supports '='")
            self.times[name] = value
            return lambda ev: True
        return Cmp(name, op, value)

    def in_list(self) -> list[str]:
        if self.take().kind != "LP":
            raise SplError("IN must be followed by a parenthesised list.")
        values = []
        while True:
            t = self.take()
            if t.kind == "RP":
                break
            if t.kind == "COMMA":
                continue
            values.append(t.text)
        if not values:
            raise SplError("Empty IN list.")
        return values


def _prefilter(node: Pred) -> dict[str, list[str]]:
    """Metadata constraints that hold for every match (top-level AND only), for the SQL index."""
    items = node.items if isinstance(node, And) else [node]
    out: dict[str, list[str]] = {}
    for p in items:
        if isinstance(p, Cmp) and p.op == "=" and p.field in META:
            out.setdefault(p.field, []).append(p.value)
        elif isinstance(p, In) and p.field in META:
            out.setdefault(p.field, []).append(p.values)  # type: ignore[arg-type]
    return out


# ---------------------------------------------------------------- commands


def _num(v: Any) -> float | None:
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


def _fmt(x: float | int | None) -> str:
    if x is None:
        return ""
    if isinstance(x, float) and x.is_integer():
        return str(int(x))
    return str(round(x, 6)) if isinstance(x, float) else str(x)


_AGG = re.compile(r"^(count|dc|min|max|avg|sum|latest|earliest|values)(?:\(([^)]*)\))?$", re.I)


@dataclass
class Agg:
    fn: str
    field: str | None
    name: str

    def compute(self, events: list[Event]) -> Any:
        fn, f = self.fn, self.field
        if fn == "count":
            return str(len(events) if not f else sum(1 for e in events if e.get(f) is not None))
        vals = [v for e in events for v in _values(e, f)] if f else []
        if fn == "dc":
            return str(len(set(vals)))
        if fn == "values":
            return sorted(set(vals))
        if fn in ("latest", "earliest"):
            timed = [(e.time, _values(e, f)) for e in events if e.get(f) is not None]
            if not timed:
                return ""
            pick = max(timed, key=lambda x: x[0]) if fn == "latest" else min(timed, key=lambda x: x[0])
            return pick[1][0]
        nums = [n for n in map(_num, vals) if n is not None]
        if not nums:
            return ""
        return _fmt({"min": min, "max": max, "sum": sum, "avg": lambda xs: sum(xs) / len(xs)}[fn](nums))


def _parse_aggs(text: str) -> tuple[list[Agg], list[str]]:
    """'count, max(ms) as worst by host, reason' -> aggregations and by-fields."""
    m = re.split(r"\s+by\s+", text, maxsplit=1, flags=re.I)
    agg_text, by_text = m[0], (m[1] if len(m) > 1 else "")
    aggs = []
    for part in re.findall(r"\w+(?:\([^)]*\))?(?:\s+as\s+\w+)?", agg_text, flags=re.I):
        am = re.match(r"^(\w+(?:\([^)]*\))?)(?:\s+as\s+(\w+))?$", part.strip(), re.I)
        fm = _AGG.match(am.group(1)) if am else None
        if not fm:
            raise SplError(f"Unsupported aggregation '{part}'")
        fn, f = fm.group(1).lower(), (fm.group(2) or "").strip() or None
        if fn != "count" and not f:
            raise SplError(f"{fn} needs a field")
        aggs.append(Agg(fn, f, am.group(2) or (fn if not f else f"{fn}({f})")))
    if not aggs:
        raise SplError("stats needs at least one aggregation")
    by = [b for b in re.split(r"[\s,]+", by_text.strip()) if b]
    return aggs, by


def _stats(rows: list[Event], args: str) -> list[dict]:
    aggs, by = _parse_aggs(args)
    if not by:
        return [{a.name: a.compute(rows) for a in aggs}]
    groups: dict[tuple, list[Event]] = {}
    for ev in rows:
        keys = [_values(ev, b) for b in by]
        if any(not k for k in keys):
            continue                                   # Splunk drops events missing a by-field
        groups.setdefault(tuple(k[0] for k in keys), []).append(ev)
    out = []
    for key in sorted(groups):
        row = dict(zip(by, key))
        row.update({a.name: a.compute(groups[key]) for a in aggs})
        out.append(row)
    return out


def _timechart(rows: list[Event], args: str, earliest: float | None, latest: float) -> list[dict]:
    span = 60
    m = re.search(r"\bspan=(\S+)", args)
    if m:
        span = span_seconds(m.group(1))
        args = (args[: m.start()] + args[m.end():]).strip()
    aggs, by = _parse_aggs(args)
    if len(by) > 1 or (by and len(aggs) > 1):
        raise SplError("timechart supports one aggregation with one by-field")
    start = earliest if earliest is not None else (min((e.time for e in rows), default=latest))
    first, last = math.floor(start / span) * span, math.floor(max(latest - 1e-6, start) / span) * span
    buckets: dict[float, list[Event]] = {}
    for ev in rows:
        buckets.setdefault(math.floor(ev.time / span) * span, []).append(ev)
    series = sorted({_values(e, by[0])[0] for e in rows if _values(e, by[0])}) if by else []
    out, t = [], first
    while t <= last and len(out) < 10000:
        evs = buckets.get(t, [])
        row: dict[str, Any] = {"_time": iso(t)}
        if by:
            for s in series:
                row[s] = aggs[0].compute([e for e in evs if s in _values(e, by[0])]) or "0"
        else:
            for a in aggs:
                row[a.name] = a.compute(evs) or ("0" if a.fn in ("count", "dc") else "")
        out.append(row)
        t += span
    return out


def event_row(ev: Event) -> dict[str, Any]:
    """An event as the export endpoint returns it: _time, _raw, metadata and extracted fields
    (Kubernetes object metadata other than name and namespace is left out to keep rows small)."""
    row: dict[str, Any] = {"_time": iso(ev.time), "_raw": ev.raw}
    for k, v in ev.fields.items():
        if k.startswith("metadata.") and k not in ("metadata.name", "metadata.namespace"):
            continue
        row[k] = v
    return row


def _row_get(row: dict | Event, name: str) -> Any:
    return row.get(name) if isinstance(row, dict) else row.get(name)


# ---------------------------------------------------------------- the whole search


@dataclass
class Search:
    predicate: Pred
    prefilter: dict[str, list]
    times: dict[str, str]
    commands: list[tuple[str, str]]

    def window(self, now: float, earliest: str | None = None, latest: str | None = None) -> tuple[float | None, float]:
        e = self.times.get("earliest", earliest)
        l = self.times.get("latest", latest)
        return (parse_time(e, now) if e else None), (parse_time(l, now) if l else now)

    def index_window(self, now: float) -> tuple[float | None, float | None]:
        e, l = self.times.get("_index_earliest"), self.times.get("_index_latest")
        return (parse_time(e, now) if e else None), (parse_time(l, now) if l else None)


COMMANDS = ("stats", "timechart", "head", "table", "fields", "sort", "dedup")


def parse(spl: str) -> Search:
    parts = split_pipeline(spl)
    first = parts[0]
    if first.lower().startswith("search "):
        first = first[7:]
    elif first.lower() == "search":
        first = ""
    if not first.strip():
        raise SplError("Search must start with search terms (e.g. index=ocp).")
    parser = _SearchParser(first)
    pred = parser.parse()
    commands = []
    for part in parts[1:]:
        name, _, args = part.partition(" ")
        name = name.lower()
        if name not in COMMANDS:
            raise SplError(f"Unknown search command '{name}'. splunk-sim supports: {', '.join(COMMANDS)}.")
        if name == "stats":
            _parse_aggs(args)
        elif name == "timechart":
            _parse_aggs(re.sub(r"\bspan=\S+", "", args))
        elif name == "head" and args.strip() and not args.strip().isdigit():
            raise SplError("head takes a number")
        commands.append((name, args.strip()))
    return Search(pred, _prefilter(pred), parser.times, commands)


def run(search: Search, candidates: Iterable[Event], now: float | None = None,
        earliest: str | None = None, latest: str | None = None, limit: int = 10000,
        index_window: tuple[float | None, float | None] = (None, None)) -> list[dict]:
    """Filter candidate events (newest first) and apply the pipeline. Returns result rows.
    earliest/latest are the request's defaults (earliest_time/latest_time); the search's own
    modifiers win. index_window bounds _indextime (the scheduler uses it)."""
    now = time.time() if now is None else now
    t0, t1 = search.window(now, earliest, latest)
    i0, i1 = search.index_window(now)
    i0 = i0 if i0 is not None else index_window[0]
    i1 = i1 if i1 is not None else index_window[1]
    events = [ev for ev in candidates
              if (t0 is None or ev.time >= t0) and ev.time <= t1
              and (i0 is None or ev.indextime > i0) and (i1 is None or ev.indextime <= i1)
              and search.predicate(ev)]
    events.sort(key=lambda e: e.time, reverse=True)
    rows: list[Any] = events
    transformed = False
    for name, args in search.commands:
        if name == "stats":
            if transformed:
                raise SplError("stats over transformed results is not supported")
            rows, transformed = _stats(rows, args), True
        elif name == "timechart":
            if transformed:
                raise SplError("timechart must come before transforming commands")
            rows, transformed = _timechart(rows, args, t0, t1), True
        elif name == "head":
            rows = rows[: int(args or 10)]
        elif name in ("table", "fields"):
            names = [f for f in re.split(r"[\s,]+", args) if f and f != "+"]
            if names and names[0] == "-":
                raise SplError("fields - is not supported")
            full = (event_row(r) if isinstance(r, Event) else r for r in rows)
            rows = [{n: r[n] for n in names if n in r} for r in full]
            transformed = True
        elif name == "sort":
            keys = [k for k in re.split(r"[\s,]+", args) if k and not k.isdigit()]
            for key in reversed(keys):
                desc, k = key.startswith("-"), key.lstrip("+-")
                rows = sorted(rows, key=lambda r: (_num(_row_get(r, k)) is None, _num(_row_get(r, k)) or 0,
                                                   str(_row_get(r, k) or "")), reverse=desc)
        elif name == "dedup":
            seen, kept = set(), []
            for r in rows:
                key = tuple(str(_row_get(r, f)) for f in re.split(r"[\s,]+", args) if f)
                if key not in seen:
                    seen.add(key)
                    kept.append(r)
            rows = kept
    out = [event_row(r) if isinstance(r, Event) else r for r in rows]
    return out[:limit]
