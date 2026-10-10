"""The SWQL subset solarwinds-sim understands, compiled to one parameterised SQLite SELECT.

    SELECT [DISTINCT] [TOP n] expr [AS alias], ... FROM <Entity> [[AS] alias]
    [WHERE cond] [GROUP BY expr, ...] [HAVING cond] [ORDER BY expr [ASC|DESC], ...]

Expressions: columns (Name, I.Name), navigation properties (I.Node.Caption, Node.Caption,
Interface.Name), literals, @parameters, = != <> < <= > >= LIKE IN IS [NOT] NULL AND OR NOT, + - * /,
COUNT/SUM/AVG/MIN/MAX, GETUTCDATE(), GETDATE(), ADDDATE('minute', n, d), ADDSECOND/ADDMINUTE/
ADDHOUR/ADDDAY(n, d), MINUTEDIFF/HOURDIFF/DAYDIFF/SECONDDIFF(a, b), TOLOCAL, TOUTC, ISNULL, ROUND, ABS.
Only SELECT exists in the grammar, column names are checked against the schema, and every
value is bound as a parameter: there is no way to write or to inject SQL.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from typing import Any

from sims.solarwinds_sim.schema import BOOL, DATETIME, ENTITIES, INT, REAL, TEXT, Entity


class SwqlError(ValueError):
    """Malformed or unsupported SWQL; the API answers 400 with the message."""


DB_FMT = "%Y-%m-%d %H:%M:%S"


def to_db(dt: datetime) -> str:
    return dt.astimezone(UTC).strftime(DB_FMT)


def from_db(text: str | None) -> str | None:
    """'2026-10-10 09:15:00' -> '2026-10-10T09:15:00.0000000Z' (SWIS JSON for UTC values)."""
    if text is None:
        return None
    return text.replace(" ", "T") + ".0000000Z"


def parse_datetime(text: str) -> datetime:
    t = text.strip().replace("Z", "+00:00")
    try:
        dt = datetime.fromisoformat(t)
    except ValueError:
        try:
            dt = datetime.strptime(text.strip(), "%m/%d/%Y %H:%M:%S")
        except ValueError:
            raise SwqlError(f"'{text}' is not a valid DateTime") from None
    return dt if dt.tzinfo else dt.replace(tzinfo=UTC)


# ---------------------------------------------------------------- tokens

_TOKEN = re.compile(r"""
    (?P<ws>\s+)
  | (?P<num>\d+\.\d+|\d+)
  | (?P<str>'(?:[^']|'')*')
  | (?P<param>@[A-Za-z_]\w*)
  | (?P<ident>[A-Za-z_]\w*|\[[^\]]+\])
  | (?P<op><>|!=|<=|>=|[=<>+\-*/(),.])
""", re.X)

KEYWORDS = {"select", "distinct", "top", "from", "as", "where", "and", "or", "not", "in", "like", "is",
            "null", "order", "by", "asc", "desc", "group", "having", "true", "false", "with"}


@dataclass
class Tok:
    kind: str       # num, str, param, ident, kw, op, end
    value: str


def tokenize(text: str) -> list[Tok]:
    out, pos = [], 0
    while pos < len(text):
        m = _TOKEN.match(text, pos)
        if not m:
            raise SwqlError(f"Unexpected character '{text[pos]}' at position {pos}")
        pos = m.end()
        kind = m.lastgroup
        value = m.group(kind)
        if kind == "ws":
            continue
        if kind == "str":
            value = value[1:-1].replace("''", "'")
        elif kind == "ident":
            if value.startswith("["):
                value = value[1:-1]
            elif value.lower() in KEYWORDS:
                kind, value = "kw", value.lower()
        out.append(Tok(kind, value))
    out.append(Tok("end", ""))
    return out


# ---------------------------------------------------------------- AST


@dataclass
class Col:
    path: list[str]


@dataclass
class Lit:
    value: Any


@dataclass
class Param:
    name: str


@dataclass
class Func:
    name: str
    args: list[Any]
    star: bool = False


@dataclass
class Bin:
    op: str
    left: Any
    right: Any


@dataclass
class Unary:
    op: str
    item: Any


@dataclass
class InList:
    item: Any
    values: list[Any]
    negate: bool


@dataclass
class IsNull:
    item: Any
    negate: bool


@dataclass
class Query:
    entity: Entity
    alias: str | None
    items: list[tuple[Any, str | None]]
    distinct: bool = False
    top: int | None = None
    where: Any = None
    group_by: list[Any] = field(default_factory=list)
    having: Any = None
    order_by: list[tuple[Any, bool]] = field(default_factory=list)


class _Parser:
    def __init__(self, text: str):
        self.toks = tokenize(text)
        self.i = 0

    @property
    def tok(self) -> Tok:
        return self.toks[self.i]

    def next(self) -> Tok:
        t = self.toks[self.i]
        self.i += 1
        return t

    def accept(self, kind: str, value: str | None = None) -> Tok | None:
        t = self.tok
        if t.kind == kind and (value is None or t.value == value):
            self.i += 1
            return t
        return None

    def expect(self, kind: str, value: str | None = None) -> Tok:
        t = self.accept(kind, value)
        if t is None:
            want = value or kind
            got = self.tok.value or "end of query"
            raise SwqlError(f"Expected '{want}' but found '{got}'")
        return t

    def query(self) -> Query:
        self.expect("kw", "select")
        distinct = bool(self.accept("kw", "distinct"))
        top = None
        if self.accept("kw", "top"):
            top = int(self.expect("num").value)
        if self.tok.kind == "op" and self.tok.value == "*":
            raise SwqlError("SELECT * is not supported in SWQL; name the columns")
        items = [self.select_item()]
        while self.accept("op", ","):
            items.append(self.select_item())
        self.expect("kw", "from")
        name = [self.expect("ident").value]
        while self.accept("op", "."):
            name.append(self.expect("ident").value)
        entity = ENTITIES.get(".".join(name).lower())
        if entity is None:
            raise SwqlError(f"Source entity [{'.'.join(name)}] not found in catalog")
        alias = None
        if self.accept("kw", "as"):
            alias = self.expect("ident").value
        elif self.tok.kind == "ident":
            alias = self.next().value
        q = Query(entity, alias, items, distinct, top)
        if self.accept("kw", "where"):
            q.where = self.expr()
        if self.accept("kw", "group"):
            self.expect("kw", "by")
            q.group_by = [self.expr()]
            while self.accept("op", ","):
                q.group_by.append(self.expr())
        if self.accept("kw", "having"):
            q.having = self.expr()
        if self.accept("kw", "order"):
            self.expect("kw", "by")
            while True:
                e = self.expr()
                desc = bool(self.accept("kw", "desc"))
                if not desc:
                    self.accept("kw", "asc")
                q.order_by.append((e, desc))
                if not self.accept("op", ","):
                    break
        if self.tok.kind == "kw" and self.tok.value == "with":
            raise SwqlError("WITH clauses are not supported by solarwinds-sim")
        if self.tok.kind != "end":
            raise SwqlError(f"Unexpected '{self.tok.value}'")
        return q

    def select_item(self) -> tuple[Any, str | None]:
        e = self.expr()
        alias = None
        if self.accept("kw", "as"):
            alias = self.next().value
        elif self.tok.kind == "ident":
            alias = self.next().value
        return e, alias

    def expr(self) -> Any:
        left = self.and_expr()
        while self.accept("kw", "or"):
            left = Bin("OR", left, self.and_expr())
        return left

    def and_expr(self) -> Any:
        left = self.not_expr()
        while self.accept("kw", "and"):
            left = Bin("AND", left, self.not_expr())
        return left

    def not_expr(self) -> Any:
        if self.accept("kw", "not"):
            return Unary("NOT", self.not_expr())
        return self.comparison()

    def comparison(self) -> Any:
        left = self.additive()
        t = self.tok
        if t.kind == "op" and t.value in ("=", "!=", "<>", "<", "<=", ">", ">="):
            self.next()
            return Bin("!=" if t.value == "<>" else t.value, left, self.additive())
        negate = bool(self.accept("kw", "not"))
        if self.accept("kw", "like"):
            return Bin("NOT LIKE" if negate else "LIKE", left, self.additive())
        if self.accept("kw", "in"):
            self.expect("op", "(")
            values = [self.additive()]
            while self.accept("op", ","):
                values.append(self.additive())
            self.expect("op", ")")
            return InList(left, values, negate)
        if negate:
            raise SwqlError("NOT must be followed by LIKE or IN here")
        if self.accept("kw", "is"):
            neg = bool(self.accept("kw", "not"))
            self.expect("kw", "null")
            return IsNull(left, neg)
        return left

    def additive(self) -> Any:
        left = self.term()
        while self.tok.kind == "op" and self.tok.value in ("+", "-"):
            left = Bin(self.next().value, left, self.term())
        return left

    def term(self) -> Any:
        left = self.unary()
        while self.tok.kind == "op" and self.tok.value in ("*", "/"):
            left = Bin(self.next().value, left, self.unary())
        return left

    def unary(self) -> Any:
        if self.accept("op", "-"):
            item = self.unary()
            return Lit(-item.value) if isinstance(item, Lit) and isinstance(item.value, (int, float)) else Unary("-", item)
        return self.primary()

    def primary(self) -> Any:
        t = self.next()
        if t.kind == "num":
            return Lit(float(t.value) if "." in t.value else int(t.value))
        if t.kind == "str":
            return Lit(t.value)
        if t.kind == "param":
            return Param(t.value[1:])
        if t.kind == "kw" and t.value in ("true", "false"):
            return Lit(t.value == "true")
        if t.kind == "kw" and t.value == "null":
            return Lit(None)
        if t.kind == "op" and t.value == "(":
            e = self.expr()
            self.expect("op", ")")
            return e
        if t.kind == "ident":
            if self.accept("op", "("):
                if self.accept("op", "*"):
                    self.expect("op", ")")
                    return Func(t.value.upper(), [], star=True)
                args = []
                if not self.accept("op", ")"):
                    args.append(self.expr())
                    while self.accept("op", ","):
                        args.append(self.expr())
                    self.expect("op", ")")
                return Func(t.value.upper(), args)
            path = [t.value]
            while self.accept("op", "."):
                path.append(self.expect("ident").value)
            return Col(path)
        raise SwqlError(f"Unexpected '{t.value or 'end of query'}'")


def parse(text: str) -> Query:
    return _Parser(text).query()


# ---------------------------------------------------------------- compile to SQLite

AGGREGATES = {"COUNT", "SUM", "AVG", "MIN", "MAX"}
_ADD = {"ADDSECOND": "second", "ADDMINUTE": "minute", "ADDHOUR": "hour", "ADDDAY": "day"}
_DIFF = {"SECONDDIFF": 86400, "MINUTEDIFF": 1440, "HOURDIFF": 24, "DAYDIFF": 1}
_PART = {"second": "seconds", "ss": "seconds", "s": "seconds", "minute": "minutes", "mi": "minutes",
         "n": "minutes", "hour": "hours", "hh": "hours", "day": "days", "dd": "days", "d": "days",
         "week": "weeks", "wk": "weeks", "month": "months", "mm": "months", "year": "years", "yy": "years"}


@dataclass
class Compiled:
    sql: str
    params: list[Any]
    columns: list[tuple[str, str]]        # (result name, type)


class _Compiler:
    def __init__(self, q: Query, params: dict[str, Any], now: datetime):
        self.q, self.params_in, self.now = q, {k.lower(): v for k, v in params.items()}, now
        self.args: list[Any] = []
        self.joins: list[str] = []
        self.join_alias: dict[tuple[str, ...], tuple[str, Entity]] = {(): ("t0", q.entity)}

    # -------------------------------------------------------- columns and navigation

    def column(self, path: list[str]) -> tuple[str, str]:
        parts = list(path)
        if self.q.alias and len(parts) > 1 and parts[0].lower() == self.q.alias.lower():
            parts = parts[1:]
        key: tuple[str, ...] = ()
        table_alias, entity = self.join_alias[key]
        for nav in parts[:-1]:
            target = entity.navigation(nav)
            if target is None:
                raise SwqlError(f"Cannot resolve property {nav} on {entity.swis}")
            key = key + (nav.lower(),)
            if key not in self.join_alias:
                swis, fk = target
                ent = ENTITIES[swis]
                alias = f"j{len(self.join_alias)}"
                self.joins.append(f'LEFT JOIN {ent.table} {alias} ON {alias}."{fk}" = {table_alias}."{fk}"')
                self.join_alias[key] = (alias, ent)
            table_alias, entity = self.join_alias[key]
        name = entity.column(parts[-1])
        if name is None:
            raise SwqlError(f"Cannot resolve property {parts[-1]} on {entity.swis}")
        return f'{table_alias}."{name}"', entity.columns[name]

    # -------------------------------------------------------- constants

    def const(self, node: Any) -> tuple[bool, Any]:
        """Fold constant expressions (literals, parameters, date functions of constants)."""
        if isinstance(node, Lit):
            return True, node.value
        if isinstance(node, Param):
            if node.name.lower() not in self.params_in:
                raise SwqlError(f"Parameter @{node.name} was not supplied")
            return True, self.params_in[node.name.lower()]
        if isinstance(node, Unary) and node.op == "-":
            ok, v = self.const(node.item)
            return (ok, -v) if ok and isinstance(v, (int, float)) else (False, None)
        if isinstance(node, Func):
            n = node.name
            if n in ("GETUTCDATE", "GETDATE") and not node.args:
                return True, self.now
            folded = [self.const(a) for a in node.args]
            if not all(ok for ok, _ in folded):
                return False, None
            vals = [v for _, v in folded]
            if n == "ADDDATE" and len(vals) == 3:
                return True, _shift(_as_dt(vals[2]), _PART.get(str(vals[0]).lower()), vals[1])
            if n in _ADD and len(vals) == 2:
                return True, _shift(_as_dt(vals[1]), _ADD[n] + "s", vals[0])
            if n in ("TOLOCAL", "TOUTC") and len(vals) == 1:
                return True, _as_dt(vals[0])
        return False, None

    def bind(self, value: Any) -> str:
        if isinstance(value, datetime):
            value = to_db(value)
        elif isinstance(value, bool):
            value = int(value)
        self.args.append(value)
        return "?"

    # -------------------------------------------------------- expressions

    def expr(self, node: Any) -> tuple[str, str]:
        ok, value = self.const(node)
        if ok:
            kind = DATETIME if isinstance(value, datetime) else BOOL if isinstance(value, bool) else \
                INT if isinstance(value, int) else REAL if isinstance(value, float) else TEXT
            return ("NULL", TEXT) if value is None else (self.bind(value), kind)
        if isinstance(node, Col):
            return self.column(node.path)
        if isinstance(node, Unary):
            sql, kind = self.expr(node.item)
            return (f"(NOT {sql})", BOOL) if node.op == "NOT" else (f"(-{sql})", kind)
        if isinstance(node, IsNull):
            sql, _ = self.expr(node.item)
            return f"({sql} IS {'NOT ' if node.negate else ''}NULL)", BOOL
        if isinstance(node, InList):
            sql, kind = self.expr(node.item)
            vals = [self.typed(v, kind) for v in node.values]
            return f"({sql} {'NOT ' if node.negate else ''}IN ({', '.join(vals)}))", BOOL
        if isinstance(node, Bin):
            if node.op in ("AND", "OR"):
                return f"({self.expr(node.left)[0]} {node.op} {self.expr(node.right)[0]})", BOOL
            if node.op in ("=", "!=", "<", "<=", ">", ">="):
                left, lk = self.expr(node.left)
                right = self.typed(node.right, lk)
                if lk == TEXT and node.op in ("=", "!="):
                    return f"({left} {node.op} {right} COLLATE NOCASE)", BOOL
                return f"({left} {node.op} {right})", BOOL
            if node.op in ("LIKE", "NOT LIKE"):
                left, _ = self.expr(node.left)
                right, _ = self.expr(node.right)
                return f"({left} {node.op} {right})", BOOL
            left, lk = self.expr(node.left)
            right, rk = self.expr(node.right)
            if node.op == "+" and TEXT in (lk, rk):
                return f"({left} || {right})", TEXT
            if node.op == "/":
                return f"(CAST({left} AS REAL) / {right})", REAL
            return f"({left} {node.op} {right})", REAL if REAL in (lk, rk) else lk
        if isinstance(node, Func):
            return self.func(node)
        raise SwqlError(f"Unsupported expression {node!r}")

    def typed(self, node: Any, kind: str) -> str:
        """Compile the right-hand side of a comparison; a string compared with a DateTime is parsed."""
        ok, value = self.const(node)
        if ok and kind == DATETIME and isinstance(value, str):
            return self.bind(parse_datetime(value))
        return self.expr(node)[0]

    def func(self, f: Func) -> tuple[str, str]:
        n, args = f.name, f.args
        if n in AGGREGATES:
            if n == "COUNT":
                return ("COUNT(*)" if f.star else f"COUNT({self.expr(args[0])[0]})"), INT
            if len(args) != 1:
                raise SwqlError(f"{n} takes one argument")
            sql, kind = self.expr(args[0])
            return f"{n}({sql})", (REAL if n == "AVG" else kind)
        if n == "ADDDATE" and len(args) == 3:
            ok, part = self.const(args[0])
            unit = _PART.get(str(part).lower()) if ok else None
            if not unit:
                raise SwqlError("ADDDATE needs a date part such as 'minute'")
            n_sql, _ = self.expr(args[1])
            d_sql, _ = self.expr(args[2])
            return f"datetime({d_sql}, printf('%+d {unit}', {n_sql}))", DATETIME
        if n in _ADD and len(args) == 2:
            n_sql, _ = self.expr(args[0])
            d_sql, _ = self.expr(args[1])
            return f"datetime({d_sql}, printf('%+d {_ADD[n]}s', {n_sql}))", DATETIME
        if n in _DIFF and len(args) == 2:
            a, _ = self.expr(args[0])
            b, _ = self.expr(args[1])
            return f"CAST((julianday({b}) - julianday({a})) * {_DIFF[n]} AS INTEGER)", INT
        if n in ("TOLOCAL", "TOUTC") and len(args) == 1:
            return self.expr(args[0])
        if n == "ISNULL" and len(args) == 2:
            a, kind = self.expr(args[0])
            return f"IFNULL({a}, {self.expr(args[1])[0]})", kind
        if n == "ROUND" and len(args) in (1, 2):
            a, _ = self.expr(args[0])
            return f"ROUND({a}{', ' + self.expr(args[1])[0] if len(args) == 2 else ''})", REAL
        if n in ("ABS", "TOUPPER", "TOLOWER", "UPPER", "LOWER") and len(args) == 1:
            a, kind = self.expr(args[0])
            sql_fn = {"TOUPPER": "UPPER", "TOLOWER": "LOWER"}.get(n, n)
            return f"{sql_fn}({a})", kind
        raise SwqlError(f"Function {n} is not supported by solarwinds-sim")

    # -------------------------------------------------------- query

    def compile(self) -> Compiled:
        q = self.q
        select, columns, names = [], [], set()
        for i, (node, alias) in enumerate(q.items, 1):
            sql, kind = self.expr(node)
            name = alias or (node.path[-1] if isinstance(node, Col) else f"{node.name}" if isinstance(node, Func) else f"C{i}")
            while name in names:
                name += f"{i}"
            names.add(name)
            select.append(f'{sql} AS "{name}"')
            columns.append((name, kind))
        aliases = {name.lower(): f'"{name}"' for name, _ in columns}
        body = []
        if q.where is not None:
            body.append("WHERE " + self.expr(q.where)[0])
        if q.group_by:
            body.append("GROUP BY " + ", ".join(self.expr(g)[0] for g in q.group_by))
        if q.having is not None:
            body.append("HAVING " + self.expr(q.having)[0])
        if q.order_by:
            parts = []
            for node, desc in q.order_by:
                if isinstance(node, Col) and len(node.path) == 1 and node.path[0].lower() in aliases \
                        and q.entity.column(node.path[0]) is None:
                    sql = aliases[node.path[0].lower()]
                else:
                    sql = self.expr(node)[0]
                parts.append(sql + (" DESC" if desc else ""))
            body.append("ORDER BY " + ", ".join(parts))
        if q.top is not None:
            body.append(f"LIMIT {int(q.top)}")
        sql = (f"SELECT {'DISTINCT ' if q.distinct else ''}{', '.join(select)} FROM {q.entity.table} t0 "
               + " ".join(self.joins) + (" " if self.joins else "") + " ".join(body))
        return Compiled(sql, self.args, columns)


def _as_dt(v: Any) -> datetime:
    if isinstance(v, datetime):
        return v
    if isinstance(v, str):
        return parse_datetime(v)
    raise SwqlError(f"{v!r} is not a DateTime")


def _shift(dt: datetime, unit: str | None, n: Any) -> datetime:
    if not unit:
        raise SwqlError("Unknown date part")
    n = int(n)
    if unit == "months":
        month = dt.month - 1 + n
        return dt.replace(year=dt.year + month // 12, month=month % 12 + 1)
    if unit == "years":
        return dt.replace(year=dt.year + n)
    return dt + timedelta(**{unit: n})


def compile_query(text: str, params: dict[str, Any] | None = None, now: datetime | None = None) -> Compiled:
    return _Compiler(parse(text), params or {}, now or datetime.now(UTC)).compile()


def shape(rows: list[tuple], columns: list[tuple[str, str]]) -> list[dict[str, Any]]:
    """SQLite rows -> SWIS JSON results."""
    out = []
    for r in rows:
        row = {}
        for (name, kind), v in zip(columns, r):
            if kind == DATETIME and isinstance(v, str):
                v = from_db(v)
            elif kind == BOOL and v is not None:
                v = bool(v)
            row[name] = v
        out.append(row)
    return out
