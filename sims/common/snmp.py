"""Minimal SNMPv2c: BER codec, a read-only agent and a small client.

Just enough of RFC 3416 for IF-MIB polling: GET, GETNEXT and GETBULK, with the types IF-MIB
uses (INTEGER, OCTET STRING, OID, Counter32, Gauge32, TimeTicks, Counter64). SET is answered
with notWritable: the simulated switches are read-only to everyone.
No dependencies, so the switch sim and the SolarWinds poller stay small.
"""

from __future__ import annotations

import asyncio
import bisect
import itertools
import time
from typing import Callable, Iterable

OID = tuple[int, ...]

# Tags
INTEGER, OCTET_STRING, NULL, OBJECT_ID, SEQUENCE = 0x02, 0x04, 0x05, 0x06, 0x30
IP_ADDRESS, COUNTER32, GAUGE32, TIMETICKS, COUNTER64 = 0x40, 0x41, 0x42, 0x43, 0x46
NO_SUCH_OBJECT, NO_SUCH_INSTANCE, END_OF_MIB_VIEW = 0x80, 0x81, 0x82
GET, GETNEXT, RESPONSE, SET, GETBULK = 0xA0, 0xA1, 0xA2, 0xA3, 0xA5

NOT_WRITABLE = 17
V2C = 1

Value = tuple[int, object]  # (tag, python value)


def oid(text: str) -> OID:
    return tuple(int(p) for p in text.strip(".").split("."))


def oid_str(o: OID) -> str:
    return ".".join(map(str, o))


# ---------------------------------------------------------------- BER encoding


def _len(n: int) -> bytes:
    if n < 0x80:
        return bytes([n])
    body = n.to_bytes((n.bit_length() + 7) // 8, "big")
    return bytes([0x80 | len(body)]) + body


def tlv(tag: int, body: bytes) -> bytes:
    return bytes([tag]) + _len(len(body)) + body


def _int_body(n: int, signed: bool) -> bytes:
    if signed:
        size = max(1, (n + (n < 0)).bit_length() // 8 + 1)
        return n.to_bytes(size, "big", signed=True)
    body = n.to_bytes(max(1, (n.bit_length() + 7) // 8), "big")
    return b"\x00" + body if body[0] & 0x80 else body


def _oid_body(o: OID) -> bytes:
    out = bytearray([40 * o[0] + o[1]])
    for sub in o[2:]:
        chunk = [sub & 0x7F]
        sub >>= 7
        while sub:
            chunk.append(0x80 | (sub & 0x7F))
            sub >>= 7
        out += bytes(reversed(chunk))
    return bytes(out)


def encode_value(v: Value) -> bytes:
    tag, val = v
    if tag == INTEGER:
        return tlv(tag, _int_body(int(val), signed=True))
    if tag in (COUNTER32, GAUGE32, TIMETICKS):
        return tlv(tag, _int_body(int(val) % 2**32, signed=False))
    if tag == COUNTER64:
        return tlv(tag, _int_body(int(val) % 2**64, signed=False))
    if tag == OCTET_STRING:
        return tlv(tag, val.encode() if isinstance(val, str) else bytes(val))
    if tag == OBJECT_ID:
        return tlv(tag, _oid_body(val))
    if tag == IP_ADDRESS:
        return tlv(tag, bytes(int(p) for p in str(val).split(".")))
    if tag in (NULL, NO_SUCH_OBJECT, NO_SUCH_INSTANCE, END_OF_MIB_VIEW):
        return tlv(tag, b"")
    raise ValueError(f"cannot encode tag {tag:#x}")


def encode_message(community: str, pdu_tag: int, request_id: int, a: int, b: int,
                   varbinds: Iterable[tuple[OID, Value]]) -> bytes:
    """a, b are error-status/error-index, or non-repeaters/max-repetitions for GETBULK."""
    vbs = b"".join(tlv(SEQUENCE, tlv(OBJECT_ID, _oid_body(o)) + encode_value(v)) for o, v in varbinds)
    pdu = tlv(pdu_tag, encode_value((INTEGER, request_id)) + encode_value((INTEGER, a))
              + encode_value((INTEGER, b)) + tlv(SEQUENCE, vbs))
    return tlv(SEQUENCE, encode_value((INTEGER, V2C)) + encode_value((OCTET_STRING, community)) + pdu)


# ---------------------------------------------------------------- BER decoding


def _read_tlv(data: bytes, pos: int) -> tuple[int, bytes, int]:
    tag = data[pos]
    n = data[pos + 1]
    pos += 2
    if n & 0x80:
        k = n & 0x7F
        n = int.from_bytes(data[pos:pos + k], "big")
        pos += k
    return tag, data[pos:pos + n], pos + n


def _decode_oid(body: bytes) -> OID:
    out = [body[0] // 40, body[0] % 40]
    sub = 0
    for byte in body[1:]:
        sub = (sub << 7) | (byte & 0x7F)
        if not byte & 0x80:
            out.append(sub)
            sub = 0
    return tuple(out)


def decode_value(tag: int, body: bytes) -> Value:
    if tag == INTEGER:
        return tag, int.from_bytes(body, "big", signed=True)
    if tag in (COUNTER32, GAUGE32, TIMETICKS, COUNTER64):
        return tag, int.from_bytes(body, "big", signed=False)
    if tag == OCTET_STRING:
        return tag, body
    if tag == OBJECT_ID:
        return tag, _decode_oid(body)
    if tag == IP_ADDRESS:
        return tag, ".".join(map(str, body))
    return tag, None


def _children(body: bytes) -> list[tuple[int, bytes]]:
    out, pos = [], 0
    while pos < len(body):
        tag, inner, pos = _read_tlv(body, pos)
        out.append((tag, inner))
    return out


def decode_message(data: bytes) -> tuple[int, str, int, int, int, int, list[tuple[OID, Value]]]:
    """-> (version, community, pdu_tag, request_id, a, b, varbinds)"""
    tag, body, _ = _read_tlv(data, 0)
    if tag != SEQUENCE:
        raise ValueError("not an SNMP message")
    (_, ver), (_, comm), (pdu_tag, pdu) = _children(body)
    fields = _children(pdu)
    req_id, a, b = (decode_value(INTEGER, f[1])[1] for f in fields[:3])
    varbinds = []
    for _, vb in _children(fields[3][1]):
        (_, o), (vtag, vbody) = _children(vb)
        varbinds.append((_decode_oid(o), decode_value(vtag, vbody)))
    return (decode_value(INTEGER, ver)[1], comm.decode(), pdu_tag, req_id, a, b, varbinds)


# ---------------------------------------------------------------- agent


class MibView:
    """A sorted snapshot of OID -> value, rebuilt from a callable at most every `ttl_s`."""

    def __init__(self, build: Callable[[], dict[OID, Value]], ttl_s: float = 1.0):
        self._build = build
        self._ttl = ttl_s
        self._at = -1e9
        self._oids: list[OID] = []
        self._vals: dict[OID, Value] = {}

    def _refresh(self) -> None:
        now = time.monotonic()
        if now - self._at >= self._ttl:
            self._vals = self._build()
            self._oids = sorted(self._vals)
            self._at = now

    def get(self, o: OID) -> Value:
        self._refresh()
        if o in self._vals:
            return self._vals[o]
        i = bisect.bisect_left(self._oids, o[:-1])
        column_exists = i < len(self._oids) and self._oids[i][: len(o) - 1] == o[:-1]
        return (NO_SUCH_INSTANCE if column_exists else NO_SUCH_OBJECT, None)

    def next(self, o: OID) -> tuple[OID, Value]:
        self._refresh()
        i = bisect.bisect_right(self._oids, o)
        if i >= len(self._oids):
            return o, (END_OF_MIB_VIEW, None)
        k = self._oids[i]
        return k, self._vals[k]


def handle_request(view: MibView, community: str, packet: bytes) -> bytes | None:
    try:
        ver, comm, pdu, req_id, a, b, varbinds = decode_message(packet)
    except (ValueError, IndexError):
        return None
    if ver != V2C or comm != community:
        return None  # wrong version or community: real agents stay silent
    if pdu == GET:
        out = [(o, view.get(o)) for o, _ in varbinds]
    elif pdu == GETNEXT:
        out = [view.next(o) for o, _ in varbinds]
    elif pdu == GETBULK:
        non_rep, max_rep = max(a, 0), max(b, 0)
        out = [view.next(o) for o, _ in varbinds[:non_rep]]
        cursors = [o for o, _ in varbinds[non_rep:]]
        for _ in range(min(max_rep, 64)):
            row = [view.next(c) for c in cursors]
            out += row
            cursors = [o for o, _ in row]
            if all(v[0] == END_OF_MIB_VIEW for _, v in row):
                break
    elif pdu == SET:
        return encode_message(community, RESPONSE, req_id, NOT_WRITABLE, 1, varbinds)
    else:
        return None
    return encode_message(community, RESPONSE, req_id, 0, 0, out)


class _AgentProtocol(asyncio.DatagramProtocol):
    def __init__(self, view: MibView, community: str):
        self.view, self.community = view, community

    def connection_made(self, transport):
        self.transport = transport

    def datagram_received(self, data, addr):
        reply = handle_request(self.view, self.community, data)
        if reply:
            self.transport.sendto(reply, addr)


async def serve_agent(view: MibView, host: str = "0.0.0.0", port: int = 161, community: str = "public"):
    loop = asyncio.get_running_loop()
    transport, _ = await loop.create_datagram_endpoint(lambda: _AgentProtocol(view, community), local_addr=(host, port))
    return transport


# ---------------------------------------------------------------- client


class _ClientProtocol(asyncio.DatagramProtocol):
    def __init__(self):
        self.waiters: dict[int, asyncio.Future] = {}

    def datagram_received(self, data, addr):
        try:
            msg = decode_message(data)
        except (ValueError, IndexError):
            return
        fut = self.waiters.pop(msg[3], None)
        if fut and not fut.done():
            fut.set_result(msg)


class SnmpClient:
    """async with SnmpClient("cisco-sim-1") as c: rows = await c.walk(oid("1.3.6.1.2.1.2.2.1.19"))"""

    _ids = itertools.count(1)

    def __init__(self, host: str, port: int = 161, community: str = "public", timeout_s: float = 2.0, retries: int = 1):
        self.host, self.port, self.community = host, port, community
        self.timeout_s, self.retries = timeout_s, retries

    async def __aenter__(self) -> SnmpClient:
        loop = asyncio.get_running_loop()
        self._transport, self._proto = await loop.create_datagram_endpoint(
            _ClientProtocol, remote_addr=(self.host, self.port))
        return self

    async def __aexit__(self, *exc) -> None:
        self._transport.close()

    async def _request(self, pdu: int, varbinds: list[OID], a: int = 0, b: int = 0) -> list[tuple[OID, Value]]:
        for attempt in range(self.retries + 1):
            req_id = next(self._ids) % 2**31
            fut = asyncio.get_running_loop().create_future()
            self._proto.waiters[req_id] = fut
            self._transport.sendto(encode_message(self.community, pdu, req_id, a, b, [(o, (NULL, None)) for o in varbinds]))
            try:
                _, _, _, _, err, idx, out = await asyncio.wait_for(fut, self.timeout_s)
            except TimeoutError:
                self._proto.waiters.pop(req_id, None)
                if attempt == self.retries:
                    raise
                continue
            if err:
                raise RuntimeError(f"SNMP error-status {err} at index {idx}")
            return out
        raise TimeoutError

    async def get(self, *oids: OID) -> list[tuple[OID, Value]]:
        return await self._request(GET, list(oids))

    async def walk(self, root: OID, max_rep: int = 25) -> list[tuple[OID, Value]]:
        """GETBULK walk of one subtree."""
        out, cursor = [], root
        while True:
            batch = await self._request(GETBULK, [cursor], 0, max_rep)
            for o, v in batch:
                if o[: len(root)] != root or v[0] == END_OF_MIB_VIEW:
                    return out
                out.append((o, v))
            cursor = batch[-1][0]
