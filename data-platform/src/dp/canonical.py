"""Canonical row encoding (spec §5).

Sorted columns, UTF-8, explicit null, ints in decimal, floats as IEEE-754 binary64 bits in hex with
one canonical NaN, -0.0 kept distinct, timestamps already as UTC nanosecond ints, lists in order,
maps with sorted keys. Every value is type-tagged and length-prefixed, so no two values share bytes.
"""

import hashlib
import math
import struct

_NAN_BITS = "7ff8000000000000"


def _enc(v: object) -> bytes:
    if v is None:
        return b"N;"
    if isinstance(v, bool):
        return b"B1;" if v else b"B0;"
    if isinstance(v, int):
        return b"I" + str(v).encode() + b";"
    if isinstance(v, float):
        bits = _NAN_BITS if math.isnan(v) else struct.pack(">d", v).hex()
        return b"F" + bits.encode() + b";"
    if isinstance(v, str):
        b = v.encode("utf-8")
        return b"S" + str(len(b)).encode() + b":" + b + b";"
    if isinstance(v, (bytes, bytearray)):
        return b"Y" + str(len(v)).encode() + b":" + bytes(v) + b";"
    if isinstance(v, (list, tuple)):
        return b"L" + str(len(v)).encode() + b":" + b"".join(_enc(x) for x in v) + b";"
    if isinstance(v, dict):
        items = sorted(v.items(), key=lambda kv: str(kv[0]))
        body = b"".join(_enc(str(k)) + _enc(x) for k, x in items)
        return b"M" + str(len(items)).encode() + b":" + body + b";"
    raise TypeError(f"no canonical encoding for {type(v).__name__}")


def encode_row(row: dict) -> bytes:
    return _enc(dict(row))


def row_hash(row: dict) -> str:
    return hashlib.sha256(encode_row(row)).hexdigest()


def digest(items: list[bytes]) -> str:
    """Order-independent digest of a multiset of encoded rows, each length-prefixed."""
    h = hashlib.sha256()
    for it in sorted(items):
        h.update(str(len(it)).encode() + b":" + it)
    return h.hexdigest()
