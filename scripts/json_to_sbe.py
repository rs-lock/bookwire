#!/usr/bin/env python3
"""
json_to_sbe.py — encode a captured Binance depth JSONL fixture into SBE binary.

Layout follows Binance's real spot SBE schema `stream_1_0.xml`, message
`DepthDiffStreamEvent` (templateId 10003):

    messageHeader (8B, LE): blockLength:u16 templateId:u16 schemaId:u16 version:u16
    root block  (26B, LE):  eventTime:i64(us)  firstBookUpdateId:i64  lastBookUpdateId:i64
                            priceExponent:i8  qtyExponent:i8
    group bids  (groupSize16: blockLength:u16=16 numInGroup:u16) then per level:
                            price:i64(mantissa)  qty:i64(mantissa)
    group asks  (same)
    data symbol (varString8: len:u8 + ascii bytes)

Decimal value = mantissa * 10^exponent. Our fixture is fixed-point scale 8, so we
set priceExponent = qtyExponent = -8 and the mantissa IS that scaled integer.

NOTE ON FIDELITY: Binance publishes SBE only for *spot*, whose schema has no `pu`
(prevUpdateId) or `T` (transactTime). This fixture is *futures* data, so `pu`/`T`
are dropped — they don't exist in the official layout and don't affect decode cost.
The resulting bytes are therefore Binance's real DepthDiffStreamEvent layout,
populated with our depth data. Honest label: "Binance spot SBE layout, futures data".

Each message is length-delimited in the output file: a u32 LE byte length, then the
SBE message. The decoder reads [len][msg][len][msg]... until EOF.

Usage:
    python3 json_to_sbe.py <input.jsonl> [output.sbe]

Input lines may be either `<recv_ts_ns> {json}` (capture format) or bare `{json}`.
Non-depth lines (e.g. the subscribe ack) are skipped. Stdlib only.
"""

import json
import os
import struct
import sys
from decimal import Decimal

# Schema identity from stream_1_0.xml. Decode speed doesn't depend on these; the
# decoder just needs to agree. Adjust to the exact <messageSchema id=.. version=..>
# if you want byte-strict fidelity to a specific schema revision.
SCHEMA_ID = 1
SCHEMA_VERSION = 0
TEMPLATE_ID = 10003

ROOT_BLOCK_LENGTH = 26   # i64*3 + i8*2
LEVEL_BLOCK_LENGTH = 16  # i64 price + i64 qty
PRICE_EXPONENT = -8
QTY_EXPONENT = -8

SCALE = Decimal(10) ** 8  # fixed-point scale (must match core SCALE_DECIMALS = 8)

# Struct formats (all little-endian, matching SBE default byteOrder).
_HDR = struct.Struct("<HHHH")          # messageHeader
_ROOT = struct.Struct("<qqqbb")        # eventTime, firstU, lastU, priceExp, qtyExp
_GROUP_DIM = struct.Struct("<HH")      # groupSize16: blockLength, numInGroup
_LEVEL = struct.Struct("<qq")          # price mantissa, qty mantissa


def to_mantissa(s: str) -> int:
    """Decimal string -> integer mantissa at exponent -8 (i.e. value * 10^8)."""
    return int((Decimal(s) * SCALE).to_integral_value())


def encode_levels(levels) -> bytes:
    out = bytearray(_GROUP_DIM.pack(LEVEL_BLOCK_LENGTH, len(levels)))
    for price, qty in levels:
        out += _LEVEL.pack(to_mantissa(price), to_mantissa(qty))
    return bytes(out)


def encode_message(d: dict) -> bytes:
    # Futures event time is in ms; the schema field is microseconds -> *1000.
    event_time_us = int(d["E"]) * 1000
    first_u = int(d["U"])
    last_u = int(d["u"])
    symbol = str(d.get("s", "")).encode("ascii")

    body = bytearray()
    body += _ROOT.pack(event_time_us, first_u, last_u, PRICE_EXPONENT, QTY_EXPONENT)
    body += encode_levels(d.get("b", []))
    body += encode_levels(d.get("a", []))
    body += struct.pack("<B", len(symbol)) + symbol  # varString8 symbol

    header = _HDR.pack(ROOT_BLOCK_LENGTH, TEMPLATE_ID, SCHEMA_ID, SCHEMA_VERSION)
    return header + bytes(body)


def parse_line(line: str):
    """Return the depth `data` dict for a line, or None to skip it."""
    line = line.strip()
    if not line:
        return None
    # Strip an optional `<recv_ts_ns> ` capture prefix.
    first, sep, rest = line.partition(" ")
    if sep and first.isdigit() and rest.lstrip().startswith("{"):
        line = rest
    try:
        obj = json.loads(line)
    except json.JSONDecodeError:
        return None
    if not isinstance(obj, dict):
        return None  # bare number / array / stray line — not a message
    d = obj.get("data", obj)  # unwrap combined-stream envelope if present
    # A real depth diff carries U/u and level arrays; anything else (sub ack) skip.
    if not isinstance(d, dict) or "u" not in d or "U" not in d:
        return None
    return d


def main() -> int:
    if len(sys.argv) < 2:
        print(__doc__)
        return 2

    inp = sys.argv[1]
    if len(sys.argv) >= 3:
        out = sys.argv[2]
    else:
        head, tail = os.path.split(inp)
        base = tail.rsplit(".", 1)[0]
        out = os.path.join(head, f"sbe_{base}.bin")

    encoded = skipped = total_bytes = 0
    with open(inp, "r") as fin, open(out, "wb") as fout:
        for line in fin:
            d = parse_line(line)
            if d is None:
                skipped += 1
                continue
            msg = encode_message(d)
            fout.write(struct.pack("<I", len(msg)))  # length-delimited framing
            fout.write(msg)
            encoded += 1
            total_bytes += 4 + len(msg)

    print(f"encoded {encoded} messages, skipped {skipped} lines")
    print(f"wrote {total_bytes} bytes -> {out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
