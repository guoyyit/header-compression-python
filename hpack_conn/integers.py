"""HPACK-style variable-length integer coding (RFC 7541 section 5.1).

Deterministic: the same (value, prefix_bits, prefix_flags) always yields
the same byte sequence, and decoding is the exact inverse.
"""

from __future__ import annotations

from .errors import DecodeError, RejectReason

#: Safety bound on the continuation-loop shift; 7*10 bits covers uint64
#: with margin and stops unbounded loops on hostile input.
_MAX_CONTINUATION_OCTETS = 10


def encode_integer(value: int, prefix_bits: int, prefix_flags: int = 0) -> bytes:
    """Encode ``value`` with an N-bit prefix.

    ``prefix_flags`` are the high bits OR-ed into the first octet
    (e.g. the pattern bits of the surrounding representation).
    """
    if value < 0:
        raise ValueError("cannot encode a negative integer")
    if not 1 <= prefix_bits <= 8:
        raise ValueError("prefix_bits must be in 1..8")
    if prefix_flags & 0xFF != prefix_flags:
        raise ValueError("prefix_flags must fit in one octet")

    max_prefix = (1 << prefix_bits) - 1
    if value < max_prefix:
        return bytes([prefix_flags | value])

    out = bytearray([prefix_flags | max_prefix])
    value -= max_prefix
    while value >= 0x80:
        out.append((value & 0x7F) | 0x80)
        value >>= 7
    out.append(value)
    return bytes(out)


def decode_integer(data: bytes, prefix_bits: int, offset: int = 0) -> tuple[int, int]:
    """Decode an integer starting at ``offset``.

    Returns ``(value, next_offset)``.

    Raises:
        DecodeError: ``TRUNCATED_INPUT`` if the buffer ends early, or
            ``MALFORMED_INPUT`` if the continuation is implausibly long.
    """
    if not 1 <= prefix_bits <= 8:
        raise ValueError("prefix_bits must be in 1..8")
    if offset >= len(data):
        raise DecodeError(
            RejectReason.TRUNCATED_INPUT,
            f"integer prefix missing at offset {offset}",
        )

    max_prefix = (1 << prefix_bits) - 1
    value = data[offset] & max_prefix
    if value < max_prefix:
        return value, offset + 1

    shift = 0
    pos = offset + 1
    for _ in range(_MAX_CONTINUATION_OCTETS):
        if pos >= len(data):
            raise DecodeError(
                RejectReason.TRUNCATED_INPUT,
                f"integer continuation truncated at offset {pos}",
            )
        byte = data[pos]
        pos += 1
        value += (byte & 0x7F) << shift
        shift += 7
        if not byte & 0x80:
            return value, pos
    raise DecodeError(
        RejectReason.MALFORMED_INPUT,
        "integer continuation exceeds 10 octets",
    )
