"""Prefix-integer codec (RFC 7541 section 5.1 style).

Deterministic: the same (value, prefix_bits, flags) always encodes to the
same byte sequence.
"""

from __future__ import annotations

from .errors import IntegerDecodingError, TruncatedDataError

#: Integers are limited to 63 bits so peers on any platform agree.
MAX_INTEGER = (1 << 63) - 1


def encode_integer(value: int, prefix_bits: int, first_byte_flags: int = 0) -> bytes:
    """Encode ``value`` using a ``prefix_bits``-wide prefix.

    ``first_byte_flags`` are OR-ed into the high bits of the first byte.
    """
    if not 0 <= value <= MAX_INTEGER:
        raise ValueError(f"integer out of range: {value}")
    if not 1 <= prefix_bits <= 8:
        raise ValueError(f"bad prefix width: {prefix_bits}")
    prefix_max = (1 << prefix_bits) - 1
    if first_byte_flags & prefix_max:
        raise ValueError("flag bits overlap the integer prefix")
    if value < prefix_max:
        return bytes((first_byte_flags | value,))
    out = bytearray((first_byte_flags | prefix_max,))
    value -= prefix_max
    while value >= 0x80:
        out.append((value & 0x7F) | 0x80)
        value >>= 7
    out.append(value)
    return bytes(out)


def decode_integer(data: bytes, offset: int, prefix_bits: int) -> tuple[int, int]:
    """Decode an integer starting at ``offset``.

    Returns ``(value, new_offset)``.
    """
    if not 1 <= prefix_bits <= 8:
        raise ValueError(f"bad prefix width: {prefix_bits}")
    if offset >= len(data):
        raise TruncatedDataError("integer prefix missing")
    prefix_max = (1 << prefix_bits) - 1
    value = data[offset] & prefix_max
    offset += 1
    if value < prefix_max:
        return value, offset
    shift = 0
    while True:
        if offset >= len(data):
            raise TruncatedDataError("integer continuation truncated")
        byte = data[offset]
        offset += 1
        value += (byte & 0x7F) << shift
        shift += 7
        if value > MAX_INTEGER or shift > 63:
            raise IntegerDecodingError("integer overflow")
        if not byte & 0x80:
            return value, offset
