"""Prefix-integer codec (RFC 7541 section 5.1 style).

Deterministic: the same (value, prefix_bits, flags) always encodes to the
same byte sequence.
"""

from __future__ import annotations


def encode_integer(value: int, prefix_bits: int, first_byte_flags: int = 0) -> bytes:
    """Encode ``value`` using a ``prefix_bits``-wide prefix.

    ``first_byte_flags`` are OR-ed into the high bits of the first byte.
    """
    raise NotImplementedError


def decode_integer(data: bytes, offset: int, prefix_bits: int) -> tuple[int, int]:
    """Decode an integer starting at ``offset``.

    Returns ``(value, new_offset)``. Raises on truncation or overflow.
    """
    raise NotImplementedError
