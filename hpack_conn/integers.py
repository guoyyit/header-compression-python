"""HPACK-style variable-length integer coding (RFC 7541 section 5.1).

Deterministic: the same (value, prefix_bits) always yields the same bytes.
"""

from __future__ import annotations


def encode_integer(value: int, prefix_bits: int, prefix_flags: int = 0) -> bytes:
    """Encode ``value`` using an N-bit prefix. Skeleton."""
    raise NotImplementedError


def decode_integer(data: bytes, prefix_bits: int, offset: int = 0) -> tuple[int, int]:
    """Decode an integer starting at ``offset``.

    Returns ``(value, next_offset)``. Skeleton.
    """
    raise NotImplementedError
