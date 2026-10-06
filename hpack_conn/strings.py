"""String literal coding (RFC 7541 section 5.2, without Huffman coding).

Plain length-prefixed UTF-8 literals keep output deterministic and
byte-for-byte reproducible for identical input and configuration.
"""

from __future__ import annotations


def encode_string(value: str, prefix_flags: int = 0) -> bytes:
    """Encode a string literal. Skeleton."""
    raise NotImplementedError


def decode_string(data: bytes, offset: int = 0) -> tuple[str, int]:
    """Decode a string literal starting at ``offset``.

    Returns ``(value, next_offset)``. Skeleton.
    """
    raise NotImplementedError
