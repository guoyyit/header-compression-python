"""String literal codec.

Wire format: one length prefix (7-bit prefix integer, high bit reserved and
always 0 -- no Huffman) followed by raw UTF-8 bytes. Deterministic by
construction.
"""

from __future__ import annotations


def encode_string(value: bytes) -> bytes:
    """Encode raw bytes as a string literal."""
    raise NotImplementedError


def decode_string(data: bytes, offset: int) -> tuple[bytes, int]:
    """Decode a string literal at ``offset``; returns ``(raw, new_offset)``."""
    raise NotImplementedError


def encode_text(value: str) -> bytes:
    """Encode a ``str`` as a string literal (UTF-8)."""
    raise NotImplementedError


def decode_text(data: bytes, offset: int) -> tuple[str, int]:
    """Decode a string literal at ``offset`` into ``str``."""
    raise NotImplementedError
