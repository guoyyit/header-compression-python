"""String literal codec.

Wire format: one length prefix (7-bit prefix integer, high bit reserved and
always 0 -- no Huffman) followed by raw UTF-8 bytes. Deterministic by
construction.
"""

from __future__ import annotations

from .errors import StringDecodingError, TruncatedDataError
from .integers import decode_integer, encode_integer


def encode_string(value: bytes) -> bytes:
    """Encode raw bytes as a string literal."""
    return encode_integer(len(value), 7) + value


def decode_string(data: bytes, offset: int) -> tuple[bytes, int]:
    """Decode a string literal at ``offset``; returns ``(raw, new_offset)``."""
    length, offset = decode_integer(data, offset, 7)
    end = offset + length
    if end > len(data):
        raise TruncatedDataError(
            f"string literal truncated: want {length} bytes, have {len(data) - offset}"
        )
    return data[offset:end], end


def encode_text(value: str) -> bytes:
    """Encode a ``str`` as a string literal (UTF-8)."""
    return encode_string(value.encode("utf-8"))


def decode_text(data: bytes, offset: int) -> tuple[str, int]:
    """Decode a string literal at ``offset`` into ``str``."""
    raw, offset = decode_string(data, offset)
    try:
        return raw.decode("utf-8"), offset
    except UnicodeDecodeError as exc:
        raise StringDecodingError(f"invalid UTF-8 in string literal: {exc}") from exc
