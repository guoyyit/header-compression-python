"""String literal coding (RFC 7541 section 5.2, without Huffman coding).

Plain length-prefixed UTF-8 literals keep output deterministic and
byte-for-byte reproducible for identical input and configuration.
The high bit of the length octet is reserved (Huffman flag, always 0).
"""

from __future__ import annotations

from .errors import DecodeError, RejectReason
from .integers import decode_integer, encode_integer

_HUFFMAN_FLAG = 0x80


def encode_string(value: str, prefix_flags: int = 0) -> bytes:
    """Encode a string literal as length-prefixed UTF-8 octets."""
    raw = value.encode("utf-8")
    return encode_integer(len(raw), 7, prefix_flags) + raw


def decode_string(data: bytes, offset: int = 0) -> tuple[str, int]:
    """Decode a string literal starting at ``offset``.

    Returns ``(value, next_offset)``.

    Raises:
        DecodeError: on truncated input, Huffman-flagged input (not
            supported), or invalid UTF-8.
    """
    if offset >= len(data):
        raise DecodeError(
            RejectReason.TRUNCATED_INPUT,
            f"string length missing at offset {offset}",
        )
    if data[offset] & _HUFFMAN_FLAG:
        raise DecodeError(
            RejectReason.MALFORMED_INPUT,
            "Huffman-coded strings are not supported by this profile",
        )
    length, pos = decode_integer(data, 7, offset)
    end = pos + length
    if end > len(data):
        raise DecodeError(
            RejectReason.TRUNCATED_INPUT,
            f"string body truncated: need {length} octets at {pos}, "
            f"have {len(data) - pos}",
        )
    try:
        value = data[pos:end].decode("utf-8")
    except UnicodeDecodeError as exc:
        raise DecodeError(
            RejectReason.MALFORMED_INPUT, f"invalid UTF-8 in string: {exc}"
        ) from exc
    return value, end
