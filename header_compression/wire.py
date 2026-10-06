"""Wire-format primitives shared by encoder and decoder.

Header block layout::

    <required insert count: 8-bit prefix int>
    <block sequence number: 8-bit prefix int>
    <field line>*

The block sequence number is connection-global and monotonic; the decoder
echoes it in the header block acknowledgement so the encoder can match
acks to blocks even when blocked streams decode out of order.

Field line first-byte patterns::

    1xxx xxxx  indexed, static table        (7-bit prefix index)
    01xx xxxx  indexed, dynamic table       (6-bit prefix absolute index)
    001x xxxx  literal, static name ref     (5-bit prefix index), value
    0001 xxxx  literal, dynamic name ref    (4-bit prefix abs index), value
    0000 1000  literal, literal name        name, value

Encoder-stream instruction first-byte patterns::

    1xxx xxxx  insert, static name ref      (7-bit prefix index), value
    01xx xxxx  insert, dynamic name ref     (6-bit prefix abs index), value
    0010 0000  insert, literal name         name, value
    000x xxxx  set dynamic table capacity   (5-bit prefix value)

Decoder-stream instruction first-byte patterns::

    1xxx xxxx  header block acknowledgement (7-bit prefix block seq)
    01xx xxxx  insert count increment       (6-bit prefix delta)

All multi-byte items are length-prefixed, so truncation is always
detectable; reserved bit patterns must be zero on the wire.
"""

from __future__ import annotations

from dataclasses import dataclass

from .errors import InvalidRepresentationError, TruncatedDataError
from .integers import decode_integer, encode_integer
from .strings import decode_text, encode_text


def _require(data: bytes, offset: int, what: str) -> int:
    if offset >= len(data):
        raise TruncatedDataError(f"{what} missing")
    return data[offset]


# ---------------------------------------------------------------- field lines
@dataclass(frozen=True)
class FieldLine:
    """One decoded field line.

    kind: "indexed_static" | "indexed_dynamic" | "literal_static"
        | "literal_dynamic" | "literal"
    """

    kind: str
    index: int | None = None
    name: str | None = None
    value: str | None = None


def encode_indexed_static(index: int) -> bytes:
    return encode_integer(index, 7, 0x80)


def encode_indexed_dynamic(absolute_index: int) -> bytes:
    return encode_integer(absolute_index, 6, 0x40)


def encode_literal_static_name(index: int, value: str) -> bytes:
    return encode_integer(index, 5, 0x20) + encode_text(value)


def encode_literal_dynamic_name(absolute_index: int, value: str) -> bytes:
    return encode_integer(absolute_index, 4, 0x10) + encode_text(value)


def encode_literal(name: str, value: str) -> bytes:
    return b"\x08" + encode_text(name) + encode_text(value)


def decode_field_line(data: bytes, offset: int) -> tuple[FieldLine, int]:
    first = _require(data, offset, "field line")
    if first & 0x80:
        index, offset = decode_integer(data, offset, 7)
        return FieldLine("indexed_static", index=index), offset
    if first & 0x40:
        index, offset = decode_integer(data, offset, 6)
        return FieldLine("indexed_dynamic", index=index), offset
    if first & 0x20:
        index, offset = decode_integer(data, offset, 5)
        value, offset = decode_text(data, offset)
        return FieldLine("literal_static", index=index, value=value), offset
    if first & 0x10:
        index, offset = decode_integer(data, offset, 4)
        value, offset = decode_text(data, offset)
        return FieldLine("literal_dynamic", index=index, value=value), offset
    if first == 0x08:
        name, offset = decode_text(data, offset + 1)
        value, offset = decode_text(data, offset)
        return FieldLine("literal", name=name, value=value), offset
    raise InvalidRepresentationError(f"reserved field line pattern 0x{first:02x}")


# ----------------------------------------------------- encoder instructions
@dataclass(frozen=True)
class EncoderInstruction:
    """kind: "insert_static" | "insert_dynamic" | "insert_literal"
    | "set_capacity"."""

    kind: str
    index: int | None = None
    name: str | None = None
    value: str | None = None
    capacity: int | None = None


def encode_insert_static_name(index: int, value: str) -> bytes:
    return encode_integer(index, 7, 0x80) + encode_text(value)


def encode_insert_dynamic_name(absolute_index: int, value: str) -> bytes:
    return encode_integer(absolute_index, 6, 0x40) + encode_text(value)


def encode_insert_literal(name: str, value: str) -> bytes:
    return b"\x20" + encode_text(name) + encode_text(value)


def encode_set_capacity(capacity: int) -> bytes:
    return encode_integer(capacity, 5, 0x00)


def decode_encoder_instruction(
    data: bytes, offset: int
) -> tuple[EncoderInstruction, int]:
    first = _require(data, offset, "encoder instruction")
    if first & 0x80:
        index, offset = decode_integer(data, offset, 7)
        value, offset = decode_text(data, offset)
        return EncoderInstruction("insert_static", index=index, value=value), offset
    if first & 0x40:
        index, offset = decode_integer(data, offset, 6)
        value, offset = decode_text(data, offset)
        return EncoderInstruction("insert_dynamic", index=index, value=value), offset
    if first & 0x20:
        if first & 0x1F:
            raise InvalidRepresentationError(
                f"reserved bits set in insert-literal 0x{first:02x}"
            )
        name, offset = decode_text(data, offset + 1)
        value, offset = decode_text(data, offset)
        return EncoderInstruction("insert_literal", name=name, value=value), offset
    capacity, offset = decode_integer(data, offset, 5)
    return EncoderInstruction("set_capacity", capacity=capacity), offset


# ----------------------------------------------------- decoder instructions
@dataclass(frozen=True)
class DecoderInstruction:
    """kind: "header_ack" | "insert_count_increment"."""

    kind: str
    block_seq: int | None = None
    delta: int | None = None


def encode_header_ack(block_seq: int) -> bytes:
    return encode_integer(block_seq, 7, 0x80)


def encode_insert_count_increment(delta: int) -> bytes:
    return encode_integer(delta, 6, 0x40)


def decode_decoder_instruction(
    data: bytes, offset: int
) -> tuple[DecoderInstruction, int]:
    first = _require(data, offset, "decoder instruction")
    if first & 0x80:
        block_seq, offset = decode_integer(data, offset, 7)
        return DecoderInstruction("header_ack", block_seq=block_seq), offset
    if first & 0x40:
        delta, offset = decode_integer(data, offset, 6)
        return DecoderInstruction("insert_count_increment", delta=delta), offset
    raise InvalidRepresentationError(
        f"reserved decoder instruction pattern 0x{first:02x}"
    )


# ------------------------------------------------------------ block prefix
def encode_block_prefix(required_insert_count: int, block_seq: int) -> bytes:
    return encode_integer(required_insert_count, 8) + encode_integer(block_seq, 8)


def decode_block_prefix(data: bytes, offset: int) -> tuple[int, int, int]:
    """Returns ``(required_insert_count, block_seq, new_offset)``."""
    required_insert_count, offset = decode_integer(data, offset, 8)
    block_seq, offset = decode_integer(data, offset, 8)
    return required_insert_count, block_seq, offset
