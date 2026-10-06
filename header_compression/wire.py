"""Wire-format primitives shared by encoder and decoder.

Header block layout::

    <required insert count: 8-bit prefix int>
    <field line>*

Field line first-byte patterns::

    1xxx xxxx  indexed, static table        (7-bit prefix index)
    01xx xxxx  indexed, dynamic table       (6-bit prefix absolute index)
    001x xxxx  literal, static name ref     (5-bit prefix index), value
    0001 xxxx  literal, dynamic name ref    (4-bit prefix abs index), value
    0000 1xxx  literal, literal name        name, value

Encoder-stream instruction first-byte patterns::

    1xxx xxxx  insert, static name ref      (7-bit prefix index), value
    01xx xxxx  insert, dynamic name ref     (6-bit prefix abs index), value
    001x xxxx  insert, literal name         name, value
    000x xxxx  set dynamic table capacity   (6-bit prefix value)

Decoder-stream instruction first-byte patterns::

    1xxx xxxx  header block acknowledgement (7-bit prefix stream id)
    01xx xxxx  insert count increment       (6-bit prefix delta)
"""

from __future__ import annotations

from dataclasses import dataclass


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
    raise NotImplementedError


def encode_indexed_dynamic(absolute_index: int) -> bytes:
    raise NotImplementedError


def encode_literal_static_name(index: int, value: str) -> bytes:
    raise NotImplementedError


def encode_literal_dynamic_name(absolute_index: int, value: str) -> bytes:
    raise NotImplementedError


def encode_literal(name: str, value: str) -> bytes:
    raise NotImplementedError


def decode_field_line(data: bytes, offset: int) -> tuple[FieldLine, int]:
    raise NotImplementedError


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
    raise NotImplementedError


def encode_insert_dynamic_name(absolute_index: int, value: str) -> bytes:
    raise NotImplementedError


def encode_insert_literal(name: str, value: str) -> bytes:
    raise NotImplementedError


def encode_set_capacity(capacity: int) -> bytes:
    raise NotImplementedError


def decode_encoder_instruction(
    data: bytes, offset: int
) -> tuple[EncoderInstruction, int]:
    raise NotImplementedError


# ----------------------------------------------------- decoder instructions
@dataclass(frozen=True)
class DecoderInstruction:
    """kind: "header_ack" | "insert_count_increment"."""

    kind: str
    stream_id: int | None = None
    delta: int | None = None


def encode_header_ack(stream_id: int) -> bytes:
    raise NotImplementedError


def encode_insert_count_increment(delta: int) -> bytes:
    raise NotImplementedError


def decode_decoder_instruction(
    data: bytes, offset: int
) -> tuple[DecoderInstruction, int]:
    raise NotImplementedError


# ------------------------------------------------------------ block prefix
def encode_block_prefix(required_insert_count: int) -> bytes:
    raise NotImplementedError


def decode_block_prefix(data: bytes, offset: int) -> tuple[int, int]:
    raise NotImplementedError
