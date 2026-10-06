"""Header block decoder bound to one connection's dynamic table.

Mirrors :mod:`hpack_conn.encoder`. Decoding mutates the decoder-side
dynamic table exactly as the encoder mutated its own, so both ends evolve
through the same deterministic sequence of table states.
"""

from __future__ import annotations

from .errors import CompressionError, DecodeError, RejectReason
from .integers import decode_integer
from .strings import decode_string
from .tables import DynamicTable, HeaderField, StaticTable

_FLAG_INDEXED = 0x80
_FLAG_INCREMENTAL = 0x40
_FLAG_SIZE_UPDATE = 0x20
_FLAG_NEVER_INDEXED = 0x10


class Decoder:
    """Decodes header blocks against the connection dynamic table."""

    def __init__(self, table: DynamicTable, static_table: StaticTable | None = None) -> None:
        self._table = table
        self._static = static_table or StaticTable()

    @property
    def table(self) -> DynamicTable:
        return self._table

    def set_capacity(self, capacity: int) -> None:
        """Apply a negotiated capacity change signalled by the peer."""
        self._table.set_capacity(capacity)

    def decode(self, data: bytes) -> list[tuple[str, str]]:
        """Decode one header block.

        Raises:
            DecodeError: on malformed, truncated or out-of-range input.
                A failure never mutates the table beyond the insertions
                the peer actually performed before the fault; callers
                treat any DecodeError as connection-fatal (see
                :class:`ConnectionContext`) so state cannot diverge.
        """
        headers: list[tuple[str, str]] = []
        pos = 0
        saw_field = False
        while pos < len(data):
            byte = data[pos]
            if byte & _FLAG_INDEXED:
                index, pos = decode_integer(data, 7, pos)
                headers.append(self._resolve(index))
                saw_field = True
            elif byte & _FLAG_INCREMENTAL:
                name, value, pos = self._decode_literal(data, pos, 6)
                try:
                    self._table.insert(name, value)
                except CompressionError as exc:
                    raise DecodeError(exc.reason, str(exc)) from exc
                headers.append((name, value))
                saw_field = True
            elif byte & _FLAG_SIZE_UPDATE:
                if saw_field:
                    raise DecodeError(
                        RejectReason.MALFORMED_INPUT,
                        "table size update after header fields",
                    )
                capacity, pos = decode_integer(data, 5, pos)
                try:
                    self._table.set_capacity(capacity)
                except CompressionError as exc:
                    # Re-raise as a decode failure so the peer's
                    # violation is observable at this layer.
                    raise DecodeError(exc.reason, str(exc)) from exc
            else:  # 0000xxxx without indexing / 0001xxxx never indexed
                name, value, pos = self._decode_literal(data, pos, 4)
                headers.append((name, value))
                saw_field = True
        return headers

    def _decode_literal(
        self, data: bytes, pos: int, prefix_bits: int
    ) -> tuple[str, str, int]:
        name_index, pos = decode_integer(data, prefix_bits, pos)
        if name_index == 0:
            name, pos = decode_string(data, pos)
        else:
            name = self._resolve(name_index)[0]
        value, pos = decode_string(data, pos)
        return name, value, pos

    def _resolve(self, index: int) -> tuple[str, str]:
        if index == 0:
            raise DecodeError(RejectReason.INVALID_INDEX, "index 0 is reserved")
        if index <= len(self._static):
            entry = self._static.get(index)
            return entry.name, entry.value
        dyn_index = index - len(self._static)
        entry = self._table.get(dyn_index)  # raises INVALID_INDEX if absent
        return entry.name, entry.value
