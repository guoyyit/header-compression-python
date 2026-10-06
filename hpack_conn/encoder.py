"""Header block encoder bound to one connection's dynamic table.

Representation formats (RFC 7541 section 6):
  - Indexed:                1xxxxxxx  (7-bit prefix index)
  - Literal + incremental:  01xxxxxx  (6-bit prefix name index, inserts)
  - Table size update:      001xxxxx  (5-bit prefix, block-leading only)
  - Literal w/o indexing:   0000xxxx  (4-bit prefix name index)

Index space: static table occupies 1..N_static, the dynamic table follows
at N_static+1.. with N_static+1 naming the NEWEST dynamic entry.
"""

from __future__ import annotations

from .errors import EncodeError, RejectReason
from .integers import encode_integer
from .strings import encode_string
from .tables import DynamicTable, HeaderField, StaticTable

_FLAG_INDEXED = 0x80
_FLAG_INCREMENTAL = 0x40
_FLAG_SIZE_UPDATE = 0x20


class Encoder:
    """Encodes header lists into header blocks.

    Deterministic: identical (header sequence, configuration) always
    produces identical byte sequences.
    """

    def __init__(
        self,
        table: DynamicTable,
        static_table: StaticTable | None = None,
        indexing: bool = True,
    ) -> None:
        self._table = table
        self._static = static_table or StaticTable()
        self._indexing = indexing
        self._pending_size_updates: list[bytes] = []

    @property
    def table(self) -> DynamicTable:
        return self._table

    def set_capacity(self, capacity: int) -> bytes:
        """Apply a negotiated capacity change.

        The size-update representation is queued to lead the next emitted
        header block (so the peer applies it before decoding any reference
        in that block) and also returned for inspection.
        """
        self._table.set_capacity(capacity)
        update = encode_integer(capacity, 5, _FLAG_SIZE_UPDATE)
        self._pending_size_updates.append(update)
        return update

    def encode(self, headers: list[tuple[str, str]]) -> bytes:
        """Encode a header list into one header block."""
        out = bytearray()
        for update in self._pending_size_updates:
            out += update
        self._pending_size_updates.clear()
        for name, value in headers:
            out += self._encode_field(name, value)
        return bytes(out)

    def _encode_field(self, name: str, value: str) -> bytes:
        static_index = self._static.find(name, value)
        if static_index is not None:
            entry = self._static.get(static_index)
            if entry.value == value:
                return encode_integer(static_index, 7, _FLAG_INDEXED)

        dyn_index = self._table.find(name, value)
        if dyn_index is not None:
            entry = self._table.get(dyn_index)
            if entry.value == value:
                return encode_integer(self._combined(dyn_index), 7, _FLAG_INDEXED)

        # No exact match: emit a literal. Use the best available name
        # index (dynamic first, then static) to keep the block small.
        name_index = 0
        if dyn_index is not None:
            name_index = self._combined(dyn_index)
        elif static_index is not None:
            name_index = static_index

        if self._indexing and HeaderField(name, value).size <= self._table.capacity:
            self._table.insert(name, value)
            return (
                encode_integer(name_index, 6, _FLAG_INCREMENTAL)
                + (b"" if name_index else encode_string(name))
                + encode_string(value)
            )
        return (
            encode_integer(name_index, 4, 0x00)
            + (b"" if name_index else encode_string(name))
            + encode_string(value)
        )

    def _combined(self, dyn_index: int) -> int:
        """Map a 1-based dynamic index into the combined index space."""
        return len(self._static) + dyn_index

    def split_combined(self, index: int) -> tuple[str, int]:
        """Inverse of :meth:`_combined`; exposed for tests/symmetry."""
        if 1 <= index <= len(self._static):
            return "static", index
        if index > len(self._static):
            return "dynamic", index - len(self._static)
        raise EncodeError(RejectReason.INVALID_INDEX, f"index {index} is zero")
