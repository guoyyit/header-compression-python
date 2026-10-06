"""Header block encoder bound to one connection's dynamic table.

Representation formats (RFC 7541 section 6):
  - Indexed:                1xxxxxxx  (7-bit prefix index)
  - Literal + incremental:  01xxxxxx  (6-bit prefix name index, inserts)
  - Table size update:      001xxxxx  (5-bit prefix, block-leading only)
  - Literal w/o indexing:   0000xxxx  (4-bit prefix name index)

Index space: static table occupies 1..N_static, the dynamic table follows
at N_static+1.. with N_static+1 naming the NEWEST dynamic entry.

After each :meth:`encode`, :attr:`required_insertions` reports the highest
insertion generation this block references from EARLIER blocks; entries
the block inserts itself are replayed by the decoder as it decodes and
therefore do not count. This lets independent streams proceed without
waiting on unrelated insertions.
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

    ``strict_limits`` selects the policy for entries that cannot be
    indexed because they exceed the table capacity / memory ceiling:
    ``False`` (default) emits them without indexing (RFC 7541 style);
    ``True`` rejects the encode with a distinguishable
    :class:`~hpack_conn.errors.CapacityError` reason instead.
    """

    def __init__(
        self,
        table: DynamicTable,
        static_table: StaticTable | None = None,
        indexing: bool = True,
        strict_limits: bool = False,
    ) -> None:
        self._table = table
        self._static = static_table or StaticTable()
        self._indexing = indexing
        self._strict_limits = strict_limits
        self._pending_size_updates: list[bytes] = []
        self._required_insertions = 0
        self._ref_floor = 0
        self._uses_dynamic = False

    @property
    def table(self) -> DynamicTable:
        return self._table

    @property
    def required_insertions(self) -> int:
        """Highest earlier-block insertion generation the last encoded
        block depends on (0 = no dependency)."""
        return self._required_insertions

    @property
    def last_block_uses_dynamic(self) -> bool:
        """Whether the last encoded block touches the dynamic table
        (size update, insertion, or dynamic reference). Such blocks must
        be applied by the peer in encode order; blocks that do not touch
        the table may be decoded in any order."""
        return self._uses_dynamic

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
        self._uses_dynamic = bool(self._pending_size_updates)
        for update in self._pending_size_updates:
            out += update
        self._pending_size_updates.clear()
        # Generations up to this point belong to earlier blocks; anything
        # newer is inserted by this block itself and self-satisfied.
        self._ref_floor = self._table.insertion_count
        self._required_insertions = 0
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
                self._track_reference(dyn_index)
                self._uses_dynamic = True
                return encode_integer(self._combined(dyn_index), 7, _FLAG_INDEXED)

        # No exact match: emit a literal. Use the best available name
        # index (dynamic first, then static) to keep the block small.
        name_index = 0
        if dyn_index is not None:
            # A name reference into the dynamic table is a dependency too.
            self._track_reference(dyn_index)
            self._uses_dynamic = True
            name_index = self._combined(dyn_index)
        elif static_index is not None:
            name_index = static_index

        if self._indexing and HeaderField(name, value).size <= self._table.capacity:
            self._table.insert(name, value)
            self._uses_dynamic = True
            return (
                encode_integer(name_index, 6, _FLAG_INCREMENTAL)
                + (b"" if name_index else encode_string(name))
                + encode_string(value)
            )
        if self._indexing and self._strict_limits:
            # The entry cannot be indexed; surface the distinguishable
            # rejection reason from the table instead of silently
            # degrading. The table validates before mutating, so state
            # is untouched by the failure.
            self._table.insert(name, value)
        return (
            encode_integer(name_index, 4, 0x00)
            + (b"" if name_index else encode_string(name))
            + encode_string(value)
        )

    def _combined(self, dyn_index: int) -> int:
        """Map a 1-based dynamic index into the combined index space."""
        return len(self._static) + dyn_index

    def _track_reference(self, dyn_index: int) -> None:
        """Record a dependency on the entry at ``dyn_index`` if it was
        inserted by an earlier block (self-inserted entries excluded)."""
        generation = self._table.insertion_count - dyn_index + 1
        if generation <= self._ref_floor:
            self._required_insertions = max(self._required_insertions, generation)

    def split_combined(self, index: int) -> tuple[str, int]:
        """Inverse of :meth:`_combined`; exposed for tests/symmetry."""
        if 1 <= index <= len(self._static):
            return "static", index
        if index > len(self._static):
            return "dynamic", index - len(self._static)
        raise EncodeError(RejectReason.INVALID_INDEX, f"index {index} is zero")
