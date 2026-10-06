"""Stateful encoder endpoint (one per connection).

The encoder owns the encoder-side view of the dynamic table, emits encoder
stream instructions (insertions, capacity changes) and header blocks, and
tracks acknowledgements coming back on the decoder stream so it never
evicts an entry an unacknowledged header block still references.

Encoding is deterministic: for the same sequence of calls (including the
same acknowledgement deliveries) the emitted bytes are identical.
"""

from __future__ import annotations

import threading

from . import wire
from .errors import (
    CapacityExceededError,
    CapacityReductionBlockedError,
    DecoderStreamError,
)
from .tables import DynamicTable, entry_size, find_static


class EncoderEndpoint:
    """Encodes header lists into header blocks; thread-safe."""

    def __init__(
        self,
        max_table_capacity: int,
        table_capacity: int = 0,
        sensitive_headers: frozenset[str] = frozenset(),
    ) -> None:
        if table_capacity > max_table_capacity:
            raise ValueError("table capacity exceeds maximum")
        self._lock = threading.RLock()
        self._table = DynamicTable(max_table_capacity)
        self._sensitive = frozenset(h.lower() for h in sensitive_headers)
        self._pending = bytearray()
        self._known_received_count = 0
        self._block_seq = 0
        # block_seq -> absolute indices referenced by that unacked block
        self._unacked: dict[int, tuple[int, ...]] = {}
        # absolute index -> number of unacked blocks referencing it
        self._refcounts: dict[int, int] = {}
        if table_capacity:
            self._set_capacity_locked(table_capacity)

    # ------------------------------------------------------------ properties
    @property
    def insert_count(self) -> int:
        """Absolute index the next inserted entry will get."""
        return self._table.insert_count

    @property
    def known_received_count(self) -> int:
        """Insert count the peer decoder has acknowledged receiving."""
        return self._known_received_count

    @property
    def capacity(self) -> int:
        return self._table.capacity

    @property
    def table(self) -> DynamicTable:
        """Encoder-side view of the dynamic table (read-only use)."""
        return self._table

    @property
    def pending_ack_count(self) -> int:
        """Number of header blocks not yet acknowledged by the peer."""
        return len(self._unacked)

    # --------------------------------------------------------- capacity mgmt
    def set_capacity(self, capacity: int) -> None:
        """Queue a Set Dynamic Table Capacity instruction.

        The change takes effect at a well-defined point in the encoder
        stream, so both peers apply it at the same position in the
        insertion order. Raises CapacityExceededError if above our
        maximum, or CapacityReductionBlockedError if the reduction would
        evict entries still referenced by unacknowledged header blocks.
        """
        with self._lock:
            if capacity > self._table.max_capacity:
                raise CapacityExceededError(
                    f"capacity {capacity} exceeds maximum {self._table.max_capacity}"
                )
            self._set_capacity_locked(capacity)

    def _set_capacity_locked(self, capacity: int) -> None:
        if capacity < self._table.capacity:
            blocked = self._guarded_evictions(capacity)
            if blocked:
                raise CapacityReductionBlockedError(
                    f"reducing capacity to {capacity} would evict {blocked} "
                    "entries still referenced by unacknowledged header blocks"
                )
        self._table.set_capacity(capacity)
        self._pending += wire.encode_set_capacity(capacity)

    def _guarded_evictions(self, capacity: int) -> int:
        """Count live entries that a shrink to ``capacity`` would evict and
        that are still referenced by unacknowledged header blocks."""
        size = self._table.size
        blocked = 0
        for entry in reversed(self._table.entries_newest_first()):  # oldest first
            if size <= capacity:
                break
            if self._refcounts.get(entry.absolute_index, 0):
                blocked += 1
            size -= entry.size
        return blocked

    def _can_insert_locked(self, size: int) -> bool:
        """True if inserting an entry of ``size`` evicts only entries no
        unacknowledged header block references."""
        if size > self._table.capacity:
            return False
        current = self._table.size
        for entry in reversed(self._table.entries_newest_first()):  # oldest first
            if current + size <= self._table.capacity:
                return True
            if self._refcounts.get(entry.absolute_index, 0):
                return False
            current -= entry.size
        return True

    # -------------------------------------------------------------- encoding
    def encode_header_block(
        self, stream_id: int, headers: list[tuple[str, str]]
    ) -> bytes:
        """Encode one header list for ``stream_id``; deterministic."""
        with self._lock:
            body = bytearray()
            refs: list[int] = []
            for name, value in headers:
                name = name.lower()
                body += self._encode_field(name, value, refs)
            block_seq = self._block_seq
            self._block_seq += 1
            if refs:
                self._unacked[block_seq] = tuple(refs)
                for index in refs:
                    self._refcounts[index] = self._refcounts.get(index, 0) + 1
            ric = max(refs) + 1 if refs else 0
            return wire.encode_block_prefix(ric, block_seq) + bytes(body)

    def _encode_field(self, name: str, value: str, refs: list[int]) -> bytes:
        if name in self._sensitive:
            return wire.encode_literal(name, value)
        static_index, static_exact = find_static(name, value)
        if static_exact:
            return wire.encode_indexed_static(static_index)  # type: ignore[arg-type]
        dyn_entry, dyn_exact = self._table.find(name, value)
        if dyn_exact and dyn_entry is not None:
            refs.append(dyn_entry.absolute_index)
            return wire.encode_indexed_dynamic(dyn_entry.absolute_index)
        if self._can_insert_locked(entry_size(name, value)):
            refs.append(self._table.insert_count)
            if static_index is not None:
                self._pending += wire.encode_insert_static_name(static_index, value)
            elif dyn_entry is not None:
                self._pending += wire.encode_insert_dynamic_name(
                    dyn_entry.absolute_index, value
                )
            else:
                self._pending += wire.encode_insert_literal(name, value)
            entry = self._table.insert(name, value)
            return wire.encode_indexed_dynamic(entry.absolute_index)
        if static_index is not None:
            return wire.encode_literal_static_name(static_index, value)
        return wire.encode_literal(name, value)

    # ------------------------------------------------------------ stream I/O
    def drain_encoder_stream(self) -> bytes:
        """Return and clear pending encoder-stream instructions."""
        with self._lock:
            data = bytes(self._pending)
            self._pending.clear()
            return data

    def feed_decoder_stream(self, data: bytes) -> None:
        """Apply decoder-stream instructions (acks / count increments)."""
        with self._lock:
            offset = 0
            while offset < len(data):
                instruction, offset = wire.decode_decoder_instruction(data, offset)
                if instruction.kind == "header_ack":
                    self._apply_ack_locked(instruction.block_seq)  # type: ignore[arg-type]
                else:
                    self._apply_increment_locked(instruction.delta)  # type: ignore[arg-type]

    def _apply_ack_locked(self, block_seq: int) -> None:
        refs = self._unacked.pop(block_seq, None)
        if refs is None:
            raise DecoderStreamError(
                f"ack for unknown or already-acked block {block_seq}"
            )
        for index in refs:
            remaining = self._refcounts.get(index, 0) - 1
            if remaining > 0:
                self._refcounts[index] = remaining
            else:
                self._refcounts.pop(index, None)

    def _apply_increment_locked(self, delta: int) -> None:
        if delta <= 0:
            raise DecoderStreamError("insert count increment must be positive")
        new_count = self._known_received_count + delta
        if new_count > self._table.insert_count:
            raise DecoderStreamError(
                f"insert count increment to {new_count} exceeds "
                f"insert count {self._table.insert_count}"
            )
        self._known_received_count = new_count
