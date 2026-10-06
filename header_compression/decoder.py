"""Stateful decoder endpoint (one per connection).

Applies encoder-stream instructions in order (the single authoritative
insertion order), decodes header blocks, parks streams whose required
insert count has not arrived yet, and emits decoder-stream
acknowledgements.

Blocking is logical, not thread-blocking: a parked stream's bytes are
stored and decoded once its dependencies arrive, so independent streams
always proceed and there is no lock to deadlock on. The number of parked
streams is bounded by ``max_blocked_streams``.
"""

from __future__ import annotations

import threading
from collections import OrderedDict

from . import wire
from .errors import (
    BlockedStreamLimitError,
    CapacityExceededError,
    UnknownIndexError,
)
from .tables import STATIC_TABLE, DynamicTable


class DecoderEndpoint:
    """Decodes header blocks; thread-safe."""

    def __init__(self, max_table_capacity: int, max_blocked_streams: int) -> None:
        if max_blocked_streams <= 0:
            raise ValueError("max_blocked_streams must be positive")
        self._lock = threading.RLock()
        self._table = DynamicTable(max_table_capacity)
        self._table.set_capacity(0)  # encoder must open the table explicitly
        self._max_blocked_streams = max_blocked_streams
        self._received_insert_count = 0
        # (stream_id, block_seq) -> (required_insert_count, raw block)
        self._blocked: "OrderedDict[tuple[int, int], tuple[int, bytes]]" = OrderedDict()
        # block_seq -> decoded headers (doubles as the acked-set)
        self._decoded: "OrderedDict[int, list[tuple[str, str]]]" = OrderedDict()
        self._decoded_cache_limit = max(128, 2 * max_blocked_streams)
        self._pending = bytearray()

    # ------------------------------------------------------------ properties
    @property
    def received_insert_count(self) -> int:
        return self._received_insert_count

    @property
    def blocked_stream_ids(self) -> list[int]:
        return sorted({stream_id for stream_id, _ in self._blocked})

    @property
    def blocked_count(self) -> int:
        return len(self._blocked)

    @property
    def table(self) -> DynamicTable:
        """Decoder-side view of the dynamic table (read-only use)."""
        return self._table

    # ------------------------------------------------------- encoder stream
    def feed_encoder_stream(
        self, data: bytes
    ) -> list[tuple[int, list[tuple[str, str]]]]:
        """Apply encoder-stream bytes; returns newly unblocked
        ``(stream_id, headers)`` pairs in stream-id order.

        Wire-format parsing happens before any state mutation, so
        malformed input never leaves a half-applied batch behind.
        """
        with self._lock:
            instructions: list[wire.EncoderInstruction] = []
            offset = 0
            while offset < len(data):
                instruction, offset = wire.decode_encoder_instruction(data, offset)
                instructions.append(instruction)
            inserted = 0
            for instruction in instructions:
                inserted += self._apply_locked(instruction)
            if inserted:
                self._received_insert_count += inserted
                self._pending += wire.encode_insert_count_increment(inserted)
            return self._unblock_locked()

    def _apply_locked(self, instruction: wire.EncoderInstruction) -> int:
        kind = instruction.kind
        if kind == "set_capacity":
            capacity = instruction.capacity
            assert capacity is not None
            if capacity > self._table.max_capacity:
                raise CapacityExceededError(
                    f"peer set capacity {capacity} above maximum "
                    f"{self._table.max_capacity}"
                )
            self._table.set_capacity(capacity)
            return 0
        if kind == "insert_static":
            index = instruction.index
            assert index is not None and instruction.value is not None
            if index >= len(STATIC_TABLE):
                raise UnknownIndexError(f"static index {index} out of range")
            self._table.insert(STATIC_TABLE[index][0], instruction.value)
            return 1
        if kind == "insert_dynamic":
            index = instruction.index
            assert index is not None and instruction.value is not None
            name = self._table.get(index).name
            self._table.insert(name, instruction.value)
            return 1
        if kind == "insert_literal":
            assert instruction.name is not None and instruction.value is not None
            self._table.insert(instruction.name, instruction.value)
            return 1
        raise AssertionError(f"unknown encoder instruction {kind}")  # unreachable

    # ----------------------------------------------------------- header blocks
    def decode_header_block(
        self, stream_id: int, block: bytes
    ) -> list[tuple[str, str]] | None:
        """Decode a header block.

        Returns the header list, or ``None`` if the stream is now parked
        waiting for dynamic-table entries. Raises BlockedStreamLimitError
        if the configured bounded wait limit is exceeded.
        """
        with self._lock:
            ric, block_seq, offset = wire.decode_block_prefix(block, 0)
            cached = self._decoded.get(block_seq)
            if cached is not None:
                return list(cached)  # duplicate delivery: no re-ack, no divergence
            if ric > self._received_insert_count:
                key = (stream_id, block_seq)
                if key not in self._blocked:
                    if len(self._blocked) >= self._max_blocked_streams:
                        raise BlockedStreamLimitError(
                            f"blocked stream limit {self._max_blocked_streams} "
                            "exceeded"
                        )
                    self._blocked[key] = (ric, block)
                return None
            return self._decode_locked(stream_id, block, ric, block_seq, offset)

    def _decode_locked(
        self,
        stream_id: int,
        block: bytes,
        ric: int,
        block_seq: int,
        offset: int,
    ) -> list[tuple[str, str]]:
        headers: list[tuple[str, str]] = []
        while offset < len(block):
            line, offset = wire.decode_field_line(block, offset)
            headers.append(self._resolve_locked(line))
        self._decoded[block_seq] = headers
        self._decoded.move_to_end(block_seq)
        while len(self._decoded) > self._decoded_cache_limit:
            self._decoded.popitem(last=False)
        self._pending += wire.encode_header_ack(block_seq)
        return list(headers)

    def _resolve_locked(self, line: wire.FieldLine) -> tuple[str, str]:
        kind = line.kind
        if kind == "indexed_static":
            assert line.index is not None
            if line.index >= len(STATIC_TABLE):
                raise UnknownIndexError(f"static index {line.index} out of range")
            return STATIC_TABLE[line.index]
        if kind == "indexed_dynamic":
            assert line.index is not None
            entry = self._table.get(line.index)
            return entry.name, entry.value
        if kind == "literal_static":
            assert line.index is not None and line.value is not None
            if line.index >= len(STATIC_TABLE):
                raise UnknownIndexError(f"static index {line.index} out of range")
            return STATIC_TABLE[line.index][0], line.value
        if kind == "literal_dynamic":
            assert line.index is not None and line.value is not None
            return self._table.get(line.index).name, line.value
        if kind == "literal":
            assert line.name is not None and line.value is not None
            return line.name, line.value
        raise AssertionError(f"unknown field line kind {kind}")  # unreachable

    # -------------------------------------------------------------- unblocking
    def _unblock_locked(self) -> list[tuple[int, list[tuple[str, str]]]]:
        ready = [
            key
            for key, (ric, _) in self._blocked.items()
            if ric <= self._received_insert_count
        ]
        results: list[tuple[int, list[tuple[str, str]]]] = []
        for key in sorted(ready):
            stream_id, block_seq = key
            ric, block = self._blocked.pop(key)
            _, _, offset = wire.decode_block_prefix(block, 0)
            headers = self._decode_locked(stream_id, block, ric, block_seq, offset)
            results.append((stream_id, headers))
        return results

    # ------------------------------------------------------------- decoder stream
    def drain_decoder_stream(self) -> bytes:
        """Return and clear pending decoder-stream instructions."""
        with self._lock:
            data = bytes(self._pending)
            self._pending.clear()
            return data
