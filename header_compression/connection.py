"""Per-connection compression context: encoder + decoder + sequencing.

State is created on connection open, rebuilt deterministically on reset,
and released on close. Contexts share no mutable state, so connections are
fully isolated from each other.

Incoming encoder-stream batches carry explicit sequence numbers and are
applied strictly in order: out-of-order batches are buffered, duplicates
dropped, so the two peers' table states cannot diverge under reordering
or retransmission.
"""

from __future__ import annotations

import threading
from dataclasses import dataclass, field

from .decoder import DecoderEndpoint
from .encoder import EncoderEndpoint
from .errors import CompressionError

DEFAULT_MAX_TABLE_CAPACITY = 4096
DEFAULT_MAX_BLOCKED_STREAMS = 64
DEFAULT_SENSITIVE_HEADERS = frozenset({"authorization", "proxy-authorization", "cookie"})

#: How many recorded errors are kept per connection (observability bound).
MAX_RECORDED_ERRORS = 256


@dataclass(frozen=True)
class CompressionConfig:
    """Tunables for one connection's compression context.

    max_table_capacity: hard memory ceiling for the dynamic table; the
        peer may never raise the table above this.
    table_capacity: capacity our encoder opens the table with.
    max_blocked_streams: bounded wait limit for streams parked on
        unsynchronised table entries.
    sensitive_headers: names that are never indexed nor inserted.
    """

    max_table_capacity: int = DEFAULT_MAX_TABLE_CAPACITY
    table_capacity: int = DEFAULT_MAX_TABLE_CAPACITY
    max_blocked_streams: int = DEFAULT_MAX_BLOCKED_STREAMS
    sensitive_headers: frozenset[str] = DEFAULT_SENSITIVE_HEADERS

    def __post_init__(self) -> None:
        if self.max_table_capacity < 0:
            raise ValueError("max_table_capacity must be >= 0")
        if not 0 <= self.table_capacity <= self.max_table_capacity:
            raise ValueError("table_capacity must be within [0, max_table_capacity]")
        if self.max_blocked_streams <= 0:
            raise ValueError("max_blocked_streams must be positive")


@dataclass
class ConnectionCompressionContext:
    """Encoder/decoder pair bound to one connection id."""

    connection_id: str
    config: CompressionConfig = field(default_factory=CompressionConfig)

    def __post_init__(self) -> None:
        self._lock = threading.RLock()
        self._errors: list[CompressionError] = []
        self._closed = False
        self._build_state()

    def _build_state(self) -> None:
        self._encoder = EncoderEndpoint(
            self.config.max_table_capacity,
            self.config.table_capacity,
            self.config.sensitive_headers,
        )
        self._decoder = DecoderEndpoint(
            self.config.max_table_capacity,
            self.config.max_blocked_streams,
        )
        self._out_seq = 0
        self._in_seq_expected = 0
        self._in_buffer: dict[int, bytes] = {}

    def _ensure_open(self) -> None:
        if self._closed:
            raise CompressionError(f"connection {self.connection_id!r} is closed")

    @property
    def closed(self) -> bool:
        return self._closed

    @property
    def encoder(self) -> EncoderEndpoint:
        return self._encoder

    @property
    def decoder(self) -> DecoderEndpoint:
        return self._decoder

    # -- encoder side (our outgoing header blocks) ----------------------
    def encode_headers(
        self, stream_id: int, headers: list[tuple[str, str]]
    ) -> bytes:
        with self._lock:
            self._ensure_open()
            return self._encoder.encode_header_block(stream_id, headers)

    def next_encoder_batch(self) -> tuple[int, bytes] | None:
        """Return ``(seq, payload)`` for the next encoder-stream batch."""
        with self._lock:
            self._ensure_open()
            payload = self._encoder.drain_encoder_stream()
            if not payload:
                return None
            seq = self._out_seq
            self._out_seq += 1
            return seq, payload

    def feed_decoder_stream(self, data: bytes) -> None:
        with self._lock:
            self._ensure_open()
            self._encoder.feed_decoder_stream(data)

    # -- decoder side (incoming header blocks) --------------------------
    def feed_encoder_stream(
        self, seq: int, data: bytes
    ) -> list[tuple[int, list[tuple[str, str]]]]:
        """Apply a sequenced encoder-stream batch.

        Batches are applied strictly in sequence order: out-of-order
        batches are buffered, duplicates dropped, so the two peers' table
        states cannot diverge under reordering or retransmission.
        """
        with self._lock:
            self._ensure_open()
            if seq < self._in_seq_expected:
                return []  # duplicate batch: already applied
            self._in_buffer.setdefault(seq, data)
            results: list[tuple[int, list[tuple[str, str]]]] = []
            while self._in_seq_expected in self._in_buffer:
                batch = self._in_buffer.pop(self._in_seq_expected)
                results.extend(self._decoder.feed_encoder_stream(batch))
                self._in_seq_expected += 1
            return results

    def decode_headers(
        self, stream_id: int, block: bytes
    ) -> list[tuple[str, str]] | None:
        with self._lock:
            self._ensure_open()
            return self._decoder.decode_header_block(stream_id, block)

    def drain_decoder_stream(self) -> bytes:
        with self._lock:
            self._ensure_open()
            return self._decoder.drain_decoder_stream()

    # -- lifecycle --------------------------------------------------------
    def reset(self) -> None:
        """Rebuild encoder/decoder state deterministically."""
        with self._lock:
            self._ensure_open()
            self._build_state()

    def close(self) -> None:
        """Release all per-connection state; idempotent."""
        with self._lock:
            if self._closed:
                return
            self._in_buffer.clear()
            self._closed = True

    def record_error(self, exc: CompressionError) -> None:
        with self._lock:
            if len(self._errors) < MAX_RECORDED_ERRORS:
                self._errors.append(exc)

    @property
    def errors(self) -> list[CompressionError]:
        with self._lock:
            return list(self._errors)

    def state_snapshot(self) -> dict:
        """Debug/observability view of the connection's compression state."""
        with self._lock:
            return {
                "connection_id": self.connection_id,
                "closed": self._closed,
                "encoder": {
                    "capacity": self._encoder.capacity,
                    "table_size": self._encoder.table.size,
                    "insert_count": self._encoder.insert_count,
                    "known_received_count": self._encoder.known_received_count,
                    "pending_acks": self._encoder.pending_ack_count,
                },
                "decoder": {
                    "capacity": self._decoder.table.capacity,
                    "table_size": self._decoder.table.size,
                    "received_insert_count": self._decoder.received_insert_count,
                    "blocked_streams": self._decoder.blocked_stream_ids,
                },
                "sequencing": {
                    "next_out_seq": self._out_seq,
                    "next_in_seq": self._in_seq_expected,
                    "buffered_batches": sorted(self._in_buffer),
                },
                "errors": [f"{type(e).__name__}: {e}" for e in self._errors],
            }
