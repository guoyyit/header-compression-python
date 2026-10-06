"""Per-connection compression context: encoder + decoder + sequencing.

State is created on connection open, rebuilt deterministically on reset,
and released on close. Contexts share no mutable state, so connections are
fully isolated from each other.
"""

from __future__ import annotations

import threading
from dataclasses import dataclass, field

from .errors import CompressionError

DEFAULT_MAX_TABLE_CAPACITY = 4096
DEFAULT_MAX_BLOCKED_STREAMS = 64
DEFAULT_SENSITIVE_HEADERS = frozenset({"authorization", "proxy-authorization", "cookie"})


@dataclass(frozen=True)
class CompressionConfig:
    """Tunables for one connection's compression context."""

    max_table_capacity: int = DEFAULT_MAX_TABLE_CAPACITY
    table_capacity: int = DEFAULT_MAX_TABLE_CAPACITY
    max_blocked_streams: int = DEFAULT_MAX_BLOCKED_STREAMS
    sensitive_headers: frozenset[str] = DEFAULT_SENSITIVE_HEADERS


@dataclass
class ConnectionCompressionContext:
    """Encoder/decoder pair bound to one connection id."""

    connection_id: str
    config: CompressionConfig = field(default_factory=CompressionConfig)

    def __post_init__(self) -> None:
        raise NotImplementedError

    @property
    def closed(self) -> bool:
        raise NotImplementedError

    # -- encoder side (our outgoing header blocks) ----------------------
    def encode_headers(
        self, stream_id: int, headers: list[tuple[str, str]]
    ) -> bytes:
        raise NotImplementedError

    def next_encoder_batch(self) -> tuple[int, bytes] | None:
        """Return ``(seq, payload)`` for the next encoder-stream batch."""
        raise NotImplementedError

    def feed_decoder_stream(self, data: bytes) -> None:
        raise NotImplementedError

    # -- decoder side (incoming header blocks) --------------------------
    def feed_encoder_stream(
        self, seq: int, data: bytes
    ) -> list[tuple[int, list[tuple[str, str]]]]:
        """Apply a sequenced encoder-stream batch.

        Batches are applied strictly in sequence order: out-of-order
        batches are buffered, duplicates dropped, so the two peers' table
        states cannot diverge under reordering or retransmission.
        """
        raise NotImplementedError

    def decode_headers(
        self, stream_id: int, block: bytes
    ) -> list[tuple[str, str]] | None:
        raise NotImplementedError

    def drain_decoder_stream(self) -> bytes:
        raise NotImplementedError

    # -- lifecycle --------------------------------------------------------
    def reset(self) -> None:
        """Rebuild encoder/decoder state deterministically."""
        raise NotImplementedError

    def close(self) -> None:
        """Release all per-connection state; idempotent."""
        raise NotImplementedError

    def record_error(self, exc: CompressionError) -> None:
        raise NotImplementedError

    @property
    def errors(self) -> list[CompressionError]:
        raise NotImplementedError
