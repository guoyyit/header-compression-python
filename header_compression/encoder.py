"""Stateful encoder endpoint (one per connection).

The encoder owns the encoder-side view of the dynamic table, emits encoder
stream instructions (insertions, capacity changes) and header blocks, and
tracks acknowledgements coming back on the decoder stream so it never
evicts an entry an unacknowledged header block still references.
"""

from __future__ import annotations

import threading


class EncoderEndpoint:
    """Encodes header lists into header blocks; thread-safe."""

    def __init__(
        self,
        max_table_capacity: int,
        table_capacity: int = 0,
        sensitive_headers: frozenset[str] = frozenset(),
    ) -> None:
        raise NotImplementedError

    @property
    def insert_count(self) -> int:
        """Absolute index the next inserted entry will get."""
        raise NotImplementedError

    @property
    def known_received_count(self) -> int:
        """Insert count the peer decoder has acknowledged."""
        raise NotImplementedError

    @property
    def capacity(self) -> int:
        raise NotImplementedError

    def set_capacity(self, capacity: int) -> None:
        """Queue a Set Dynamic Table Capacity instruction.

        Raises CapacityExceededError if above our maximum, or
        CapacityReductionBlockedError if the reduction would evict entries
        still referenced by unacknowledged header blocks.
        """
        raise NotImplementedError

    def encode_header_block(
        self, stream_id: int, headers: list[tuple[str, str]]
    ) -> bytes:
        """Encode one header list for ``stream_id``; deterministic."""
        raise NotImplementedError

    def drain_encoder_stream(self) -> bytes:
        """Return and clear pending encoder-stream instructions."""
        raise NotImplementedError

    def feed_decoder_stream(self, data: bytes) -> None:
        """Apply decoder-stream instructions (acks / count increments)."""
        raise NotImplementedError
