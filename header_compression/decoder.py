"""Stateful decoder endpoint (one per connection).

Applies encoder-stream instructions in order (the single authoritative
insertion order), decodes header blocks, blocks streams whose required
insert count has not arrived yet, and emits decoder-stream
acknowledgements.
"""

from __future__ import annotations

import threading


class DecoderEndpoint:
    """Decodes header blocks; thread-safe."""

    def __init__(self, max_table_capacity: int, max_blocked_streams: int) -> None:
        raise NotImplementedError

    @property
    def received_insert_count(self) -> int:
        raise NotImplementedError

    @property
    def blocked_stream_ids(self) -> list[int]:
        raise NotImplementedError

    def feed_encoder_stream(
        self, data: bytes
    ) -> list[tuple[int, list[tuple[str, str]]]]:
        """Apply encoder-stream bytes; returns newly unblocked
        ``(stream_id, headers)`` pairs in stream-id order."""
        raise NotImplementedError

    def decode_header_block(
        self, stream_id: int, block: bytes
    ) -> list[tuple[str, str]] | None:
        """Decode a header block.

        Returns the header list, or ``None`` if the stream is now blocked
        waiting for dynamic-table entries. Raises BlockedStreamLimitError
        if the configured bounded wait limit is exceeded.
        """
        raise NotImplementedError

    def drain_decoder_stream(self) -> bytes:
        """Return and clear pending decoder-stream instructions."""
        raise NotImplementedError
