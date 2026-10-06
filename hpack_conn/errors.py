"""Error types for the connection-layer header compression layer.

Every failure carries a machine-distinguishable ``reason`` so callers can
tell apart e.g. memory-limit rejections from malformed input, and so a
failure is observable and reproducible instead of a silent drop.
"""

from __future__ import annotations

import enum


class RejectReason(enum.Enum):
    """Machine-distinguishable rejection / failure reasons."""

    #: Decoding hit a malformed byte sequence (bad integer, bad index, ...).
    MALFORMED_INPUT = "malformed_input"
    #: Input ended before a complete representation could be decoded.
    TRUNCATED_INPUT = "truncated_input"
    #: Inserting an entry would exceed the configured memory ceiling.
    MEMORY_LIMIT_EXCEEDED = "memory_limit_exceeded"
    #: A single entry is larger than the dynamic table could ever hold.
    ENTRY_TOO_LARGE = "entry_too_large"
    #: Peer referenced a dynamic-table index that is not (yet) present.
    INVALID_INDEX = "invalid_index"
    #: Encoder/decoder dynamic table capacity negotiation was violated.
    CAPACITY_SYNC_VIOLATION = "capacity_sync_violation"
    #: A blocked stream waited longer than the configured bound.
    BLOCKED_STREAM_TIMEOUT = "blocked_stream_timeout"
    #: Operation attempted on a closed/reset connection context.
    CONNECTION_CLOSED = "connection_closed"


class CompressionError(Exception):
    """Base class for all header-compression failures."""

    def __init__(self, reason: RejectReason, message: str) -> None:
        self.reason = reason
        super().__init__(f"{reason.value}: {message}")


class DecodeError(CompressionError):
    """Raised when a header block cannot be decoded safely."""


class EncodeError(CompressionError):
    """Raised when a header list cannot be encoded."""


class CapacityError(CompressionError):
    """Raised when a memory limit or capacity negotiation is violated."""


class BlockedStreamError(CompressionError):
    """Raised when a blocked stream cannot be resumed within its bound."""
