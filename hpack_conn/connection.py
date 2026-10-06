"""Connection-level compression context and lifecycle management."""

from __future__ import annotations

import enum


class ConnectionPhase(enum.Enum):
    OPEN = "open"
    RESET = "reset"
    CLOSED = "closed"


class ConnectionContext:
    """Owns the encoder/decoder pair and dynamic tables of ONE connection.

    Contexts are strictly isolated: no state is shared between connections
    other than the immutable static table. Skeleton.
    """

    def __init__(self, connection_id: str, max_capacity: int = 4096,
                 memory_limit: int | None = None,
                 max_blocked_wait: float = 5.0) -> None:
        raise NotImplementedError

    @property
    def phase(self) -> ConnectionPhase:
        raise NotImplementedError

    def encode_headers(self, stream_id: int, headers: list[tuple[str, str]]) -> bytes:
        raise NotImplementedError

    def decode_headers(self, stream_id: int, data: bytes) -> list[tuple[str, str]]:
        raise NotImplementedError

    def update_capacity(self, capacity: int) -> bytes:
        """Negotiate a new dynamic-table capacity with the peer."""
        raise NotImplementedError

    def reset(self) -> None:
        raise NotImplementedError

    def close(self) -> None:
        raise NotImplementedError


class ConnectionManager:
    """Creates, tracks and releases per-connection compression contexts."""

    def __init__(self, **context_options) -> None:
        raise NotImplementedError

    def open_connection(self, connection_id: str) -> ConnectionContext:
        raise NotImplementedError

    def get(self, connection_id: str) -> ConnectionContext | None:
        raise NotImplementedError

    def close_connection(self, connection_id: str) -> None:
        raise NotImplementedError

    def reset_connection(self, connection_id: str) -> ConnectionContext:
        raise NotImplementedError
