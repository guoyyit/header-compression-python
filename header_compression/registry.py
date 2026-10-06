"""Connection registry: owns one compression context per connection."""

from __future__ import annotations

import threading

from .connection import CompressionConfig, ConnectionCompressionContext


class ConnectionRegistry:
    """Creates, tracks and releases per-connection compression state."""

    def __init__(self, config: CompressionConfig | None = None) -> None:
        raise NotImplementedError

    def get_or_create(self, connection_id: str) -> ConnectionCompressionContext:
        raise NotImplementedError

    def get(self, connection_id: str) -> ConnectionCompressionContext | None:
        raise NotImplementedError

    def reset(self, connection_id: str) -> ConnectionCompressionContext:
        raise NotImplementedError

    def close(self, connection_id: str) -> bool:
        """Close and drop the context; returns True if it existed."""
        raise NotImplementedError

    def active_ids(self) -> list[str]:
        raise NotImplementedError

    def __len__(self) -> int:
        raise NotImplementedError
