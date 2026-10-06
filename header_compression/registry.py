"""Connection registry: owns one compression context per connection."""

from __future__ import annotations

import threading

from .connection import CompressionConfig, ConnectionCompressionContext
from .errors import CompressionError


class ConnectionRegistry:
    """Creates, tracks and releases per-connection compression state.

    Every connection gets its own isolated context; closing or resetting
    one connection never touches another's state.
    """

    def __init__(self, config: CompressionConfig | None = None) -> None:
        self._config = config or CompressionConfig()
        self._lock = threading.Lock()
        self._contexts: dict[str, ConnectionCompressionContext] = {}

    @property
    def config(self) -> CompressionConfig:
        return self._config

    def get_or_create(self, connection_id: str) -> ConnectionCompressionContext:
        with self._lock:
            context = self._contexts.get(connection_id)
            if context is None or context.closed:
                context = ConnectionCompressionContext(connection_id, self._config)
                self._contexts[connection_id] = context
            return context

    def get(self, connection_id: str) -> ConnectionCompressionContext | None:
        with self._lock:
            return self._contexts.get(connection_id)

    def require(self, connection_id: str) -> ConnectionCompressionContext:
        context = self.get(connection_id)
        if context is None:
            raise CompressionError(f"unknown connection {connection_id!r}")
        return context

    def reset(self, connection_id: str) -> ConnectionCompressionContext:
        context = self.require(connection_id)
        context.reset()
        return context

    def close(self, connection_id: str) -> bool:
        """Close and drop the context; returns True if it existed."""
        with self._lock:
            context = self._contexts.pop(connection_id, None)
        if context is None:
            return False
        context.close()
        return True

    def active_ids(self) -> list[str]:
        with self._lock:
            return sorted(self._contexts)

    def __len__(self) -> int:
        with self._lock:
            return len(self._contexts)
