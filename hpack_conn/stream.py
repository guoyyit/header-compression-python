"""Per-stream state and blocked-stream bookkeeping for one connection."""

from __future__ import annotations

import enum
from dataclasses import dataclass, field


class StreamPhase(enum.Enum):
    OPEN = "open"
    BLOCKED = "blocked"
    DONE = "done"
    FAILED = "failed"


@dataclass(slots=True)
class StreamState:
    """Tracks one stream's dependency on dynamic-table insertions. Skeleton."""

    stream_id: int
    required_insertions: int = 0
    phase: StreamPhase = StreamPhase.OPEN
    headers: list[tuple[str, str]] | None = None
    error: Exception | None = None


class StreamRegistry:
    """Registers streams, blocks them on unsatisfied dependencies and
    resumes them once the dynamic table catches up. Skeleton."""

    def __init__(self, max_blocked_wait: float = 5.0) -> None:
        raise NotImplementedError

    def open_stream(self, stream_id: int) -> StreamState:
        raise NotImplementedError

    def block_stream(self, stream_id: int, required_insertions: int) -> StreamState:
        raise NotImplementedError

    def on_insertions(self, insertion_count: int) -> list[int]:
        """Notify of table growth; returns stream ids that were resumed."""
        raise NotImplementedError

    def close_stream(self, stream_id: int) -> None:
        raise NotImplementedError

    def check_timeouts(self, now: float) -> list[int]:
        """Fail streams whose wait exceeded the configured bound."""
        raise NotImplementedError
