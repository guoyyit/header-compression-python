"""Per-stream state and blocked-stream bookkeeping for one connection.

A stream BLOCKS when its header block references dynamic-table insertions
the decoder has not applied yet (``required_insertions`` greater than the
decoder table's ``insertion_count``). It resumes automatically once the
table catches up. Waits are bounded: ``max_blocked_wait`` caps the wait
in seconds and ``max_blocked_streams`` caps how many streams may wait at
once, so a stuck peer cannot exhaust memory.
"""

from __future__ import annotations

import enum
import math
import time
from collections.abc import Callable
from dataclasses import dataclass, field

from .errors import BlockedStreamError, RejectReason


class StreamPhase(enum.Enum):
    OPEN = "open"
    BLOCKED = "blocked"
    DONE = "done"
    FAILED = "failed"


@dataclass(slots=True)
class StreamState:
    """Tracks one stream's dependency on dynamic-table insertions."""

    stream_id: int
    required_insertions: int = 0
    phase: StreamPhase = StreamPhase.OPEN
    headers: list[tuple[str, str]] | None = None
    error: Exception | None = None
    blocked_since: float | None = None


class StreamRegistry:
    """Registers streams and resumes blocked ones as the table grows."""

    def __init__(
        self,
        max_blocked_wait: float = 5.0,
        max_blocked_streams: int = 1024,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        if not math.isfinite(max_blocked_wait) or max_blocked_wait <= 0:
            raise ValueError("max_blocked_wait must be a positive finite number")
        if max_blocked_streams <= 0:
            raise ValueError("max_blocked_streams must be positive")
        self._max_blocked_wait = max_blocked_wait
        self._max_blocked_streams = max_blocked_streams
        self._clock = clock
        self._streams: dict[int, StreamState] = {}

    @property
    def max_blocked_wait(self) -> float:
        return self._max_blocked_wait

    @property
    def max_blocked_streams(self) -> int:
        return self._max_blocked_streams

    def get(self, stream_id: int) -> StreamState | None:
        return self._streams.get(stream_id)

    def blocked_count(self) -> int:
        return sum(1 for s in self._streams.values() if s.phase is StreamPhase.BLOCKED)

    def open_stream(self, stream_id: int) -> StreamState:
        if stream_id in self._streams:
            raise BlockedStreamError(
                RejectReason.MALFORMED_INPUT,
                f"stream {stream_id} already registered",
            )
        state = StreamState(stream_id=stream_id)
        self._streams[stream_id] = state
        return state

    def block_stream(self, stream_id: int, required_insertions: int) -> StreamState:
        state = self._streams.get(stream_id)
        if state is None:
            state = self.open_stream(stream_id)
        if state.phase is StreamPhase.FAILED:
            raise BlockedStreamError(
                RejectReason.CONNECTION_CLOSED,
                f"stream {stream_id} already failed: {state.error}",
            )
        if state.phase is not StreamPhase.BLOCKED:
            if self.blocked_count() >= self._max_blocked_streams:
                raise BlockedStreamError(
                    RejectReason.MEMORY_LIMIT_EXCEEDED,
                    f"blocked stream limit {self._max_blocked_streams} reached",
                )
            state.blocked_since = self._clock()
        state.phase = StreamPhase.BLOCKED
        state.required_insertions = required_insertions
        return state

    def on_insertions(self, insertion_count: int) -> list[int]:
        """Notify of table growth; returns stream ids that were resumed.

        Resumption order is deterministic: ascending required insertions,
        then ascending stream id.
        """
        ready = [
            s
            for s in self._streams.values()
            if s.phase is StreamPhase.BLOCKED
            and s.required_insertions <= insertion_count
        ]
        ready.sort(key=lambda s: (s.required_insertions, s.stream_id))
        for state in ready:
            state.phase = StreamPhase.OPEN
            state.blocked_since = None
        return [s.stream_id for s in ready]

    def check_timeouts(self, now: float | None = None) -> list[int]:
        """Fail streams whose wait exceeded the configured bound."""
        now = self._clock() if now is None else now
        failed: list[int] = []
        for state in self._streams.values():
            if state.phase is StreamPhase.BLOCKED and state.blocked_since is not None:
                if now - state.blocked_since > self._max_blocked_wait:
                    state.phase = StreamPhase.FAILED
                    state.error = BlockedStreamError(
                        RejectReason.BLOCKED_STREAM_TIMEOUT,
                        f"stream {state.stream_id} waited more than "
                        f"{self._max_blocked_wait}s for table insertions "
                        f"{state.required_insertions}",
                    )
                    failed.append(state.stream_id)
        return sorted(failed)

    def close_stream(self, stream_id: int) -> None:
        self._streams.pop(stream_id, None)

    def reset(self) -> None:
        self._streams.clear()
