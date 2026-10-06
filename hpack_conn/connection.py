"""Connection-level compression context and lifecycle management.

Wire framing per header block (integers use the variable-length coding of
:mod:`hpack_conn.integers`)::

    [gseq][prev_dep][flags][header block ...]

* ``gseq`` -- connection-global monotone sequence number of this frame
  (per direction). Also used to detect duplicate deliveries.
* ``prev_dep`` -- the ``gseq`` of the previous frame whose block touches
  the dynamic table (0 if none). Only meaningful when flag TOUCHES is set.
* ``flags`` -- bit 0 (TOUCHES): the block touches the dynamic table
  (insertion, dynamic reference, or table size update).

Synchronization semantics
-------------------------
Blocks that touch the dynamic table MUST be applied by the decoder in
encode order, because the wire indices inside a block are relative to the
table state the encoder saw. A touching frame is therefore applied only
once every touching frame before it (its ``prev_dep`` chain) has been
applied; until then the stream is BLOCKED and resumes automatically once
the checkpoint advances. Blocks that do not touch the table are decoded
immediately in arrival order and never wait. At any moment the decoder
table state corresponds to a deterministic prefix of the encoder's
insertion order, so out-of-order or duplicate deliveries cannot make the
two ends diverge.
"""

from __future__ import annotations

import enum
import threading
import time
from collections.abc import Callable

from .decoder import Decoder
from .encoder import Encoder
from .errors import (
    CompressionError,
    DecodeError,
    RejectReason,
)
from .integers import decode_integer, encode_integer
from .stream import StreamPhase, StreamRegistry, StreamState
from .tables import DynamicTable, StaticTable

#: Bound on how many decoded results are cached for duplicate suppression.
_RESULT_CACHE_SIZE = 128

_FLAG_TOUCHES = 0x01


class ConnectionPhase(enum.Enum):
    OPEN = "open"
    CLOSED = "closed"


class ConnectionContext:
    """Owns the encoder/decoder pair and dynamic tables of ONE connection.

    Contexts are strictly isolated: the only shared object between
    connections is the immutable static table. A decode failure poisons
    this context's decode side (subsequent decodes raise) until
    :meth:`reset` re-creates the state deterministically; the encode
    side and every other connection are unaffected.
    """

    def __init__(
        self,
        connection_id: str,
        max_capacity: int = 4096,
        memory_limit: int | None = None,
        max_blocked_wait: float = 5.0,
        max_blocked_streams: int = 1024,
        strict_limits: bool = False,
        static_table: StaticTable | None = None,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        self.connection_id = connection_id
        self._max_capacity = max_capacity
        self._memory_limit = memory_limit
        self._max_blocked_wait = max_blocked_wait
        self._max_blocked_streams = max_blocked_streams
        self._strict_limits = strict_limits
        self._static = static_table or StaticTable()
        self._clock = clock
        self._lock = threading.RLock()
        self._phase = ConnectionPhase.OPEN
        self._build_state()

    # -- state (re)creation -------------------------------------------

    def _build_state(self) -> None:
        enc_table = DynamicTable(self._max_capacity, self._memory_limit)
        dec_table = DynamicTable(self._max_capacity, self._memory_limit)
        self._encoder = Encoder(enc_table, self._static,
                                strict_limits=self._strict_limits)
        self._decoder = Decoder(dec_table, self._static)
        self._registry = StreamRegistry(
            max_blocked_wait=self._max_blocked_wait,
            max_blocked_streams=self._max_blocked_streams,
            clock=self._clock,
        )
        # encode side (one direction)
        self._next_gseq = 0
        self._last_touch_gseq = 0
        # decode side (peer's direction)
        self._applied_checkpoint = 0
        self._decoded: dict[int, list[tuple[str, str]]] = {}
        self._pending: dict[int, tuple[int, int, bytes]] = {}
        self._dec_failed: CompressionError | None = None

    # -- properties ----------------------------------------------------

    @property
    def phase(self) -> ConnectionPhase:
        return self._phase

    @property
    def encoder_table(self) -> DynamicTable:
        return self._encoder.table

    @property
    def decoder_table(self) -> DynamicTable:
        return self._decoder.table

    def stream_state(self, stream_id: int) -> StreamState | None:
        return self._registry.get(stream_id)

    # -- encode ---------------------------------------------------------

    def encode_headers(self, stream_id: int, headers: list[tuple[str, str]]) -> bytes:
        """Encode and frame a header block for ``stream_id``."""
        del stream_id  # framing is connection-global; streams share the table
        with self._lock:
            self._ensure_open()
            self._next_gseq += 1
            gseq = self._next_gseq
            block = self._encoder.encode(headers)
            touches = self._encoder.last_block_uses_dynamic
            flags = _FLAG_TOUCHES if touches else 0
            prev_dep = self._last_touch_gseq if touches else 0
            if touches:
                self._last_touch_gseq = gseq
            return (
                encode_integer(gseq, 8)
                + encode_integer(prev_dep, 8)
                + bytes([flags])
                + block
            )

    # -- decode ---------------------------------------------------------

    def decode_headers(
        self, stream_id: int, data: bytes
    ) -> list[tuple[str, str]] | None:
        """Decode a framed header block.

        Returns the headers, or ``None`` if the stream is now BLOCKED
        waiting for earlier table-touching frames; blocked streams resume
        automatically once their checkpoint is applied, and their result
        becomes available via :meth:`stream_state`.
        """
        with self._lock:
            self._ensure_open()
            self._ensure_dec_healthy()
            gseq, pos = decode_integer(data, 8, 0)
            prev_dep, pos = decode_integer(data, 8, pos)
            if pos >= len(data):
                raise DecodeError(
                    RejectReason.TRUNCATED_INPUT,
                    f"frame flags missing at offset {pos}",
                )
            flags = data[pos]
            pos += 1
            touches = bool(flags & _FLAG_TOUCHES)
            block = data[pos:]

            if gseq in self._decoded:
                return list(self._decoded[gseq])

            if touches and prev_dep > self._applied_checkpoint:
                self._pending[stream_id] = (gseq, prev_dep, block)
                self._registry.block_stream(stream_id, prev_dep)
                return None

            headers = self._apply(gseq, touches, block, stream_id)
            self._drain()
            return headers

    def _apply(
        self, gseq: int, touches: bool, block: bytes, stream_id: int
    ) -> list[tuple[str, str]]:
        """Decode one block and record it; any failure poisons the decoder."""
        try:
            headers = self._decoder.decode(block)
        except CompressionError as exc:
            self._dec_failed = exc
            raise
        if touches:
            self._applied_checkpoint = max(self._applied_checkpoint, gseq)
        self._remember_result(gseq, headers)
        state = self._registry.get(stream_id)
        if state is not None and state.phase is not StreamPhase.FAILED:
            state.phase = StreamPhase.DONE
            state.headers = headers
            state.blocked_since = None
        return headers

    def _drain(self) -> None:
        """Resume blocked streams whose checkpoint is now applied."""
        resumed = self._registry.on_insertions(self._applied_checkpoint)
        for stream_id in resumed:
            pending = self._pending.pop(stream_id, None)
            if pending is None:
                continue
            gseq, _prev_dep, block = pending
            self._apply(gseq, True, block, stream_id)
        if resumed:
            self._drain()

    def _remember_result(
        self, gseq: int, headers: list[tuple[str, str]]
    ) -> None:
        if len(self._decoded) >= _RESULT_CACHE_SIZE:
            self._decoded.pop(next(iter(self._decoded)))
        self._decoded[gseq] = list(headers)

    # -- capacity negotiation -------------------------------------------

    def update_capacity(self, capacity: int) -> bytes:
        """Negotiate a new encoder dynamic-table capacity.

        The change takes effect at the next emitted header block, which
        the size update leads, so the peer applies it before decoding any
        reference in that block: no undecodable reference can occur.
        """
        with self._lock:
            self._ensure_open()
            return self._encoder.set_capacity(capacity)

    # -- blocked-stream maintenance --------------------------------------

    def check_timeouts(self, now: float | None = None) -> list[int]:
        """Fail streams that exceeded the bounded wait; returns their ids."""
        with self._lock:
            failed = self._registry.check_timeouts(now)
            for stream_id in failed:
                self._pending.pop(stream_id, None)
            return failed

    # -- lifecycle --------------------------------------------------------

    def reset(self) -> None:
        """Deterministically release and re-create all compression state."""
        with self._lock:
            self._ensure_open()
            self._build_state()

    def close(self) -> None:
        """Release all state; further use raises ``CONNECTION_CLOSED``."""
        with self._lock:
            self._build_state()  # drop references to tables/streams
            self._phase = ConnectionPhase.CLOSED

    # -- guards -----------------------------------------------------------

    def _ensure_open(self) -> None:
        if self._phase is not ConnectionPhase.OPEN:
            raise CompressionError(
                RejectReason.CONNECTION_CLOSED,
                f"connection {self.connection_id!r} is closed",
            )

    def _ensure_dec_healthy(self) -> None:
        if self._dec_failed is not None:
            raise CompressionError(
                self._dec_failed.reason,
                f"decode state of connection {self.connection_id!r} is "
                f"poisoned by an earlier failure; reset() required",
            )


class ConnectionManager:
    """Creates, tracks and releases per-connection compression contexts."""

    def __init__(self, **context_options) -> None:
        self._options = context_options
        self._contexts: dict[str, ConnectionContext] = {}
        self._lock = threading.Lock()

    def open_connection(self, connection_id: str) -> ConnectionContext:
        with self._lock:
            if connection_id in self._contexts:
                raise CompressionError(
                    RejectReason.MALFORMED_INPUT,
                    f"connection {connection_id!r} already exists",
                )
            ctx = ConnectionContext(connection_id, **self._options)
            self._contexts[connection_id] = ctx
            return ctx

    def get(self, connection_id: str) -> ConnectionContext | None:
        return self._contexts.get(connection_id)

    def connection_count(self) -> int:
        return len(self._contexts)

    def close_connection(self, connection_id: str) -> None:
        with self._lock:
            ctx = self._contexts.pop(connection_id, None)
        if ctx is not None:
            ctx.close()

    def reset_connection(self, connection_id: str) -> ConnectionContext:
        ctx = self._contexts.get(connection_id)
        if ctx is None:
            raise CompressionError(
                RejectReason.CONNECTION_CLOSED,
                f"connection {connection_id!r} does not exist",
            )
        ctx.reset()
        return ctx
