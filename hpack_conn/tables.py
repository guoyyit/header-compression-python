"""Static and dynamic header tables (RFC 7541 sections 2.3, 4).

Dynamic-table indices are 1-based with 1 naming the NEWEST entry, matching
RFC 7541 section 2.3.3. ``insertion_count`` is a monotone counter of every
insertion ever performed; it is the synchronization currency streams use
to wait for table state to catch up.
"""

from __future__ import annotations

from collections import deque
from dataclasses import dataclass

from .errors import CapacityError, DecodeError, RejectReason


@dataclass(frozen=True, slots=True)
class HeaderField:
    """An immutable header field (name/value pair)."""

    name: str
    value: str

    @property
    def size(self) -> int:
        """Table size accounting: name + value + 32 bytes overhead."""
        return len(self.name.encode("utf-8")) + len(self.value.encode("utf-8")) + 32


_STATIC_ENTRIES: tuple[HeaderField, ...] = tuple(
    HeaderField(name, value)
    for name, value in [
        (":authority", ""), (":method", "GET"), (":method", "POST"),
        (":path", "/"), (":path", "/index.html"),
        (":scheme", "http"), (":scheme", "https"),
        (":status", "200"), (":status", "204"), (":status", "206"),
        (":status", "304"), (":status", "400"), (":status", "404"),
        (":status", "500"),
        ("accept-charset", ""), ("accept-encoding", "gzip, deflate"),
        ("accept-language", ""), ("accept-ranges", ""), ("accept", ""),
        ("access-control-allow-origin", ""), ("age", ""), ("allow", ""),
        ("authorization", ""), ("cache-control", ""),
        ("content-disposition", ""), ("content-encoding", ""),
        ("content-language", ""), ("content-length", ""),
        ("content-location", ""), ("content-range", ""),
        ("content-type", ""), ("cookie", ""), ("date", ""), ("etag", ""),
        ("expect", ""), ("expires", ""), ("from", ""), ("host", ""),
        ("if-match", ""), ("if-modified-since", ""), ("if-none-match", ""),
        ("if-range", ""), ("if-unmodified-since", ""), ("last-modified", ""),
        ("link", ""), ("location", ""), ("max-forwards", ""),
        ("proxy-authenticate", ""), ("proxy-authorization", ""),
        ("range", ""), ("referer", ""), ("refresh", ""),
        ("retry-after", ""), ("server", ""), ("set-cookie", ""),
        ("strict-transport-security", ""), ("transfer-encoding", ""),
        ("user-agent", ""), ("vary", ""), ("via", ""),
        ("www-authenticate", ""),
    ]
)


class StaticTable:
    """The fixed static table shared (read-only) by every connection."""

    def __init__(self) -> None:
        self._entries = _STATIC_ENTRIES
        self._by_name: dict[str, list[int]] = {}
        self._exact: dict[tuple[str, str], int] = {}
        for i, entry in enumerate(self._entries, start=1):
            self._by_name.setdefault(entry.name, []).append(i)
            self._exact.setdefault((entry.name, entry.value), i)

    def __len__(self) -> int:
        return len(self._entries)

    def get(self, index: int) -> HeaderField:
        if not 1 <= index <= len(self._entries):
            raise DecodeError(
                RejectReason.INVALID_INDEX,
                f"static table index {index} out of range 1..{len(self._entries)}",
            )
        return self._entries[index - 1]

    def find(self, name: str, value: str | None = None) -> int | None:
        """Find an exact (name, value) match, else a name-only match."""
        if value is not None:
            exact = self._exact.get((name, value))
            if exact is not None:
                return exact
        candidates = self._by_name.get(name)
        return candidates[0] if candidates else None


class DynamicTable:
    """Connection-local dynamic table with capacity + memory ceiling.

    ``capacity`` is the currently negotiated table capacity (may change
    during the connection lifetime). ``memory_limit`` is a hard ceiling
    that capacity may never exceed; both are enforced on insertion.
    """

    def __init__(self, max_capacity: int, memory_limit: int | None = None) -> None:
        if max_capacity < 0:
            raise ValueError("max_capacity must be >= 0")
        if memory_limit is not None and memory_limit < 0:
            raise ValueError("memory_limit must be >= 0")
        self._max_capacity = max_capacity
        self._memory_limit = memory_limit
        # The memory ceiling is a hard bound: the effective capacity can
        # never exceed it, even if the protocol maximum does.
        self._capacity = min(max_capacity, memory_limit) if memory_limit is not None else max_capacity
        self._entries: deque[HeaderField] = deque()  # left = newest
        self._size = 0
        self._insertion_count = 0

    @property
    def capacity(self) -> int:
        return self._capacity

    @property
    def max_capacity(self) -> int:
        return self._max_capacity

    @property
    def memory_limit(self) -> int | None:
        return self._memory_limit

    @property
    def size(self) -> int:
        return self._size

    @property
    def insertion_count(self) -> int:
        return self._insertion_count

    def __len__(self) -> int:
        return len(self._entries)

    def set_capacity(self, capacity: int) -> None:
        """Change the negotiated capacity, evicting oldest entries as needed."""
        if capacity < 0:
            raise CapacityError(
                RejectReason.CAPACITY_SYNC_VIOLATION,
                f"capacity {capacity} is negative",
            )
        if capacity > self._max_capacity:
            raise CapacityError(
                RejectReason.CAPACITY_SYNC_VIOLATION,
                f"capacity {capacity} exceeds protocol maximum {self._max_capacity}",
            )
        if self._memory_limit is not None and capacity > self._memory_limit:
            raise CapacityError(
                RejectReason.MEMORY_LIMIT_EXCEEDED,
                f"capacity {capacity} exceeds memory limit {self._memory_limit}",
            )
        self._capacity = capacity
        self._evict_to_fit()

    def insert(self, name: str, value: str) -> HeaderField:
        """Insert an entry at the head, evicting oldest entries as needed.

        Raises:
            CapacityError: ``ENTRY_TOO_LARGE`` if the entry alone exceeds
                the current capacity, ``MEMORY_LIMIT_EXCEEDED`` if it
                exceeds the hard memory ceiling.
        """
        entry = HeaderField(name, value)
        if self._memory_limit is not None and entry.size > self._memory_limit:
            raise CapacityError(
                RejectReason.MEMORY_LIMIT_EXCEEDED,
                f"entry of {entry.size} octets exceeds memory limit "
                f"{self._memory_limit}",
            )
        if entry.size > self._capacity:
            raise CapacityError(
                RejectReason.ENTRY_TOO_LARGE,
                f"entry of {entry.size} octets exceeds table capacity "
                f"{self._capacity}",
            )
        self._evict_to_fit(extra=entry.size)
        self._entries.appendleft(entry)
        self._size += entry.size
        self._insertion_count += 1
        return entry

    def get(self, index: int) -> HeaderField:
        """Return the entry at 1-based ``index`` (1 = newest)."""
        if not 1 <= index <= len(self._entries):
            raise DecodeError(
                RejectReason.INVALID_INDEX,
                f"dynamic table index {index} out of range 1..{len(self._entries)}",
            )
        return self._entries[index - 1]

    def find(self, name: str, value: str | None = None) -> int | None:
        """Find an exact (name, value) match, else a name-only match.

        Returns the 1-based index (1 = newest) or ``None``.
        """
        name_match: int | None = None
        for i, entry in enumerate(self._entries, start=1):
            if entry.name == name:
                if value is not None and entry.value == value:
                    return i
                if name_match is None:
                    name_match = i
        return name_match

    def evict_oldest(self) -> HeaderField | None:
        """Evict the oldest entry, returning it (or ``None`` if empty)."""
        if not self._entries:
            return None
        entry = self._entries.pop()
        self._size -= entry.size
        return entry

    def _evict_to_fit(self, extra: int = 0) -> None:
        while self._entries and self._size + extra > self._capacity:
            self.evict_oldest()
