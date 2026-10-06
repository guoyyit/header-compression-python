"""Static table and dynamic table with capacity management and eviction."""

from __future__ import annotations

from collections import deque
from dataclasses import dataclass

from .errors import EntryTooLargeError, UnknownIndexError

#: HPACK static table (RFC 7541 Appendix A), 0-based on the wire here.
STATIC_TABLE: tuple[tuple[str, str], ...] = (
    (":authority", ""),
    (":scheme", "http"),
    (":scheme", "https"),
    (":path", "/"),
    (":path", "/index.html"),
    (":method", "GET"),
    (":method", "POST"),
    (":status", "200"),
    (":status", "204"),
    (":status", "206"),
    (":status", "304"),
    (":status", "400"),
    (":status", "404"),
    (":status", "500"),
    ("accept-charset", ""),
    ("accept-encoding", "gzip, deflate"),
    ("accept-language", ""),
    ("accept-ranges", ""),
    ("accept", ""),
    ("access-control-allow-origin", ""),
    ("age", ""),
    ("allow", ""),
    ("authorization", ""),
    ("cache-control", ""),
    ("content-disposition", ""),
    ("content-encoding", ""),
    ("content-language", ""),
    ("content-length", ""),
    ("content-location", ""),
    ("content-range", ""),
    ("content-type", ""),
    ("cookie", ""),
    ("date", ""),
    ("etag", ""),
    ("expect", ""),
    ("expires", ""),
    ("from", ""),
    ("host", ""),
    ("if-match", ""),
    ("if-modified-since", ""),
    ("if-none-match", ""),
    ("if-range", ""),
    ("if-unmodified-since", ""),
    ("last-modified", ""),
    ("link", ""),
    ("location", ""),
    ("max-forwards", ""),
    ("proxy-authenticate", ""),
    ("proxy-authorization", ""),
    ("range", ""),
    ("referer", ""),
    ("refresh", ""),
    ("retry-after", ""),
    ("server", ""),
    ("set-cookie", ""),
    ("strict-transport-security", ""),
    ("transfer-encoding", ""),
    ("user-agent", ""),
    ("vary", ""),
    ("via", ""),
    ("www-authenticate", ""),
)

#: Per-entry bookkeeping overhead, as in RFC 7541 section 4.1.
ENTRY_OVERHEAD = 32


def entry_size(name: str, value: str) -> int:
    """Size of one table entry in bytes."""
    return len(name.encode("utf-8")) + len(value.encode("utf-8")) + ENTRY_OVERHEAD


def find_static(name: str, value: str) -> tuple[int | None, bool]:
    """Return ``(index, exact)`` for the best static-table match.

    Exact matches win; otherwise the lowest index with a matching name.
    Deterministic: first exact match scanning from index 0, else first
    name-only match.
    """
    name_index: int | None = None
    for index, (entry_name, entry_value) in enumerate(STATIC_TABLE):
        if entry_name == name:
            if entry_value == value:
                return index, True
            if name_index is None:
                name_index = index
    return (name_index, False) if name_index is not None else (None, False)


@dataclass(frozen=True)
class TableEntry:
    """One dynamic-table entry."""

    name: str
    value: str
    absolute_index: int
    size: int


class DynamicTable:
    """FIFO-evicting dynamic table addressed by absolute index.

    Absolute indices start at 0 and increase by one per insertion, so both
    peers refer to the same entry by the same index regardless of
    evictions.
    """

    def __init__(self, max_capacity: int) -> None:
        if max_capacity < 0:
            raise ValueError("negative capacity")
        self._max_capacity = max_capacity
        self._capacity = max_capacity
        self._entries: deque[TableEntry] = deque()
        self._size = 0
        self._insert_count = 0

    @property
    def max_capacity(self) -> int:
        return self._max_capacity

    @property
    def capacity(self) -> int:
        return self._capacity

    @property
    def size(self) -> int:
        return self._size

    @property
    def insert_count(self) -> int:
        """Total number of entries ever inserted (== next absolute index)."""
        return self._insert_count

    @property
    def dropped_count(self) -> int:
        """Number of entries evicted so far (== lowest live absolute index)."""
        return self._insert_count - len(self._entries)

    def __len__(self) -> int:
        return len(self._entries)

    def set_capacity(self, capacity: int) -> list[TableEntry]:
        """Shrink/grow the capacity; returns evicted entries (oldest first)."""
        if capacity < 0:
            raise ValueError("negative capacity")
        if capacity > self._max_capacity:
            raise ValueError(
                f"capacity {capacity} exceeds maximum {self._max_capacity}"
            )
        self._capacity = capacity
        return self._evict_to_fit()

    def insert(self, name: str, value: str) -> TableEntry:
        """Insert an entry, evicting oldest entries until it fits."""
        size = entry_size(name, value)
        if size > self._capacity:
            raise EntryTooLargeError(
                f"entry of {size} bytes exceeds table capacity {self._capacity}"
            )
        while self._size + size > self._capacity:
            self._evict_oldest()
        entry = TableEntry(name, value, self._insert_count, size)
        self._entries.append(entry)
        self._size += size
        self._insert_count += 1
        return entry

    def get(self, absolute_index: int) -> TableEntry:
        """Fetch by absolute index; raises if evicted or never inserted."""
        if absolute_index < 0 or absolute_index >= self._insert_count:
            raise UnknownIndexError(
                f"absolute index {absolute_index} not yet inserted "
                f"(insert count {self._insert_count})"
            )
        position = absolute_index - self.dropped_count
        if position < 0:
            raise UnknownIndexError(
                f"absolute index {absolute_index} already evicted "
                f"(dropped {self.dropped_count})"
            )
        return self._entries[position]

    def find(self, name: str, value: str) -> tuple[TableEntry | None, bool]:
        """Best match, newest first: ``(entry, exact)``."""
        name_match: TableEntry | None = None
        for entry in reversed(self._entries):
            if entry.name == name:
                if entry.value == value:
                    return entry, True
                if name_match is None:
                    name_match = entry
        return (name_match, False) if name_match is not None else (None, False)

    def entries_newest_first(self) -> list[TableEntry]:
        return list(reversed(self._entries))

    def _evict_oldest(self) -> TableEntry:
        entry = self._entries.popleft()
        self._size -= entry.size
        return entry

    def _evict_to_fit(self) -> list[TableEntry]:
        evicted: list[TableEntry] = []
        while self._size > self._capacity:
            evicted.append(self._evict_oldest())
        return evicted
