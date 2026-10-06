"""Static table and dynamic table with capacity management and eviction."""

from __future__ import annotations

from dataclasses import dataclass

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
    raise NotImplementedError


def find_static(name: str, value: str) -> tuple[int | None, bool]:
    """Return ``(index, exact)`` for the best static-table match.

    Exact matches win; otherwise the lowest index with a matching name.
    """
    raise NotImplementedError


@dataclass(frozen=True)
class TableEntry:
    """One dynamic-table entry."""

    name: str
    value: str
    absolute_index: int
    size: int


class DynamicTable:
    """FIFO-evicting dynamic table addressed by absolute index."""

    def __init__(self, max_capacity: int) -> None:
        raise NotImplementedError

    @property
    def capacity(self) -> int:
        raise NotImplementedError

    @property
    def size(self) -> int:
        raise NotImplementedError

    @property
    def insert_count(self) -> int:
        """Total number of entries ever inserted (== next absolute index)."""
        raise NotImplementedError

    @property
    def dropped_count(self) -> int:
        """Number of entries evicted so far (== lowest live absolute index)."""
        raise NotImplementedError

    def __len__(self) -> int:
        raise NotImplementedError

    def set_capacity(self, capacity: int) -> list[TableEntry]:
        """Shrink/grow the capacity; returns evicted entries (oldest first)."""
        raise NotImplementedError

    def insert(self, name: str, value: str) -> TableEntry:
        """Insert an entry, evicting oldest entries until it fits."""
        raise NotImplementedError

    def get(self, absolute_index: int) -> TableEntry:
        """Fetch by absolute index; raises if evicted or never inserted."""
        raise NotImplementedError

    def entries_newest_first(self) -> list[TableEntry]:
        raise NotImplementedError
