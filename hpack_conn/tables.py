"""Static and dynamic header tables (RFC 7541 sections 2.3, 4)."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class HeaderField:
    """An immutable header field (name/value pair)."""

    name: str
    value: str

    @property
    def size(self) -> int:
        """Table size accounting: name + value + 32 bytes overhead."""
        return len(self.name.encode("utf-8")) + len(self.value.encode("utf-8")) + 32


class StaticTable:
    """The fixed static table shared by every connection. Skeleton."""

    def get(self, index: int) -> HeaderField:
        raise NotImplementedError

    def find(self, name: str, value: str | None = None) -> int | None:
        raise NotImplementedError


class DynamicTable:
    """Connection-local dynamic table with capacity + memory ceiling. Skeleton."""

    def __init__(self, max_capacity: int, memory_limit: int | None = None) -> None:
        raise NotImplementedError

    @property
    def capacity(self) -> int:
        raise NotImplementedError

    @property
    def size(self) -> int:
        raise NotImplementedError

    @property
    def insertion_count(self) -> int:
        raise NotImplementedError

    def set_capacity(self, capacity: int) -> None:
        raise NotImplementedError

    def insert(self, name: str, value: str) -> HeaderField:
        raise NotImplementedError

    def get(self, index: int) -> HeaderField:
        raise NotImplementedError

    def find(self, name: str, value: str | None = None) -> int | None:
        raise NotImplementedError

    def evict_oldest(self) -> HeaderField | None:
        raise NotImplementedError
