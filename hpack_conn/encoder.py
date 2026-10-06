"""Header block encoder bound to one connection's dynamic table."""

from __future__ import annotations

from .tables import DynamicTable, HeaderField, StaticTable


class Encoder:
    """Encodes header lists into header blocks. Skeleton."""

    def __init__(self, table: DynamicTable, static_table: StaticTable | None = None) -> None:
        raise NotImplementedError

    def set_capacity(self, capacity: int) -> bytes:
        """Apply a negotiated capacity change; returns the protocol update bytes."""
        raise NotImplementedError

    def encode(self, headers: list[tuple[str, str]]) -> bytes:
        raise NotImplementedError
