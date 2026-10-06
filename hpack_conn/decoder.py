"""Header block decoder bound to one connection's dynamic table."""

from __future__ import annotations

from .tables import DynamicTable, HeaderField, StaticTable


class Decoder:
    """Decodes header blocks against the connection dynamic table. Skeleton."""

    def __init__(self, table: DynamicTable, static_table: StaticTable | None = None) -> None:
        raise NotImplementedError

    def set_capacity(self, capacity: int) -> None:
        """Apply a negotiated capacity change signalled by the peer."""
        raise NotImplementedError

    def decode(self, data: bytes) -> list[tuple[str, str]]:
        raise NotImplementedError
