"""Dynamic table capacity management and eviction tests."""

import pytest

from header_compression.errors import EntryTooLargeError, UnknownIndexError
from header_compression.tables import (
    ENTRY_OVERHEAD,
    DynamicTable,
    entry_size,
    find_static,
)


def make_table(capacity: int = 256) -> DynamicTable:
    return DynamicTable(capacity)


class TestCapacityEviction:
    def test_oldest_evicted_first(self):
        size = entry_size("aa", "1")
        assert size == entry_size("bb", "2") == entry_size("cc", "3")
        table = make_table(2 * size)
        table.insert("aa", "1")
        table.insert("bb", "2")
        assert len(table) == 2
        table.insert("cc", "3")  # forces eviction of "aa"
        assert table.dropped_count == 1
        assert [e.name for e in table.entries_newest_first()] == ["cc", "bb"]
        with pytest.raises(UnknownIndexError, match="evicted"):
            table.get(0)

    def test_absolute_indices_stable_across_eviction(self):
        table = make_table(256)
        indices = [table.insert(f"n{i}", "v").absolute_index for i in range(4)]
        assert indices == [0, 1, 2, 3]
        assert table.get(2).name == "n2"

    def test_insert_until_exact_fit(self):
        size = entry_size("n0", "v")
        table = make_table(3 * size)
        for i in range(3):
            table.insert(f"n{i}", "v")
        assert table.size == 3 * size and len(table) == 3
        table.insert("n3", "v")
        assert len(table) == 3 and table.dropped_count == 1

    def test_entry_larger_than_capacity_rejected(self):
        table = make_table(entry_size("a", "b") - 1)
        with pytest.raises(EntryTooLargeError):
            table.insert("a", "b")
        assert len(table) == 0 and table.insert_count == 0

    def test_entry_size_includes_overhead(self):
        assert entry_size("ab", "cde") == 2 + 3 + ENTRY_OVERHEAD


class TestSetCapacity:
    def test_shrink_evicts_oldest(self):
        size = entry_size("n0", "v")
        table = make_table(4 * size)
        for i in range(4):
            table.insert(f"n{i}", "v")
        evicted = table.set_capacity(size)
        assert [e.name for e in evicted] == ["n0", "n1", "n2"]
        assert table.get(3).name == "n3"

    def test_shrink_to_zero(self):
        table = make_table(1024)
        table.insert("a", "b")
        evicted = table.set_capacity(0)
        assert len(evicted) == 1 and len(table) == 0
        with pytest.raises(EntryTooLargeError):
            table.insert("a", "b")

    def test_grow_within_max(self):
        table = make_table(128)
        table.set_capacity(64)
        table.set_capacity(128)
        assert table.capacity == 128

    def test_grow_beyond_max_rejected(self):
        table = make_table(128)
        with pytest.raises(ValueError):
            table.set_capacity(129)

    def test_zero_capacity_table(self):
        table = make_table(0)
        with pytest.raises(EntryTooLargeError):
            table.insert("a", "b")


class TestLookup:
    def test_static_exact_and_name_match(self):
        assert find_static(":method", "GET") == (5, True)
        index, exact = find_static(":method", "DELETE")
        assert index == 5 and not exact

    def test_static_miss(self):
        assert find_static("x-absent", "v") == (None, False)

    def test_newest_dynamic_match_wins(self):
        table = make_table(1024)
        table.insert("n", "old")
        table.insert("n", "new")
        entry, exact = table.find("n", "new")
        assert exact and entry.absolute_index == 1
        entry, exact = table.find("n", "other")
        assert not exact and entry.absolute_index == 1  # newest name match
