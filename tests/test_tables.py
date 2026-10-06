import pytest

from hpack_conn.errors import CapacityError, DecodeError, RejectReason
from hpack_conn.tables import DynamicTable, HeaderField, StaticTable


class TestHeaderField:
    def test_size_accounting(self):
        assert HeaderField("a", "b").size == 1 + 1 + 32

    def test_size_counts_utf8_octets(self):
        assert HeaderField("中", "").size == 3 + 0 + 32


class TestStaticTable:
    def test_has_61_entries(self):
        assert len(StaticTable()) == 61

    def test_get_first(self):
        assert StaticTable().get(1) == HeaderField(":authority", "")

    def test_get_out_of_range(self):
        with pytest.raises(DecodeError) as exc:
            StaticTable().get(62)
        assert exc.value.reason is RejectReason.INVALID_INDEX

    def test_find_exact(self):
        assert StaticTable().find(":scheme", "https") == 7

    def test_find_name_only(self):
        assert StaticTable().find(":path") == 4

    def test_find_exact_preferred_over_name(self):
        table = StaticTable()
        assert table.find(":path", "/index.html") == 5
        assert table.find(":path", "/other") == 4  # falls back to name match

    def test_find_missing(self):
        assert StaticTable().find("x-nope") is None


class TestDynamicTable:
    def test_insert_and_get(self):
        table = DynamicTable(4096)
        table.insert("a", "1")
        table.insert("b", "2")
        assert table.get(1) == HeaderField("b", "2")  # newest first
        assert table.get(2) == HeaderField("a", "1")
        assert len(table) == 2

    def test_insertion_count_is_monotone(self):
        table = DynamicTable(4096)
        for i in range(5):
            table.insert(f"k{i}", "v")
        assert table.insertion_count == 5
        table.evict_oldest()
        assert table.insertion_count == 5  # eviction does not rewind

    def test_size_accounting(self):
        table = DynamicTable(4096)
        table.insert("ab", "cd")
        assert table.size == 2 + 2 + 32

    def test_eviction_on_capacity(self):
        # each entry: 1+1+32 = 34; capacity 70 fits exactly two
        table = DynamicTable(70)
        table.insert("a", "1")
        table.insert("b", "2")
        assert len(table) == 2
        table.insert("c", "3")  # forces eviction of oldest ("a")
        assert len(table) == 2
        assert table.get(1) == HeaderField("c", "3")
        assert table.get(2) == HeaderField("b", "2")
        assert table.size == 68

    def test_evict_oldest_empty(self):
        assert DynamicTable(4096).evict_oldest() is None

    def test_entry_too_large_rejected(self):
        table = DynamicTable(64)
        with pytest.raises(CapacityError) as exc:
            table.insert("k", "v" * 64)
        assert exc.value.reason is RejectReason.ENTRY_TOO_LARGE
        assert len(table) == 0  # failed insert leaves table untouched

    def test_memory_limit_rejects_before_capacity(self):
        table = DynamicTable(4096, memory_limit=100)
        with pytest.raises(CapacityError) as exc:
            table.insert("k", "v" * 100)
        assert exc.value.reason is RejectReason.MEMORY_LIMIT_EXCEEDED

    def test_set_capacity_shrink_evicts(self):
        table = DynamicTable(4096)
        for i in range(4):
            table.insert(f"k{i}", "v")  # 34 each -> 136
        table.set_capacity(70)
        assert len(table) == 2
        assert table.get(1) == HeaderField("k3", "v")

    def test_set_capacity_grow_within_max(self):
        table = DynamicTable(4096)
        table.set_capacity(100)
        assert table.capacity == 100
        table.set_capacity(4096)
        assert table.capacity == 4096

    def test_set_capacity_above_max_rejected(self):
        table = DynamicTable(4096)
        with pytest.raises(CapacityError) as exc:
            table.set_capacity(4097)
        assert exc.value.reason is RejectReason.CAPACITY_SYNC_VIOLATION

    def test_set_capacity_above_memory_limit_rejected(self):
        table = DynamicTable(4096, memory_limit=512)
        with pytest.raises(CapacityError) as exc:
            table.set_capacity(513)
        assert exc.value.reason is RejectReason.MEMORY_LIMIT_EXCEEDED

    def test_zero_capacity_holds_nothing(self):
        table = DynamicTable(4096)
        table.set_capacity(0)
        with pytest.raises(CapacityError):
            table.insert("a", "b")
        assert len(table) == 0

    def test_find_exact_and_name(self):
        table = DynamicTable(4096)
        table.insert("x-a", "1")
        table.insert("x-a", "2")
        assert table.find("x-a", "2") == 1
        assert table.find("x-a") == 1  # newest name match
        assert table.find("x-a", "1") == 2
        assert table.find("missing") is None

    def test_get_out_of_range(self):
        table = DynamicTable(4096)
        with pytest.raises(DecodeError) as exc:
            table.get(1)
        assert exc.value.reason is RejectReason.INVALID_INDEX
