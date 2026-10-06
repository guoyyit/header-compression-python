from hpack_conn.encoder import Encoder
from hpack_conn.tables import DynamicTable, HeaderField


def make_encoder(capacity=4096, **kw):
    table = DynamicTable(capacity)
    return Encoder(table, **kw), table


class TestEncodeBasics:
    def test_static_exact_match_is_indexed(self):
        enc, _ = make_encoder()
        # :method GET is static index 2 -> 0x80 | 2
        assert enc.encode([(":method", "GET")]) == bytes([0x82])

    def test_literal_with_incremental_indexing(self):
        enc, table = make_encoder()
        block = enc.encode([("x-new", "1")])
        # 0x40 | 0 (no name index), then name, then value
        assert block == b"\x40\x05x-new\x011"
        assert table.get(1) == HeaderField("x-new", "1")

    def test_second_use_hits_dynamic_table(self):
        enc, _ = make_encoder()
        enc.encode([("x-new", "1")])
        block = enc.encode([("x-new", "1")])
        # dynamic index 1 -> combined 61+1=62 -> 0x80|62
        assert block == bytes([0x80 | 62])

    def test_name_reference_from_static(self):
        enc, _ = make_encoder()
        block = enc.encode([(":path", "/other")])
        # name :path is static index 4 -> 0x40|4, then value
        assert block == b"\x44\x06/other"

    def test_deterministic_bytes(self):
        enc1, _ = make_encoder()
        enc2, _ = make_encoder()
        seq = [[("a", "1")], [("b", "2")], [("a", "1")]]
        for headers in seq:
            assert enc1.encode(headers) == enc2.encode(headers)

    def test_indexing_disabled_uses_without_indexing_form(self):
        enc, table = make_encoder(indexing=False)
        block = enc.encode([("x-new", "1")])
        assert block == b"\x00\x05x-new\x011"
        assert len(table) == 0

    def test_oversized_entry_not_indexed(self):
        enc, table = make_encoder(capacity=40)
        block = enc.encode([("x-big", "v" * 100)])
        assert block[0] == 0x00  # without indexing
        assert len(table) == 0


class TestCapacityUpdates:
    def test_set_capacity_returns_update_bytes(self):
        enc, table = make_encoder()
        # 30 fits the 5-bit prefix directly
        update = enc.set_capacity(30)
        assert update == bytes([0x20 | 30])
        assert table.capacity == 30

    def test_set_capacity_large_uses_continuation(self):
        enc, _ = make_encoder()
        # 100 = 31 (prefix max) + 69 (continuation)
        assert enc.set_capacity(100) == bytes([0x3F, 69])

    def test_update_leads_next_block(self):
        enc, _ = make_encoder()
        enc.set_capacity(30)
        block = enc.encode([(":method", "GET")])
        assert block == bytes([0x20 | 30, 0x82])

    def test_update_emitted_once(self):
        enc, _ = make_encoder()
        enc.set_capacity(30)
        enc.encode([(":method", "GET")])
        assert enc.encode([(":method", "GET")]) == bytes([0x82])

    def test_multiple_updates_all_emitted_in_order(self):
        enc, _ = make_encoder()
        enc.set_capacity(30)
        enc.set_capacity(20)
        block = enc.encode([(":method", "GET")])
        assert block == bytes([0x20 | 30, 0x20 | 20, 0x82])
