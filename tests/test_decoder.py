import pytest

from hpack_conn.decoder import Decoder
from hpack_conn.encoder import Encoder
from hpack_conn.errors import DecodeError, RejectReason
from hpack_conn.tables import DynamicTable


def make_pair(capacity=4096, **kw):
    enc_table, dec_table = DynamicTable(capacity), DynamicTable(capacity)
    return Encoder(enc_table, **kw), Decoder(dec_table), enc_table, dec_table


class TestRoundtrip:
    def test_static_only(self):
        enc, dec, _, _ = make_pair()
        headers = [(":method", "GET"), (":path", "/"), (":scheme", "https")]
        assert dec.decode(enc.encode(headers)) == [tuple(h) for h in headers]

    def test_dynamic_insert_then_reference(self):
        enc, dec, _, _ = make_pair()
        h1 = [("x-token", "abc")]
        assert dec.decode(enc.encode(h1)) == h1
        # second block references the dynamic entry
        assert dec.decode(enc.encode(h1)) == h1

    def test_mixed_sequence_keeps_tables_in_sync(self):
        enc, dec, enc_t, dec_t = make_pair()
        sequence = [
            [(":method", "GET"), ("x-a", "1")],
            [("x-a", "1"), ("x-b", "2")],
            [(":path", "/index.html"), ("x-b", "2"), ("x-c", "3")],
        ]
        for headers in sequence:
            assert dec.decode(enc.encode(headers)) == headers
        assert enc_t.size == dec_t.size
        assert len(enc_t) == len(dec_t)
        assert enc_t.insertion_count == dec_t.insertion_count
        for i in range(1, len(enc_t) + 1):
            assert enc_t.get(i) == dec_t.get(i)

    def test_byte_for_byte_determinism_across_instances(self):
        blocks = []
        for _ in range(2):
            enc, _, _, _ = make_pair()
            out = [enc.encode([("k%d" % i, "v%d" % i)]) for i in range(10)]
            blocks.append(out)
        assert blocks[0] == blocks[1]

    def test_capacity_update_roundtrip(self):
        enc, dec, enc_t, dec_t = make_pair()
        enc.set_capacity(128)
        block = enc.encode([("x-a", "1")])
        assert dec.decode(block) == [("x-a", "1")]
        assert dec_t.capacity == 128 == enc_t.capacity

    def test_eviction_stays_in_sync(self):
        enc, dec, enc_t, dec_t = make_pair(capacity=70)
        for i in range(5):  # each entry 34 octets -> constant eviction
            headers = [(f"k{i}", "v")]
            assert dec.decode(enc.encode(headers)) == headers
        assert len(enc_t) == len(dec_t) == 2
        assert enc_t.get(1) == dec_t.get(1)


class TestDecodeFailures:
    def test_truncated_block(self):
        enc, dec, _, _ = make_pair()
        block = enc.encode([("x-new", "value")])
        with pytest.raises(DecodeError) as exc:
            dec.decode(block[:-1])
        assert exc.value.reason is RejectReason.TRUNCATED_INPUT

    def test_empty_block_decodes_to_empty(self):
        _, dec, _, _ = make_pair()
        assert dec.decode(b"") == []

    def test_invalid_static_index(self):
        _, dec, _, _ = make_pair()
        with pytest.raises(DecodeError) as exc:
            dec.decode(bytes([0x80 | 62]))  # static+dynamic have < 62 here
        assert exc.value.reason is RejectReason.INVALID_INDEX

    def test_index_zero_reserved(self):
        _, dec, _, _ = make_pair()
        with pytest.raises(DecodeError) as exc:
            dec.decode(bytes([0x80]))
        assert exc.value.reason is RejectReason.INVALID_INDEX

    def test_size_update_after_field_rejected(self):
        enc, dec, _, _ = make_pair()
        block = enc.encode([(":method", "GET")]) + bytes([0x20 | 10])
        with pytest.raises(DecodeError) as exc:
            dec.decode(block)
        assert exc.value.reason is RejectReason.MALFORMED_INPUT

    def test_size_update_above_max_rejected(self):
        from hpack_conn.integers import encode_integer

        _, dec, _, _ = make_pair(capacity=64)
        bad = encode_integer(200, 5, 0x20)  # capacity 200 > max 64
        with pytest.raises(DecodeError) as exc:
            dec.decode(bad)
        assert exc.value.reason is RejectReason.CAPACITY_SYNC_VIOLATION

    def test_entry_larger_than_capacity_rejected(self):
        enc, dec, _, _ = make_pair()
        enc.set_capacity(0)  # encoder will not index anything now
        # hand-craft: literal-with-incremental for an entry of 34+ octets
        # with decoder capacity forced to 0
        dec.set_capacity(0)
        block = b"\x40\x01k\x01v"  # 34-octet entry into capacity-0 table
        with pytest.raises(DecodeError) as exc:
            dec.decode(block)
        assert exc.value.reason is RejectReason.ENTRY_TOO_LARGE
