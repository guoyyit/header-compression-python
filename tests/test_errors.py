"""Safe failure on malformed/truncated input; per-connection isolation."""

import pytest

from header_compression import (
    CompressionConfig,
    ConnectionCompressionContext,
    ConnectionRegistry,
    wire,
)
from header_compression.errors import (
    CompressionError,
    DecoderStreamError,
    InvalidRepresentationError,
    TruncatedDataError,
    UnknownIndexError,
)

from .conftest import pump


def make_pair():
    config = CompressionConfig()
    return (
        ConnectionCompressionContext("a", config),
        ConnectionCompressionContext("b", config),
    )


class TestMalformedInput:
    @pytest.mark.parametrize(
        "bad_block",
        [
            b"",  # empty: no prefix at all
            b"\x01",  # prefix truncated: missing block seq
            b"\x00\x00\x08\x05ab",  # truncated literal name
            b"\x00\x00\x85",  # truncated after indexed field line? (valid!)
            b"\x00\x00\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\x7f",  # huge integer
            b"\x00\x00\x07",  # reserved field line pattern
            b"\x00\x00\x08\x01a",  # literal name without value
        ],
    )
    def test_bad_header_blocks_fail_cleanly(self, bad_block):
        _, b = make_pair()
        if bad_block == b"\x00\x00\x85":
            # actually valid: static index 5; assert it decodes instead
            assert b.decode_headers(1, bad_block) == [(":method", "GET")]
            return
        with pytest.raises(CompressionError):
            b.decode_headers(1, bad_block)

    def test_truncation_is_distinguishable(self):
        _, b = make_pair()
        with pytest.raises(TruncatedDataError):
            b.decode_headers(1, b"\x00\x00\x08\x05ab")

    def test_reserved_pattern_is_distinguishable(self):
        _, b = make_pair()
        with pytest.raises(InvalidRepresentationError):
            b.decode_headers(1, b"\x00\x00\x07")

    def test_reference_to_never_inserted_entry(self):
        a, b = make_pair()
        pump(a, b)
        # give the decoder one real entry so the block is not merely parked
        a.encode_headers(1, [("x-real", "v")])
        pump(a, b)
        assert b.decoder.received_insert_count == 1
        block = wire.encode_block_prefix(1, 0) + wire.encode_indexed_dynamic(9)
        with pytest.raises(UnknownIndexError, match="not yet inserted"):
            b.decode_headers(3, block)

    def test_reference_to_evicted_entry(self):
        config = CompressionConfig(table_capacity=64, max_table_capacity=64)
        a = ConnectionCompressionContext("a", config)
        b = ConnectionCompressionContext("b", config)
        pump(a, b)
        # insert two entries; the second evicts the first (38 bytes each > 64/2)
        block1 = a.encode_headers(1, [("x-old", "1")])
        pump(a, b)
        b.decode_headers(1, block1)
        a.feed_decoder_stream(b.drain_decoder_stream())  # ack: "x-old" evictable
        block3 = a.encode_headers(3, [("x-new", "2")])
        pump(a, b)
        # hand-craft a block referencing the evicted absolute index 0;
        # ric=2 matches the decoder's received count so it decodes at once
        assert b.decoder.received_insert_count == 2
        block = wire.encode_block_prefix(2, 99) + wire.encode_indexed_dynamic(0)
        with pytest.raises(UnknownIndexError, match="evicted"):
            b.decode_headers(9, block)

    def test_garbage_encoder_stream_fails_without_state_mutation(self):
        a, b = make_pair()
        pump(a, b)
        size_before = b.decoder.table.size
        count_before = b.decoder.received_insert_count
        next_seq = b.state_snapshot()["sequencing"]["next_in_seq"]
        with pytest.raises(InvalidRepresentationError):
            b.feed_encoder_stream(next_seq, b"\x21")  # reserved bits set
        # failed batch left no partial application behind
        assert b.decoder.table.size == size_before
        assert b.decoder.received_insert_count == count_before

    def test_decoder_stream_protocol_errors(self):
        a, b = make_pair()
        pump(a, b)
        with pytest.raises(DecoderStreamError, match="unknown or already-acked"):
            a.feed_decoder_stream(wire.encode_header_ack(123))
        with pytest.raises(DecoderStreamError, match="must be positive"):
            a.feed_decoder_stream(wire.encode_insert_count_increment(0))
        with pytest.raises(DecoderStreamError, match="exceeds insert count"):
            a.feed_decoder_stream(wire.encode_insert_count_increment(5))

    def test_error_is_reproducible(self):
        _, b = make_pair()
        first = second = None
        for _ in range(2):
            try:
                b.decode_headers(1, b"\x00\x00\x08\x05ab")
            except CompressionError as exc:
                if first is None:
                    first = (type(exc), str(exc))
                else:
                    second = (type(exc), str(exc))
        assert first == second


class TestConnectionIsolation:
    def test_failure_on_one_connection_does_not_pollute_others(self):
        registry = ConnectionRegistry(CompressionConfig())
        bad = registry.get_or_create("bad")
        good = registry.get_or_create("good")
        pump(good, registry.get_or_create("peer"))
        with pytest.raises(TruncatedDataError):
            bad.decode_headers(1, b"\x00\x00\x08\x05ab")
        # the good connection still encodes/decodes fine
        peer = registry.get_or_create("peer")
        block = good.encode_headers(1, [("x-ok", "1")])
        pump(good, peer)
        assert peer.decode_headers(1, block) == [("x-ok", "1")]
        # and the bad connection's table state is untouched by the good one
        assert bad.decoder.table.insert_count == 0

    def test_contexts_share_no_mutable_state(self):
        registry = ConnectionRegistry(CompressionConfig())
        c1 = registry.get_or_create("c1")
        c2 = registry.get_or_create("c2")
        c1.encode_headers(1, [("x-only-c1", "v")])
        assert c2.encoder.insert_count == 0
        assert c1.encoder.insert_count == 1

    def test_reset_rebuilds_state_deterministically(self):
        registry = ConnectionRegistry(CompressionConfig())
        ctx = registry.get_or_create("c")
        ctx.encode_headers(1, [("x-before-reset", "v")])
        assert ctx.encoder.insert_count == 1
        registry.reset("c")
        assert ctx.encoder.insert_count == 0
        assert ctx.decoder.received_insert_count == 0
        # encoding again produces the very same bytes as a fresh connection
        fresh = ConnectionRegistry(CompressionConfig()).get_or_create("fresh")
        assert ctx.encode_headers(1, [("x-a", "1")]) == fresh.encode_headers(1, [("x-a", "1")])

    def test_close_releases_state_and_is_idempotent(self):
        registry = ConnectionRegistry(CompressionConfig())
        ctx = registry.get_or_create("c")
        ctx.encode_headers(1, [("x", "v")])
        assert registry.close("c") is True
        assert ctx.closed
        assert registry.close("c") is False  # already gone
        with pytest.raises(CompressionError, match="closed"):
            ctx.encode_headers(1, [("x", "v")])
        # a new connection with the same id starts from a clean slate
        new_ctx = registry.get_or_create("c")
        assert new_ctx is not ctx
        assert new_ctx.encoder.insert_count == 0

    def test_error_recording_is_bounded_and_per_connection(self):
        registry = ConnectionRegistry(CompressionConfig())
        ctx = registry.get_or_create("c")
        for _ in range(300):
            try:
                ctx.decode_headers(1, b"\x00\x00\x08\x05ab")
            except CompressionError as exc:
                ctx.record_error(exc)
        assert 0 < len(ctx.errors) <= 256
        other = registry.get_or_create("other")
        assert other.errors == []
