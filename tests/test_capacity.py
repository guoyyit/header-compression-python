"""Dynamic table capacity adjustment during a connection's lifetime."""

import pytest

from header_compression import CompressionConfig, ConnectionCompressionContext, wire
from header_compression.errors import (
    CapacityExceededError,
    CapacityReductionBlockedError,
)

from .conftest import pump


def make_pair(capacity=1024, max_capacity=4096):
    config = CompressionConfig(table_capacity=capacity, max_table_capacity=max_capacity)
    return (
        ConnectionCompressionContext("a", config),
        ConnectionCompressionContext("b", config),
    )


def sync(a, b):
    """Deliver all pending traffic in both directions."""
    pump(a, b)
    pump(b, a)


def roundtrip(a, b, stream_id, headers):
    """Encode on a, sync, decode on b, deliver acks back to a."""
    block = a.encode_headers(stream_id, headers)
    sync(a, b)
    out = b.decode_headers(stream_id, block)
    assert out == headers
    sync(a, b)  # deliver decoder-stream acks
    return out


class TestCapacityAdjustment:
    def test_initial_capacity_negotiated_via_encoder_stream(self):
        a, b = make_pair(capacity=512)
        assert b.decoder.table.capacity == 0  # not open until instructed
        sync(a, b)
        assert b.decoder.table.capacity == 512

    def test_shrink_applies_at_agreed_point(self):
        a, b = make_pair(capacity=1024)
        for i in range(4):
            roundtrip(a, b, 2 * i + 1, [(f"x-h{i}", "v")])
        assert b.decoder.table.insert_count == 4

        a.encoder.set_capacity(64)
        # encoder-side model shrank immediately...
        assert a.encoder.capacity == 64
        # ...decoder applies it when the instruction arrives: the same point
        # in the insertion order on both sides.
        sync(a, b)
        assert b.decoder.table.capacity == 64
        assert b.decoder.table.size <= 64
        # both sides agree on which entries survived
        assert (
            a.encoder.table.entries_newest_first()
            == b.decoder.table.entries_newest_first()
        )

    def test_grow_after_shrink(self):
        a, b = make_pair(capacity=1024)
        sync(a, b)
        a.encoder.set_capacity(0)
        sync(a, b)
        assert b.decoder.table.capacity == 0
        a.encoder.set_capacity(256)
        sync(a, b)
        roundtrip(a, b, 1, [("x-after-grow", "yes")])
        assert b.decoder.table.get(0).name == "x-after-grow"

    def test_shrink_to_zero_blocks_insertions(self):
        a, b = make_pair(capacity=1024)
        sync(a, b)
        a.encoder.set_capacity(0)
        block = a.encode_headers(1, [("x-not-stored", "v")])
        pump(a, b)  # delivers the set-capacity instruction
        assert b.decode_headers(1, block) == [("x-not-stored", "v")]
        assert b.decoder.table.insert_count == 0  # literal, nothing inserted

    def test_decoder_rejects_capacity_above_maximum(self):
        a, b = make_pair(capacity=4096, max_capacity=4096)
        sync(a, b)
        next_seq = b.state_snapshot()["sequencing"]["next_in_seq"]
        with pytest.raises(CapacityExceededError, match="above maximum"):
            b.feed_encoder_stream(next_seq, wire.encode_set_capacity(5000))

    def test_encoder_rejects_capacity_above_maximum(self):
        a, _ = make_pair(capacity=128)
        with pytest.raises(CapacityExceededError, match="exceeds maximum"):
            a.encoder.set_capacity(4097)


class TestEvictionGuard:
    def test_reduction_blocked_by_unacked_references(self):
        a, b = make_pair(capacity=256)
        blocks = [a.encode_headers(2 * i + 1, [(f"x-guard-{i}", "v")]) for i in range(4)]
        pump(a, b)  # decoder has the entries; blocks not decoded yet
        with pytest.raises(CapacityReductionBlockedError):
            a.encoder.set_capacity(0)
        # decode the blocks and deliver the acks: entries become evictable
        for i, block in enumerate(blocks):
            assert b.decode_headers(2 * i + 1, block) is not None
        a.feed_decoder_stream(b.drain_decoder_stream())
        a.encoder.set_capacity(0)
        assert a.encoder.capacity == 0

    def test_insert_skipped_when_eviction_would_drop_unacked(self):
        config = CompressionConfig(table_capacity=80, max_table_capacity=80)
        a = ConnectionCompressionContext("a", config)
        b = ConnectionCompressionContext("b", config)
        pump(a, b)
        # two 38-byte entries fill 76 of 80 bytes; both blocks unacked
        block1 = a.encode_headers(1, [("x-one", "1")])
        block3 = a.encode_headers(3, [("x-two", "2")])
        pump(a, b)
        # a third insert would have to evict "x-one", still referenced by
        # stream 1's unacknowledged block -> insertion is skipped instead
        block = a.encode_headers(5, [("x-three", "3")])
        assert a.next_encoder_batch() is None
        pump(a, b)
        assert b.decode_headers(5, block) == [("x-three", "3")]
        # once acks arrive the entries are evictable and insertion resumes
        assert b.decode_headers(1, block1) == [("x-one", "1")]
        assert b.decode_headers(3, block3) == [("x-two", "2")]
        a.feed_decoder_stream(b.drain_decoder_stream())
        a.encode_headers(7, [("x-four", "4")])
        assert a.next_encoder_batch() is not None
