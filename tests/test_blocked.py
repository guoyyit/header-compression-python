"""Blocked streams: waiting on unsynchronised table entries."""

import pytest

from header_compression import CompressionConfig, ConnectionCompressionContext
from header_compression.errors import BlockedStreamLimitError

from .conftest import pump


def make_pair(max_blocked=4):
    config = CompressionConfig(max_blocked_streams=max_blocked)
    return (
        ConnectionCompressionContext("a", config),
        ConnectionCompressionContext("b", config),
    )


class TestBlockingAndResume:
    def test_stream_blocks_until_dependency_arrives(self):
        a, b = make_pair()
        pump(a, b)  # open the table
        block = a.encode_headers(1, [("x-dep", "v")])
        batch = a.next_encoder_batch()
        assert batch is not None
        # block arrives before its dependency -> parked, not an error
        assert b.decode_headers(1, block) is None
        assert b.decoder.blocked_stream_ids == [1]
        # dependency arrives -> automatically resumed
        unblocked = b.feed_encoder_stream(*batch)
        assert unblocked == [(1, [("x-dep", "v")])]
        assert b.decoder.blocked_stream_ids == []

    def test_independent_streams_not_blocked(self):
        a, b = make_pair()
        pump(a, b)
        dependent = a.encode_headers(1, [("x-new-entry", "v")])
        held_batch = a.next_encoder_batch()
        independent = a.encode_headers(3, [(":method", "GET")])  # static only
        # independent stream decodes fine while stream 1 waits
        assert b.decode_headers(1, dependent) is None
        assert b.decode_headers(3, independent) == [(":method", "GET")]
        assert b.decoder.blocked_stream_ids == [1]
        unblocked = b.feed_encoder_stream(*held_batch)
        assert unblocked == [(1, [("x-new-entry", "v")])]

    def test_multiple_streams_resume_in_stream_order(self):
        a, b = make_pair()
        pump(a, b)
        blocks = {}
        for stream_id in (9, 3, 7):
            blocks[stream_id] = a.encode_headers(stream_id, [(f"x-s{stream_id}", "v")])
        batches = []
        while (batch := a.next_encoder_batch()) is not None:
            batches.append(batch)
        for stream_id in (9, 3, 7):
            assert b.decode_headers(stream_id, blocks[stream_id]) is None
        assert b.decoder.blocked_stream_ids == [3, 7, 9]
        unblocked = []
        for batch in batches:
            unblocked.extend(b.feed_encoder_stream(*batch))
        assert [stream_id for stream_id, _ in unblocked] == [3, 7, 9]

    def test_partial_dependency_arrival(self):
        a, b = make_pair()
        pump(a, b)
        # stream 1 depends on two insertions; deliver them one at a time
        block = a.encode_headers(1, [("x-first", "1"), ("x-second", "2")])
        seq, payload = a.next_encoder_batch()
        # split the batch into two feeds by decoding instruction boundaries
        from header_compression import wire

        first_end = len(wire.encode_insert_literal("x-first", "1"))
        assert b.decode_headers(1, block) is None
        assert b.feed_encoder_stream(seq, payload[:first_end]) == []
        assert b.decoder.blocked_stream_ids == [1]  # still waiting for the second
        unblocked = b.feed_encoder_stream(seq + 1, payload[first_end:])
        # note: split feeds use consecutive seqs at the connection layer;
        # here we feed the decoder endpoint directly, order is what matters
        assert unblocked == [(1, [("x-first", "1"), ("x-second", "2")])]


class TestBoundedWait:
    def test_blocked_limit_is_enforced(self):
        a, b = make_pair(max_blocked=2)
        pump(a, b)
        for stream_id in (1, 3):
            block = a.encode_headers(stream_id, [(f"x-b{stream_id}", "v")])
            assert b.decode_headers(stream_id, block) is None
        # third distinct blocked stream exceeds the configured bound
        block = a.encode_headers(5, [("x-b5", "v")])
        with pytest.raises(BlockedStreamLimitError, match="blocked stream limit"):
            b.decode_headers(5, block)
        # the parked streams are untouched by the rejection
        assert b.decoder.blocked_stream_ids == [1, 3]

    def test_blocked_limit_configurable(self):
        config = CompressionConfig(max_blocked_streams=1)
        b = ConnectionCompressionContext("b", config)
        assert b.decoder._max_blocked_streams == 1
        with pytest.raises(ValueError):
            CompressionConfig(max_blocked_streams=0)

    def test_repeated_delivery_of_blocked_block_is_idempotent(self):
        a, b = make_pair()
        pump(a, b)
        block = a.encode_headers(1, [("x-dup", "v")])
        batch = a.next_encoder_batch()
        assert b.decode_headers(1, block) is None
        assert b.decode_headers(1, block) is None  # retransmission: still one entry
        assert b.decoder.blocked_count == 1
        unblocked = b.feed_encoder_stream(*batch)
        assert unblocked == [(1, [("x-dup", "v")])]
