"""Concurrent multi-stream sharing, out-of-order and duplicate updates."""

import threading

from header_compression import CompressionConfig, ConnectionCompressionContext

from .conftest import pump


def make_pair():
    config = CompressionConfig()
    return (
        ConnectionCompressionContext("a", config),
        ConnectionCompressionContext("b", config),
    )


class TestOutOfOrderUpdates:
    def test_out_of_order_batches_buffered_then_applied_in_order(self):
        a, b = make_pair()
        batches = []
        for i in range(3):
            a.encode_headers(2 * i + 1, [(f"x-o{i}", "v")])
            batches.append(a.next_encoder_batch())
        assert len(batches) == 3
        # deliver in reverse order: nothing applies until the gap is filled
        assert b.feed_encoder_stream(*batches[2]) == []
        assert b.decoder.received_insert_count == 0
        assert b.feed_encoder_stream(*batches[1]) == []
        assert b.decoder.received_insert_count == 0
        b.feed_encoder_stream(*batches[0])
        assert b.decoder.received_insert_count == 3
        # insertion order is the encoder's order, not arrival order
        names = [e.name for e in b.decoder.table.entries_newest_first()]
        assert names == ["x-o2", "x-o1", "x-o0"]

    def test_duplicate_batches_dropped(self):
        a, b = make_pair()
        a.encode_headers(1, [("x-dup", "v")])
        batch = a.next_encoder_batch()
        b.feed_encoder_stream(*batch)
        count_after_first = b.decoder.received_insert_count
        assert b.feed_encoder_stream(*batch) == []  # duplicate: no-op
        assert b.decoder.received_insert_count == count_after_first
        assert len(b.decoder.table) == 1

    def test_duplicate_and_reordered_combined(self):
        a, b = make_pair()
        batches = []
        for i in range(4):
            a.encode_headers(2 * i + 1, [(f"x-c{i}", "v")])
            batches.append(a.next_encoder_batch())
        # chaotic delivery: duplicates interleaved with reordering
        for batch in (batches[1], batches[0], batches[1], batches[3], batches[0]):
            b.feed_encoder_stream(*batch)
        assert b.decoder.received_insert_count == 2
        b.feed_encoder_stream(*batches[2])
        assert b.decoder.received_insert_count == 4
        names = [e.name for e in b.decoder.table.entries_newest_first()]
        assert names == ["x-c3", "x-c2", "x-c1", "x-c0"]

    def test_decode_state_matches_deterministic_insertion_order(self):
        """Any decode sees the table as of a definite prefix of insertions."""
        a, b = make_pair()
        blocks = []
        batches = []
        for i in range(5):
            blocks.append(a.encode_headers(2 * i + 1, [(f"x-det-{i}", "v")]))
            batches.append(a.next_encoder_batch())  # one batch per insertion
        # blocks decoded after feeding the first k batches see exactly the
        # entries from those k batches
        for k in range(len(batches)):
            b.feed_encoder_stream(*batches[k])
            out = b.decode_headers(2 * k + 1, blocks[k])
            assert out == [(f"x-det-{k}", "v")]
            assert b.decoder.received_insert_count == k + 1


class TestThreads:
    def test_concurrent_streams_on_shared_connection(self):
        a, b = make_pair()
        pump(a, b)
        n_threads, n_blocks = 8, 20
        barrier = threading.Barrier(n_threads)
        errors: list[BaseException] = []

        def worker(tid: int):
            try:
                barrier.wait(timeout=10)
                for i in range(n_blocks):
                    stream_id = 2 * (tid * n_blocks + i) + 1
                    headers = [(f"x-t{tid}", f"v{i}"), (":method", "GET")]
                    block = a.encode_headers(stream_id, headers)
                    pump(a, b)
                    out = b.decode_headers(stream_id, block)
                    assert out == headers, (out, headers)
                    pump(a, b)  # acks back
            except BaseException as exc:  # noqa: BLE001
                errors.append(exc)

        threads = [threading.Thread(target=worker, args=(t,)) for t in range(n_threads)]
        for t in threads:
            t.start()
        for t in threads:
            t.join(timeout=60)
        assert not errors, errors
        # both sides converged to the same table state
        assert a.encoder.insert_count == b.decoder.received_insert_count
        assert (
            a.encoder.table.entries_newest_first()
            == b.decoder.table.entries_newest_first()
        )
        assert a.encoder.pending_ack_count == 0

    def test_concurrent_connections_isolated(self):
        config = CompressionConfig()
        contexts = [
            ConnectionCompressionContext(f"conn-{i}", config) for i in range(4)
        ]
        errors: list[BaseException] = []

        def worker(idx: int):
            try:
                ctx = contexts[idx]
                for i in range(30):
                    ctx.encode_headers(2 * i + 1, [(f"x-conn{idx}", f"v{i}")])
            except BaseException as exc:  # noqa: BLE001
                errors.append(exc)

        threads = [threading.Thread(target=worker, args=(i,)) for i in range(4)]
        for t in threads:
            t.start()
        for t in threads:
            t.join(timeout=60)
        assert not errors, errors
        # each connection only ever saw its own header names
        for idx, ctx in enumerate(contexts):
            names = {e.name for e in ctx.encoder.table.entries_newest_first()}
            assert names == {f"x-conn{idx}"}
