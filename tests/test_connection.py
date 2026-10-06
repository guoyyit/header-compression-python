import pytest

from hpack_conn.connection import ConnectionContext, ConnectionManager, ConnectionPhase
from hpack_conn.errors import CompressionError, DecodeError, RejectReason
from hpack_conn.stream import StreamPhase


def make_pair(**kw):
    a = ConnectionContext("a", **kw)
    b = ConnectionContext("b", **kw)
    return a, b


def deliver(src, dst, stream_id, headers):
    """Encode on src, decode on dst (in-order delivery)."""
    return dst.decode_headers(stream_id, src.encode_headers(stream_id, headers))


class TestPairedRoundtrip:
    def test_basic_exchange(self):
        a, b = make_pair()
        headers = [(":method", "GET"), ("x-a", "1")]
        assert deliver(a, b, 1, headers) == headers
        assert a.encoder_table.insertion_count == b.decoder_table.insertion_count

    def test_tables_stay_in_sync_over_many_streams(self):
        a, b = make_pair()
        for sid in range(1, 20):
            headers = [(f"x-k{sid}", str(sid)), (":path", "/")]
            assert deliver(a, b, sid, headers) == headers
        ea, db = a.encoder_table, b.decoder_table
        assert (ea.size, len(ea), ea.insertion_count) == (db.size, len(db), db.insertion_count)
        for i in range(1, len(ea) + 1):
            assert ea.get(i) == db.get(i)

    def test_bidirectional(self):
        a, b = make_pair()
        assert deliver(a, b, 1, [("x-req", "1")]) == [("x-req", "1")]
        assert deliver(b, a, 1, [("x-resp", "2")]) == [("x-resp", "2")]


class TestCapacityNegotiation:
    def test_capacity_change_takes_effect_at_next_block(self):
        a, b = make_pair()
        deliver(a, b, 1, [("x-a", "1")])
        a.update_capacity(64)
        # the very next block carries the size update first
        assert deliver(a, b, 2, [("x-b", "2")]) == [("x-b", "2")]
        assert a.encoder_table.capacity == 64
        assert b.decoder_table.capacity == 64

    def test_shrink_evicts_both_sides_identically(self):
        a, b = make_pair()
        for i in range(4):
            deliver(a, b, i, [(f"k{i}", "v")])
        a.update_capacity(40)  # fits exactly one 34-octet entry
        deliver(a, b, 99, [("after", "shrink")])
        ea, db = a.encoder_table, b.decoder_table
        assert len(ea) == len(db) == 1
        assert ea.get(1) == db.get(1)

    def test_no_undecodable_reference_during_shrink(self):
        a, b = make_pair()
        deliver(a, b, 1, [("x-keep", "v")])
        a.update_capacity(0)  # evicts everything on the encoder
        # encoder can no longer reference x-keep; decoder must agree
        headers = deliver(a, b, 2, [("x-keep", "v")])
        assert headers == [("x-keep", "v")]
        assert len(b.decoder_table) == 0

    def test_capacity_above_max_rejected(self):
        a, _ = make_pair(max_capacity=128)
        with pytest.raises(CompressionError) as exc:
            a.update_capacity(129)
        assert exc.value.reason is RejectReason.CAPACITY_SYNC_VIOLATION

    def test_grow_after_shrink(self):
        a, b = make_pair()
        a.update_capacity(64)
        deliver(a, b, 1, [("x-a", "1")])
        a.update_capacity(4096)
        assert deliver(a, b, 2, [("x-b", "2")]) == [("x-b", "2")]
        assert b.decoder_table.capacity == 4096


class TestBlockedStreams:
    def test_out_of_order_block_and_resume(self):
        a, b = make_pair()
        f1 = a.encode_headers(1, [("x-dep", "1")])   # inserts into table
        f2 = a.encode_headers(2, [("x-dep", "1")])   # references insertion
        # deliver stream 2 first: its dependency is missing -> blocked
        assert b.decode_headers(2, f2) is None
        assert b.stream_state(2).phase is StreamPhase.BLOCKED
        # delivering stream 1 satisfies the dependency -> auto-resume
        assert b.decode_headers(1, f1) == [("x-dep", "1")]
        state = b.stream_state(2)
        assert state.phase is StreamPhase.DONE
        assert state.headers == [("x-dep", "1")]

    def test_independent_stream_not_blocked(self):
        a, b = make_pair()
        f1 = a.encode_headers(1, [("x-dep", "1")])
        f2 = a.encode_headers(2, [("x-dep", "1")])
        f3 = a.encode_headers(3, [(":method", "GET")])  # static only
        assert b.decode_headers(2, f2) is None  # blocked
        # stream 3 has no dependency: decodes immediately
        assert b.decode_headers(3, f3) == [(":method", "GET")]
        assert b.decode_headers(1, f1) == [("x-dep", "1")]
        assert b.stream_state(2).phase is StreamPhase.DONE

    def test_blocked_wait_is_bounded(self):
        clock_val = [0.0]
        a = ConnectionContext("a")
        b = ConnectionContext("b", max_blocked_wait=1.0,
                              clock=lambda: clock_val[0])
        f1 = a.encode_headers(1, [("x-dep", "1")])
        f2 = a.encode_headers(2, [("x-dep", "1")])
        assert b.decode_headers(2, f2) is None
        clock_val[0] += 2.0
        assert b.check_timeouts() == [2]
        state = b.stream_state(2)
        assert state.phase is StreamPhase.FAILED
        assert state.error.reason is RejectReason.BLOCKED_STREAM_TIMEOUT

    def test_duplicate_delivery_does_not_diverge(self):
        a, b = make_pair()
        frame = a.encode_headers(1, [("x-dup", "1")])
        first = b.decode_headers(1, frame)
        again = b.decode_headers(1, frame)  # duplicate
        assert again == first
        # table applied exactly once
        assert b.decoder_table.insertion_count == 1
        assert len(b.decoder_table) == 1

    def test_interleaved_out_of_order_many_streams(self):
        a, b = make_pair()
        frames = {}
        for sid in range(1, 11):
            frames[sid] = a.encode_headers(sid, [("x-chain", str(sid))])
        # deliver in reverse order: every block depends on prior inserts
        for sid in range(10, 0, -1):
            result = b.decode_headers(sid, frames[sid])
            if sid == 1:
                assert result == [("x-chain", "1")]
            else:
                assert result is None
        # all resumed, in deterministic dependency order
        for sid in range(2, 11):
            state = b.stream_state(sid)
            assert state.phase is StreamPhase.DONE
            assert state.headers == [("x-chain", str(sid))]
        assert a.encoder_table.insertion_count == b.decoder_table.insertion_count


class TestLifecycleAndIsolation:
    def test_close_releases_state(self):
        mgr = ConnectionManager()
        ctx = mgr.open_connection("c1")
        ctx.encode_headers(1, [("x-a", "1")])
        mgr.close_connection("c1")
        assert mgr.get("c1") is None
        assert ctx.phase is ConnectionPhase.CLOSED
        with pytest.raises(CompressionError) as exc:
            ctx.encode_headers(1, [("x-b", "2")])
        assert exc.value.reason is RejectReason.CONNECTION_CLOSED

    def test_reset_recreates_fresh_state(self):
        a, b = make_pair()
        deliver(a, b, 1, [("x-a", "1")])
        a.reset()
        assert a.encoder_table.insertion_count == 0
        assert a.decoder_table.insertion_count == 0
        assert a.phase is ConnectionPhase.OPEN

    def test_connections_are_isolated(self):
        mgr = ConnectionManager()
        c1 = mgr.open_connection("c1")
        c2 = mgr.open_connection("c2")
        c1.encode_headers(1, [("x-secret", "1")])
        assert c2.encoder_table.insertion_count == 0
        assert c2.decoder_table.insertion_count == 0
        assert c1.encoder_table is not c2.encoder_table

    def test_decode_failure_poisons_only_that_connection(self):
        a, b = make_pair()
        c, d = make_pair()
        c.connection_id = "c"
        # corrupt frame on b: valid frame with truncated tail
        frame = a.encode_headers(1, [("x-long-name", "x-long-value")])
        with pytest.raises(DecodeError):
            b.decode_headers(1, frame[:-2])
        # b's decode side is poisoned until reset
        with pytest.raises(CompressionError) as exc:
            b.decode_headers(1, a.encode_headers(1, [(":method", "GET")]))
        assert "poisoned" in str(exc.value)
        # d (other connection) decodes c's traffic unaffected
        assert deliver(c, d, 1, [("x-ok", "1")]) == [("x-ok", "1")]
        # after a reset BOTH ends re-synchronize (connection re-established)
        a.reset()
        b.reset()
        assert deliver(a, b, 2, [("x-after", "reset")]) == [("x-after", "reset")]

    def test_manager_rejects_duplicate_ids(self):
        mgr = ConnectionManager()
        mgr.open_connection("c1")
        with pytest.raises(CompressionError):
            mgr.open_connection("c1")

    def test_manager_passes_options(self):
        mgr = ConnectionManager(max_capacity=128, max_blocked_wait=0.5)
        ctx = mgr.open_connection("c1")
        assert ctx.encoder_table.max_capacity == 128


class TestMemoryLimits:
    def test_default_policy_omits_indexing_for_oversized_entry(self):
        a, b = make_pair(memory_limit=64)
        # default policy: sent without indexing, still decodable
        headers = [("x-big", "v" * 100)]
        assert deliver(a, b, 1, headers) == headers
        assert len(a.encoder_table) == 0
        assert len(b.decoder_table) == 0

    def test_strict_policy_rejects_oversized_entry_on_encode(self):
        a = ConnectionContext("a", max_capacity=4096, memory_limit=64,
                              strict_limits=True)
        with pytest.raises(CompressionError) as exc:
            a.encode_headers(1, [("x-big", "v" * 100)])
        assert exc.value.reason is RejectReason.MEMORY_LIMIT_EXCEEDED

    def test_strict_rejection_does_not_silent_drop_state(self):
        # entries within the limit still encode fine after a rejection
        a, b = make_pair(memory_limit=64, strict_limits=True)
        with pytest.raises(CompressionError):
            a.encode_headers(1, [("x-big", "v" * 100)])
        assert deliver(a, b, 1, [("x-ok", "1")]) == [("x-ok", "1")]
        # and the previously inserted entry is still referenceable
        assert deliver(a, b, 2, [("x-ok", "1")]) == [("x-ok", "1")]
