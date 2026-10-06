"""Concurrency and adversarial-ordering tests for shared dynamic tables."""

import random
import threading

from hpack_conn.connection import ConnectionContext
from hpack_conn.stream import StreamPhase


def test_cascade_resume_in_dependency_order():
    """Out-of-order delivery of a long dependency chain resumes in order."""
    a, b = ConnectionContext("a"), ConnectionContext("b")
    frames = {}
    # block N references what block N-1 inserted -> strict chain
    for sid in range(1, 31):
        frames[sid] = a.encode_headers(sid, [("x-chain", str(sid))])
    for sid in range(30, 0, -1):
        result = b.decode_headers(sid, frames[sid])
        assert result == (None if sid != 1 else [("x-chain", "1")])
    for sid in range(2, 31):
        state = b.stream_state(sid)
        assert state.phase is StreamPhase.DONE
        assert state.headers == [("x-chain", str(sid))]
    ea, db = a.encoder_table, b.decoder_table
    assert ea.insertion_count == db.insertion_count
    for i in range(1, len(ea) + 1):
        assert ea.get(i) == db.get(i)


def test_shuffled_delivery_never_diverges():
    """Random arrival orders all converge to the same table state."""
    reference_final = None
    for seed in range(10):
        a, b = ConnectionContext("a"), ConnectionContext("b")
        frames = []
        for sid in range(1, 21):
            headers = [(f"x-h{sid % 5}", str(sid)), (":method", "GET")]
            frames.append((sid, a.encode_headers(sid, headers)))
        random.Random(seed).shuffle(frames)
        for sid, frame in frames:
            b.decode_headers(sid, frame)
        # drain any blocked streams by re-walking is unnecessary: every
        # dependency is satisfiable, so nothing may remain blocked
        assert b._registry.blocked_count() == 0
        ea, db = a.encoder_table, b.decoder_table
        state = (ea.insertion_count, db.insertion_count, ea.size, db.size,
                 [db.get(i) for i in range(1, len(db) + 1)])
        if reference_final is None:
            reference_final = state
        assert state == reference_final


def test_concurrent_encoders_shared_context():
    """Threads encode on one context; shuffled frames decode consistently."""
    a, b = ConnectionContext("a"), ConnectionContext("b")
    frames: list[tuple[int, bytes]] = []
    lock = threading.Lock()

    def produce(stream_base: int) -> None:
        for i in range(25):
            sid = stream_base + i
            frame = a.encode_headers(sid, [("x-t", str(sid)), (":path", "/")])
            with lock:
                frames.append((sid, frame))

    threads = [threading.Thread(target=produce, args=(n * 1000,))
               for n in range(4)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    random.Random(42).shuffle(frames)
    results = {}
    for sid, frame in frames:
        results[sid] = b.decode_headers(sid, frame)
    assert b._registry.blocked_count() == 0
    ea, db = a.encoder_table, b.decoder_table
    assert ea.insertion_count == db.insertion_count
    assert ea.size == db.size
    for i in range(1, len(ea) + 1):
        assert ea.get(i) == db.get(i)


def test_concurrent_connections_are_isolated():
    """Threads driving separate managers never share table state."""
    errors = []

    def run(cid: str) -> None:
        try:
            local = ConnectionContext(cid)
            peer = ConnectionContext(cid + "-peer")
            for i in range(50):
                headers = [(f"x-{cid}", str(i))]
                assert peer.decode_headers(i, local.encode_headers(i, headers)) == headers
            assert local.encoder_table.insertion_count == 50
        except Exception as exc:  # pragma: no cover - failure path
            errors.append(exc)

    threads = [threading.Thread(target=run, args=(f"c{n}",)) for n in range(8)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert errors == []
