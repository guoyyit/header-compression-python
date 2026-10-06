import pytest

from hpack_conn.errors import BlockedStreamError, RejectReason
from hpack_conn.stream import StreamPhase, StreamRegistry


class FakeClock:
    def __init__(self):
        self.now = 0.0

    def __call__(self):
        return self.now

    def advance(self, dt):
        self.now += dt


def make_registry(**kw):
    clock = FakeClock()
    return StreamRegistry(clock=clock, **kw), clock


class TestBasics:
    def test_open_stream(self):
        reg, _ = make_registry()
        state = reg.open_stream(1)
        assert state.phase is StreamPhase.OPEN
        assert reg.get(1) is state

    def test_duplicate_open_rejected(self):
        reg, _ = make_registry()
        reg.open_stream(1)
        with pytest.raises(BlockedStreamError):
            reg.open_stream(1)

    def test_block_implicitly_opens(self):
        reg, _ = make_registry()
        state = reg.block_stream(7, required_insertions=3)
        assert state.phase is StreamPhase.BLOCKED
        assert state.required_insertions == 3

    def test_close_stream(self):
        reg, _ = make_registry()
        reg.open_stream(1)
        reg.close_stream(1)
        assert reg.get(1) is None


class TestResume:
    def test_resume_when_dependency_satisfied(self):
        reg, _ = make_registry()
        reg.block_stream(1, required_insertions=3)
        assert reg.on_insertions(2) == []
        assert reg.get(1).phase is StreamPhase.BLOCKED
        assert reg.on_insertions(3) == [1]
        assert reg.get(1).phase is StreamPhase.OPEN

    def test_independent_streams_not_blocked(self):
        reg, _ = make_registry()
        reg.block_stream(1, required_insertions=10)
        reg.block_stream(2, required_insertions=2)
        # stream 2's dependency is satisfied first; stream 1 keeps waiting
        assert reg.on_insertions(2) == [2]
        assert reg.get(1).phase is StreamPhase.BLOCKED
        assert reg.get(2).phase is StreamPhase.OPEN

    def test_resume_order_is_deterministic(self):
        reg, _ = make_registry()
        reg.block_stream(9, required_insertions=5)
        reg.block_stream(3, required_insertions=5)
        reg.block_stream(5, required_insertions=2)
        assert reg.on_insertions(5) == [5, 3, 9]

    def test_no_deadlock_on_zero_progress(self):
        reg, _ = make_registry()
        reg.block_stream(1, required_insertions=100)
        # repeated notifications without growth never resume, never hang
        for _ in range(1000):
            assert reg.on_insertions(0) == []


class TestTimeouts:
    def test_wait_bound_is_configurable(self):
        reg, clock = make_registry(max_blocked_wait=2.0)
        reg.block_stream(1, required_insertions=5)
        clock.advance(1.0)
        assert reg.check_timeouts() == []
        clock.advance(1.5)
        assert reg.check_timeouts() == [1]
        state = reg.get(1)
        assert state.phase is StreamPhase.FAILED
        assert state.error.reason is RejectReason.BLOCKED_STREAM_TIMEOUT

    def test_invalid_wait_bound_rejected(self):
        with pytest.raises(ValueError):
            StreamRegistry(max_blocked_wait=0)
        with pytest.raises(ValueError):
            StreamRegistry(max_blocked_wait=float("inf"))

    def test_blocked_stream_count_bounded(self):
        reg, _ = make_registry(max_blocked_streams=2)
        reg.block_stream(1, 10)
        reg.block_stream(2, 10)
        with pytest.raises(BlockedStreamError) as exc:
            reg.block_stream(3, 10)
        assert exc.value.reason is RejectReason.MEMORY_LIMIT_EXCEEDED

    def test_failed_stream_cannot_reblock(self):
        reg, clock = make_registry(max_blocked_wait=1.0)
        reg.block_stream(1, 5)
        clock.advance(2.0)
        reg.check_timeouts()
        with pytest.raises(BlockedStreamError):
            reg.block_stream(1, 5)

    def test_reset_clears_everything(self):
        reg, _ = make_registry()
        reg.block_stream(1, 5)
        reg.reset()
        assert reg.get(1) is None
        assert reg.blocked_count() == 0
