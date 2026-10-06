"""End-to-end codec tests: roundtrip, determinism, table hits."""

from header_compression import CompressionConfig, ConnectionCompressionContext

from .conftest import pump


def make_pair(**overrides) -> tuple[ConnectionCompressionContext, ConnectionCompressionContext]:
    config = CompressionConfig(**overrides)
    return (
        ConnectionCompressionContext("a", config),
        ConnectionCompressionContext("b", config),
    )


def exchange(a, b, stream_id, headers):
    """Encode on a, ship encoder stream + block to b, decode, return headers."""
    block = a.encode_headers(stream_id, headers)
    while (batch := a.next_encoder_batch()) is not None:
        seq, payload = batch
        b.feed_encoder_stream(seq, payload)
    out = b.decode_headers(stream_id, block)
    assert out is not None
    b_acks = b.drain_decoder_stream()
    if b_acks:
        a.feed_decoder_stream(b_acks)
    return out


class TestRoundtrip:
    def test_static_exact_match(self, pair):
        a, b = pair
        assert exchange(a, b, 1, [(":method", "GET")]) == [(":method", "GET")]

    def test_literal_then_dynamic_hit(self, pair):
        a, b = pair
        headers = [("x-token", "abc123")]
        assert exchange(a, b, 1, headers) == headers
        # second exchange must hit the dynamic table
        assert exchange(a, b, 3, headers) == headers
        assert b.decoder.table.insert_count == 1

    def test_static_name_with_new_value_inserts(self, pair):
        a, b = pair
        headers = [(":path", "/items/42")]
        assert exchange(a, b, 1, headers) == headers
        assert exchange(a, b, 2, headers) == headers
        entry = b.decoder.table.get(0)
        assert (entry.name, entry.value) == (":path", "/items/42")

    def test_multiple_streams_share_table(self, pair):
        a, b = pair
        for stream_id in (1, 3, 5, 7):
            assert exchange(a, b, stream_id, [("x-shared", "v")]) == [("x-shared", "v")]
        assert b.decoder.table.insert_count == 1  # inserted once, reused

    def test_sensitive_headers_never_indexed(self, pair):
        a, b = pair
        headers = [("authorization", "Bearer xyz"), ("cookie", "session=1")]
        assert exchange(a, b, 1, headers) == headers
        assert exchange(a, b, 2, headers) == headers
        assert b.decoder.table.insert_count == 0

    def test_names_are_lowercased(self, pair):
        a, b = pair
        assert exchange(a, b, 1, [("X-Upper", "V")]) == [("x-upper", "V")]

    def test_empty_header_list(self, pair):
        a, b = pair
        assert exchange(a, b, 1, []) == []


class TestDeterminism:
    SCRIPT = [
        (1, [(":method", "GET"), (":path", "/"), ("x-a", "1")]),
        (3, [(":method", "POST"), (":path", "/submit"), ("x-a", "1")]),
        (5, [("x-b", "2"), ("x-a", "1"), ("authorization", "Bearer t")]),
        (7, [(":status", "200"), ("content-type", "text/html"), ("x-b", "2")]),
    ]

    def run_script(self):
        a, b = make_pair()
        blocks = []
        for stream_id, headers in self.SCRIPT:
            blocks.append(a.encode_headers(stream_id, headers))
            while (batch := a.next_encoder_batch()) is not None:
                b.feed_encoder_stream(*batch)
            acks = b.drain_decoder_stream()
            if acks:
                a.feed_decoder_stream(acks)
        return blocks

    def test_byte_identical_output_for_same_input(self):
        first = self.run_script()
        second = self.run_script()
        assert first == second
        assert all(isinstance(block, bytes) for block in first)

    def test_first_use_inserts_second_use_reuses(self):
        a, b = make_pair()
        headers = [("x-long-header-name", "x-long-header-value" * 4)]
        a.encode_headers(1, headers)
        first_batch = a.next_encoder_batch()
        assert first_batch is not None  # insert instruction emitted
        pump(a, b)
        a.encode_headers(3, headers)
        assert a.next_encoder_batch() is None  # dynamic hit: nothing new
