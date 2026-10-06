"""Primitive codec tests: prefix integers and string literals."""

import pytest

from header_compression.errors import IntegerDecodingError, TruncatedDataError
from header_compression.integers import MAX_INTEGER, decode_integer, encode_integer
from header_compression.strings import decode_string, decode_text, encode_string, encode_text


class TestIntegers:
    def test_rfc7541_examples(self):
        # RFC 7541 Appendix C.1: 10 with 5-bit prefix
        assert encode_integer(10, 5) == bytes([0x0A])
        assert decode_integer(bytes([0x0A]), 0, 5) == (10, 1)
        # RFC 7541 Appendix C.2: 1337 with 5-bit prefix
        assert encode_integer(1337, 5) == bytes([0x1F, 0x9A, 0x0A])
        assert decode_integer(bytes([0x1F, 0x9A, 0x0A]), 0, 5) == (1337, 3)

    @pytest.mark.parametrize("value", [0, 1, 30, 31, 127, 128, 255, 300, 2**20, MAX_INTEGER])
    @pytest.mark.parametrize("prefix_bits", [1, 4, 5, 6, 7, 8])
    def test_roundtrip(self, value, prefix_bits):
        encoded = encode_integer(value, prefix_bits, 0x00)
        assert decode_integer(encoded, 0, prefix_bits) == (value, len(encoded))

    def test_flags_preserved_in_first_byte(self):
        encoded = encode_integer(5, 4, 0xA0)
        assert encoded == bytes([0xA5])

    def test_truncated_prefix(self):
        with pytest.raises(TruncatedDataError):
            decode_integer(b"", 0, 5)

    def test_truncated_continuation(self):
        with pytest.raises(TruncatedDataError):
            decode_integer(bytes([0x1F, 0x9A]), 0, 5)

    def test_overflow_rejected(self):
        with pytest.raises(IntegerDecodingError):
            decode_integer(bytes([0x1F]) + b"\xff" * 10 + b"\x7f", 0, 5)

    def test_out_of_range_value(self):
        with pytest.raises(ValueError):
            encode_integer(MAX_INTEGER + 1, 5)
        with pytest.raises(ValueError):
            encode_integer(-1, 5)


class TestStrings:
    def test_roundtrip(self):
        encoded = encode_text("hello wörld")
        assert decode_text(encoded, 0) == ("hello wörld", len(encoded))

    def test_raw_bytes_roundtrip(self):
        encoded = encode_string(b"\x00\xff\x10")
        assert decode_string(encoded, 0) == (b"\x00\xff\x10", len(encoded))

    def test_empty_string(self):
        assert encode_text("") == b"\x00"
        assert decode_text(b"\x00", 0) == ("", 1)

    def test_truncated_payload(self):
        with pytest.raises(TruncatedDataError):
            decode_text(b"\x05ab", 0)

    def test_deterministic(self):
        assert encode_text("same input") == encode_text("same input")
