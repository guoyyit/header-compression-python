import pytest

from hpack_conn.errors import DecodeError, RejectReason
from hpack_conn.integers import decode_integer, encode_integer


class TestEncodeInteger:
    # Vectors from RFC 7541 appendix C.1
    def test_rfc_example_10_with_5bit_prefix(self):
        assert encode_integer(10, 5) == bytes([0b01010])

    def test_rfc_example_1337_with_5bit_prefix(self):
        assert encode_integer(1337, 5) == bytes([0b11111, 0b10011010, 0b00001010])

    def test_rfc_example_42_with_8bit_prefix(self):
        assert encode_integer(42, 8) == bytes([42])

    def test_prefix_flags_are_ored_in(self):
        assert encode_integer(10, 5, prefix_flags=0b10100000) == bytes([0b10101010])

    def test_zero(self):
        assert encode_integer(0, 5) == bytes([0])

    def test_boundary_value_uses_continuation(self):
        # 31 == max 5-bit prefix -> needs continuation octet 0
        assert encode_integer(31, 5) == bytes([31, 0])

    def test_deterministic(self):
        assert encode_integer(1337, 5) == encode_integer(1337, 5)

    def test_negative_rejected(self):
        with pytest.raises(ValueError):
            encode_integer(-1, 5)


class TestDecodeInteger:
    def test_roundtrip_small(self):
        for value in range(0, 300):
            encoded = encode_integer(value, 5)
            decoded, next_offset = decode_integer(encoded, 5)
            assert decoded == value
            assert next_offset == len(encoded)

    def test_roundtrip_large(self):
        for value in (2**16, 2**32, 2**63):
            encoded = encode_integer(value, 8)
            decoded, _ = decode_integer(encoded, 8)
            assert decoded == value

    def test_rfc_vector(self):
        data = bytes([0b11111, 0b10011010, 0b00001010])
        value, offset = decode_integer(data, 5)
        assert (value, offset) == (1337, 3)

    def test_respects_prefix_flags(self):
        value, _ = decode_integer(bytes([0b10101010]), 5)
        assert value == 10

    def test_offset(self):
        data = b"\x00" + encode_integer(42, 8)
        value, offset = decode_integer(data, 8, offset=1)
        assert (value, offset) == (42, 2)

    def test_truncated_prefix(self):
        with pytest.raises(DecodeError) as exc:
            decode_integer(b"", 5)
        assert exc.value.reason is RejectReason.TRUNCATED_INPUT

    def test_truncated_continuation(self):
        with pytest.raises(DecodeError) as exc:
            decode_integer(bytes([31, 0x80]), 5)
        assert exc.value.reason is RejectReason.TRUNCATED_INPUT

    def test_unbounded_continuation_rejected(self):
        with pytest.raises(DecodeError) as exc:
            decode_integer(bytes([31] + [0xFF] * 11), 5)
        assert exc.value.reason is RejectReason.MALFORMED_INPUT
