import pytest

from hpack_conn.errors import DecodeError, RejectReason
from hpack_conn.strings import decode_string, encode_string


class TestEncodeString:
    def test_ascii(self):
        assert encode_string("abc") == b"\x03abc"

    def test_empty(self):
        assert encode_string("") == b"\x00"

    def test_utf8_multibyte(self):
        encoded = encode_string("中文")
        assert encoded == "中文".encode("utf-8").__len__().to_bytes(1, "big") + "中文".encode("utf-8")

    def test_long_string_uses_continuation_length(self):
        value = "x" * 200
        encoded = encode_string(value)
        assert encoded[0] == 127  # 7-bit prefix saturated
        assert encoded[1] == 200 - 127
        assert encoded[2:] == b"x" * 200

    def test_prefix_flags(self):
        assert encode_string("a", prefix_flags=0x00)[0] == 1

    def test_deterministic(self):
        assert encode_string("hello") == encode_string("hello")


class TestDecodeString:
    def test_roundtrip(self):
        for value in ("", "a", "hello world", "中文测试", "x" * 1000):
            decoded, offset = decode_string(encode_string(value))
            assert decoded == value
            assert offset == len(encode_string(value))

    def test_offset(self):
        data = b"\xff" + encode_string("hi")
        value, offset = decode_string(data, offset=1)
        assert (value, offset) == ("hi", 4)

    def test_truncated_length(self):
        with pytest.raises(DecodeError) as exc:
            decode_string(b"")
        assert exc.value.reason is RejectReason.TRUNCATED_INPUT

    def test_truncated_body(self):
        with pytest.raises(DecodeError) as exc:
            decode_string(b"\x05ab")
        assert exc.value.reason is RejectReason.TRUNCATED_INPUT

    def test_huffman_flag_rejected(self):
        with pytest.raises(DecodeError) as exc:
            decode_string(b"\x83abc")
        assert exc.value.reason is RejectReason.MALFORMED_INPUT

    def test_invalid_utf8_rejected(self):
        with pytest.raises(DecodeError) as exc:
            decode_string(b"\x01\xff")
        assert exc.value.reason is RejectReason.MALFORMED_INPUT
