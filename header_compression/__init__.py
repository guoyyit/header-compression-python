"""Stateful connection-layer header compression with dynamic-table sync."""

from .connection import CompressionConfig, ConnectionCompressionContext
from .decoder import DecoderEndpoint
from .encoder import EncoderEndpoint
from .errors import (
    BlockedStreamLimitError,
    CapacityExceededError,
    CapacityReductionBlockedError,
    CompressionError,
    DecoderStreamError,
    EntryTooLargeError,
    IntegerDecodingError,
    InvalidRepresentationError,
    StringDecodingError,
    TruncatedDataError,
    UnknownIndexError,
    WireFormatError,
)
from .registry import ConnectionRegistry

__all__ = [
    "BlockedStreamLimitError",
    "CapacityExceededError",
    "CapacityReductionBlockedError",
    "CompressionConfig",
    "CompressionError",
    "ConnectionCompressionContext",
    "ConnectionRegistry",
    "DecoderEndpoint",
    "DecoderStreamError",
    "EncoderEndpoint",
    "EntryTooLargeError",
    "IntegerDecodingError",
    "InvalidRepresentationError",
    "StringDecodingError",
    "TruncatedDataError",
    "UnknownIndexError",
    "WireFormatError",
]
