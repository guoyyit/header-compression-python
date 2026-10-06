"""Error hierarchy for the header compression layer.

Every failure raised by this package derives from :class:`CompressionError`
so callers can distinguish compression failures from other errors, and each
concrete subclass is a distinguishable rejection/failure reason.
"""

from __future__ import annotations


class CompressionError(Exception):
    """Base class for all header compression errors."""


class WireFormatError(CompressionError):
    """Malformed or truncated wire data (safe to surface, reproducible)."""


class TruncatedDataError(WireFormatError):
    """Input ended before a complete item could be decoded."""


class IntegerDecodingError(WireFormatError):
    """A variable-length integer was malformed or exceeded the size limit."""


class StringDecodingError(WireFormatError):
    """A string literal was malformed (bad length, bad encoding)."""


class InvalidRepresentationError(WireFormatError):
    """A field line / instruction byte pattern is reserved or unknown."""


class UnknownIndexError(WireFormatError):
    """A reference points at an entry that was evicted or never existed."""


class CapacityExceededError(CompressionError):
    """Peer attempted to raise the dynamic table beyond our maximum."""


class EntryTooLargeError(CompressionError):
    """A single entry is larger than the whole dynamic table capacity."""


class CapacityReductionBlockedError(CompressionError):
    """Capacity reduction would evict entries still referenced by
    unacknowledged header blocks; retry after acknowledgements arrive."""


class BlockedStreamLimitError(CompressionError):
    """Too many streams are waiting on unsynchronised table entries."""


class DecoderStreamError(CompressionError):
    """Decoder-stream instructions (acknowledgements) were invalid."""
