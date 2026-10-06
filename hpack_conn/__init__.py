"""Connection-layer stateful header compression with dynamic-table sync."""

from .connection import ConnectionContext, ConnectionManager, ConnectionPhase
from .decoder import Decoder
from .encoder import Encoder
from .errors import (
    BlockedStreamError,
    CapacityError,
    CompressionError,
    DecodeError,
    EncodeError,
    RejectReason,
)
from .stream import StreamPhase, StreamRegistry, StreamState
from .tables import DynamicTable, HeaderField, StaticTable

__all__ = [
    "BlockedStreamError",
    "CapacityError",
    "CompressionError",
    "ConnectionContext",
    "ConnectionManager",
    "ConnectionPhase",
    "DecodeError",
    "Decoder",
    "DynamicTable",
    "EncodeError",
    "Encoder",
    "HeaderField",
    "RejectReason",
    "StaticTable",
    "StreamPhase",
    "StreamRegistry",
    "StreamState",
]
