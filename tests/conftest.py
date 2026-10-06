"""Shared fixtures/helpers: deterministic loopback between two contexts."""

from __future__ import annotations

import pytest

from header_compression import CompressionConfig, ConnectionCompressionContext


def pump(src: ConnectionCompressionContext, dst: ConnectionCompressionContext) -> list:
    """Deliver all pending encoder-stream batches src->dst and acks dst->src.

    Returns the list of ``(stream_id, headers)`` unblocked on ``dst``.
    """
    unblocked: list = []
    while (batch := src.next_encoder_batch()) is not None:
        seq, payload = batch
        unblocked.extend(dst.feed_encoder_stream(seq, payload))
    acks = dst.drain_decoder_stream()
    if acks:
        src.feed_decoder_stream(acks)
    return unblocked


@pytest.fixture()
def pair() -> tuple[ConnectionCompressionContext, ConnectionCompressionContext]:
    config = CompressionConfig()
    return (
        ConnectionCompressionContext("a", config),
        ConnectionCompressionContext("b", config),
    )
