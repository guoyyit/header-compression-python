"""FastAPI integration for the connection-layer header compression.

A :class:`ConnectionRegistry` owns one compression context per connection
id. Clients pick their connection id via the ``X-Connection-ID`` request
header (a fresh one is minted and returned in the response when absent).
Compression errors are recorded on the connection context and surfaced as
structured 4xx responses with a distinguishable ``reason``.
"""

from __future__ import annotations

import os
import re
import uuid

from fastapi import FastAPI, Request, Response
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field

from header_compression import (
    CompressionConfig,
    CompressionError,
    ConnectionRegistry,
)

CONNECTION_ID_HEADER = "x-connection-id"

#: Routes that address a connection explicitly in the path.
_CONNECTION_PATH = re.compile(r"^/connections/([^/]+)")


def _resolve_connection_id(request: Request) -> str:
    """Connection id comes from the path, then the header, else is minted."""
    match = _CONNECTION_PATH.match(request.url.path)
    if match:
        return match.group(1)
    return request.headers.get(CONNECTION_ID_HEADER) or uuid.uuid4().hex


def _config_from_env() -> CompressionConfig:
    return CompressionConfig(
        max_table_capacity=int(os.environ.get("HC_MAX_TABLE_CAPACITY", "4096")),
        table_capacity=int(os.environ.get("HC_TABLE_CAPACITY", "4096")),
        max_blocked_streams=int(os.environ.get("HC_MAX_BLOCKED_STREAMS", "64")),
    )


app = FastAPI(title="header-compression-python")
registry = ConnectionRegistry(_config_from_env())


@app.middleware("http")
async def compression_context_middleware(request: Request, call_next):
    connection_id = _resolve_connection_id(request)
    request.state.connection_id = connection_id
    # attach only if it already exists; endpoints create lazily on first use
    request.state.compression = registry.get(connection_id)
    response: Response = await call_next(request)
    response.headers[CONNECTION_ID_HEADER] = connection_id
    return response


@app.exception_handler(CompressionError)
async def compression_error_handler(request: Request, exc: CompressionError):
    context = registry.get(getattr(request.state, "connection_id", ""))
    if context is not None:
        context.record_error(exc)
    return JSONResponse(
        status_code=422,
        content={
            "error": type(exc).__name__,
            "reason": str(exc),
            "connection_id": getattr(request.state, "connection_id", None),
        },
    )


@app.get("/")
def read_root():
    return {"Hello": "World"}


@app.get("/connections")
def list_connections():
    return {"connections": registry.active_ids()}


@app.get("/connections/{connection_id}/state")
def connection_state(connection_id: str):
    return registry.get_or_create(connection_id).state_snapshot()


class EncoderStreamBatch(BaseModel):
    seq: int = Field(ge=0)
    data: str  # hex-encoded encoder-stream payload


@app.post("/connections/{connection_id}/encoder-stream")
def feed_encoder_stream(connection_id: str, batch: EncoderStreamBatch):
    context = registry.get_or_create(connection_id)
    unblocked = context.feed_encoder_stream(batch.seq, bytes.fromhex(batch.data))
    return {
        "unblocked": [
            {"stream_id": stream_id, "headers": headers}
            for stream_id, headers in unblocked
        ],
        "decoder_stream": context.drain_decoder_stream().hex(),
    }


class HeaderBlockSubmission(BaseModel):
    block: str  # hex-encoded header block


@app.post("/connections/{connection_id}/streams/{stream_id}/headers")
def decode_header_block(
    connection_id: str, stream_id: int, submission: HeaderBlockSubmission
):
    context = registry.get_or_create(connection_id)
    headers = context.decode_headers(stream_id, bytes.fromhex(submission.block))
    if headers is None:
        return {"status": "blocked", "stream_id": stream_id}
    return {
        "status": "decoded",
        "stream_id": stream_id,
        "headers": headers,
        "decoder_stream": context.drain_decoder_stream().hex(),
    }


class EncodeRequest(BaseModel):
    stream_id: int = Field(ge=0)
    headers: list[tuple[str, str]]


@app.post("/connections/{connection_id}/encode")
def encode_headers(connection_id: str, request: EncodeRequest):
    context = registry.get_or_create(connection_id)
    block = context.encode_headers(request.stream_id, request.headers)
    batches = []
    while (batch := context.next_encoder_batch()) is not None:
        seq, payload = batch
        batches.append({"seq": seq, "data": payload.hex()})
    return {"block": block.hex(), "encoder_stream": batches}


@app.post("/connections/{connection_id}/reset")
def reset_connection(connection_id: str):
    registry.get_or_create(connection_id).reset()
    return {"status": "reset", "connection_id": connection_id}


@app.delete("/connections/{connection_id}")
def close_connection(connection_id: str):
    if not registry.close(connection_id):
        return JSONResponse(
            status_code=404, content={"error": "unknown connection"}
        )
    return {"status": "closed", "connection_id": connection_id}
