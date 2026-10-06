"""FastAPI integration: connection lifecycle + header compression API.

The app owns a :class:`ConnectionManager`. Each connection id maps to an
isolated :class:`ConnectionContext`; request handlers below are the
request-processing integration points, and the create/reset/close routes
are the connection-lifecycle integration points.
"""

from __future__ import annotations

from typing import Annotated

from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field

from .connection import ConnectionContext, ConnectionManager
from .errors import CompressionError, RejectReason
from .stream import StreamPhase


class EncodeRequest(BaseModel):
    stream_id: int = Field(ge=0)
    headers: list[tuple[str, str]]


class DecodeRequest(BaseModel):
    stream_id: int = Field(ge=0)
    frame: str  # hex-encoded framed header block


class CapacityRequest(BaseModel):
    capacity: int = Field(ge=0)


_REASON_STATUS = {
    RejectReason.MALFORMED_INPUT: 422,
    RejectReason.TRUNCATED_INPUT: 422,
    RejectReason.INVALID_INDEX: 422,
    RejectReason.CAPACITY_SYNC_VIOLATION: 409,
    RejectReason.MEMORY_LIMIT_EXCEEDED: 413,
    RejectReason.ENTRY_TOO_LARGE: 413,
    RejectReason.BLOCKED_STREAM_TIMEOUT: 408,
    RejectReason.CONNECTION_CLOSED: 410,
}


def _error_response(exc: CompressionError) -> JSONResponse:
    return JSONResponse(
        status_code=_REASON_STATUS.get(exc.reason, 400),
        content={"error": exc.reason.value, "detail": str(exc)},
    )


def _snapshot(ctx: ConnectionContext) -> dict:
    return {
        "connection_id": ctx.connection_id,
        "phase": ctx.phase.value,
        "encoder_table": {
            "size": ctx.encoder_table.size,
            "capacity": ctx.encoder_table.capacity,
            "entries": len(ctx.encoder_table),
            "insertion_count": ctx.encoder_table.insertion_count,
        },
        "decoder_table": {
            "size": ctx.decoder_table.size,
            "capacity": ctx.decoder_table.capacity,
            "entries": len(ctx.decoder_table),
            "insertion_count": ctx.decoder_table.insertion_count,
        },
    }


def create_app(manager: ConnectionManager | None = None) -> FastAPI:
    app = FastAPI(title="header-compression")
    app.state.manager = manager or ConnectionManager()

    @app.exception_handler(CompressionError)
    async def compression_errors(_: Request, exc: CompressionError):
        return _error_response(exc)

    def require_connection(cid: str) -> ConnectionContext:
        ctx = app.state.manager.get(cid)
        if ctx is None:
            raise HTTPException(status_code=404, detail="unknown connection")
        return ctx

    @app.get("/")
    def read_root():
        return {"Hello": "World"}

    @app.post("/connections/{cid}", status_code=201)
    def open_connection(cid: str):
        try:
            ctx = app.state.manager.open_connection(cid)
        except CompressionError as exc:
            raise HTTPException(status_code=409, detail=str(exc)) from exc
        return _snapshot(ctx)

    @app.get("/connections/{cid}")
    def connection_state(cid: str):
        return _snapshot(require_connection(cid))

    @app.post("/connections/{cid}/reset")
    def reset_connection(cid: str):
        try:
            ctx = app.state.manager.reset_connection(cid)
        except CompressionError as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc
        return _snapshot(ctx)

    @app.delete("/connections/{cid}")
    def close_connection(cid: str):
        if app.state.manager.get(cid) is None:
            raise HTTPException(status_code=404, detail="unknown connection")
        app.state.manager.close_connection(cid)
        return {"connection_id": cid, "phase": "closed"}

    @app.post("/connections/{cid}/capacity")
    def update_capacity(cid: str, body: CapacityRequest):
        ctx = require_connection(cid)
        update = ctx.update_capacity(body.capacity)
        return {"update_hex": update.hex(), "capacity": body.capacity}

    @app.post("/connections/{cid}/encode")
    def encode(cid: str, body: EncodeRequest):
        ctx = require_connection(cid)
        frame = ctx.encode_headers(body.stream_id, [tuple(h) for h in body.headers])
        return {"frame": frame.hex()}

    @app.post("/connections/{cid}/decode")
    def decode(cid: str, body: DecodeRequest):
        ctx = require_connection(cid)
        try:
            frame = bytes.fromhex(body.frame)
        except ValueError as exc:
            raise HTTPException(status_code=422, detail="bad hex frame") from exc
        headers = ctx.decode_headers(body.stream_id, frame)
        if headers is None:
            state = ctx.stream_state(body.stream_id)
            return {
                "status": "blocked",
                "required_checkpoint": state.required_insertions if state else None,
            }
        return {"status": "decoded", "headers": headers}

    @app.get("/connections/{cid}/streams/{stream_id}")
    def stream_state(cid: str, stream_id: int):
        ctx = require_connection(cid)
        state = ctx.stream_state(stream_id)
        if state is None:
            raise HTTPException(status_code=404, detail="unknown stream")
        return {
            "stream_id": state.stream_id,
            "phase": state.phase.value,
            "headers": state.headers,
            "error": str(state.error) if state.error else None,
        }

    @app.post("/connections/{cid}/timeouts")
    def check_timeouts(cid: str):
        ctx = require_connection(cid)
        return {"failed": ctx.check_timeouts()}

    return app
