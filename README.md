# header-compression-python

Connection-layer stateful header compression (HPACK-style profile) with
dynamic-table synchronization across concurrent streams, plus a FastAPI
integration for connection lifecycle management.

## Quick start

```bash
uv sync
uv run pytest            # run the test suite
uv run fastapi dev       # serve the HTTP API (see docs)
```

## Library usage

```python
from hpack_conn import ConnectionContext

alice = ConnectionContext("alice")
bob = ConnectionContext("bob")

frame = alice.encode_headers(stream_id=1, headers=[(":method", "GET"), ("x-a", "1")])
headers = bob.decode_headers(stream_id=1, data=frame)  # None => blocked
```

See [docs/header-compression.md](docs/header-compression.md) for the
wire format, synchronization semantics, configuration options, error
reasons, and the HTTP API.
