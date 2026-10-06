# Connection-Layer Stateful Header Compression

`hpack_conn` provides HPACK-style (RFC 7541 profile) stateful header
compression with connection-scoped dynamic tables, deterministic output,
bounded blocked-stream waiting, and a FastAPI integration for connection
lifecycle management.

## Architecture

```
ConnectionManager                 one per application
└── ConnectionContext             one per connection (strictly isolated)
    ├── Encoder  + DynamicTable   encode direction
    ├── Decoder  + DynamicTable   decode direction
    └── StreamRegistry            blocked-stream bookkeeping
```

The only object shared between connections is the immutable static
table. All mutable state (tables, sequence numbers, blocked streams,
poison flags) lives inside the context and is created/released
deterministically by `open` / `reset` / `close`.

## Wire format

Header block representations (RFC 7541 section 6 profile, no Huffman):

| Pattern   | Meaning                                   |
|-----------|-------------------------------------------|
| `1xxxxxxx`| indexed field (7-bit prefix index)        |
| `01xxxxxx`| literal, incremental indexing (inserts)   |
| `001xxxxx`| dynamic table size update (block-leading) |
| `0000xxxx`| literal, never indexed / without indexing |

Integers use RFC 7541 section 5.1 variable-length coding; strings are
length-prefixed UTF-8. Static table indices are 1..61, dynamic entries
follow at 62+ (62 = newest). Identical input and configuration always
produce byte-for-byte identical output.

Connection framing (what `ConnectionContext` emits per block):

```
[gseq][prev_dep][flags][header block ...]
```

* `gseq` — connection-global monotone frame sequence (per direction);
  also used to detect duplicate deliveries.
* `prev_dep` — `gseq` of the previous frame whose block **touches** the
  dynamic table (0 if none); meaningful only when TOUCHES is set.
* `flags` — bit 0 = TOUCHES (the block inserts into, references, or
  resizes the dynamic table).

## Synchronization semantics

**Checkpoint ordering.** Wire indices inside a block are relative to the
table state the encoder saw, so blocks that touch the dynamic table must
be applied in encode order. A touching frame is applied only when its
`prev_dep` checkpoint has been applied; otherwise its stream becomes
BLOCKED and resumes automatically once the checkpoint advances. Blocks
that do not touch the table (static-only / non-indexed literals) are
decoded immediately in arrival order and never wait.

Consequences:

* At any moment the decoder table equals a deterministic prefix of the
  encoder's insertion order — out-of-order and duplicate deliveries
  cannot make the two ends diverge.
* Duplicate frames (same `gseq`) return the cached decode result and
  never mutate the table twice.
* Independent streams are never blocked by unrelated traffic.

**Bounded waiting.** A blocked stream fails with
`blocked_stream_timeout` after `max_blocked_wait` seconds (checked via
`ConnectionContext.check_timeouts()`), and at most
`max_blocked_streams` streams may wait at once (further blocks are
rejected with `memory_limit_exceeded`). Both bounds are configurable and
validated to be finite.

**Capacity negotiation.** `update_capacity(c)` queues a table-size
update that leads the next emitted block, so the peer applies it before
decoding any reference in that block — the effective point is identical
on both ends and no undecodable reference can occur during a resize.
Shrinks evict oldest entries on both sides identically. A capacity above
`max_capacity` is rejected with `capacity_sync_violation`.

**Memory limits.** `memory_limit` is a hard ceiling: the effective
capacity never exceeds it, and an entry larger than the ceiling is
rejected with `memory_limit_exceeded`. An entry larger than the current
capacity (but under the ceiling) is rejected with `entry_too_large` at
the table/decoder layer; at the encoder the policy is configurable:
`strict_limits=False` (default) emits such fields without indexing
(RFC-style degradation), `strict_limits=True` rejects the encode with
the distinguishable reason. Failed insertions never mutate table state,
so nothing needed by later decodes is silently dropped.

**Failure model.** Malformed or truncated input raises `DecodeError`
with a machine-readable reason and poisons the connection's decode side
(subsequent decodes raise until `reset()`), because compression state is
no longer verifiably in sync. The encode side and all other connections
are unaffected. `reset()` deterministically re-creates fresh state; both
peers must reset together (connection re-establishment).

## Error reasons

| Reason                     | Meaning                                        |
|----------------------------|------------------------------------------------|
| `malformed_input`          | invalid byte sequence / framing                |
| `truncated_input`          | input ended mid-representation                 |
| `invalid_index`            | reference to a non-existent table entry        |
| `capacity_sync_violation`  | capacity outside negotiated bounds             |
| `memory_limit_exceeded`    | hard memory ceiling would be exceeded          |
| `entry_too_large`          | single entry exceeds current table capacity    |
| `blocked_stream_timeout`   | bounded wait for table sync expired            |
| `connection_closed`        | operation on a closed/reset context            |

## Configuration

`ConnectionContext` / `ConnectionManager(**options)`:

| Option                | Default | Meaning                              |
|-----------------------|---------|--------------------------------------|
| `max_capacity`        | 4096    | protocol maximum table capacity      |
| `memory_limit`        | None    | hard ceiling on capacity/entries     |
| `max_blocked_wait`    | 5.0     | seconds a stream may stay blocked    |
| `max_blocked_streams` | 1024    | max simultaneously blocked streams   |
| `strict_limits`       | False   | reject (vs. not index) oversize fields |

## HTTP API

`hpack_conn.service.create_app()` mounts the integration used by
`main.py`:

| Route                              | Purpose                        |
|------------------------------------|--------------------------------|
| `POST /connections/{cid}`          | open (201; 409 if duplicate)   |
| `GET /connections/{cid}`           | state snapshot                 |
| `POST /connections/{cid}/reset`    | deterministic state reset      |
| `DELETE /connections/{cid}`        | close and release              |
| `POST /connections/{cid}/capacity` | negotiate new capacity         |
| `POST /connections/{cid}/encode`   | encode headers → framed bytes  |
| `POST /connections/{cid}/decode`   | decode frame → headers/blocked |
| `GET /connections/{cid}/streams/{sid}` | stream state/result        |
| `POST /connections/{cid}/timeouts` | fail overdue blocked streams   |

Errors are returned as `{"error": <reason>, "detail": ...}` with a
status code derived from the reason (422 malformed/truncated,
409 capacity conflict, 413 memory, 408 timeout, 410 closed).

## Testing

`uv run pytest` covers: integer/string coding vectors, static/dynamic
table semantics, capacity eviction and negotiation, encoder/decoder
round-trips and byte determinism, blocked-stream resume and timeout,
out-of-order/duplicate delivery, multi-threaded shared-table use,
connection isolation, poisoned-decoder recovery, and the HTTP API.
