# conduit-connector-sdk (Python)

Python SDK for building [Conduit](https://github.com/ConduitIO/conduit) source and
destination connectors. Connectors built with this SDK run as standalone gRPC
subprocess plugins — no changes to Conduit itself are required to run one.

> **Status: pre-alpha, under active development.** No release has shipped yet.
> The public API (`Source`, `Destination`, `Config`, `Record`) is not stable
> until the v0.19 Phase-1 acceptance criteria in
> [`docs/design/20260707-python-connector-sdk.md`](docs/design/20260707-python-connector-sdk.md)
> are met. Treat everything here as subject to change without a deprecation
> notice until then.
>
> **What "GA" means for this repo's first tagged release (v0.20 plan, WS2),
> stated precisely so it isn't over-read:** GA here means exactly two
> things -- the acceptance suite (`conduit.testing.acceptance`) is green in
> CI, and a first tag is published via PyPI Trusted Publishing. It does
> **not** mean production hardening, edge-case parity with the Go SDK, or a
> 1.0. The value of that tag is narrow and specific: it unblocks the Rust
> SDK build, which waits on a *tagged release* existing, not on a merge to
> `main`. Read every claim in this README with that scope in mind.

## What this is

- A gRPC client/server implementation of `conduit-connector-protocol` v2
  (`SourcePlugin` / `DestinationPlugin` / `SpecifierPlugin`), wrapped in an
  idiomatic Python API: `async`/`await` connector methods (with sync-method
  auto-dispatch to a thread pool), `pydantic`-based config with automatic
  parameter introspection, and a `bytes | dict` OpenCDC record model instead of
  Go's `Data` interface.
- The Python analog of [`conduit-connector-sdk`](https://github.com/ConduitIO/conduit-connector-sdk)
  (Go). Behavioral parity is a goal; API-shape parity is not — see the design
  doc's "Alternatives considered" section for why.

## Batching (`sdk.batch.size` / `sdk.batch.delay`)

Every connector gets two SDK-injected config parameters for free -- authors
never declare them on their own `BaseConfig` -- matching the Go SDK's
`DestinationWithBatch`/`SourceWithBatch` middleware exactly:

- `sdk.batch.size` (int, default `0`): maximum records per batch before an
  early flush.
- `sdk.batch.delay` (Go-duration string, default `0s`): maximum time an
  incomplete batch waits before flushing.

**Activation threshold, matched exactly from the Go SDK:** batching is only
active when `size > 1` or `delay > 0`. A `size` of `0` *or* `1` with no
delay is passthrough -- no accumulation, no background task, one wire
message in for one wire message/record out (Go's `destination.go:182`/
`source_middleware.go:709` condition, reproduced verbatim, not rounded to a
more "intuitive" `size >= 1`).

**On the source side**, this SDK's `Source` ABC has no `read_batch`/`ReadN`
override point yet (deferred past this phase), so batching buffers
individual `read()` calls -- Go's own *fallback* behavior when a connector
doesn't implement `ReadN`, not its optimized path. This is a real, current
capability gap versus Go, documented (not hidden) in
`conduit/_batch.py`'s module docstring.

**On the destination side**, incoming `Run` batches are flattened and
re-grouped by size/delay before `write()` is called -- so `write()` may now
receive a batch spanning several incoming wire messages. A buffered-but-
below-threshold remainder is always flushed (written and acked) when the
stream ends or `Stop()` drains the connector -- batching never drops
records, per invariant 3 (at-least-once is the floor).

## Avro schema support

`conduit.schema.AvroSchema` (optional -- install with
`conduit-connector-sdk[avro]`) encodes/decodes plain, headerless Avro
binary -- the exact wire format `conduit-commons`' `schema/avro` package
produces via `github.com/iskorotkov/avro/v2`, the maintained fork of the
archived `hamba/avro/v2` it adopted in #279 (**not** the Confluent Schema
Registry wire format; there's no magic-byte/schema-ID header on either
side). This is a cross-language compatibility claim, not a Python-only
round-trip: the golden fixture records bytes from *both* encoders
(`tests/testdata/avro_golden.json`), and a committed Go verifier
(`tools/avro_fixture_gen`) re-derives the Go bytes live and confirms Go
decodes this SDK's bytes -- see `tests/test_schema_avro.py`.

`encode()` validates every value against the schema *before* writing
bytes, with the same strictness the Go codec applies at marshal time:
`int`/`long` fields require Python `int` (`bool` is rejected),
`float`/`double` fields require `float`, and a key the schema has no
field for is rejected -- never silent coercion, never a different value on
the wire (invariant 6). fastavro by itself silently truncates some of
these (`42.9` into a `long` writes `42`); this SDK matches the Go codec
instead, which errors on all of them. One documented divergence: Go
*drops* unknown record keys, this SDK rejects them with `TypeError`.

This SDK does **not** ship a schema registry client (no `SchemaService`
gRPC stubs are generated) -- an author supplies schema text themselves and
is responsible for keeping it consistent with the
`opencdc.{key,payload}.schema.{subject,version}` record metadata (see
`conduit.record.Metadata.set_payload_schema`/`get_payload_schema`).

## What this is not (yet)

See the design doc's Phase 2/3 breakdown for what's still deliberately
deferred: a `read_batch`/`ReadN` override point for source connectors (see
"Batching" above for the current fallback-only behavior), a schema registry
client, the acceptance-test harness's full test corpus beyond the current
categories, and any performance claim versus the Go SDK (none ships without
a committed `benchi` result, per the org's CLAUDE.md).

## Delivery semantics

What this SDK guarantees, and — just as important — what it does not:

**Guaranteed:**

- **At-least-once.** A source record is never acknowledged to the
  connector's own `ack()` hook until Conduit's `Run` stream sends its
  position back via `ack_positions` (never speculatively when the record is
  merely produced) -- see `conduit.source._SourceServicer._consume_acks`.
- **No silent partial-batch acking.** A destination write failure never
  assumes an unmentioned record succeeded -- `write()` either returns
  cleanly (full-batch success), raises `BatchWriteError` with an exhaustive,
  construction-time-validated per-index accounting, or (any other
  exception) nacks the *entire* batch. There is no code path that infers
  "everything not explicitly marked as failed" succeeded — see
  `conduit.errors.BatchWriteError` and `conduit.destination._DestinationServicer._write_batch`.
- **Batching never drops records.** A buffered-but-below-threshold
  remainder is always flushed (and, on the destination side, acked) before
  a controlled shutdown (stream end, `Stop()`, or a SIGTERM-triggered
  drain) completes.
- **Graceful shutdown by default.** SIGTERM drains an in-flight read/write
  loop (including any buffered batch) before `teardown()` runs, bounded by
  a watchdog so a genuinely wedged connector still exits.

**Not guaranteed / explicit limits:**

- **Positions/state are the connector author's responsibility.** This SDK
  round-trips whatever `bytes` a `Source.read()`/`open()` implementation
  returns; it does not itself provide crash-safe position storage.
- **A hard-cancelled (not drained) `Run` can lose a buffered-but-unflushed
  batching remainder.** The controlled shutdown paths above (stream end,
  `Stop()`, SIGTERM-triggered drain) all flush correctly; an externally
  torn-down RPC (e.g. the framework cancelling the whole call, not a normal
  drain) is a documented, narrow-window exception -- see
  `conduit._batch.collect_batches`'s docstring for exactly why and how this
  mirrors an already-accepted tradeoff elsewhere in this SDK (an in-flight,
  never-acked write on a hard stop).
- **No exactly-once.** Like the Go SDK, redelivery on restart is possible;
  connectors must tolerate at-least-once delivery.
- **Structured (`dict`) payloads lose precision on integers beyond `2**53`**
  (via `google.protobuf.Struct`'s double-precision representation) --
  silently, by design of the wire format, not a bug in this SDK. Pinned by
  `tests/test_record_codec.py`.
- **No schema registry integration, no schema evolution/compatibility
  checking** -- see "Avro schema support" above.

## Requirements

- Python 3.11+
- [`uv`](https://docs.astral.sh/uv/) for dependency management (recommended;
  `pip install -e .[dev]` also works)

## Repo layout

```text
src/conduit/
  __init__.py       # public API surface
  config.py         # BaseConfig, Field, to_parameters()
  record.py          # Record / Change / Operation / Metadata
  schema.py           # AvroSchema -- optional, `conduit-connector-sdk[avro]`
  source.py             # Source ABC
  destination.py         # Destination ABC
  _batch.py                # sdk.batch.size/delay middleware (source + destination)
  serve.py                   # handshake + gRPC server bootstrap
  _handshake.py                # magic cookie, protocol negotiation, stdout line
  _build.py                      # `conduit-connector-sdk build` implementation
  _cli.py                          # `conduit-connector-sdk` console-script entry point
  _grpc/                             # generated protobuf/grpc stubs (buf generate output)
  testing/                            # acceptance-test harness (acceptance.py, fixtures.py)
examples/http-poll-source/             # worked example connector
docs/design/                             # design docs for this repo
tests/                                     # unit tests
```

## Building a standalone connector artifact

Conduit launches a standalone connector as a subprocess with a **clean
environment** — no inherited `PATH` (design doc §1.1.6). A `pip
install`-then-shebang-script connector (`#!/usr/bin/env python3`, or an
activated venv) cannot launch this way: there's no `PATH` for `env` to
search. `conduit-connector-sdk build` closes that gap:

```shell
conduit-connector-sdk build examples/http-poll-source -o http-poll-source.pyz
./http-poll-source.pyz   # directly executable — no `python` prefix, no venv activation
```

This produces one file with an **absolute** interpreter shebang (resolved
at build time, never looked up via `PATH`), bundling every third-party
dependency your connector needs — including compiled-extension
dependencies like `grpcio`/`pydantic`'s `pydantic-core`, which a plain
[`zipapp`](https://docs.python.org/3/library/zipapp.html) can't load
in-place: the artifact extracts itself to a per-build cache directory on
first run (the same fundamental approach `shiv`/`pex` use), then executes
your connector's real entry point from those extracted files. Later
launches of the same build reuse the cache.

**Precondition:** run `build` from an environment where your connector's
own dependencies are already installed (however you installed them — pip,
uv, poetry) — it vendors from what's already resolved, not a fresh
`pip install`. See `conduit/_build.py`'s module docstring for the full
rationale and known limitations.

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md). This SDK sits on Conduit's data path —
read [`docs/design/20260707-python-connector-sdk.md`](docs/design/20260707-python-connector-sdk.md)
before proposing changes to the wire adapter, ack/nack logic, or handshake.

## License

[Apache License 2.0](LICENSE).
