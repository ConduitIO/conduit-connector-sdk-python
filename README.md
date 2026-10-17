# conduit (Python)

One Python distribution for [Conduit](https://github.com/ConduitIO/conduit),
shipping two namespaces under the top-level `conduit` import package:

- **`conduit.connector`** — the connector SDK: build a source or destination
  connector as a standalone gRPC subprocess plugin.
- **`conduit.client`** — the gRPC control client: define and run pipelines
  against a local or remote Conduit engine.

`conduit` is a [PEP 420](https://peps.python.org/pep-0420/) namespace package,
so both namespaces install together without a top-level `__init__.py`
collision. The two pre-merge repos (`conduit-connector-sdk-python` and
`conduit-client-python`) are one distribution before either namespace is
tagged GA — see
[`docs/design/20261016-python-ga-merged-distribution.md`](docs/design/20261016-python-ga-merged-distribution.md).

> **Status: pre-alpha, under active development.** No release has shipped.
> Neither namespace's public API is stable until it ships. Treat everything
> here as subject to change without a deprecation notice until then.

> **Distribution name.** The PyPI distribution is **`conduitio`** (the bare
> `conduit` name was unavailable; `conduitio` is reserved via
> `.github/workflows/reserve-conduitio.yml`). The *import package* is
> `conduit`. `pip install conduitio`, then `import conduit.connector` /
> `import conduit.client`.

## Quickstart — connector

```python
from conduit.connector import BackoffRetry, Record, Source, serve
from conduit.connector.config import BaseConfig, Field, Specification


class Config(BaseConfig):
    url: str = Field(description="HTTP endpoint to poll, expects ?since=<cursor>.")
    poll_interval_ms: int = Field(default=1000, ge=100)


class MySource(Source[Config]):
    async def read(self) -> Record:
        raise BackoffRetry()  # no new rows yet; the SDK paces retries


if __name__ == "__main__":
    serve(Specification(name="my-source", version="0.1.0", author="you"), source=MySource)
```

See `examples/http-poll-source/` for a fully working source, and
`docs/design/20260707-python-connector-sdk.md` for the SDK design.

## Quickstart — client

```python
import conduit.client as conduit

pipeline = (
    conduit.Pipeline("orders-sync")
    .source("generator", settings={"format.type": "structured"}, operations="create")
    .destination("log", level="info")
)

with conduit.local(state_dir="./conduit-state") as client:
    run = client.run(pipeline)
    run.wait_running()
    print(run.status())
    run.stop()
```

The client manages a **local or remote** engine over gRPC: `conduit.local()`
downloads and supervises a pinned `conduit` binary for dev/notebooks, and
`conduit.connect(addr)` dials an already-deployed engine for production. Only
the Go bindings embed Conduit in-process — Python is always out-of-process,
per `docs/design/20260724-embed-bindings-via-grpc.md`.

## Requirements and support window

- **Python 3.11+** (`requires-python = ">=3.11"`, enforced at install).
- [`uv`](https://docs.astral.sh/uv/) for dependency management (recommended;
  `pip install -e .[dev]` also works).

**Support window.** We support the current and two preceding CPython feature
releases, with a floor of 3.11 today. CI tests the two most recent minors
(currently 3.12 and 3.13); the floor advances by one minor when the oldest
supported minor reaches upstream CPython end-of-life, announced in the
changelog at least one minor release ahead. A breaking change to *either*
namespace bumps the one shared semantic version.

## Repo layout

```text
src/conduit/                    # PEP 420 namespace package (no __init__.py)
  _version.py                   # single shared __version__
  connector/                    # conduit.connector — the SDK
    __init__.py                 # public API surface (Source, Destination, Record, ...)
    source.py destination.py    # Source/Destination ABCs + gRPC servicers
    record.py schema.py config.py serve.py errors.py
    _batch.py                   # sdk.batch.size/delay middleware (source + destination)
    _handshake.py               # go-plugin magic cookie + protocol negotiation
    _build.py _cli.py           # `conduit-connector-sdk build` implementation
    testing/                    # acceptance-test harness (acceptance.py, fixtures.py)
  client/                       # conduit.client — the control client
    __init__.py                 # Pipeline, local, connect, Client, Run, RunStatus
    pipeline.py client.py run.py errors.py
    _local.py _provision.py     # binary provisioning + subprocess supervision
  _grpc/                        # generated protobuf/grpc stubs (buf generate output)
examples/                       # worked example connector + client quickstart
docs/design/                    # design docs for both namespaces
tests/                          # SDK unit tests
tests/client/                   # client unit + integration tests
```

## The connector SDK (`conduit.connector`)

A gRPC client/server implementation of `conduit-connector-protocol` v2
(`SourcePlugin` / `DestinationPlugin` / `SpecifierPlugin`), wrapped in an
idiomatic Python API: `async`/`await` connector methods (with sync-method
auto-dispatch to a thread pool), `pydantic`-based config with automatic
parameter introspection, and a `bytes | dict` OpenCDC record model.

- **Batching** (`sdk.batch.size` / `sdk.batch.delay`) is injected on every
  connector, matching the Go SDK's `DestinationWithBatch`/`SourceWithBatch`
  middleware. Batching never drops records (invariant 3).
- **Avro schema support** (`conduit.connector.schema.AvroSchema`, install with
  `conduitio[avro]`) encodes/decodes plain, headerless Avro binary, wire-
  compatible with `conduit-commons`' `schema/avro` package.
- **Acceptance suite** (`conduit.connector.testing.acceptance`) is the shared
  contract a connector must pass — versioned (`CONTRACT_VERSION`), runnable
  in-process in a connector's own `pytest` module.

### Building a standalone connector artifact

Conduit launches a standalone connector with a **clean environment** — no
inherited `PATH`. `conduit-connector-sdk build` packages a connector into one
directly-executable file with an absolute interpreter shebang and every
third-party dependency vendored:

```shell
conduit-connector-sdk build examples/http-poll-source -o http-poll-source.pyz
./http-poll-source.pyz
```

Run `build` from an environment where the connector's own dependencies are
already installed (it vendors what's resolved, not a fresh `pip install`).

### Delivery semantics

**Guaranteed:** at-least-once (a source record is acked only after Conduit's
`ack_positions` returns its position); no silent partial-batch acking;
batching never drops records; graceful shutdown by default (SIGTERM drains
in-flight work, bounded by a hung-loop watchdog).

**Not guaranteed:** exactly-once (redelivery on restart is possible);
crash-safe position storage (that's the connector author's responsibility —
the SDK round-trips whatever `bytes` the source returns); structured payloads
lose integer precision beyond `2**53` (the wire format's `Struct` is double-
precision, by design).

## The client (`conduit.client`)

A sync gRPC client over Conduit's control-plane API (`proto/api/v1/api.proto`).
Slice 1 (Case A: named connector plugins only):

- `conduit.Pipeline(id).source(...).destination(...).process(...).dlq(...)` —
  a builder producing exact `PipelineService`/`ConnectorService`/
  `ProcessorService` payloads.
- `conduit.connect(addr)` — dial an already-running engine (production shape).
- `conduit.local(...)` — provision, spawn, and supervise a pinned `conduit`
  binary (dev/notebook shape). **Not a production story** — no independent
  lifecycle.
- Errors are always `conduit.client.ConduitError` (stable code, message, and
  optional config path/suggestion), never a raw `grpc.RpcError`.

`inline_source`/`inline_destination` (Case B, driving a host-implemented
Python connector) is a later slice, not built here.

## Testing

```bash
uv sync --all-extras
uv run pytest -v                      # unit tests (SDK + client); no network
uv run pytest -v -m integration       # spins up a real conduit binary end-to-end
```

The integration test (`tests/client/integration/test_local_generator_log.py`)
runs `conduit.local()` for real against a `generator -> log` pipeline. It
downloads a real release binary on first use — if the sandbox has no network,
it is honestly marked `skip` with the reason stated, never faked.

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md). The connector SDK sits on Conduit's
data path — read `docs/design/20260707-python-connector-sdk.md` before
proposing changes to the wire adapter, ack/nack logic, or handshake.

## License

[Apache License 2.0](LICENSE).
