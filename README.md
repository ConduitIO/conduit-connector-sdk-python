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

## What this is not (yet)

See the design doc's Phase 2/3 breakdown for what's deliberately deferred:
author-side batching (`read_batch`), schema/Avro middleware, the acceptance-test
harness's full test corpus beyond the v0.19 core categories, PyPI
trusted-publisher release automation, and any performance claim versus the Go
SDK (none ships without a committed `benchi` result, per the org's CLAUDE.md).

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
  source.py           # Source ABC
  destination.py       # Destination ABC
  serve.py              # handshake + gRPC server bootstrap
  _handshake.py          # magic cookie, protocol negotiation, stdout line
  _grpc/                  # generated protobuf/grpc stubs (buf generate output)
  testing/                 # acceptance-test harness (acceptance.py, fixtures.py)
examples/http-poll-source/  # worked example connector
docs/design/                 # design docs for this repo
tests/                         # unit tests
```

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md). This SDK sits on Conduit's data path —
read [`docs/design/20260707-python-connector-sdk.md`](docs/design/20260707-python-connector-sdk.md)
before proposing changes to the wire adapter, ack/nack logic, or handshake.

## License

[Apache License 2.0](LICENSE).
