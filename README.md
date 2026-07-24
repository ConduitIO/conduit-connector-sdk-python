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
  _build.py                # `conduit-connector-sdk build` implementation
  _cli.py                   # `conduit-connector-sdk` console-script entry point
  _grpc/                      # generated protobuf/grpc stubs (buf generate output)
  testing/                     # acceptance-test harness (acceptance.py, fixtures.py)
examples/http-poll-source/      # worked example connector
docs/design/                     # design docs for this repo
tests/                             # unit tests
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
