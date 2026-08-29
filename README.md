# conduit-client (Python)

Python client library for defining and running [Conduit](https://github.com/ConduitIO/conduit)
pipelines in code, over Conduit's existing control-plane gRPC API
(`proto/api/v1/api.proto`). No changes to Conduit itself are required.

> **Status: pre-alpha, Slice 1.** Implements
> [`docs/design/20260724-embed-grpc-client-libraries.md`](docs/design/20260724-embed-grpc-client-libraries.md)'s
> "Build slices -- Slice 1": Case A (named connector plugins) only. No release has
> shipped; the public API (`Pipeline`, `local`, `connect`, `Client`, `Run`) is
> not stable until it ships. See "Open questions for DeVaris" below.

## What this is

- A **builder** for pipeline configs (`Pipeline(id).source(...).destination(...).process(...)`)
  that produces the exact request payloads Conduit's `PipelineService`/
  `ConnectorService`/`ProcessorService` RPCs expect -- no YAML, no shelling out
  to the `conduit` CLI.
- A **sync gRPC client** (`conduit.connect(addr)`) for an already-running,
  independently deployed Conduit engine -- the production shape.
- A **local-engine supervisor** (`conduit.local(...)`) that downloads a
  pinned `conduit` release binary on first use, spawns it with its API
  enabled on loopback, and hands you a client bound to it -- for dev,
  notebooks, and one-off jobs. **Not a production story** -- see "Deployment
  modes" below.
- Errors are always `conduit.ConduitError` (a stable code, message, and
  optional config path/suggestion) -- never a raw `grpc.RpcError` or
  traceback.

Why gRPC and not a C-ABI/FFI shared library: see
[`docs/design/20260724-embed-bindings-via-grpc.md`](docs/design/20260724-embed-bindings-via-grpc.md)
(the ADR) -- in short, the hot record-processing path never crosses the
embedding boundary, only low-frequency lifecycle/status calls do, so gRPC's
cost (a loopback network hop on those calls) is real but small, and it comes
with a working story for driving an already-deployed remote engine that a
C-ABI structurally cannot have.

## Quickstart

```python
import conduit

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

First run downloads a pinned `conduit` release binary (see "Binary
provisioning" below); later runs reuse the cached copy. `state_dir` is
required to be a real, reused directory for anything beyond a one-off
experiment -- see "Deployment modes."

## Requirements

- Python 3.11+
- [`uv`](https://docs.astral.sh/uv/) for dependency management (recommended;
  `pip install -e .[dev]` also works)
- Network access on first `conduit.local()` call per version (binary
  download), unless you pass `binary=` to point at an already-present
  executable.

## Repo layout

```text
src/conduit/
  __init__.py    # public API surface
  pipeline.py    # Pipeline builder -> BuildPlan (pure, no I/O)
  client.py      # Client (gRPC stubs), connect()
  run.py         # Run handle, RunStatus
  errors.py      # ConduitError, grpc.RpcError -> ConduitError translation
  _local.py      # local(): binary provisioning + subprocess supervision
  _provision.py  # download-on-first-use, checksum-verified binary provisioning
  _grpc/         # generated protobuf/grpc stubs (buf generate output)
docs/design/     # design doc + ADR this repo implements (copied from ConduitIO/conduit)
tests/unit/      # builder/error/provisioning unit tests (no network, no subprocess)
tests/integration/  # spins up a real conduit via local() -- see "Testing" below
```

## The client API surface

- **`conduit.Pipeline(id, *, name=None, description="")`** -- fluent builder.
  `.source(plugin, *, name="", settings=None, **kwargs)`, `.destination(...)`
  (same shape), `.process(plugin, *, condition="", workers=None, settings=None,
  **kwargs)` (pipeline-level; per-connector processor attachment is a natural
  follow-up, not yet exposed), `.dlq(plugin="builtin:log", *, window_size=None,
  window_nack_threshold=None, settings=None, **kwargs)`.
  Config values are coerced to strings -- `Connector.Config.settings`/
  `Processor.Config.settings` are a flat `map<string, string>` on the wire
  today. **Real connector config keys are often not valid Python identifiers**
  (e.g. the builtin generator connector's `format.type`, `sdk.batch.size`) --
  use the explicit `settings={"format.type": "structured"}` dict for those;
  plain identifier-shaped keys can use `**kwargs` (`operations="create"`)
  instead, and both can be combined in the same call. **Typed per-connector
  config** (`postgres.Source(url=..., tables=[...])` with real parameter
  types, generated from each plugin's param spec) is the documented
  fast-follow, not hand-written per connector.
- **`conduit.local(state_dir=None, *, version=None, binary=None, startup_timeout=30.0)`**
  -- provisions (if needed), spawns, and waits for a `conduit` subprocess;
  returns a client-like `LocalConduit` (context manager, or call `.close()`
  yourself).
- **`conduit.connect(addr, *, credentials=None, check_version=True, timeout=5.0)`**
  -- dials an already-running engine; returns a `Client`.
- **`client.run(pipeline, *, start=True, timeout=10.0) -> Run`** -- creates the
  pipeline, its connectors and processors, applies the DLQ if configured, and
  starts it (unless `start=False`).
- **`run.wait_running(timeout=30.0, poll_interval=0.2)`**, **`run.status()`**
  (-> `RunStatus`: `status`, `error`, `stopped_reason`, `is_running`,
  `is_degraded`), **`run.stop(force=False, timeout=10.0)`**.

## RPCs this client drives

Exactly the incremental CRUD-plus-lifecycle RPCs in `proto/api/v1/api.proto`
(not `PlanPipeline`/`ApplyPipeline`'s whole-document shape, which is a
different, YAML-provisioner-aligned surface):

- `PipelineService`: `CreatePipeline`, `GetPipeline`, `StartPipeline`,
  `StopPipeline`, `UpdateDLQ`.
- `ConnectorService`: `CreateConnector`.
- `ProcessorService`: `CreateProcessor`.
- `InformationService`: `GetInfo` (engine version check at connect-time).

`UpdatePipeline`/`DeletePipeline`/`UpdateConnector`/`DeleteConnector`/
`UpdateProcessor`/`DeleteProcessor`/`ListPipelines`/etc. are defined on the
generated stubs (nothing stops a caller reaching for `Client._pipelines`
directly) but have no builder-level convenience yet -- Slice 1 scope is
"define + run," not full lifecycle management.

## Deployment modes -- framed honestly

- **Mode 1, `conduit.local()`**: engine co-located with your process, tied to
  its lifecycle. Fits dev, notebooks, one-off jobs. **Never sell this as a
  production pipeline story** -- no independent lifecycle, no
  restart-without-the-host-restarting, no fleet management. `state_dir`
  defaults to `./.conduit/state` (stable across repeated runs in the same
  directory) rather than an ephemeral temp dir, but a host-process crash still
  takes the engine down with it.
- **Mode 2, `conduit.connect(addr)`**: a deployed, long-running Conduit
  service, managed independently (systemd, Kubernetes, ...). This is what a
  production pipeline should use.

`inline_source`/`inline_destination` (Case B, driving a host-implemented
Python connector via a new engine-side "external connector" feature) is
**Slice 2**, not built here -- it needs its own engine-side design-doc sign-off
pass per the design doc (Tier 1, touches connector acquisition).

## Binary provisioning

`conduit.local()` never looks at `PATH`. It downloads a specific,
version-pinned GoReleaser release asset
(`conduit_<version>_<Os>_<Arch>.tar.gz`, `.zip` on Windows) from
[GitHub Releases](https://github.com/ConduitIO/conduit/releases), verifies its
SHA-256 against the release's published `checksums.txt`, extracts the
`conduit` binary, and caches it under a per-version directory in your user
cache dir (`platformdirs.user_cache_dir("conduit-client-python")`). A second
call with the same version reuses the cache with no network access. Pass
`binary=` to point at an already-present executable instead (skips
provisioning entirely) -- useful for CI images that pre-bake a specific build.

The default version is pinned in `conduit._provision.DEFAULT_CONDUIT_VERSION`
(currently the latest stable release at the time this was written); override
with `local(version="0.19.0")` or the `CONDUIT_CLIENT_ENGINE_VERSION` env var.

## Testing

```bash
uv sync --all-extras
uv run pytest -v -m "not integration"   # unit tests: no network, no subprocess
uv run pytest -v -m integration         # spins up a real conduit binary end-to-end
```

The integration test (`tests/integration/test_local_generator_log.py`) runs
`conduit.local()` for real and drives a `generator -> log` pipeline through
`run()`/`wait_running()`/`stop()`. It downloads a real release binary on
first use -- **if the sandbox running these tests has no network access, it
is honestly marked `skip` with the reason stated** (see the test file), never
faked or mocked into looking like a pass.

## Open questions for DeVaris

See the design doc's "Open questions for DeVaris" for the full list; the ones
this repo's existence doesn't yet resolve:

1. **PyPI distribution name.** ~~This repo uses the working name
   `conduit-client` in `pyproject.toml` -- not settled against
   `conduit-embed`/bare `conduit`.~~ Settled 2026-08-29 by DeVaris:
   `conduit-client`. Claimed permanently by the first publish.
2. **Release target** (v0.19 fast-follow vs. v0.20 anchor).
3. Binary provisioning here is **download-on-first-use with checksum
   verification** (not a bundled-per-platform wheel) -- lighter package,
   needs network + GitHub Releases availability. Confirms the design doc's
   open question 3 in the "lighter, needs network" direction; flag if that's
   wrong for the intended distribution story.

## License

[Apache License 2.0](LICENSE).
