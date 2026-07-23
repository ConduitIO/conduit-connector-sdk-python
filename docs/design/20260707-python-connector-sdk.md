# Python connector SDK

> **Provenance.** This doc is landed in this repo from
> `ConduitIO/conduit`'s `docs/design-documents/20260707-python-connector-sdk.md`,
> reviewed 2026-07-07 and marked SOUND (technical) / SOUND-WITH-CONCERNS (API)
> with one pre-Phase-1 blocker (B1). It is the build plan's technical design;
> the build/sequencing plan itself lives in
> `conduit-v019-plans/workstreams/python-connector-sdk.md` (not part of this
> repo). **Three must-fixes from the pre-implementation build review are folded
> in below**, marked ▶ MUST-FIX at their exact location: (1) the two load-bearing
> Go-source citations this design leans on are re-verified directly against
> the repos in this build session, not re-trusted from the earlier review; (2)
> the Phase-1 shutdown acceptance test is tightened from a timing/log heuristic
> to a deterministic RPC-invocation assertion; (3) a hung-event-loop failure
> mode is added to the enumerated list, since it has no Go analog.

## Summary

Design and phased plan for `conduit-connector-sdk-python`, a new repo delivering an
idiomatic Python SDK for building Conduit **source and destination connectors** that
run as standalone (subprocess, gRPC, go-plugin-handshake) plugins — no Conduit code
changes required. This is the detailed follow-on to the Python tier of
"SDK & embedding developer experience" (persona A1: "Python — gRPC-standalone first ...
First `libconduit` consumer on the author side"). This document is planning/design
only in its original form; this repo is where it stops being planning-only.

The technical crux — and the part most likely to sink the project if under-specified
— is replicating HashiCorp go-plugin's subprocess handshake from a pure-Python
process with no Go runtime. That handshake is fully characterized below with exact
constants, cited to the Go source Conduit and the SDK actually run. It is a plain
stdout line + a standard gRPC server; nothing in it requires Go. The rest of the
document designs a Python API that mirrors the Go SDK's semantics (interfaces,
config validation, acceptance contract, batching) while being idiomatically
Python rather than a transliteration (async-native, pydantic-based config +
paramgen-by-introspection, `bytes | dict` records instead of a `Data` interface,
exceptions instead of Go's `(n, err)` partial-write convention).

## Context

### The problem

- The plugin author's SDK is Go-only today (`conduit-connector-sdk`). Python —
  the dominant language for the data/AI audience Conduit is targeting — has no
  SDK, only an unrelated, experimental, **WASM-based**
  `conduit-processor-sdk-python` (see "Relationship to the existing Python
  processor prototype" below), which is a different extension mechanism for a
  different plugin kind and is not reusable here as-is.
- "A connector" is defined by conformance to the connector protocol
  (`conduit-connector-protocol`) and by passing the acceptance-test suite — not by
  language. Today that suite (`AcceptanceTest`,
  `conduit-connector-sdk/acceptance_testing.go:54-58`) exists only in Go, so there is
  no way for a Python-authored connector to prove conformance.
- Standalone connectors are launched by Conduit as **subprocesses** using
  HashiCorp go-plugin over gRPC (`conduit/pkg/plugin/connector/standalone/
  dispenser.go:55` → `conduit-connector-protocol/pconnector/client/client.go`). A
  Python process must replicate that launch protocol exactly, or Conduit cannot
  start it at all — this is a hard gate before any Python-authored business logic
  matters.

### ▶ MUST-FIX 1: citation re-verification (this build session, 2026-07-23)

Two Go-source citations this design (and the v0.19 build plan) lean on for the
invariant-1/3/4 parity argument were re-read directly against
`conduit-connector-sdk` in this session, not re-trusted from the 2026-07-07
review or the plan doc's own claim:

- **`destination.go:345-350`** — confirmed **correct, verbatim**. The exact
  guard exists:

  ```go
  if n == len(batch) && err != nil {
      err = fmt.Errorf("connector reported a successful write of all records "+
          "in the batch and simultaneously returned an error, this is probably "+
          "a bug in the connector. Original error: %w", err)
      n = 0 // nack all messages in the batch
  } else if n < len(batch) && err == nil {
      err = fmt.Errorf("batch contained %d messages, connector has only "+
          "written %d without reporting the error, this is probably a bug "+
          "in the connector", len(batch), n)
  }
  ```

  This is real, defensive code the Go SDK runs on every batch write — it is not
  a hypothetical failure mode invented for this design doc's argument. It
  directly substantiates §2.5/B1: Go's `(n, err)` contract is easy enough to
  violate that the SDK itself must guard against it at runtime; Python's
  exception-based adapter (§2.5) makes the equivalent mistake structurally
  unrepresentable instead of merely detected-and-logged.
- **`source.go:270-295`** — confirmed **correct**. The region is a single,
  plain `for` loop (`runRead`, no goroutine spawned per read) that: builds a
  `backoff.Backoff{Factor: 2, Min: 100ms, Max: 5s}`, calls `readFn` serially,
  and on `ErrBackoffRetry` sleeps via `time.After(b.Duration())` before
  `continue`-ing the same loop. There is no concurrent invocation of the read
  path anywhere in this function. This directly substantiates the invariant-4
  claim in the build plan: the Go SDK's own internal read loop introduces no
  reordering or parallelism of its own, so the Python SDK reusing the identical
  backoff constants (§2.5) and an equally serial `async for`-free single-task
  read loop is genuine behavioral parity, not an assumption.

Both citations were wrong-until-verified in the sense that no one had actually
opened the files for this build; both check out. No correction to the design's
claims is needed as a result of this verification — recorded here so the
Tier-1 sign-off (CLAUDE.md) has a real verification trail instead of a citation
chain that was never re-walked past the original review.

### Relationship to the existing Python processor prototype

`ConduitIO/conduit-processor-sdk-python` already exists — description "[WIP]
Experimental Python SDK for Conduit processors". Its file tree shows: `world.wit` +
`componentize-py` + a hand-rolled `malloc`/pointer WASM ABI matching the Go SDK's
WASM ABI, and protobuf stubs generated via `buf generate` with the
**`protocolbuffers/python` + `protocolbuffers/pyi`** remote BSR plugins
(`proto/buf.gen.yaml`) — i.e., standard protoc-style `_pb2.py`/`_pb2.pyi`
output, not `betterproto`. Two takeaways:

1. **It is not prior art for this SDK's transport.** It targets the WASM
   component-model path for _processors_; connectors use go-plugin/gRPC
   subprocesses, a different mechanism entirely. Do not conflate the two —
   this design doc is gRPC-only.
2. **It is real precedent for codegen tooling**: the org already generates Python
   protobuf code via `buf generate` against BSR modules
   (`buf.build/conduitio/conduit-connector-protocol`, confirmed present at
   `conduit-connector-protocol/proto/buf.yaml`, depending on
   `buf.build/conduitio/conduit-commons`). This SDK follows the same
   pattern for consistency across the org's Python surface.

### Constraints (from CLAUDE.md, binding on this design)

- Language floor: Python 3.11+, matching the house style already used for
  tooling elsewhere in the org.
- Errors are API: every user-facing error needs a stable shape and an actionable
  message — same bar as Conduit's own CLI/API errors.
- No speculative generality: this SDK targets the gRPC-standalone connector path
  only. WASM is the processor extension mechanism, not the connector one — see
  the relationship note above. Adding WASM-connector support is out of scope
  without a documented demand signal.
- Docs and template move with the code: Phase 1 ships an example connector, not
  just a library.
- Design doc before code for anything touching a public contract — this **is**
  that doc for the SDK's own public surface (the base classes, config model, wire
  behavior). Downstream connector authors will build against what's decided here,
  so treat the API shape as a public contract from v0.1.

## Decision

### 1. Protocol & handshake (the crux)

This is the make-or-break section. Every constant below is cited to the exact Go
source Conduit runs today.

#### 1.1 The handshake is not Go-specific

Conduit (client) and the Go SDK (server) both import the **same shared symbol**
for their `HandshakeConfig` — not just matching values by convention:

```go
// conduit-connector-protocol/pconnector/pconnector.go:19-22
var HandshakeConfig = plugin.HandshakeConfig{
    MagicCookieKey:   "CONDUIT_PLUGIN_MAGIC_COOKIE",
    MagicCookieValue: "204e8e812c3a1bb73b838928c575b42a105dd2e9aa449be481bc4590486df53f",
}
```

- Server (SDK) side: `conduit-connector-protocol/pconnector/server/serve.go:42`
  passes it into `plugin.ServeConfig`; `conduit-connector-sdk/serve.go:106` just
  calls `server.Serve(...)`.
- Client (Conduit) side: `conduit-connector-protocol/pconnector/client/client.go:48`
  passes the identical value into `plugin.ClientConfig`. Conduit itself never
  constructs its own `plugin.NewClient` — `conduit/pkg/plugin/connector/
  standalone/dispenser.go:55` delegates to `client.New(...)` in the protocol repo.

A Python plugin must:

1. Read `os.environ["CONDUIT_PLUGIN_MAGIC_COOKIE"]` and compare it to the value
   above at startup; exit non-zero with a diagnostic if it doesn't match or is
   absent (mirrors `go-plugin@v1.8.0/server.go:247-266`, which is enforced only by
   the server-side library, not by anything Conduit itself checks beyond
   requiring the line — a Python implementation must do this check itself, there
   is no free lunch from the Go runtime).
2. Negotiate protocol version via the `PLUGIN_PROTOCOL_VERSIONS` env var
   (`go-plugin@v1.8.0/client.go:642`) and pick the highest version it supports
   that the client also lists (`go-plugin@v1.8.0/server.go:148-221`). Two
   protocol versions exist today: v1 = `conduit-connector-protocol/pconnector/v1/
   version.go:19` (`const Version = 1`, package doc-commented `Deprecated: v1 is
   deprecated. Use v2 instead.`), v2 = `pconnector/v2/version.go:18`
   (`const Version = 2`). **Target v2** — v1 is deprecated at the source.
3. Print exactly one handshake line to **stdout** (not stderr — stdout is the
   channel go-plugin's client parses) once the gRPC server is listening:

   ```text
   CORE-PROTOCOL-VERSION|APP-PROTOCOL-VERSION|NETWORK|ADDRESS|PROTOCOL|SERVER-CERT
   ```

   Format and field semantics from `go-plugin@v1.8.0/server.go:426-445`; parse
   side at `go-plugin@v1.8.0/client.go:838-926` (splits on `|`, requires ≥4
   parts). Concretely: `CORE-PROTOCOL-VERSION` is always literal `1`
   (`server.go:33`, unrelated to the v1/v2 app-protocol negotiated above —
   confusingly, go-plugin has its own "core" protocol version separate from
   Conduit's connector-protocol version); `APP-PROTOCOL-VERSION` is `2` (or `1`);
   `NETWORK` is `tcp` or `unix`; `ADDRESS` is the listen address; `PROTOCOL` is
   literal `grpc` (NetRPC is explicitly disabled on both sides via
   `plugin.NetRPCUnsupportedPlugin`, `server/serve.go:125` and
   `client/client.go:93`); `SERVER-CERT` is only interpreted if longer than 50
   chars (AutoMTLS) — **Conduit's client never sets `AutoMTLS: true`**
   (absent from `pconnector/client/client.go`), so **leave this field empty**; no
   TLS is in play.
4. Choose **TCP**, not Unix domain socket, for the listen transport. go-plugin's
   own server defaults to a Unix socket on non-Windows and TCP only on Windows
   (`server.go:528-535`), but the client-side parser accepts either
   (`client.go:888-901`) — nothing requires matching the Go default. TCP on
   `127.0.0.1:0` (OS-assigned port) is simpler to implement correctly in Python
   (`asyncio`/`grpc.aio` both support it natively) and sidesteps Unix-socket
   temp-directory/permission bookkeeping. This also gets Windows support "for
   free" per CLAUDE.md's Windows CI requirement, rather than needing a
   platform branch.
5. Serve, on that listener, a **plain gRPC server** — nothing exotic. go-plugin's
   `GRPCServer.Init()` additionally registers `grpc.health.v1.Health` (service
   name `"plugin"`, `GRPCServiceName = "plugin"`, set to `SERVING`,
   `grpc_server.go:24-81`), gRPC reflection, a `GRPCBroker` service, and a
   `GRPCController` service (carries the `Shutdown` RPC). Of these, two matter in
   practice for a Python implementation and the rest are cosmetic:
   - **`grpc.health.v1.Health`** — register it (grpcio ships
     `grpc_health.v1` support); low effort, part of the documented contract.
   - **`GRPCController.Shutdown`** — Conduit's `Close()` RPCs this on teardown
     (`grpc_client.go:106-108`); if unimplemented, `Close()` errors and Conduit
     force-kills the process ~2s later instead (`client.go:530-567`) — the
     pipeline still tears down correctly, just not gracefully from go-plugin's
     point of view. **Implement it** (acknowledge, run `teardown()` to
     completion, then stop the server / `os._exit(0)`) so shutdown is clean
     rather than relying on the timeout fallback, per CLAUDE.md invariant 7
     (graceful shutdown by default). See ▶ MUST-FIX 3 below for the bounded
     deadline this needs.
   - `GRPCBroker` exists for nested/multiplexed plugin scenarios (a plugin that
     itself dispenses sub-plugins over the same connection). The connector
     protocol's `SourcePlugin`/`DestinationPlugin`/`SpecifierPlugin` services run
     directly on the single primary gRPC connection — **a no-op broker stub is
     sufficient**; Python does not need to reimplement go-plugin's internal
     yamux-based multiplexer.
   - gRPC reflection is optional polish (aids `grpcurl`/debugging); include it,
     it's a few lines with `grpc_reflection`.
6. Exec with a **clean environment**. Conduit's dispenser sets
   `cmd.Env = make([]string, 0)` before appending its own vars
   (`pconnector/client/client.go:45`) — the subprocess gets _only_ go-plugin's
   own vars plus the Conduit-set ones below, **no inherited `PATH`**. Practical
   consequence for Python: the connector's launch shebang/entry point must be an
   absolute interpreter path (e.g. baked into a PyInstaller/zipapp artifact, or a
   wrapper script that resolves its own venv by absolute path) — anything that
   relies on `PATH`-based `python3` resolution at exec time will fail. This is
   flagged explicitly in Risks & Open Questions and drives the packaging
   decision in §3.

#### 1.2 Env vars a Python subprocess should read

| Var | Source | Purpose |
| --- | --- | --- |
| `CONDUIT_PLUGIN_MAGIC_COOKIE` | go-plugin | handshake cookie, must match §1.1 |
| `PLUGIN_PROTOCOL_VERSIONS` | go-plugin | client-supported protocol versions to negotiate against |
| `PLUGIN_MIN_PORT`/`PLUGIN_MAX_PORT` | go-plugin | only relevant if restricting the TCP port range; not required |
| `CONDUIT_CONNECTOR_UTILITIES_GRPC_TARGET` | `pconnutils/env_vars.go:18` | address of Conduit's connector-utilities gRPC service (schema registry access, etc.) |
| `CONDUIT_CONNECTOR_TOKEN` | `pconnutils/env_vars.go:19` | auth token for calling back into Conduit |
| `CONDUIT_CONNECTOR_ID` | `pconnector/env_vars.go:18-20` | this connector instance's ID |
| `CONDUIT_CONNECTOR_LOG_LEVEL` | same | log level to honor |
| `CONDUIT_CONNECTOR_MAX_RECEIVE_RECORD_SIZE` | same | gRPC max message size to configure |

Unused/irrelevant given the TCP choice: `PLUGIN_UNIX_SOCKET_DIR`,
`PLUGIN_UNIX_SOCKET_GROUP`, `PLUGIN_CLIENT_CERT` (AutoMTLS only).

#### 1.3 RPC surface to implement (protocol v2)

From `conduit-connector-protocol/proto/connector/v2/{source,destination,specifier}.proto`,
confirmed directly (this repo has a local checkout of `conduit-connector-protocol`
used to write the codegen config in §1.5 — not just cited secondhand):

- **`SourcePlugin`**: `Configure` (unary), `Open`
  (unary), `Run` (**bidirectional stream** — plugin streams record batches out,
  Conduit streams ack-position batches back, independently/concurrently), `Stop`
  (unary), `Teardown` (unary), `LifecycleOnCreated`/`OnUpdated`/`OnDeleted`
  (unary each). `Run`'s doc comment states the ack-ordering contract explicitly:
  *"Acknowledgments will be sent back to the connector in the same order as the
  records produced by the connector. If a record could not be processed by
  Conduit, the stream will be closed without an acknowledgment being sent back"*
  — this is the wire-level statement of invariant 4/1 the Python adapter must
  honor.
- **`DestinationPlugin`**: same shape — `Run` is
  bidi, Conduit streams record batches in, plugin streams back per-record acks
  (each carrying an optional error string) — `Configure`, `Open`, `Stop`,
  `Teardown`, three lifecycle hooks. Notably: `Destination.Stop.Request` and
  `Source.Stop.Response` both carry a `last_position` field the plugin/Conduit
  use to know when the stream has fully drained — relevant to graceful shutdown
  (§ Failure modes).
- **`SpecifierPlugin`**: `Specify` (unary) → name,
  summary, description, version, author, `source_params`/`destination_params`.

The `Run` bidi stream is the only structurally interesting RPC — everything else
is unary request/response. `grpc.aio`'s native bidi-stream support maps onto it
directly (see §2 for how this shapes the async-vs-sync recommendation).

#### 1.4 Wire record shape (confirmed against the .proto, not inferred)

`conduit-commons` (`proto/opencdc/v1/opencdc.proto`, pulled as a BSR dependency,
not vendored locally — see §1.5):

```protobuf
message Record {
  bytes position = 1;
  Operation operation = 2;             // OPERATION_{CREATE,UPDATE,DELETE,SNAPSHOT} = 1..4
  map<string, string> metadata = 3;
  Data key = 4;
  Change payload = 5;
}
message Change {
  Data before = 1;   // optional; update/delete only
  Data after = 2;    // all ops except delete
}
message Data {
  oneof data {
    bytes raw_data = 1;
    google.protobuf.Struct structured_data = 2;
  }
}
```

This directly confirms the Go-side `opencdc.Data` interface is, at the wire
level, nothing more than a two-way oneof. Section 2.3 uses this to justify a
simpler Python representation than Go's interface indirection.

#### 1.5 Codegen: buf + protoc, not betterproto

**Recommendation: `buf generate` against
`buf.build/conduitio/conduit-connector-protocol`, using the
`protocolbuffers/python` + `protocolbuffers/pyi` remote plugins for messages plus
`grpc/python` for service stubs**, mirroring what
`conduit-processor-sdk-python/proto/buf.gen.yaml` already does for messages
plus a grpc plugin for the service definitions it doesn't need (WASM has no gRPC
service, so that repo has no precedent for the grpc plugin specifically — this
SDK adds it). See Alternatives Considered for why `betterproto` was rejected.
Lane A (this repo's `buf.gen.yaml`) generates into `src/conduit/_grpc/`, treated
as vendored/generated output, excluded from this repo's own docstring/strict-mypy
requirements (see `pyproject.toml`).

### 2. Idiomatic Python API design

The public surface a connector author writes against. Design goal: feel like a
modern Python library (dataclasses/pydantic, `async`/`await`, exceptions), not a
Go interface transliterated field-for-field. Where Go needed indirection to work
around limitations Python doesn't have (an interface for `Data`, a code-gen step
for parameter specs, a `mustEmbedUnimplementedX()` seal for forward-compat), this
design collapses it.

#### 2.1 async, not sync — and why

The `Run` RPC is a bidirectional stream: a source must be able to emit records
_and_ receive ack callbacks concurrently on the same logical connection; a
destination must receive record batches _and_ emit ack batches concurrently.
`grpc.aio` models this natively as two independent `async for` loops over one
call object. A sync `grpcio` implementation could do this too (with a background
thread pulling one direction while the RPC thread pushes the other), but that
pushes hand-rolled thread-safety onto the SDK's core loop for no benefit — the
whole surface (HTTP clients, DB drivers, message-queue clients most connectors
wrap) is I/O-bound, which is precisely asyncio's target case. **Recommendation:
`asyncio` + `grpc.aio` as the SDK runtime.**

Connector authors, though, should not be forced into `async def` if their
target system's client library is sync-only (many DB drivers still are).
**Base class methods are declared `async def`, but the SDK detects a sync
override via `inspect.iscoroutinefunction` and runs it in a thread-pool executor
transparently** — the same dual-mode ergonomic FastAPI uses for path operations.
An author writing a sync `psycopg2`-based source just writes `def read(self):
...`; an author writing an `httpx.AsyncClient`-based source writes `async def
read(self): ...`. Both are first-class, not "sync as a fallback hack."

#### 2.2 Config: pydantic v2, with paramgen replaced by introspection

Go needs `paramgen` (driven by struct tags like
`validate:"required,gt=0,lt=100,inclusion=a|b"`) because Go has no runtime
reflection rich enough to turn a struct's field types and tags into a
`config.Parameter` map without a separate generation pass. **Python doesn't have
that problem: pydantic v2's `model_fields` already carries type, default, and
constraint metadata at runtime.** The SDK provides one function,
`to_parameters(config_cls: type[BaseConfig]) -> dict[str, Parameter]`, that
introspects a pydantic model and produces the `Specify` RPC's parameter map
directly — **no codegen step, no `//go:generate`, always in sync with the model
because it's the model.** This is a genuine simplification over the Go SDK, not
just a stylistic swap.

```python
class Config(BaseConfig):
    url: str = Field(description="HTTP endpoint to poll.")
    poll_interval_ms: int = Field(default=1000, ge=100, description="Milliseconds between polls.")
    format: Literal["json", "csv"] = Field(default="json")  # -> Validation.inclusion
```

Mapping to `config.Parameter`/`Validation`: `Field(default=...)` →
`Default`; `ge=`/`le=` → `greater-than`/`less-than`; `Literal[...]` → `inclusion`;
`pattern=` → `regex`; a plain (no-default) field → `required`. `BaseConfig`
subclasses `pydantic.BaseModel` and additionally exposes `to_parameters()` as a
classmethod so the Specifier RPC handler is one line:
`Specify.Response(source_params=SourceConfig.to_parameters(), ...)`.

**A-gap flagged by the 2026-07-07 review, not yet resolved by this doc, pre-Phase-1
non-blocking:** `ParameterTypeDuration` (Go's `"5s"` syntax, not ISO-8601) and
`ValidationTypeExclusion` don't have an obvious pydantic-native mapping yet —
tracked as an open item for whoever implements `to_parameters()` (Lane B3),
not silently assumed solved by this doc.

#### 2.3 The OpenCDC record: `bytes | dict`, not an interface

Go's `opencdc.Data` interface (`Bytes()`, `Clone()`, `ToProto()`) exists to give
two structurally different Go types (`RawData []byte`, `StructuredData
map[string]interface{}`) a common contract. Per §1.4, the wire format is simply
a two-way oneof (`raw_data: bytes` / `structured_data: Struct`). Python doesn't
need the interface: `bytes` and `dict` already have their own copy semantics
(`bytes` is immutable, `dict.copy()`/`copy.deepcopy()` handle `Clone()`'s job),
and `isinstance()` at the (de)serialization boundary — not author-facing code —
is enough to pick the right oneof branch. **`Data = bytes | Mapping[str, Any]`.**

```python
@dataclass
class Change:
    before: Data | None = None  # update/delete only
    after: Data | None = None  # all ops except delete


class Operation(enum.Enum):
    CREATE = 1
    UPDATE = 2
    DELETE = 3
    SNAPSHOT = 4


@dataclass
class Record:
    position: bytes
    operation: Operation
    metadata: dict[str, str] = field(default_factory=dict)
    key: Data = b""
    payload: Change = field(default_factory=Change)
```

Plain `dataclasses`, not pydantic, for `Record` — records are produced/consumed
at high frequency in the hot path; pydantic's validation overhead isn't wanted
there, and there's nothing to validate (the wire format already constrains the
shape). Well-known metadata keys (`opencdc.collection`, `opencdc.createdAt`,
etc.) ship as `Metadata` string constants plus typed helpers
(`record.metadata.set_created_at(dt)`) mirroring Go's `GetCreatedAt`/
`SetCreatedAt` ergonomics on the underlying dict.

**B3 (from the 2026-07-07 review) — the silent fidelity case is int→float, not
bytes.** `google.protobuf.Struct` only supports JSON-like values. Passing raw
bytes in a nested `StructuredData` field fails **loudly** (no JSON
representation for `bytes` — a `TypeError` at encode time). The genuinely
**silent** case is integer precision: `{"count": 5}` round-trips through
`Struct` as a JSON number, decoded back as a Python `float` (`5.0`), and for
integers beyond `2**53` this is a real precision loss, not a cosmetic type
change. This must be: (a) documented explicitly as the `StructuredData`
payload boundary in author-facing docs, and (b) covered by Hypothesis
round-trip tests asserting **exact identity**, not just "doesn't crash" — a
test that accepts `5 == 5.0` would pass while masking the precision-loss case
for large integers. `test_record_codec.py` (AC #7) is scoped to catch this
specifically.

#### 2.4 Forward-compatible base classes without Go's seal hack

Go forces `mustEmbedUnimplementedSource()` (an unexported method only
`UnimplementedSource` provides) so a struct can't accidentally satisfy the
`Source` interface without embedding the default impl — protecting against the
interface growing a method later. Python's ABC + default-method pattern gets the
same guarantee for free: `Source`/`Destination` are `abc.ABC` subclasses with
`open`/`read`/`ack`/`teardown`/lifecycle hooks all having **default no-op (or
`NotImplementedError`-raising) bodies** except the couple of genuinely-required
ones (`read`/`write`, `config` typed by the generic parameter). Adding a new
optional method later is source-compatible automatically — no seal method
needed, no boilerplate for authors.

#### 2.5 Errors: exceptions, not `(n, err)` — the B1 fix, fail-closed by construction

Go's `Destination.Write(ctx, batch) (n int, err error)` requires the SDK to
_enforce_ an invariant that's easy to get wrong: `n == len(batch)` must imply
`err == nil`, and `n < len(batch)` must imply `err != nil`
(`destination.go:345-350`, re-verified ▶ MUST-FIX 1 above — this is real,
defensive code the Go SDK runs today, not a hypothetical). Python favors
exceptions for error propagation and this SDK follows that: `async def
write(self, records: list[Record]) -> None`; full success is "no exception
raised"; a partial-batch failure raises `BatchWriteError({index: exception,
...})`, which the SDK's adapter — not the author — translates into the correct
per-record ack/error entries on the `Run` stream.

**The B1 blocker and its fix, stated precisely (this is the load-bearing
correctness property of the whole SDK, not a nice-to-have):**

- A naive Python translation of "catch the first exception, treat every index
  not present in the exception's map as successful" is the exact same bug Go's
  `(n, err)` contract has to defend against — "absence of an error entry" read
  as "acked" is a direct invariant 1/3 violation (records never durably
  written get acked).
- **Fix, banned-by-construction, not merely documented:** the SDK's write
  adapter (Lane B5, `destination.py`) fails the **entire batch** — nacks every
  record in it — whenever `write()` raises any exception, *unless* the
  exception is a `BatchWriteError` carrying an **explicit, exhaustive**
  accounting of every index in the batch: either a contiguous `written: int`
  prefix count (Go's `n`, the common case — "everything up to index N-1
  succeeded, everything from N on failed") or an explicit `success: set[int]` /
  `failures: dict[int, Exception]` pair that together cover every index with
  no gap. **An index that appears in neither the success accounting nor the
  failure accounting is treated as failed, never as successful.** This is the
  fail-closed rule: incompleteness of the author-supplied accounting is itself
  an adapter-level error condition (logged as a probable connector bug,
  mirroring Go's own `destination.go:345-350` diagnostic), not silently
  resolved by assuming the missing indices succeeded.
- This is checked by construction in the adapter's type signature and runtime
  validation, not left to author discipline: `BatchWriteError` requires either
  `written` or an exhaustive `(success, failures)` pair at construction time;
  there is no code path in the adapter that computes "ack everything not
  explicitly marked as failed."
- Test: `test_destination_partial_write_nacks_all` (AC #1) asserts that an
  exception with an incomplete/absent success mapping nacks the *entire*
  batch, not a silently-assumed-successful prefix — this is the concrete,
  automated form of the B1 fix, not just prose in this doc.

For `Source.read()`: raising `BackoffRetry` signals "no record right now,
retry with backoff" (direct analog of Go's `ErrBackoffRetry`, consumed with a
`Factor:2, Min:100ms, Max:5s` backoff at `source.go:280-289`, re-verified
▶ MUST-FIX 1 above as a genuinely serial loop — the Python SDK reuses the same
constants for parity). Any other exception propagates as a gRPC `INTERNAL`
status with the exception's string as detail. The base `ConnectorError`
exception carries an optional `code: str | None` field, currently unused on the
wire — **the Go connector protocol has no stable error-code scheme today** —
but a protocol change adding plugin-originated `ConduitError` codes is already
flagged as landing around v0.16 per "SDK & embedding developer experience".
Reserving the field now avoids a breaking SDK change when that lands; until
then it's always `None` and unused by the wire encoding.

#### 2.6 Batching and schema: what Phase 1 (v0.19 core) defers

Go's `ReadN`/`Write([]Record)` batch APIs and `sdk.batch.size`/`sdk.batch.delay`
middleware are useful but not required for a minimally-working connector, since
the wire protocol's `Run` stream is already batch-shaped at the message level
regardless of SDK-side buffering. **Phase 1** implements the direct mapping
only: `async def read(self) -> Record` (SDK wraps single records into
single-record batches on the wire) and `async def write(self, records:
list[Record]) -> None` (destination `Run` messages are naturally batched, so
no author-side batching primitive is needed there at all). A `read_batch`
override point and `sdk.batch.size`/`delay`-equivalent config are **Phase 2
(fast-follow, v0.20)**. Schema extraction/encoding middleware (Avro
registration, `SourceWithSchemaExtraction` et al.) is **Phase 2** as well —
Phase 1 records carry raw `bytes`/`dict` only, no schema subject/version
metadata is auto-populated.

#### 2.7 End-to-end example (illustrative — not final API)

```python
"""A minimal Conduit source connector: polls an HTTP endpoint for new rows."""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime

import httpx
from conduit import BackoffRetry, Change, Operation, Record, Source, Specification, serve
from conduit.config import BaseConfig, Field


class Config(BaseConfig):
    url: str = Field(description="HTTP endpoint to poll, expects ?since=<cursor>.")
    poll_interval_ms: int = Field(default=1000, ge=100, description="Delay between empty polls.")


class HTTPPollSource(Source[Config]):
    async def open(self, position: bytes | None) -> None:
        self._client = httpx.AsyncClient()
        self._since = position.decode() if position else "0"

    async def read(self) -> Record:
        resp = await self._client.get(self.config.url, params={"since": self._since})
        rows = resp.json()
        if not rows:
            raise BackoffRetry()  # the SDK already paces retries; don't double-sleep

        row = rows[0]
        self._since = str(row["id"])
        return Record(
            position=self._since.encode(),
            operation=Operation.CREATE,
            key={"id": row["id"]},
            payload=Change(after=row),
            metadata={"opencdc.readAt": str(int(datetime.now(UTC).timestamp() * 1e9))},
        )

    async def teardown(self) -> None:
        await self._client.aclose()


if __name__ == "__main__":
    serve(Specification(name="http-poll", version="0.1.0", author="you"), source=HTTPPollSource)
```

~35 lines, no boilerplate beyond what the connector's own logic needs. `Ack`,
lifecycle hooks, and config validation all have working defaults from the base
class and don't need overriding for this connector. (Note vs. the 2026-07-07
review's A-gap: this version raises `BackoffRetry()` directly without a manual
`asyncio.sleep` — the SDK's own backoff loop paces the retry; a manual sleep
plus `BackoffRetry` double-backoffs.)

### 3. Repo & packaging

**New repo: `ConduitIO/conduit-connector-sdk-python`** — matches the naming
pattern already established by `ConduitIO/conduit-processor-sdk-python`.

Proposed layout:

```text
conduit-connector-sdk-python/
  pyproject.toml            # uv-managed; hatchling build backend
  src/conduit/
    __init__.py              # public API: Source, Destination, Record, Operation, Change, serve, errors
    config.py                 # BaseConfig, Field, to_parameters()
    record.py                 # Record/Change/Operation/Metadata
    source.py                 # Source ABC, default middleware hook points
    destination.py            # Destination ABC
    serve.py                  # handshake + gRPC server bootstrap (the §1 implementation)
    _handshake.py              # magic cookie, protocol negotiation, stdout line
    _grpc/                     # generated stubs (buf generate output) + adapters translating
                                # proto <-> Python dataclasses/pydantic models
    testing/
      acceptance.py            # the acceptance-test harness
      fixtures.py               # golden record-shape fixtures shared with other-language SDKs
  examples/http-poll-source/    # the Phase-1 worked example, runnable standalone
  buf.gen.yaml                  # codegen config (see §1.5)
  docs/design/                   # this doc
  .github/workflows/
    lint.yml  test.yml  release.yml  compat-nightly.yml
  CONTRIBUTING.md  README.md  CHANGELOG.md
```

- **Packaging**: `pyproject.toml`, `uv` for dependency management/locking,
  `hatchling` build backend. Publish to PyPI as `conduit-connector-sdk`.
- **Python floor**: 3.11+.
- **Lint/type**: `ruff` (format + lint) and `mypy --strict` on the public API
  surface.
- **Test**: `pytest`, `pytest-asyncio`, `hypothesis` for the record
  (de)serialization round-trip properties.
- **Acceptance-test harness (pulled forward into v0.19 core scope per
  CLAUDE.md's "acceptance tests define connector" standard — not deferred to
  Phase 2 as originally scoped in the 2026-07-07 review)**: mirrors
  `sdk.AcceptanceTest(t, driver)`. A `conduit.testing.AcceptanceTestDriver`
  Protocol an author implements, and a `ConfigurableAcceptanceTestDriver`
  convenience wrapper for the common case. Test categories: specifier
  existence/validity, config parameter validation (success +
  required-param-missing), resume-at-position (snapshot and CDC), read/write
  round trip, read timeout behavior, **partial-batch write correctness**
  (directly covering B1, §2.5) — kept **version-numbered** so an author knows
  exactly which contract version they passed.
- **CI matrix**: ubuntu-latest, macos-latest, windows-latest, against the two
  most recent CPython minors on the 3.11+ floor. A separate
  `compat-nightly.yml` job regenerates the gRPC stubs from the **current**
  `buf.build/conduitio/conduit-connector-protocol` HEAD and (once Lane D/F
  land) runs the acceptance suite against a current Conduit dev build — the
  drift-detection mechanism from Risks & Open Questions.
- **Release**: tag-triggered, conventional-commit changelog. Manual publish to
  PyPI for v0.x; a trusted-publisher GitHub Action (no long-lived PyPI token in
  repo secrets) is fast-follow, not blocking.
- **Example/template connector**: `examples/http-poll-source/` is the Phase-1
  worked example from §2.7, made fully runnable — the Python analog of what
  `conduit connector new` scaffolds for Go. A dedicated
  `conduit-connector-template-python` repo is **Phase 3** work, gated on
  `conduit connector new --lang python` integration.

### 4. Phased plan

#### Phase 1 (v0.19 core, this repo's current build target)

Scope: handshake (§1) + `Source`/`Destination` ABCs (§2.1, §2.4) + `Configure`/
`Specify` (§2.2, no schema/middleware) + OpenCDC record (§2.3) + basic
lifecycle (`Open`/`Run`/`Stop`/`Teardown`, unary lifecycle hooks as no-ops
unless overridden) + `BackoffRetry`/`BatchWriteError` (§2.5, B1 fix
non-negotiable) + the acceptance-test harness (pulled forward, §3) + the
worked example connector (§2.7, §3) + `--lang python` scaffolding wiring
(Conduit-repo side, not this repo).

Explicitly **not** in v0.19 core: `read_batch`/batch-size config, schema/Avro
middleware, a standalone template repo, PyPI trusted-publisher release
automation, performance-parity benchmarking vs. Go.

**▶ MUST-FIX 2: Definition of done, tightened.** A fresh checkout of this
repo's example connector, installed with `uv sync`, launched by a **real,
unmodified Conduit binary** (no Conduit-side code changes) as the source of a
pipeline whose destination is `conduit-connector-file` (or another
already-supported connector) — records flow end-to-end, get acked, and
`conduit pipelines stop` triggers a clean subprocess exit via the
`GRPCController.Shutdown` path (§1.1.5), not the 2-second force-kill fallback.

**This must be verified by a deterministic RPC-invocation assertion, not a
timing or log heuristic.** The earlier framing — "asserting on the shutdown
path taken (distinguishable via timing or a log assertion)" — is rejected: a
process that exits within N seconds, or that happens to log something
shutdown-adjacent, is consistent with **either** `Shutdown` succeeding **or**
`Shutdown` silently failing and Conduit's force-kill fallback firing early
enough to look the same from outside. A timing window cannot distinguish "the
graceful path ran" from "the graceful path is broken but the timeout is short
enough not to matter today" — and the latter is exactly the regression this
test exists to catch. Concretely, the test must instead:

1. **Unit/integration level (this repo, no real Conduit binary needed):**
   construct the SDK's actual `grpc.aio` server with its real
   `GRPCController` service implementation, connect a test gRPC client
   directly to it, call `Shutdown` through that client, and assert (a) the
   RPC returns a successful response (not a connection error, not a timeout)
   and (b) an SDK-internal, test-visible side effect proves the handler body
   actually ran end-to-end — e.g., `teardown()` on the connector instance was
   invoked exactly once before the process-stop call was issued, checked via
   a spy/mock on `teardown`, not by racing a clock.
2. **compat-nightly level (needs Lane D + a real Conduit binary, not week-1
   scope):** the harness acts as (or instruments) the go-plugin client side
   well enough to confirm the `GRPCController.Shutdown` RPC was the specific
   call that preceded process exit — e.g., a wrapping test harness that stands
   in front of the real subprocess and records which RPCs were sent/received
   before the connection closed, asserting `Shutdown` appears in that
   RPC-invocation record. A bare "did the process disappear within N seconds"
   check is insufficient and must not be substituted for this once this test
   is implemented.

This is scripted, not eyeballed once, and — critically — it is scripted in a
way that fails if `Shutdown` silently falls through to the force-kill path,
not just in a way that fails if the process hangs forever.

#### Phase 2 (fast-follow, v0.20)

- `read_batch` override + batch-size/delay config (§2.6).
- Schema extraction/encoding middleware.
- Full docs: per-connector-author tutorial, docstring-coverage-enforced
  reference, cookbook recipes.
- Golden record-shape fixture corpus shared with the Go/other-language suites.

#### Phase 3 (parity polish, CLI integration)

- `conduit connector new --lang python` scaffolds from a
  `conduit-connector-template-python` repo.
- `conduit connector test`/`conduit connector build` wire into the Python
  acceptance/integration loop the same way they do for Go.
- Performance parity pass against the Go SDK on a benchi-committed reference
  pipeline.
- `conduit connector generate` (AI-assisted scaffolding) targets Python once
  the tooling exists for Go.

## Alternatives considered

**Protobuf codegen tool — `grpcio-tools`/`buf generate` (recommended) vs.
`betterproto`.** `betterproto` produces more idiomatic Python (dataclasses,
`async` client stubs, no `_pb2.py` boilerplate) and was seriously considered for
that reason. Rejected because: (a) it lags official protobuf releases and has
had long stretches without releases historically, a maintenance-burden risk for
a protocol that must track `conduit-connector-protocol` closely; (b) the org
already has working precedent for `buf generate` with the official
`protocolbuffers/python`+`pyi` plugins in `conduit-processor-sdk-python`; (c)
the generated `_pb2.py`/`_pb2_grpc.py` stubs are treated as an internal
implementation detail behind `_grpc/` adapters (§3) — authors never see them,
so betterproto's ergonomic advantage doesn't reach the public API anyway.

**Transport — TCP (recommended) vs. Unix domain socket.** Matching go-plugin's
own non-Windows default (Unix socket) was considered. Rejected: the
client-side parser is transport-agnostic (§1.1.4), Unix sockets need
temp-directory and permission handling with no corresponding benefit here, and
TCP gets Windows support without a platform branch.

**Async-only vs. dual sync/async author API (recommended: dual).** Forcing
`async def` everywhere was considered. Rejected as author-hostile: a large
fraction of Python's data-ecosystem client libraries are sync-only, and
forcing authors to wrap them in `asyncio.to_thread` themselves just moves
boilerplate from the SDK into every connector.

**Config — pydantic v2 (recommended) vs. stdlib `dataclasses` + hand-written
validators.** Rejected for `Config` specifically because the
paramgen-by-introspection design (§2.2) requires rich, already-structured
field metadata at runtime; reimplementing that on top of bare dataclasses
would mean hand-rolling a worse version of what pydantic already provides.

**Error model — exceptions (recommended) vs. mirroring Go's `(n, err)`
contract.** Rejected: that shape exists in Go specifically because Go lacks
structured exceptions with attached data cleanly separable from control flow,
and it requires the SDK to defend an invariant (`destination.go:345-350`) that
a well-designed Python API can make structurally unrepresentable instead of
enforced-at-runtime (§2.5). Parity of _behavior_ is kept; parity of _shape_ is
not a goal in itself.

## Failure modes

Analyzed against CLAUDE.md's data-integrity invariants, scoped to what an SDK
(not the engine) is responsible for:

- **Handshake cookie mismatch / malformed stdout line** → process must exit
  non-zero with a clear stderr diagnostic before printing anything on stdout
  that isn't the handshake line itself. SDK design mitigation: redirect all
  logging to stderr by default and document this loudly for authors.
- **Crash mid-`Run` stream (source)** → in-flight, unacked records are lost from
  the SDK's perspective on restart unless the source's own `Open(position)`
  contract correctly resumes from the last acked position (invariant 2). The
  acceptance harness's resume-at-position tests (§3, pulled forward) are the
  guardrail here, not a Phase-1 gap as originally scoped.
- **Partial-batch write failure (destination)** → `BatchWriteError` (§2.5) must
  be raised with an entry for every failed index; the SDK adapter treats an
  incomplete mapping as itself an error (fail closed, not fail open) rather
  than guessing — see §2.5's precise statement of the B1 fix.
- **`GRPCController.Shutdown` not implemented correctly** → falls back to
  Conduit's 2-second force-kill (§1.1.5) — functional (no pipeline hang) but
  not graceful; in-flight writes at kill time are the same risk profile as any
  `SIGKILL` mid-batch scenario. See ▶ MUST-FIX 2 above for how this is tested,
  and ▶ MUST-FIX 3 immediately below for the failure mode this doesn't cover.
- **`PATH` not inherited (§1.1.6)** → connector fails to launch at all if
  packaged naively. Mitigated by packaging guidance in §3 and Phase 1's done
  criterion explicitly using a real Conduit launch.

### ▶ MUST-FIX 3: hung/deadlocked asyncio event loop mid-write (no Go analog)

**This failure mode has no equivalent in the Go SDK and must not be treated as
"the same shutdown problem, just in Python."** Go's runtime preemptively
schedules goroutines (asynchronous preemption since Go 1.14): even a goroutine
stuck in a tight CPU-bound loop or blocked on a syscall does not prevent the Go
runtime from delivering `SIGTERM` and running the process's signal handler,
because signal delivery and goroutine scheduling are handled by the runtime
independently of what any single goroutine is doing. `asyncio`'s signal
handling has no such independence: `loop.add_signal_handler` callbacks are
themselves scheduled *on the event loop* and only run when the loop gets a
chance to process its callback queue. If the event loop is genuinely hung —
blocked in a synchronous call that never yields (a misbehaving sync-dispatched
`write()` per §2.1 that blocks on a lock, a native extension call that never
returns, a true deadlock between two coroutines) — **`SIGTERM` is never
processed at all**, because there is no independent scheduler underneath the
single-threaded event loop to preempt the stuck call and run the handler.
Conduit's SIGTERM sender has no way to distinguish "the connector is draining
slowly" from "the connector's event loop is permanently wedged and will never
call `Shutdown` or exit on its own."

**Consequence for invariant 7 (graceful shutdown by default):** without a
mitigation, a wedged event loop means Conduit's only recourse is its external
force-kill fallback (§1.1.5) — the same 2-second timeout that exists for the
merely-unimplemented-`Shutdown` case, except here it is covering for a bug in
the SDK's or a connector's async code, not an omission. A SIGKILL at that point
is a genuine mid-write kill with no chance for `teardown()` or the ack pipeline
to run at all — worse than the ordinary "Shutdown not implemented" case,
because the connector had no opportunity to drain even the in-flight write
that was in progress when SIGTERM arrived.

**Mitigation: a bounded, SDK-internal force-kill deadline, independent of the
event loop.** The SDK's signal-handling path must not rely solely on
`loop.add_signal_handler` (which is exactly the mechanism that can't fire if
the loop is wedged). Instead: install the `SIGTERM` handler with Python's
low-level `signal.signal` (which invokes the handler via the interpreter's
signal-checking mechanism, not the asyncio loop's callback queue) to start a
**separate watchdog** — a `threading.Timer` or a dedicated OS thread — the
moment `SIGTERM` is received. That watchdog gives the event loop a bounded
window (a documented default, e.g. a few seconds, configurable) to complete
graceful shutdown (finish the in-flight `write()`, run `teardown()`, respond
to `GRPCController.Shutdown`); if the deadline elapses without the loop
confirming clean exit, the watchdog thread calls `os._exit()` unconditionally,
from outside the (possibly permanently wedged) event loop. This does not make
a wedged connector's in-flight write safe — a write that was truly stuck
mid-flight is lost either way, which is the same outcome invariant 1 already
tolerates for an unacked record (no early ack was ever sent) — but it bounds
how long Conduit waits on a connector that will never respond, and it ensures
the SDK's own process-level watchdog fires *before* relying purely on
Conduit's external 2-second fallback, giving the SDK a chance to log a
diagnostic distinguishing "wedged event loop" from "clean shutdown" before the
process dies either way.

**Test:** a dedicated case in the SIGTERM-mid-write chaos-style test (AC #4)
that deliberately wedges the event loop (e.g. a `write()` override that blocks
a background thread the loop is waiting on, never yielding back) and asserts
(a) the SDK's own watchdog fires and the process exits within its documented
bounded deadline, and (b) a diagnostic distinguishing this case from a clean
`Shutdown` is emitted to stderr before exit. This is additive to, not a
replacement for, the ordinary (non-wedged) SIGTERM-mid-write drain test.

## Upgrade/rollback & compatibility

- **Protocol version skew**: the SDK targets protocol v2 and negotiates via
  `PLUGIN_PROTOCOL_VERSIONS` (§1.1.2) — if a future protocol v3 lands, an older
  SDK still negotiates down to v2 as long as Conduit's client keeps v2 in its
  `VersionedPlugins` map. No SDK-side action needed for additive protocol
  changes; a breaking protocol change is already governed by CLAUDE.md's
  "never change `conduit-connector-protocol` without an explicit versioning
  discussion" rule, upstream of this SDK.
- **SDK API breaking changes**: this repo commits to semver from v1.0; breaking
  changes to the `Source`/`Destination`/`Config` public surface follow the
  standing announce → warn → remove policy (minimum two minor versions).
- **Rollback**: since the SDK ships as a versioned PyPI package pinned per
  connector project, rolling back is "pin an older SDK version" — no
  coordinated Conduit-side rollback needed.
- **Drift detection**: the `compat-nightly.yml` job (§3) regenerating stubs
  from BSR HEAD and running the acceptance suite against a current Conduit dev
  build is the primary mechanism preventing silent protocol drift.

## Observability

- Connector logs cross the process boundary as structured records on stderr
  (JSON lines). **Never write to stdout** except the single handshake line (§
  Failure modes).
- The SDK exposes a `conduit.testing` record inspector hook (fast-follow) so
  an author can pipe sample records through their connector locally without
  standing up a full pipeline.
- Errors raised by connector code surface through the gRPC status + detail
  string today (§2.5); once the protocol's plugin-originated `ConduitError`
  codes land, this SDK's `ConnectorError.code` field starts being populated.

## Risks & open questions

1. **The handshake is the single biggest execution risk**, not because it's
   Python-hostile (it isn't — §1 shows it's a plain stdout line plus a
   standard gRPC server) but because it's easy to get subtly wrong in ways that
   only surface as "Conduit hangs waiting for the plugin to start" with a poor
   error message. Mitigation: Phase 1's done-criterion is a real Conduit launch
   from day one, and the handshake implementation ships with its own focused
   unit tests asserting the exact line format against the go-plugin source's
   parser logic.
2. **gRPC/codegen choice locks in early.** `buf generate` + protoc stubs (§1.5)
   is the recommendation, but it's the one decision hardest to reverse later
   (the internal `_grpc/` adapter layer insulates the public API from this).
3. **Async runtime + subprocess lifecycle interaction.** `asyncio`'s
   interaction with process-exit signal handling needs care. **This risk is
   now split into two distinct sub-cases, per ▶ MUST-FIX 3 above**: (a) the
   ordinary case — an event loop that's mid-`await` when SIGTERM arrives must
   let in-flight writes drain before exit (covered by the ordinary
   SIGTERM-mid-write test), and (b) the **hung-loop** case — SIGTERM arrives
   while the loop is wedged and cannot process the signal handler at all,
   which has no Go analog and needs the bounded watchdog mitigation and its
   own dedicated test. Treating these as one risk understates (b), which is
   the one that can genuinely hang a shutdown indefinitely without the
   watchdog.
4. **Performance vs. Go is unknown and unclaimed.** No benchmark exists yet;
   per CLAUDE.md, no performance claim should be made about this SDK until a
   `benchi` run is committed to the repo.
5. **Second-SDK maintenance burden.** Every future connector-protocol change
   now has two SDKs to update, tested, and released in lockstep, on top of an
   already-solo-maintainer reality. The `compat-nightly.yml` mechanism is the
   concrete mitigation, but it doesn't remove the underlying cost.
6. **Windows subprocess launch specifics.** TCP transport removes the
   Unix-socket asymmetry, but Windows process launching (job objects, signal
   delivery — Windows has no SIGTERM) has its own quirks for a subprocess
   expected to shut down gracefully; untested until the CI matrix actually
   runs it.
7. **`google.protobuf.Struct` round-trip fidelity for `StructuredData`.**
   Covered precisely in §2.3/B3 above.

## Acceptance criteria

**For the SDK overall:**

- A connector written against this SDK, passing the versioned acceptance
  suite, is treated by Conduit as fully interchangeable with a Go connector —
  no capability gap silently assumed away.
- Every doc example compiles/runs in CI; a non-running example is a build
  failure, not a stale doc.
- No performance claim ships without a committed `benchi` result.
- The `compat-nightly.yml` job stays green continuously post-Phase-1; a red
  result is treated as a protocol-drift incident, not routine noise.

**For Phase 1 (v0.19 core) specifically — see ▶ MUST-FIX 2 for the shutdown
test's exact shape:**

1. The acceptance suite (specifier, config validation, resume-at-position,
   read/write round-trip, read timeout, partial-batch write correctness) is
   the headline AC.
2. A real, unmodified Conduit binary launches the example connector as a
   subprocess via the exact handshake in §1, with zero Conduit-side code
   changes.
3. Records flow end-to-end through a real pipeline, get correctly acked, and
   `conduit pipelines stop` tears the subprocess down via the graceful
   `GRPCController.Shutdown` path — verified per ▶ MUST-FIX 2, not by timing.
4. SIGTERM-mid-write drains cleanly (ordinary case) and the hung-loop case
   (▶ MUST-FIX 3) hits its bounded watchdog deadline with a distinguishing
   diagnostic.
5. `--lang python` scaffolds a working connector (Conduit-repo side).
6. `--lang python`'s `--json` output conforms to the existing envelope.
7. Hypothesis round-trip identity tests for the OpenCDC record codec,
   explicitly asserting the `Struct` int→float boundary (B3) for identity, not
   just "doesn't crash."
8. No performance claim ships.

## Consequences

- Conduit gains a second, officially maintained connector SDK, opening the
  Python data/AI ecosystem as a first-class connector-authoring audience
  without any change to Conduit itself — the standalone-subprocess
  architecture pays off exactly as designed.
- The org takes on a second SDK's maintenance surface, permanently, on top of
  an already-solo maintainer reality.
- The Python API's deliberate departures from Go's shape mean "port the Go
  connector" is not a mechanical translation for authors crossing languages —
  worth flagging so cross-language connector porting guides address it
  explicitly.
- This design doc's API surface becomes a public contract the moment Phase 1
  ships an example connector against it — future changes to it are governed
  by the same announce-warn-remove policy as any other public Conduit
  contract.

## Related

- `ConduitIO/conduit`'s `docs/design-documents/20260707-python-connector-sdk.md`
  — the original review-approved doc this file is landed from.
- `conduit-v019-plans/workstreams/python-connector-sdk.md` — the v0.19 build
  plan (task breakdown, lane parallelization, sign-off gate) this repo
  executes against; not itself part of this repo.
- [Conduit Plugin Architecture ADR](https://github.com/ConduitIO/conduit/blob/main/docs/architecture-decision-records/20220121-conduit-plugin-architecture.md)
  — the original ADR establishing gRPC-defined, SDK-decoupled plugin
  interfaces.
- [WASM component model ADR](https://github.com/ConduitIO/conduit/blob/main/docs/architecture-decision-records/20260704-wasm-component-model.md)
  — the _other_ extension mechanism (processors, not connectors).
- `ConduitIO/conduit-connector-protocol` — the wire contract this SDK
  implements (`proto/connector/v2/*.proto`, `pconnector/`).
- `ConduitIO/conduit-connector-sdk` — the Go reference implementation this
  design mirrors semantically.
- `ConduitIO/conduit-connector-template` — the Go scaffold this design's
  Phase 3 template repo is the Python analog of.
- `ConduitIO/conduit-processor-sdk-python` — existing, unrelated (WASM/
  processor) Python SDK prototype; codegen-tooling precedent only.
- `github.com/conduitio/conduit-commons` (`opencdc/`, `config/`) — the OpenCDC
  record and config-parameter types this SDK's Python equivalents are modeled
  on.

## Review outcome (2026-07-07) — SOUND (technical) / SOUND-WITH-CONCERNS (API)

Handshake independently byte-verified against go-plugin v1.8.0 — the crux is sound.
One BLOCKER before Phase-1 build, since folded into scope (§2.5 above):

- **B1 (data-loss blocker) — partial-batch ack.** Fixed per §2.5's precise
  fail-closed rule.
- **B3** — the silent `Struct` fidelity case is int→float, covered in §2.3.
- **A-gaps** (pre-Phase-1, non-blocking): pydantic mapping for
  `ParameterTypeDuration` and `ValidationTypeExclusion` (§2.2); the worked
  example's double-backoff is fixed in §2.7's current version.

Source ack/position flow verified correct (no early-ack risk). API idiom (async +
dual-mode, pydantic, `bytes|dict`, exceptions, ABCs, uv/pyproject) confirmed sound.

**Build-session addendum (2026-07-23):** three additional must-fixes folded in
above before Lane A/B implementation started — citation re-verification
(▶ MUST-FIX 1), a deterministic (not timing-based) shutdown test (▶ MUST-FIX 2),
and the hung-event-loop failure mode with its bounded-watchdog mitigation
(▶ MUST-FIX 3). None of these change the Decision section's chosen design;
they tighten how it's verified and add one failure mode Go's runtime model
doesn't share.
