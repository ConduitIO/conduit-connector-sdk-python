# Changelog

All notable changes to this project are documented in this file. Format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/); versions follow
[Semantic Versioning](https://semver.org/) starting at v1.0 (see the design
doc's Upgrade/rollback section for the pre-1.0 caveat).

## [Unreleased]

### Added

- Repo scaffold: `pyproject.toml` (uv/hatchling), lint/type config (ruff,
  mypy), CI workflow stubs, package layout.
- Design doc (`docs/design/20260707-python-connector-sdk.md`) landed with the
  Phase-1 review's must-fixes folded in.
- Generated gRPC/protobuf stubs for `conduit-connector-protocol` v2
  (`SourcePlugin`, `DestinationPlugin`, `SpecifierPlugin`).
- Handshake implementation (`_handshake.py`): magic-cookie check, protocol
  version negotiation, stdout handshake line.
- `Source`/`Destination` ABCs (`source.py`/`destination.py`) with dual sync/
  async method dispatch (`_dispatch.py`), backing onto the generated
  `SourcePlugin`/`DestinationPlugin` gRPC servicers.
- `serve()` entry point (`serve.py`): handshake validation, `grpc.aio` server
  bootstrap, `grpc.health.v1` health registration, and a hand-written
  `GRPCController.Shutdown` service (`_grpc/_controller.py`) for graceful
  go-plugin teardown.
- Hung-event-loop watchdog (`_ShutdownCoordinator` in `serve.py`): an
  independent `threading.Timer`-based force-exit deadline after `SIGTERM`,
  bounding a genuinely wedged event loop's shutdown time.
- `BaseConfig`/`Field`/`to_parameters()` (`config.py`): pydantic-v2-based
  config with introspection-driven `config.Parameter` mapping (no codegen).
- `Record`/`Change`/`Operation`/`Metadata` (`record.py`) and the proto
  (de)serialization adapters (`_grpc/adapters.py`), including the documented
  `google.protobuf.Struct` int→float fidelity boundary (B3).
- `BackoffRetry`/`BatchWriteError`/`ConnectorError` (`errors.py`), including
  `BatchWriteError`'s construction-time-validated, exhaustive per-index
  accounting (the B1 partial-batch-write fix).
- Acceptance-test harness (`testing/acceptance.py`, `testing/fixtures.py`):
  `AcceptanceTestDriver` Protocol, `ConfigurableAcceptanceTestDriver`,
  `AcceptanceTestSuite` (contract version `2026-07.v1`).
- Worked example connector (`examples/http-poll-source/`), exercised
  end-to-end by the acceptance suite in this repo's own test suite.
- Go-duration config support (`config.py`): `datetime.timedelta` fields now
  map to `config.Parameter.TYPE_DURATION`, serializing/parsing Go's
  `time.Duration` string syntax (`"5s"`, `"1h30m"`, `"500ms"`) via the new
  public `format_go_duration()`/`parse_go_duration()` functions. Closes the
  previously-`NotImplementedError` Duration A-gap.
- `BatchWriteError.partial(batch_size, written=N, cause=exc)` (`errors.py`):
  the recommended constructor for the common partial-batch-write case,
  carrying the real exception that caused the failure instead of a generic
  placeholder message.
- `Configure` RPC handlers now catch `pydantic.ValidationError` explicitly
  and abort with `INVALID_ARGUMENT` plus a per-field detail message
  (`errors.format_validation_error()`), rather than relying on `grpc.aio`'s
  generic `UNKNOWN`-status wrapping of an uncaught exception.
- Lifecycle hook rename: `Source`/`Destination`'s `lifecycle_on_created`/
  `lifecycle_on_updated`/`lifecycle_on_deleted` are now `on_created`/
  `on_updated`/`on_deleted` (the `lifecycle_` prefix was redundant).
- `conduit-connector-sdk build` (`_build.py`/`_cli.py`, new console-script
  entry point): packages a connector project into a single, directly
  executable artifact with an absolute-interpreter-path shebang (no `PATH`
  lookup at exec time, per design doc §1.1.6) and every third-party
  dependency vendored in, including compiled-extension dependencies
  (`grpcio`, `pydantic-core`) via an extract-on-first-run bootstrap.

### Fixed

- `serve()`'s SIGTERM-triggered graceful shutdown path (`_sigterm_shutdown`
  in `serve.py`) no longer silently hangs until the hung-loop watchdog's
  deadline if `teardown()` itself raises (e.g. a connector's `teardown()`
  running before `Open` was ever called) — `shutdown_requested` is now set
  unconditionally, with a clear diagnostic distinguishing "teardown raised"
  from "loop genuinely wedged." Found via a real end-to-end SIGTERM test
  while building `conduit-connector-sdk build`'s test coverage. The example
  connector's own `teardown()` is also now defensive against this case.

No release has been tagged yet; nothing here is installable from PyPI.
