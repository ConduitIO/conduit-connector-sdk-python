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

No release has been tagged yet; nothing here is installable from PyPI.
