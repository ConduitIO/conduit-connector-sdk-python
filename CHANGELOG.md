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

No release has been tagged yet; nothing here is installable from PyPI.
