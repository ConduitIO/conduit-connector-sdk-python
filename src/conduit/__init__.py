"""Python SDK for building Conduit source and destination connectors.

**Week-1 scaffold status:** this package currently exposes only what Lane A
of the v0.19 build plan lands: the vendored gRPC/protobuf stubs
(:mod:`conduit._grpc`) and the go-plugin handshake implementation
(:mod:`conduit._handshake`). The public author-facing surface described in
``docs/design/20260707-python-connector-sdk.md`` §2 -- ``Source``,
``Destination``, ``Record``, ``Operation``, ``Change``, ``serve``,
``BaseConfig`` -- is Lane B/C scope and is **not implemented yet**. Importing
those names from this package will fail with ``ImportError`` until Lane B
lands; do not assume otherwise from this docstring or the README's aspirational
repo-layout description.

See ``docs/design/20260707-python-connector-sdk.md`` for the full design and
``CONTRIBUTING.md`` for the Tier-1 review bar this package is held to.
"""

from __future__ import annotations

__version__ = "0.1.0.dev0"

__all__ = ["__version__"]
