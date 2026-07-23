"""Generated protobuf/grpc stubs for conduit-connector-protocol v2 (+ conduit-commons).

**Generated, vendored code below this package — never hand-edit.** Regenerate via:

    buf generate buf.build/conduitio/conduit-connector-protocol
    buf generate buf.build/conduitio/conduit-commons

(see ``buf.gen.yaml`` and ``CONTRIBUTING.md``; ``.github/workflows/compat-nightly.yml``
runs this on a schedule and fails the build if the committed stubs drift from
current BSR HEAD.)

Layout note / known tradeoff (flagged for Lane B, not hidden): protoc's Python
codegen emits *absolute* imports rooted at each ``.proto`` file's own package
path — e.g. ``connector/v2/source.proto`` becomes ``from connector.v2 import
source_pb2``, and ``opencdc/v1/opencdc.proto`` becomes ``from opencdc.v1
import opencdc_pb2`` — not imports relative to this ``conduit._grpc`` package.
Rewriting the generated files to nest those imports under ``conduit._grpc``
would mean hand-patching output explicitly marked "NO CHECKED-IN PROTOBUF
GENCODE / DO NOT EDIT", which is fragile across regenerations. Instead, this
module prepends its own directory to ``sys.path`` once, at import time, so the
absolute imports the generated code already contains resolve correctly as
long as something has imported ``conduit._grpc`` (directly or transitively)
before importing e.g. ``connector.v2.source_pb2``.

**Tradeoff, stated plainly:** this makes top-level names like ``connector``,
``config``, ``opencdc``, ``connutils``, ``metadata``, and ``schema`` resolvable
as importable modules process-wide once this package has been imported —
these names are generic enough that a real (if low-probability, given
connectors run as small standalone subprocesses) collision risk exists with
an unrelated third-party package of the same name installed in the same
environment. Acceptable for a Phase-1 vendored-stub layer behind the internal
``_grpc/`` adapter boundary (per docs/design/20260707-python-connector-sdk.md
§1.5/§Alternatives — authors never import ``conduit._grpc`` directly); revisit
if this becomes a real collision in practice (e.g. via import-time namespace
checks, or moving to a rewritten-import codegen step) rather than assumed
fine indefinitely.
"""

from __future__ import annotations

import sys
from pathlib import Path

_STUB_ROOT = str(Path(__file__).parent)
if _STUB_ROOT not in sys.path:
    sys.path.insert(0, _STUB_ROOT)
