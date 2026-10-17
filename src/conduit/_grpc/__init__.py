"""Generated protobuf/grpc stubs shared by ``conduit.connector`` and ``conduit.client``.

Two vendored surfaces live here, merged from the two pre-merge repos:

- the connector protocol v2 (``SourcePlugin``/``DestinationPlugin``/
  ``SpecifierPlugin`` + their ``opencdc``/``config`` dependencies), used by
  ``conduit.connector``; and
- the control-plane API (``api/v1/api.proto`` + its ``config``/``opencdc``/
  ``google/api``/``protoc_gen_openapiv2`` dependencies), used by
  ``conduit.client``.

``config/v1`` and ``opencdc/v1`` are shared by both surfaces (generated from
the same ``conduit-commons`` protos).

**Generated, vendored code below this package — never hand-edit.** Regenerate
via ``./tools/generate-stubs.sh`` (see that script for the exact BSR modules
and ``--path`` scoping; ``.github/workflows/compat-nightly.yml`` runs it on a
schedule and fails the build if the committed stubs drift from current BSR
HEAD). The hand-written adapters (``adapters.py``, ``_controller.py``) are the
only non-generated files here; they are the ``conduit.connector`` side's
internal boundary, not part of the generated surface.

Layout note / known tradeoff (flagged for Lane B, not hidden): protoc's Python
codegen emits *absolute* imports rooted at each ``.proto`` file's own package
path — e.g. ``connector/v2/source.proto`` becomes ``from connector.v2 import
source_pb2``, ``api/v1/api.proto`` becomes ``from api.v1 import api_pb2``, and
``opencdc/v1/opencdc.proto`` becomes ``from opencdc.v1 import opencdc_pb2`` —
not imports relative to this ``conduit._grpc`` package. Rewriting the
generated files to nest those imports under ``conduit._grpc`` would mean
hand-patching output explicitly marked "NO CHECKED-IN PROTOBUF GENCODE / DO
NOT EDIT", which is fragile across regenerations. Instead, this module
prepends its own directory to ``sys.path`` once, at import time, so the
absolute imports the generated code already contains resolve correctly as
long as something has imported ``conduit._grpc`` (directly or transitively)
before importing e.g. ``connector.v2.source_pb2`` or ``api.v1.api_pb2``.

**Tradeoff, stated plainly:** this makes top-level names like ``connector``,
``api``, ``config``, ``opencdc``, ``google``, and ``protoc_gen_openapiv2``
resolvable as importable modules process-wide once this package has been
imported — several of these are generic enough that a real (if
low-probability, given connectors run as small standalone subprocesses and the
client is a short-lived control process) collision risk exists with an
unrelated third-party package of the same name installed in the same
environment. Acceptable for a vendored-stub layer behind the internal
``_grpc/`` adapter boundary (authors never import ``conduit._grpc`` directly);
revisit if this becomes a real collision in practice (e.g. via import-time
namespace checks, or moving to a rewritten-import codegen step) rather than
assumed fine indefinitely.
"""

from __future__ import annotations

import sys
from pathlib import Path

_STUB_ROOT = str(Path(__file__).parent)
if _STUB_ROOT not in sys.path:
    sys.path.insert(0, _STUB_ROOT)
