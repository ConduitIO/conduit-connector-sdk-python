"""Generated protobuf/grpc stubs for the control-plane API (``proto/api/v1/api.proto``).

**Generated, vendored code below this package -- never hand-edit.** Regenerate via
``./tools/generate-stubs.sh`` (requires ``buf`` on ``PATH``; see that script and
``buf.gen.yaml`` for the exact BSR modules/paths pulled).

Layout note / known tradeoff (same one ``conduit-connector-sdk-python`` documents,
reused here verbatim because the cause is identical): protoc's Python codegen
emits *absolute* imports rooted at each ``.proto`` file's own package path --
e.g. ``api/v1/api.proto`` imports ``config/v1/parameter.proto`` and becomes
``from config.v1 import parameter_pb2``, not an import nested under
``conduit._grpc``. Rewriting generated output to nest those imports would mean
hand-patching files explicitly marked "NO CHECKED-IN PROTOBUF GENCODE / DO NOT
EDIT", which is fragile across regenerations. Instead, this module prepends its
own directory to ``sys.path`` once, at import time, so the absolute imports the
generated code already contains resolve correctly as long as something has
imported ``conduit._grpc`` (directly or transitively) before importing e.g.
``api.v1.api_pb2``.

**Why this package vendors more than just ``api/v1``:** ``api.proto`` imports
``config/v1/parameter.proto`` and ``opencdc/v1/opencdc.proto`` (from
``conduit-commons``), plus ``google/api/annotations.proto``,
``google/api/field_behavior.proto`` and
``protoc-gen-openapiv2/options/annotations.proto`` (used as message/method
options, e.g. ``google.api.http``) from ``googleapis``/``grpc-gateway``. The
Python protobuf runtime resolves a message's full descriptor -- including its
declared options -- through the descriptor pool at import time, so every
transitively imported ``.proto`` file needs a corresponding generated Python
module physically present, even though this client only ever constructs and
reads a handful of ``api.v1`` message types directly. ``api_pb2.py`` is the
only module here with request/response types this package's public API
touches; the ``config``, ``opencdc``, ``google.api`` and
``protoc_gen_openapiv2`` trees exist solely so ``import api.v1.api_pb2``
succeeds.

**Tradeoff, stated plainly:** this makes top-level names like ``api``,
``config``, ``opencdc``, ``google``, and ``protoc_gen_openapiv2`` resolvable as
importable modules process-wide once this package has been imported -- ``api``,
``config``, and ``google`` in particular are generic enough that a real (if
low-probability) collision risk exists with an unrelated third-party package of
the same name installed in the same environment. Acceptable for a Slice-1
vendored-stub layer behind the internal ``_grpc/`` boundary (callers never
import ``conduit._grpc`` submodules directly -- only ``conduit``'s public
API does); revisit if this becomes a real collision in practice.
"""

from __future__ import annotations

import sys
from pathlib import Path

_STUB_ROOT = str(Path(__file__).parent)
if _STUB_ROOT not in sys.path:
    sys.path.insert(0, _STUB_ROOT)
