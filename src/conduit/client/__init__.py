"""Python client library for defining and running Conduit pipelines in code.

The control client lives under the ``conduit.client`` namespace, one of two
namespaces shipped by the single ``conduit`` distribution (the other is
``conduit.connector``, the connector SDK). ``conduit`` is a PEP 420 namespace
package, so both namespaces install together without a top-level
``__init__.py`` collision.

Slice 1 (Case A: named connector plugins only -- see
``docs/design/20260724-embed-grpc-client-libraries.md``, "Build slices"), a
gRPC client over Conduit's existing control-plane API
(``proto/api/v1/api.proto``), per
``docs/design/20260724-embed-bindings-via-grpc.md`` (the ADR this design
implements). The client manages a local or remote engine over gRPC; only the
Go bindings embed Conduit in-process.

Quickstart::

    import conduit.client as conduit

    pipeline = (
        conduit.Pipeline("orders-sync")
        .source("generator", settings={"format.type": "structured"}, operations="create")
        .destination("log", level="info")
    )

    with conduit.client.local(state_dir="./conduit-state") as client:
        run = client.run(pipeline)
        run.wait_running()
        print(run.status())
        run.stop()

Public surface: :class:`Pipeline` (builder), :func:`local`/:func:`connect`
(client construction), :class:`~conduit.client.Client`, :class:`~conduit.client.Run`
/ :class:`~conduit.client.RunStatus`, and :class:`ConduitError`.

**Not yet public** (fast-follows, see the design doc's "Build slices"):
typed per-connector config (``Settings`` is a flat ``map<string, string>``
today -- every keyword argument to ``.source()``/``.destination()``/
``.process()`` is coerced to a string), ``inline_source``/
``inline_destination`` (Case B, needs the external-connector engine feature,
Slice 2), and an async client.
"""

from __future__ import annotations

from conduit._version import __version__
from conduit.client._local import LocalConduit, local
from conduit.client.client import Client, connect
from conduit.client.errors import ConduitError
from conduit.client.pipeline import Pipeline
from conduit.client.run import Run, RunStatus

__all__ = [
    "Client",
    "ConduitError",
    "LocalConduit",
    "Pipeline",
    "Run",
    "RunStatus",
    "__version__",
    "connect",
    "local",
]
