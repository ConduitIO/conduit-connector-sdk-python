"""Python client library for defining and running Conduit pipelines in code.

Slice 1 (Case A: named connector plugins only -- see
``docs/design/20260724-embed-grpc-client-libraries.md``, "Build slices"), a
gRPC client over Conduit's existing control-plane API
(``proto/api/v1/api.proto``), per
``docs/design/20260724-embed-bindings-via-grpc.md`` (the ADR this design
implements).

Quickstart::

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

Public surface: :class:`Pipeline` (builder), :func:`local`/:func:`connect`
(client construction), :class:`~conduit.client.Client`, :class:`~conduit.run.Run`
/ :class:`~conduit.run.RunStatus`, and :class:`ConduitError`.

**Not yet public** (fast-follows, see the design doc's "Build slices"):
typed per-connector config (``Settings`` is a flat ``map<string, string>``
today -- every keyword argument to ``.source()``/``.destination()``/
``.process()`` is coerced to a string), ``inline_source``/
``inline_destination`` (Case B, needs the external-connector engine feature,
Slice 2), and an async client.
"""

from __future__ import annotations

from conduit._local import LocalConduit, local
from conduit.client import Client, connect
from conduit.errors import ConduitError
from conduit.pipeline import Pipeline
from conduit.run import Run, RunStatus

__version__ = "0.1.0.dev0"

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
