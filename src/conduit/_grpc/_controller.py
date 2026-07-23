"""Hand-written HashiCorp go-plugin ``GRPCController`` service.

**Not generated, and not part of ``conduit-connector-protocol``.** go-plugin
(https://github.com/hashicorp/go-plugin) defines this service internally,
for its own bookkeeping -- it is not one of the ``conduit-connector-protocol``
BSR-published ``.proto`` files this repo's ``buf generate`` step covers (see
``buf.gen.yaml``, ``tools/generate-stubs.sh``). There is nothing to
regenerate here; this module is ordinary, hand-written application code held
to the repo's normal lint/mypy-strict/docstring bar (unlike its sibling
``*_pb2*.py``/``*.pyi`` files elsewhere in ``_grpc/``, which are vendored
codegen output).

Per ``docs/design/20260707-python-connector-sdk.md`` §1.1.5: go-plugin's
client RPCs ``GRPCController.Shutdown`` on teardown
(``grpc_client.go:106-108``); if a plugin doesn't implement it, ``Close()``
errors and Conduit force-kills the process ~2s later instead
(``client.go:530-567``) -- the pipeline still tears down, just not
gracefully. Implementing this RPC is what makes shutdown go through the
clean path rather than the timeout fallback, per ``CLAUDE.md`` invariant 7
(graceful shutdown by default).

Both the ``Shutdown`` request and response are zero-field messages on the
wire -- go-plugin's own ``plugin.Empty`` has no fields, and neither does
``google.protobuf.Empty``, so the two serialize identically. Using
``google.protobuf.Empty`` here avoids hand-rolling a wire-identical message
type or adding a codegen step for a two-message, zero-field service.
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable

import grpc
from google.protobuf import empty_pb2

_ServicerContext = "grpc.aio.ServicerContext[empty_pb2.Empty, empty_pb2.Empty]"
ShutdownHandler = Callable[[empty_pb2.Empty, _ServicerContext], Awaitable[empty_pb2.Empty]]
"""Signature of the async callable invoked for the ``Shutdown`` RPC.

Takes the (empty) request and the servicer context, returns the (empty)
response -- the standard grpc.aio unary-unary handler shape, so callers can
pass any coroutine function matching it, not just a fixed no-args callback.
"""

SERVICE_NAME = "plugin.GRPCController"
"""go-plugin's own internal service name -- verbatim, not a Conduit constant."""

SHUTDOWN_METHOD = "Shutdown"
"""The single method go-plugin's internal service defines."""


def build_controller_handler(on_shutdown: ShutdownHandler) -> grpc.GenericRpcHandler:
    """Build the generic RPC handler implementing ``plugin.GRPCController``.

    Registered via ``server.add_generic_rpc_handlers`` (see
    :mod:`conduit.serve`) rather than a generated ``add_*Servicer_to_server``
    function, since there is no generated stub for this service (see module
    docstring).

    Args:
        on_shutdown: async callable invoked when Conduit calls
            ``GRPCController.Shutdown``. Callers are responsible for
            running the connector's ``teardown()`` to completion and
            arranging for the gRPC server/process to stop *after* this
            handler returns its response -- not before, or Conduit would
            see the RPC fail rather than complete cleanly.

    Returns:
        A ``grpc.GenericRpcHandler`` ready to pass to
        ``server.add_generic_rpc_handlers((handler,))``. Works with both
        ``grpc.server()`` and ``grpc.aio.server()`` -- the handler behavior
        callable is a coroutine function, which ``grpc.aio`` dispatches
        natively via the same generic-handler registration path as sync
        ``grpc``.
    """
    rpc_method_handlers = {
        SHUTDOWN_METHOD: grpc.unary_unary_rpc_method_handler(
            on_shutdown,
            request_deserializer=empty_pb2.Empty.FromString,
            response_serializer=empty_pb2.Empty.SerializeToString,
        ),
    }
    return grpc.method_handlers_generic_handler(SERVICE_NAME, rpc_method_handlers)
