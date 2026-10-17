"""``ConduitError``: the one exception type this client ever raises for a failed call.

Per the design doc (``docs/design/20260724-embed-grpc-client-libraries.md``,
"``conduit.local()`` and ``conduit.connect(addr)``" section) and CLAUDE.md's
"errors are API" standard, a caller of this library should never see a raw
``grpc.RpcError`` or a bare stack trace from the wire. Every gRPC failure is
translated into a :class:`ConduitError` carrying the same structured fields
Conduit's server already attaches to the wire
(``pkg/foundation/cerrors/conduiterr/status.go``'s ``ToStatus``): a stable
dotted ``code`` (``google.rpc.ErrorInfo.reason``), the human ``message``, and
optional ``config_path``/``suggestion``/``docs_url`` metadata -- reused as-is,
not reinvented.
"""

from __future__ import annotations

import grpc
from google.rpc import error_details_pb2
from grpc_status import rpc_status


class ConduitError(Exception):
    """Uniform, machine-actionable error raised for any failed API call.

    Mirrors the Go-side ``conduiterr.ConduitError`` shape field-for-field
    where the wire carries it: ``code``, ``message``, ``config_path``,
    ``suggestion``, ``docs_url``. ``grpc_status`` is always populated (the
    underlying gRPC status code) even when no ``ErrorInfo`` detail is
    present -- e.g. for a raw transport failure (engine unreachable, timeout)
    that never reached Conduit's error-mapping layer at all.
    """

    def __init__(
        self,
        message: str,
        *,
        code: str = "internal.unknown",
        grpc_status_code: grpc.StatusCode = grpc.StatusCode.UNKNOWN,
        config_path: str = "",
        suggestion: str = "",
        docs_url: str = "",
        pipeline_id: str = "",
    ) -> None:
        """Initialize a structured, actionable error.

        Args:
            message: human-readable description, becomes ``str(self)``.
            code: stable dotted reason, e.g. ``"common.not_found"`` --
                Conduit's ``ErrorInfo.reason`` when present, else
                ``"internal.unknown"``.
            grpc_status_code: the underlying gRPC status category.
            config_path: JSON-pointer to the offending config field, if any.
            suggestion: human-readable fix hint, if any.
            docs_url: link to further documentation, if any.
            pipeline_id: server-assigned pipeline id already created before
                this error occurred, if any. Set by :meth:`conduit.client.Client.run`
                on a mid-sequence failure (pipeline created, then a connector/
                processor/DLQ/start call fails) -- ``run()`` deliberately does
                not roll back already-created resources (see its docstring),
                so this is the only way the caller learns the id needed to
                clean them up. Empty when the error occurred before
                ``CreatePipeline`` returned, or for errors unrelated to
                ``run()`` (e.g. :meth:`Run.status`).
        """
        super().__init__(message)
        self.code = code
        self.grpc_status_code = grpc_status_code
        self.config_path = config_path
        self.suggestion = suggestion
        self.docs_url = docs_url
        self.pipeline_id = pipeline_id

    def __str__(self) -> str:
        """Render message plus any structured fields present, one per line."""
        lines = [f"[{self.code}] {super().__str__()}"]
        if self.config_path:
            lines.append(f"  config path: {self.config_path}")
        if self.suggestion:
            lines.append(f"  suggestion: {self.suggestion}")
        if self.docs_url:
            lines.append(f"  docs: {self.docs_url}")
        if self.pipeline_id:
            lines.append(f"  pipeline id (already created): {self.pipeline_id}")
        return "\n".join(lines)

    @classmethod
    def from_rpc_error(cls, exc: grpc.RpcError) -> ConduitError:
        """Translate a caught ``grpc.RpcError`` into a :class:`ConduitError`.

        Attempts to unpack the ``google.rpc.ErrorInfo`` detail Conduit's
        server attaches (domain ``"conduit"``); falls back to a bare
        ``code``/``message`` synthesized from the gRPC status category alone
        when no such detail is present (e.g. the call never reached
        Conduit's handler -- connection refused, deadline exceeded).

        Args:
            exc: the exception caught from a unary gRPC call. Expected to
                also implement ``grpc.Call`` (true for every exception
                ``grpc``'s synchronous stubs raise), so ``rpc_status`` can
                read trailing metadata off it.

        Returns:
            A populated :class:`ConduitError`, never raising itself.
        """
        grpc_code = exc.code() if isinstance(exc, grpc.Call) else grpc.StatusCode.UNKNOWN
        message = exc.details() if isinstance(exc, grpc.Call) else str(exc)
        message = message or str(exc)

        status = None
        if isinstance(exc, grpc.Call):
            try:
                status = rpc_status.from_call(exc)
            except ValueError:
                # Trailing metadata present but not a well-formed
                # google.rpc.Status -- fall back below rather than propagate
                # a decode error from *this* translation layer.
                status = None

        if status is not None:
            for detail in status.details:
                if detail.Is(error_details_pb2.ErrorInfo.DESCRIPTOR):
                    info = error_details_pb2.ErrorInfo()
                    detail.Unpack(info)
                    if info.domain == "conduit":
                        md = info.metadata
                        return cls(
                            message,
                            code=info.reason or "internal.unknown",
                            grpc_status_code=grpc_code,
                            config_path=md.get("configPath", ""),
                            suggestion=md.get("suggestion", ""),
                            docs_url=md.get("docsUrl", ""),
                        )

        # No Conduit ErrorInfo detail: the call failed before reaching
        # Conduit's error-mapping layer (network failure, engine down,
        # deadline exceeded) or against an older engine that predates
        # structured errors. Still never a raw traceback -- just less detail.
        return cls(message, code=_code_from_status(grpc_code), grpc_status_code=grpc_code)


def _code_from_status(grpc_code: grpc.StatusCode) -> str:
    """Best-effort dotted code for a bare gRPC status with no ErrorInfo detail."""
    return {
        grpc.StatusCode.UNAVAILABLE: "transport.unavailable",
        grpc.StatusCode.DEADLINE_EXCEEDED: "transport.deadline_exceeded",
        grpc.StatusCode.CANCELLED: "transport.cancelled",
        grpc.StatusCode.NOT_FOUND: "common.not_found",
        grpc.StatusCode.INVALID_ARGUMENT: "common.invalid_argument",
        grpc.StatusCode.ALREADY_EXISTS: "common.already_exists",
        grpc.StatusCode.FAILED_PRECONDITION: "common.failed_precondition",
    }.get(grpc_code, "internal.unknown")
