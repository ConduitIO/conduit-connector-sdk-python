"""``Run``: a handle to a submitted, running (or stopped) pipeline.

Returned by :meth:`conduit.client.Client.run`. Per the design doc's failure
mode 5 ("Host<->engine control-channel loss mid-run"), losing the gRPC
connection is never conflated with the pipeline itself stopping -- every
method here re-issues a fresh RPC rather than caching state client-side, so
a transient reconnect is invisible to the caller and a real pipeline state
change is always reflected.
"""

from __future__ import annotations

import time
from dataclasses import dataclass
from typing import TYPE_CHECKING

import conduit._grpc  # noqa: F401  -- sets up sys.path, see conduit._grpc.__init__
from api.v1 import api_pb2
from conduit.errors import ConduitError

if TYPE_CHECKING:
    from conduit.client import Client

_STATUS_PREFIX = "STATUS_"
_STOPPED_REASON_PREFIX = "STOPPED_REASON_"


def _strip_enum_prefix(name: str, prefix: str) -> str:
    """Render a proto enum's ``Name()`` as a short, lowercase string.

    E.g. ``"STATUS_RUNNING"`` -> ``"running"``, ``"STOPPED_REASON_USER"`` ->
    ``"user"``. Kept as a plain string (not the raw enum) on
    :class:`RunStatus` so callers don't need to import ``api_pb2`` themselves
    for a simple status check.
    """
    return name[len(prefix) :].lower() if name.startswith(prefix) else name.lower()


@dataclass(frozen=True)
class RunStatus:
    """A pipeline's status, as of the ``GetPipeline`` call that produced it.

    ``status`` is one of ``"unspecified"``, ``"running"``, ``"stopped"``,
    ``"degraded"``, ``"recovering"`` (``Pipeline.State.Status``,
    ``proto/api/v1/api.proto``). ``error`` is non-empty only when
    ``status == "degraded"``. ``stopped_reason`` is one of ``"unspecified"``,
    ``"user"``, ``"system"`` -- only meaningful when ``status == "stopped"``.

    **No throughput/metrics field**: ``GetPipeline`` carries no such data
    (design doc, "Observability" -- there is no metrics RPC in the
    control-plane API). Scrape Conduit's own ``/metrics`` Prometheus endpoint
    directly for throughput; this is a deliberate, documented v1 gap, not an
    oversight.
    """

    status: str
    error: str
    stopped_reason: str

    @property
    def is_running(self) -> bool:
        """Shorthand for ``status == "running"``."""
        return self.status == "running"

    @property
    def is_degraded(self) -> bool:
        """Shorthand for ``status == "degraded"``."""
        return self.status == "degraded"


class Run:
    """Handle to a pipeline this client submitted via :meth:`Client.run`."""

    def __init__(self, client: Client, pipeline_id: str) -> None:
        """Bind a run handle to an already-created pipeline id.

        Not constructed directly -- returned by :meth:`Client.run`.
        """
        self._client = client
        self.pipeline_id = pipeline_id

    def status(self, *, timeout: float = 5.0) -> RunStatus:
        """Fetch the pipeline's current status via ``GetPipeline``.

        Args:
            timeout: seconds to wait for the RPC.

        Returns:
            The pipeline's current :class:`RunStatus`.

        Raises:
            ConduitError: on RPC failure, e.g. ``common.not_found`` if the
                pipeline was deleted out-of-band, or a transport error if the
                engine is unreachable -- distinguishable from "pipeline
                degraded" by :attr:`ConduitError.code` (``transport.*`` vs. a
                Conduit-domain reason), per the design doc's failure mode 1.
        """
        resp = self._client._call(
            self._client._pipelines.GetPipeline,
            api_pb2.GetPipelineRequest(id=self.pipeline_id),
            timeout=timeout,
        )
        state = resp.pipeline.state
        return RunStatus(
            status=_strip_enum_prefix(api_pb2.Pipeline.Status.Name(state.status), _STATUS_PREFIX),
            error=state.error,
            stopped_reason=_strip_enum_prefix(
                api_pb2.Pipeline.State.StoppedReason.Name(state.stopped_reason),
                _STOPPED_REASON_PREFIX,
            ),
        )

    def wait_running(self, *, timeout: float = 30.0, poll_interval: float = 0.2) -> RunStatus:
        """Poll ``status()`` until the pipeline reaches ``"running"``.

        Args:
            timeout: total seconds to wait before giving up.
            poll_interval: seconds between polls.

        Returns:
            The ``"running"`` :class:`RunStatus` once reached.

        Raises:
            ConduitError: with code ``"client.pipeline_degraded"`` if the
                pipeline transitions to ``"degraded"`` before running (the
                degraded status's own ``error`` field is included in the
                message -- never silently swallowed), or
                ``"client.wait_timeout"`` if ``timeout`` elapses first.
        """
        deadline = time.monotonic() + timeout
        last: RunStatus | None = None
        while time.monotonic() < deadline:
            last = self.status()
            if last.is_running:
                return last
            if last.is_degraded:
                raise ConduitError(
                    f"pipeline {self.pipeline_id!r} became degraded while waiting to "
                    f"run: {last.error}",
                    code="client.pipeline_degraded",
                )
            time.sleep(poll_interval)
        raise ConduitError(
            f"pipeline {self.pipeline_id!r} did not reach running within {timeout}s "
            f"(last observed status: {last.status if last else 'unknown'})",
            code="client.wait_timeout",
        )

    def stop(self, *, force: bool = False, timeout: float = 10.0) -> None:
        """Stop the pipeline via ``StopPipeline``.

        Args:
            force: if ``False`` (default), a graceful stop -- drains in-flight
                records per Invariant 7. ``True`` requests a forceful stop;
                only pass this when you have already decided graceful
                shutdown isn't an option, not as a default.
            timeout: seconds to wait for the RPC to return (the RPC itself
                returns once the stop is *initiated*, not once the pipeline
                has fully drained -- poll :meth:`status` for that).
        """
        self._client._call(
            self._client._pipelines.StopPipeline,
            api_pb2.StopPipelineRequest(id=self.pipeline_id, force=force),
            timeout=timeout,
        )
