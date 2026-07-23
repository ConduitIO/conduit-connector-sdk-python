"""Source connector base class + the ``SourcePlugin`` gRPC wire adapter.

See ``docs/design/20260707-python-connector-sdk.md`` §2.1 (async/dual-mode),
§2.4 (forward-compatible ABC), §2.5 (``BackoffRetry``), and §1.3 (the
``SourcePlugin`` RPC surface). This module holds both halves: the
author-facing :class:`Source` ABC, and the internal
:class:`_SourceServicer` that adapts it to the generated
``SourcePluginServicer`` -- co-located because the wire adapter exists
entirely to serve this one ABC and the split would only add indirection.
"""

from __future__ import annotations

import abc
import asyncio
import contextlib
from collections.abc import AsyncIterator, Mapping
from typing import Any, Generic, TypeVar

import grpc
import grpc.aio
import pydantic

import conduit._grpc  # noqa: F401  -- sets up sys.path, see conduit._grpc.__init__
from conduit._dispatch import invoke
from conduit._grpc.adapters import config_map_from_proto, record_to_proto
from conduit._introspect import resolve_config_class
from conduit.config import BaseConfig
from conduit.errors import BackoffRetry, format_validation_error
from conduit.record import Record
from connector.v2 import source_pb2, source_pb2_grpc

ConfigT = TypeVar("ConfigT", bound=BaseConfig)

# Backoff constants mirroring the Go SDK's read loop exactly
# (`backoff.Backoff{Factor: 2, Min: 100*time.Millisecond, Max: 5*time.Second}`,
# `source.go:270-295`, re-verified ▶ MUST-FIX 1 in the design doc: a plain,
# serial `for` loop with no concurrent invocation of the read path). Reusing
# these constants is genuine behavioral parity with the Go SDK, not merely a
# similar-looking default chosen independently.
BACKOFF_FACTOR = 2.0
BACKOFF_MIN_SECONDS = 0.1
BACKOFF_MAX_SECONDS = 5.0


class Source(abc.ABC, Generic[ConfigT]):
    """Base class for Conduit source connectors.

    Subclass this, parameterized with your :class:`~conduit.config.BaseConfig`
    subclass (e.g. ``class MySource(Source[MyConfig]):``) and override
    :meth:`read` at minimum. Every other method has a working default (a
    no-op, or delegating to ``self.config``), per the design doc §2.4's
    forward-compatible-ABC pattern: Python's default-method mechanism gives
    the same "can't accidentally satisfy the interface without the default
    behavior" guarantee Go gets from ``mustEmbedUnimplementedSource()``, for
    free, with no seal method and no boilerplate for authors. Adding a new
    optional method to this class later is therefore source-compatible
    automatically.

    Methods are declared ``async def``; an override may instead be a plain
    ``def`` if your target system's client library is sync-only -- see
    :mod:`conduit._dispatch` for how the SDK detects and dispatches either
    kind (§2.1).
    """

    config: ConfigT
    """The validated config instance, set by :meth:`configure` (default
    implementation) before :meth:`open` is called."""

    async def configure(self, config: ConfigT) -> None:
        """Receive and store the validated config. Default: ``self.config = config``.

        Override only if you need additional validation/setup beyond
        pydantic's own model validation; call ``super().configure(config)``
        (or set ``self.config`` yourself) to retain the assignment other
        default methods rely on.

        Args:
            config: the parsed, pydantic-validated config instance.
        """
        self.config = config

    async def open(self, position: bytes | None) -> None:
        """Prepare to start producing records. Default: no-op.

        Args:
            position: the position of the last record successfully
                processed in a previous run, or ``None`` on a fresh start.
                Per invariant 2 (positions are monotonic and crash-safe),
                :meth:`read` must resume strictly after this position, not
                skip or replay past it non-idempotently.
        """
        return None

    @abc.abstractmethod
    async def read(self) -> Record:
        """Return the next available record.

        Raise :class:`~conduit.errors.BackoffRetry` if none is available
        right now -- the SDK's own read loop paces retries using the same
        backoff the Go SDK uses (:data:`BACKOFF_FACTOR`/
        :data:`BACKOFF_MIN_SECONDS`/:data:`BACKOFF_MAX_SECONDS`); do not
        also ``asyncio.sleep()``/block before raising, or you double the
        intended backoff.

        The one genuinely required override (§2.4) -- ``abc.ABC`` refuses
        to instantiate a subclass that doesn't provide it, at construction
        time, rather than failing later when first called.

        Raises:
            conduit.errors.BackoffRetry: no record is available yet.
        """
        raise NotImplementedError("Source subclasses must override read()")

    async def ack(self, position: bytes) -> None:
        """Called when Conduit confirms durable downstream handling of ``position``.

        Default: no-op. Override to persist a resumable cursor -- this is
        the only correct place to do so, since it is called only after
        Conduit's ``ack_positions`` confirms durable handling (see
        :class:`_SourceServicer._consume_acks`, which is the sole caller),
        never speculatively when a record is merely produced.

        **You don't need to override this** unless you also need to
        acknowledge against the source system itself -- e.g. committing a
        Kafka consumer offset, deleting a message from a queue, or marking
        a row processed in an upstream system. Conduit's own position
        tracking (via what ``read()``/``open()`` return and resume from)
        works correctly with the no-op default; most connectors never
        override ``ack()``.

        Args:
            position: the acknowledged record's position.
        """
        return None

    async def teardown(self) -> None:
        """Called once, after the read loop stops, before process exit. Default: no-op."""
        return None

    async def on_created(self, config: Mapping[str, str]) -> None:
        """Called once, the first time this connector instance is ever run. Default: no-op.

        Args:
            config: the raw string config map (not yet parsed into
                :attr:`config` -- this hook runs before ``configure()``'s
                normal validated-config flow, mirroring the Go SDK).
        """
        return None

    async def on_updated(
        self, config_before: Mapping[str, str], config_after: Mapping[str, str]
    ) -> None:
        """Called when the connector's configuration changed since the last run. Default: no-op.

        Args:
            config_before: the previous raw string config map.
            config_after: the new raw string config map.
        """
        return None

    async def on_deleted(self, config: Mapping[str, str]) -> None:
        """Called once, when this connector instance was deleted. Default: no-op.

        Args:
            config: the raw string config map the connector was last
                configured with.
        """
        return None


class _Backoff:
    """Serial retry-delay generator, mirroring ``jpillora/backoff`` semantics.

    Go's SDK constructs a ``backoff.Backoff{Factor: 2, Min: 100ms, Max: 5s}``
    and calls ``.Duration()`` on each ``ErrBackoffRetry``, which returns
    ``Min * Factor**attempt`` (capped at ``Max``) and increments an internal
    attempt counter; the caller resets the counter after a successful read.
    This class reproduces that exact sequence.
    """

    def __init__(
        self,
        factor: float = BACKOFF_FACTOR,
        min_seconds: float = BACKOFF_MIN_SECONDS,
        max_seconds: float = BACKOFF_MAX_SECONDS,
    ) -> None:
        """Initialize with the backoff curve's parameters.

        Args:
            factor: multiplier applied per attempt.
            min_seconds: delay for the first retry (attempt 0).
            max_seconds: delay cap, regardless of attempt count.
        """
        self._factor = factor
        self._min = min_seconds
        self._max = max_seconds
        self._attempt = 0

    def duration(self) -> float:
        """Return the next delay, in seconds, and advance the attempt counter."""
        delay = self._min * (self._factor**self._attempt)
        self._attempt += 1
        return min(delay, self._max)

    def reset(self) -> None:
        """Reset the attempt counter after a successful (non-retry) read."""
        self._attempt = 0


class _SourceServicer(source_pb2_grpc.SourcePluginServicer):
    """Adapts a :class:`Source` instance to the generated ``SourcePluginServicer``.

    Internal; constructed by :func:`conduit.serve.serve`, never by
    connector authors directly.
    """

    def __init__(self, source: Source[Any], config_cls: type[BaseConfig]) -> None:
        """Wrap a connector instance for gRPC dispatch.

        Args:
            source: the author's ``Source`` instance. Typed ``Source[Any]``
                (not ``Source[BaseConfig]``) deliberately: generics are
                invariant in Python's type system, so a concrete
                ``Source[MyConfig]`` instance is not otherwise assignable
                here; ``config_cls`` (below) is what actually drives config
                validation, independent of this parameter's type.
            config_cls: the concrete :class:`~conduit.config.BaseConfig`
                subclass to validate ``Configure``'s config map against.
        """
        self._source = source
        self._config_cls = config_cls
        self._stop_event = asyncio.Event()
        self._stopped_event = asyncio.Event()
        self._run_started = False
        self._last_position: bytes = b""

    async def Configure(
        self,
        request: source_pb2.Source.Configure.Request,
        context: grpc.aio.ServicerContext[Any, Any],
    ) -> source_pb2.Source.Configure.Response:
        """Validate and store the plugin's config. See proto doc comment for RPC semantics.

        A ``pydantic.ValidationError`` is caught explicitly and turned into
        an ``INVALID_ARGUMENT`` status with a per-field detail message
        (:func:`~conduit.errors.format_validation_error`) -- per
        ``CLAUDE.md``'s "errors are API" standard, an author should see
        exactly which field failed and why, not ``grpc.aio``'s generic
        "Unexpected <exception class>: ..." ``UNKNOWN``-status wrapping of
        an uncaught exception.
        """
        try:
            config = self._config_cls.model_validate(config_map_from_proto(request.config))
        except pydantic.ValidationError as exc:
            await context.abort(grpc.StatusCode.INVALID_ARGUMENT, format_validation_error(exc))
            raise  # pragma: no cover -- abort() never returns; unreachable, satisfies mypy
        await invoke(self._source.configure, config)
        return source_pb2.Source.Configure.Response()

    async def Open(
        self, request: source_pb2.Source.Open.Request, context: object
    ) -> source_pb2.Source.Open.Response:
        """Prepare the source to start producing records after ``request.position``."""
        position = request.position or None
        await invoke(self._source.open, position)
        return source_pb2.Source.Open.Response()

    async def Run(
        self,
        request_iterator: AsyncIterator[source_pb2.Source.Run.Request],
        context: object,
    ) -> AsyncIterator[source_pb2.Source.Run.Response]:
        """Bidirectional stream: emit records out, consume ``ack_positions`` in.

        Both directions run concurrently on this one call object -- an
        `async for` reading the read-loop's records (yielded directly,
        driving the response stream) and a background task consuming
        ``request_iterator`` for incoming acks -- per
        ``grpc.aio``'s native bidi-stream model (design doc §2.1/§1.3).
        """
        self._run_started = True
        ack_task = asyncio.create_task(self._consume_acks(request_iterator))
        try:
            async for record in self._read_loop():
                self._last_position = record.position
                yield source_pb2.Source.Run.Response(records=[record_to_proto(record)])
        finally:
            ack_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await ack_task
            self._stopped_event.set()

    async def _consume_acks(
        self, request_iterator: AsyncIterator[source_pb2.Source.Run.Request]
    ) -> None:
        async for request in request_iterator:
            for position in request.ack_positions:
                # Invariant 1: a source record's position is acknowledged to
                # the connector only here -- only after Conduit's Run
                # request stream sends it back via `ack_positions` -- never
                # speculatively when the record is merely produced by
                # `_read_loop` below. `_read_loop` never calls
                # `self._source.ack`; this is the only call site.
                await invoke(self._source.ack, bytes(position))

    async def _read_loop(self) -> AsyncIterator[Record]:
        """Serially call ``read()``, backing off on ``BackoffRetry``.

        A single, plain loop with no concurrent invocation of ``read()`` --
        matching ``source.go:270-295``'s structure exactly (re-verified
        ▶ MUST-FIX 1), which is what makes reusing its backoff constants
        genuine parity rather than a coincidentally similar default.
        """
        backoff = _Backoff()
        while not self._stop_event.is_set():
            try:
                record = await invoke(self._source.read)
            except BackoffRetry:
                delay = backoff.duration()
                # Wait on the stop event (not a plain sleep) so `Stop()`
                # can interrupt an in-progress backoff wait promptly instead
                # of blocking shutdown for up to BACKOFF_MAX_SECONDS.
                with contextlib.suppress(TimeoutError):
                    await asyncio.wait_for(self._stop_event.wait(), timeout=delay)
                continue
            backoff.reset()
            yield record

    async def Stop(
        self, request: source_pb2.Source.Stop.Request, context: object
    ) -> source_pb2.Source.Stop.Response:
        """Signal the read loop to stop; block until it has, then report the last position."""
        self._stop_event.set()
        if self._run_started:
            await self._stopped_event.wait()
        return source_pb2.Source.Stop.Response(last_position=self._last_position)

    async def Teardown(
        self, request: source_pb2.Source.Teardown.Request, context: object
    ) -> source_pb2.Source.Teardown.Response:
        """Run the connector's teardown hook to completion."""
        await invoke(self._source.teardown)
        return source_pb2.Source.Teardown.Response()

    async def LifecycleOnCreated(
        self, request: source_pb2.Source.Lifecycle.OnCreated.Request, context: object
    ) -> source_pb2.Source.Lifecycle.OnCreated.Response:
        """Dispatch the connector's first-run lifecycle hook."""
        await invoke(self._source.on_created, config_map_from_proto(request.config))
        return source_pb2.Source.Lifecycle.OnCreated.Response()

    async def LifecycleOnUpdated(
        self, request: source_pb2.Source.Lifecycle.OnUpdated.Request, context: object
    ) -> source_pb2.Source.Lifecycle.OnUpdated.Response:
        """Dispatch the connector's config-changed lifecycle hook."""
        await invoke(
            self._source.on_updated,
            config_map_from_proto(request.config_before),
            config_map_from_proto(request.config_after),
        )
        return source_pb2.Source.Lifecycle.OnUpdated.Response()

    async def LifecycleOnDeleted(
        self, request: source_pb2.Source.Lifecycle.OnDeleted.Request, context: object
    ) -> source_pb2.Source.Lifecycle.OnDeleted.Response:
        """Dispatch the connector's deleted lifecycle hook."""
        await invoke(self._source.on_deleted, config_map_from_proto(request.config))
        return source_pb2.Source.Lifecycle.OnDeleted.Response()


def _resolve_source_config_class(source_cls: type[Source[BaseConfig]]) -> type[BaseConfig]:
    """Recover the concrete ``BaseConfig`` subclass a ``Source[Config]`` used.

    Thin, ``Source``-specific wrapper over
    :func:`conduit._introspect.resolve_config_class`.
    """
    return resolve_config_class(source_cls, Source)
