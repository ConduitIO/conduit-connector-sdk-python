"""SDK-level batching middleware: ``sdk.batch.size`` / ``sdk.batch.delay``.

Mirrors the Go SDK's batching contract exactly where the two protocols'
shapes allow, and documents every deliberate divergence at the exact point
it happens. The Go-side references this module reproduces:

- ``internal/batcher.go`` (``conduit-connector-sdk``): the generic
  size/delay-triggered batch accumulator both ``Destination``/``Source``
  batching build on.
- ``destination_middleware.go``'s ``DestinationWithBatch``/
  ``destinationWithBatch``/``writeStrategyBatch``: destination-side wiring
  -- ``sdk.batch.size``/``sdk.batch.delay`` are injected config keys (not
  authored by the connector), and batching only activates when
  ``BatchSize > 1 || BatchDelay > 0`` (``destination.go:182``) -- a
  ``size`` of 0 *or* 1 with no delay is passthrough: no accumulation, no
  background task, one wire message in for one wire message out.
- ``source_middleware.go``'s ``SourceWithBatch``/``sourceWithBatch``: the
  same two keys on the source side. Go's source-side activation
  threshold is ``BatchSize > 0`` (``source_middleware.go:709``) -- a
  ``size`` of exactly 1 activates Go's accumulator there -- but this SDK
  deliberately keeps the single shared ``size > 1 || delay > 0``
  threshold (see :attr:`BatchConfig.enabled`) for both sides: a "batch"
  of one record is wire-indistinguishable from passthrough, so the
  divergence is unobservable outside the SDK itself. Same "no read-ahead
  task at all when batching is off" property this module's
  :func:`collect_batches` preserves (callers only invoke it when
  :attr:`BatchConfig.enabled` is true; the disabled case never even
  imports this module's event loop machinery into the request path -- see
  ``conduit.source``/``conduit.destination`` for the branch point).

**Deliberate divergence from Go, documented here rather than silently:**
the Go SDK's real advantage on the source side is an optional
``ReadN(ctx, n) ([]Record, error)`` a connector can implement for a
genuinely batched read (``collectWithReadN``); absent that override, Go
itself falls back to the single-record ``collectWithRead`` path this
module's :func:`collect_batches` is modeled on. This SDK's author-facing
``Source`` ABC has no ``read_batch``/``ReadN`` override point at all yet
(explicitly deferred past v0.19 core, see
``docs/design/20260707-python-connector-sdk.md`` §2.6) -- so every Python
connector using ``sdk.batch.*`` today gets Go's *fallback* behavior
(buffer individual ``read()`` calls into batches), never Go's optimized
path. This is a real, current capability gap versus Go, not an oversight:
adding ``read_batch`` is tracked as its own follow-up, at which point
:func:`collect_batches` would gain a second, ``ReadN``-shaped caller the
same way Go's ``collectWithReadN`` sits alongside ``collectWithRead``.
"""

from __future__ import annotations

import asyncio
import datetime
from collections.abc import AsyncIterator, Mapping
from dataclasses import dataclass
from typing import TypeVar

import conduit._grpc  # noqa: F401  -- sets up sys.path, see conduit._grpc.__init__
from conduit.config import parse_go_duration
from conduit.errors import (
    INVALID_BATCH_DELAY_CODE,
    INVALID_BATCH_SIZE_CODE,
    ConfigFieldError,
    ConfigValidationError,
)
from config.v1 import parameter_pb2 as _parameter_pb2

T = TypeVar("T")

BATCH_SIZE_KEY = "sdk.batch.size"
"""Wire config key -- matches Go's `json:"sdk.batch.size"` struct tag exactly."""

BATCH_DELAY_KEY = "sdk.batch.delay"
"""Wire config key -- matches Go's `json:"sdk.batch.delay"` struct tag exactly."""

_ZERO_DELAY = datetime.timedelta(0)


@dataclass(slots=True, frozen=True)
class BatchConfig:
    """Parsed ``sdk.batch.size``/``sdk.batch.delay`` for one connector instance.

    Attributes:
        size: maximum number of items per batch before it's flushed early.
            ``0`` means "no size limit" (Go's default, ``validate:"gt=-1"``
            i.e. any non-negative value is legal) -- a batch then only
            flushes on the delay timer, which can produce arbitrarily
            large batches for a fast source/high-throughput destination if
            ``delay`` is set without ``size`` (the Go source middleware
            warns about exactly this combination at ``Open`` time; Go's
            destination-side warning is commented out in the current SDK
            -- ``destination_middleware.go:167`` -- so the source side is
            the only live warning. This SDK does not currently have an
            established logging channel to reproduce it through -- see
            the module docstring's divergence note -- so it is documented
            here instead).
        delay: maximum time an incomplete batch waits before flushing.
            ``timedelta(0)`` means "no delay limit" -- a batch then only
            flushes once ``size`` items have accumulated, which can wait
            indefinitely for a slow/low-throughput source if ``size`` is
            set without ``delay`` (the same source-side Go warning,
            mirrored the same way).
    """

    size: int = 0
    delay: datetime.timedelta = _ZERO_DELAY

    @property
    def enabled(self) -> bool:
        """Whether batching is actually active for this config.

        The destination side's activation threshold, reproduced verbatim
        from Go rather than rounded to a more "intuitive" ``size >= 1``:
        ``destination.go:182``'s ``batchConfig.BatchSize > 1 ||
        batchConfig.BatchDelay > 0``. This SDK applies the *same* single
        threshold to the source side; Go's source threshold is the looser
        ``BatchSize > 0`` (``source_middleware.go:709``, activating the
        accumulator at ``size == 1``), but a "batch" of one record is
        wire-indistinguishable from passthrough there, so this SDK does
        not pay for the extra machinery either way -- the divergence is
        deliberate and unobservable outside the SDK (see the module
        docstring). A ``size`` of exactly 1 with no delay is passthrough,
        same as ``size == 0``.
        """
        return self.size > 1 or self.delay > _ZERO_DELAY


def sdk_batch_parameters() -> dict[str, _parameter_pb2.Parameter]:
    """The two ``sdk.batch.*`` parameters every connector gets for free.

    Mirrors Go's ``DestinationWithBatch``/``SourceWithBatch`` struct tags
    (``json:"sdk.batch.size" default:"0" validate:"gt=-1"`` /
    ``json:"sdk.batch.delay" default:"0"``), which Go's ``paramgen`` merges
    into *every* connector's ``Specify`` parameter map automatically via
    ``DefaultDestinationMiddleware``/``DefaultSourceMiddleware`` embedding
    -- a Go connector author never has to declare these fields themselves,
    and Conduit's own tooling (CLI, UI) expects them present in every
    connector's advertised parameters, not just ones that happen to
    declare matching fields on their own config struct.

    ``conduit.serve`` merges this into both ``source_params``/
    ``destination_params`` for every registered connector, matching that
    behavior -- see ``conduit.serve._build_plugin_server``.

    Returns:
        A mapping with exactly the two keys :data:`BATCH_SIZE_KEY`/
        :data:`BATCH_DELAY_KEY`.
    """
    return {
        BATCH_SIZE_KEY: _parameter_pb2.Parameter(
            default="0",
            description="Maximum size of batch before it gets written/read.",
            type=_parameter_pb2.Parameter.TYPE_INT,
            validations=[
                _parameter_pb2.Validation(
                    type=_parameter_pb2.Validation.TYPE_GREATER_THAN, value="-1"
                )
            ],
        ),
        BATCH_DELAY_KEY: _parameter_pb2.Parameter(
            default="0s",
            description="Maximum delay before an incomplete batch is written/read.",
            type=_parameter_pb2.Parameter.TYPE_DURATION,
        ),
    }


def extract_batch_config(config: Mapping[str, str]) -> tuple[BatchConfig, dict[str, str]]:
    """Pull ``sdk.batch.size``/``sdk.batch.delay`` out of a raw ``Configure`` config map.

    Called by ``conduit.source._SourceServicer.Configure``/
    ``conduit.destination._DestinationServicer.Configure`` *before* the
    remaining map is handed to the connector's own
    :class:`~conduit.config.BaseConfig` subclass for
    ``model_validate`` -- these two keys are SDK-owned, never fields the
    connector author declares themselves (matching Go: they live on
    ``DestinationWithBatch``/``SourceWithBatch``, not the connector's own
    config struct).

    Args:
        config: the raw ``map<string, string>`` from the ``Configure`` RPC.

    Returns:
        A ``(BatchConfig, remaining)`` pair: the parsed batch config, and a
        **copy** of ``config`` with the two batch keys removed, ready to
        pass to the connector's own config class.

    Raises:
        ConfigValidationError: if either value is present and malformed --
            ``sdk.batch.size`` must parse as a non-negative integer;
            ``sdk.batch.delay`` must parse as a non-negative
            Go-duration-syntax string (:func:`conduit.config.parse_go_duration`).
            Both fields are checked (not fail-fast on the first bad one),
            matching :class:`~conduit.errors.ConfigValidationError`'s
            "exhaustive, not one-at-a-time" design elsewhere in this SDK.
    """
    remaining = dict(config)
    size_raw = remaining.pop(BATCH_SIZE_KEY, None)
    delay_raw = remaining.pop(BATCH_DELAY_KEY, None)

    errors: list[ConfigFieldError] = []

    size = 0
    if size_raw:  # narrows out `None` and `""` -- see the two ValueError branches below
        try:
            size = int(size_raw)
            if size < 0:
                raise ValueError("must be >= 0")
        except ValueError:
            errors.append(
                ConfigFieldError(
                    field=BATCH_SIZE_KEY,
                    code=INVALID_BATCH_SIZE_CODE,
                    message=(
                        f"{BATCH_SIZE_KEY!r} must be a non-negative integer, got {size_raw!r}"
                    ),
                )
            )

    delay = _ZERO_DELAY
    if delay_raw:  # narrows out `None` and `""`, same as the size branch above
        try:
            delay = parse_go_duration(delay_raw)
            if delay < _ZERO_DELAY:
                raise ValueError("must be >= 0")
        except ValueError:
            errors.append(
                ConfigFieldError(
                    field=BATCH_DELAY_KEY,
                    code=INVALID_BATCH_DELAY_CODE,
                    message=(
                        f"{BATCH_DELAY_KEY!r} must be a non-negative Go-duration-syntax "
                        f"string (e.g. '5s', '1h30m'), got {delay_raw!r}"
                    ),
                )
            )

    if errors:
        raise ConfigValidationError(errors)

    return BatchConfig(size=size, delay=delay), remaining


_STOP = object()
"""Sentinel distinguishing "the source iterator ended" from a real item of type ``T``."""


async def collect_batches(
    items: AsyncIterator[T],
    *,
    config: BatchConfig,
) -> AsyncIterator[list[T]]:
    """Group items from ``items`` into ``config``-bounded batches.

    Shared by both batching call sites -- the destination write path
    (``conduit.destination._DestinationServicer``, over a per-record stream
    flattened from incoming ``Run`` request batches) and the source
    read-ahead path (``conduit.source._SourceServicer``, directly over the
    existing backoff-aware ``_read_loop()``) -- because both reduce to the
    same shape once the per-item stream exists: accumulate until ``size``
    items are buffered or ``delay`` has elapsed since the first buffered
    item, whichever comes first, exactly Go's ``select``-over-(channel-recv,
    timer) shape in ``sourceWithBatch.collectWithRead``/
    ``internal.Batcher.Enqueue``. ``asyncio.wait`` over two tasks --
    "the next item" and "the delay timer" -- is this module's equivalent of
    that ``select``.

    Only called when ``config.enabled`` is true; callers keep the exact
    passthrough branch (no task, no timer, one item in for one batch of one
    item out... actually one item in for one *record* out, unbatched) in
    their own code, matching the Go SDK's own ``if BatchSize > 1 ||
    BatchDelay > 0`` branch point (``destination.go:182``; Go's *source*
    middleware branch is the looser ``BatchSize > 0`` at
    ``source_middleware.go:709``, and this SDK deliberately shares one
    threshold for both sides -- see :attr:`BatchConfig.enabled`) rather
    than hiding it inside this function.

    **Flush-on-end, not flush-in-``finally``:** when ``items`` ends
    (``anext`` yields the internal stop sentinel), any non-empty buffered
    remainder is yielded once more before this generator returns --
    matching Go's ``Stop()``-triggered ``Flush()``
    (``destination.go:271``): batching must never silently drop
    buffered-but-unflushed items on a controlled stream end, per invariant
    3 (at-least-once is the floor). This flush happens in the main
    ``try`` body's own control flow, deliberately **not** in a ``finally``
    block: an async generator's ``finally`` also runs on ``GeneratorExit``
    (e.g. if the caller's own enclosing ``async for`` is abandoned early
    due to cancellation), and ``yield``ing there would raise
    ``RuntimeError: async generator ignored GeneratorExit``. This is a
    real, accepted narrow-window limitation, not an oversight: an
    externally-cancelled ``Run`` (e.g. the whole RPC torn down by the
    framework, not a normal drain) can leave a buffered-but-unflushed
    remainder un-emitted and therefore unacked -- it is *not* silently
    dropped: nothing was acked for it, so Conduit redelivers those
    records on the next connection (at-least-once is the floor, invariant
    3). This is the same class of tradeoff as an in-flight write whose
    ack never gets out on a hard stop (see
    ``conduit.destination._DestinationServicer.drain``'s own "unavoidable,
    narrow window" note for the same reasoning). The controlled
    shutdown path -- ``drain()`` setting the stop event, which is what
    normally ends ``items`` -- goes through the flush-then-return path
    above, not this one; that's the path invariant 7 (graceful shutdown by
    default) actually depends on.

    Args:
        items: the per-item async stream to batch. Ending it (raising
            ``StopAsyncIteration``) is this function's only normal
            termination signal -- it does not itself watch any stop/cancel
            event; callers arrange for ``items`` to end when they want
            batching to stop (see ``conduit.source``/``conduit.destination``
            for how each wires that through their own ``_stop_event``).
        config: the batch thresholds. Caller-checked ``.enabled`` (see
            above) -- this function does not re-check it.

    Yields:
        Non-empty lists of ``T``, each a complete batch (either
        size-triggered, delay-triggered, or the final end-of-stream
        remainder).
    """
    buffer: list[T] = []
    delay_task: asyncio.Task[None] | None = None
    next_task: asyncio.Task[T | object] | None = None

    try:
        while True:
            if next_task is None:
                next_task = asyncio.ensure_future(anext(items, _STOP))
            waiters: set[asyncio.Task[object]] = {next_task}
            if delay_task is not None:
                waiters.add(delay_task)

            await asyncio.wait(waiters, return_when=asyncio.FIRST_COMPLETED)

            if delay_task is not None and delay_task.done():
                delay_task = None
                if buffer:
                    yield buffer
                    buffer = []
                # `next_task` may or may not be done yet -- loop back and
                # `asyncio.wait` on whatever's still pending; a task that's
                # already done resolves immediately on the next `wait`.
                continue

            if next_task.done():
                item = next_task.result()
                next_task = None
                if item is _STOP:
                    if buffer:
                        yield buffer
                    return
                buffer.append(item)  # type: ignore[arg-type]  # narrowed: item is not _STOP
                if config.size > 0 and len(buffer) >= config.size:
                    if delay_task is not None:
                        delay_task.cancel()
                        delay_task = None
                    yield buffer
                    buffer = []
                elif config.delay > _ZERO_DELAY and delay_task is None:
                    # `asyncio.sleep` floors sub-millisecond delays at ~1ms
                    # vs Go's nanosecond timers -- accepted divergence,
                    # documented at its exact point per the module docstring.
                    delay_task = asyncio.ensure_future(asyncio.sleep(config.delay.total_seconds()))
    finally:
        # Cleanup only -- deliberately no `yield` here, see docstring.
        if next_task is not None and not next_task.done():
            next_task.cancel()
        if delay_task is not None and not delay_task.done():
            delay_task.cancel()


__all__ = [
    "BATCH_DELAY_KEY",
    "BATCH_SIZE_KEY",
    "BatchConfig",
    "collect_batches",
    "extract_batch_config",
    "sdk_batch_parameters",
]
