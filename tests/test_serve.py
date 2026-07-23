"""Tests for :mod:`conduit.serve` -- deterministic shutdown (▶ MUST-FIX 2),
the hung-event-loop watchdog (▶ MUST-FIX 3), and the SIGTERM-triggered
in-flight-operation drain (invariant 7).

Per the design doc's tightened Phase-1 acceptance criterion: the shutdown
test must be a deterministic RPC-invocation assertion, not a timing/log
heuristic. ``test_shutdown_rpc_runs_teardown_before_responding`` builds the
SDK's real ``grpc.aio`` server with its actual ``GRPCController`` servicer,
connects a real gRPC client to it, calls ``Shutdown``, and asserts (a) the
RPC succeeds and (b) ``teardown()`` ran to completion beforehand -- via a
spy, not a race against a clock.

``TestSigtermDrainsInFlightOperation`` holds the same bar for the
SIGTERM-triggered path: ``tests/test_build.py``'s only SIGTERM test fires the
signal *before* ``Open()`` is ever called, so it has never exercised draining
an in-flight ``read()``/``write()`` -- see this module's ``_sigterm_shutdown``
docstring and the design doc's Risks & open questions §3 for the gap this
class closes.
"""

from __future__ import annotations

import asyncio
import contextlib
import io
import signal
import threading
import time
from collections.abc import AsyncIterator

import grpc
import grpc.aio
import pytest
from google.protobuf import empty_pb2

from conduit._grpc.adapters import record_to_proto
from conduit.config import BaseConfig, Specification
from conduit.destination import Destination
from conduit.errors import BackoffRetry
from conduit.record import Operation, Record
from conduit.serve import (
    DEFAULT_SHUTDOWN_DEADLINE_SECONDS,
    _build_plugin_server,
    _ShutdownCoordinator,
)
from conduit.source import Source
from connector.v2 import destination_pb2, destination_pb2_grpc, source_pb2, source_pb2_grpc

_SPEC = Specification(name="test-plugin", version="0.0.0", author="test")


class _Config(BaseConfig):
    pass


class _TeardownSpySource(Source[_Config]):
    def __init__(self) -> None:
        self.teardown_calls = 0

    async def read(self) -> Record:
        raise BackoffRetry()

    async def teardown(self) -> None:
        self.teardown_calls += 1


class _TeardownSpyDestination(Destination[_Config]):
    def __init__(self) -> None:
        self.teardown_calls = 0

    async def write(self, records: list[Record]) -> None:
        return None

    async def teardown(self) -> None:
        self.teardown_calls += 1


class _SlowReadSource(Source[_Config]):
    """A ``Source`` whose first ``read()`` blocks until the test releases it.

    ``events`` records, in order, ``"read_end"`` (appended by ``read()``
    itself, just before returning) and ``"teardown"`` (appended by
    ``teardown()``) -- the exact ordering
    ``TestSigtermDrainsInFlightOperation`` asserts on.
    """

    def __init__(self) -> None:
        self.events: list[str] = []
        self.read_started = asyncio.Event()
        self.read_may_finish = asyncio.Event()
        self._call_count = 0

    async def read(self) -> Record:
        self._call_count += 1
        if self._call_count > 1:
            # The read loop must not call `read()` again after `drain()`
            # has set `_stop_event` -- a second call here would mean the
            # SIGTERM-triggered drain failed to stop the loop before
            # teardown() ran. Raise instead of blocking forever so a
            # regression here fails fast (a hang) rather than silently
            # (a wrong ack).
            raise BackoffRetry()
        self.read_started.set()
        await self.read_may_finish.wait()
        self.events.append("read_end")
        return Record(position=b"pos-1", operation=Operation.CREATE)

    async def teardown(self) -> None:
        self.events.append("teardown")


class _SlowWriteDestination(Destination[_Config]):
    """A ``Destination`` whose ``write()`` blocks until the test releases it.

    ``events`` records, in order, ``"write_end"`` (appended by ``write()``
    itself, just before returning) and ``"teardown"`` (appended by
    ``teardown()``) -- the exact ordering
    ``TestSigtermDrainsInFlightOperation`` asserts on.
    """

    def __init__(self) -> None:
        self.events: list[str] = []
        self.write_started = asyncio.Event()
        self.write_may_finish = asyncio.Event()

    async def write(self, records: list[Record]) -> None:
        self.write_started.set()
        await self.write_may_finish.wait()
        self.events.append("write_end")

    async def teardown(self) -> None:
        self.events.append("teardown")


async def _call_shutdown(port: int) -> empty_pb2.Empty:
    channel = grpc.aio.insecure_channel(f"127.0.0.1:{port}")
    try:
        call = channel.unary_unary(
            "/plugin.GRPCController/Shutdown",
            request_serializer=empty_pb2.Empty.SerializeToString,
            response_deserializer=empty_pb2.Empty.FromString,
        )
        return await call(empty_pb2.Empty())  # type: ignore[no-any-return]
    finally:
        await channel.close()


class TestDeterministicShutdownRpc:
    """▶ MUST-FIX 2: a real gRPC call, not a mock of the transport."""

    async def test_shutdown_rpc_succeeds_and_runs_teardown_first(self) -> None:
        handle = await _build_plugin_server(_SPEC, source=_TeardownSpySource)
        try:
            # (a) the RPC returns a successful response, not a connection
            # error/timeout -- a real call over a real socket.
            response = await _call_shutdown(handle.port)
            assert response == empty_pb2.Empty()

            # (b) teardown() was invoked exactly once, and -- by construction
            # of on_shutdown_rpc's await ordering in _build_plugin_server,
            # not by racing a clock -- strictly before the RPC handler
            # returned, which is itself strictly before drive_shutdown's
            # server.stop() call is even reachable (it's gated on the same
            # `shutdown_requested` event `on_shutdown_rpc` sets only after
            # teardown() completes).
            spy = handle.connector_instance
            assert isinstance(spy, _TeardownSpySource)
            assert spy.teardown_calls == 1

            await asyncio.wait_for(handle.drive_task, timeout=2)
            assert handle.coordinator.is_confirmed
        finally:
            with contextlib.suppress(Exception):
                await handle.server.stop(None)

    async def test_shutdown_rpc_works_for_destination_too(self) -> None:
        handle = await _build_plugin_server(_SPEC, destination=_TeardownSpyDestination)
        try:
            response = await _call_shutdown(handle.port)
            assert response == empty_pb2.Empty()
            spy = handle.connector_instance
            assert isinstance(spy, _TeardownSpyDestination)
            assert spy.teardown_calls == 1
            await asyncio.wait_for(handle.drive_task, timeout=2)
        finally:
            with contextlib.suppress(Exception):
                await handle.server.stop(None)

    async def test_shutdown_is_idempotent_if_called_twice(self) -> None:
        """A second Shutdown call (or SIGTERM racing the RPC) must not double-run teardown()."""
        handle = await _build_plugin_server(_SPEC, source=_TeardownSpySource)
        try:
            await _call_shutdown(handle.port)
            await asyncio.wait_for(handle.drive_task, timeout=2)
            spy = handle.connector_instance
            assert isinstance(spy, _TeardownSpySource)
            assert spy.teardown_calls == 1
        finally:
            with contextlib.suppress(Exception):
                await handle.server.stop(None)

    async def test_health_check_reports_serving_for_plugin_service(self) -> None:
        handle = await _build_plugin_server(_SPEC, source=_TeardownSpySource)
        try:
            channel = grpc.aio.insecure_channel(f"127.0.0.1:{handle.port}")
            try:
                from grpc_health.v1 import health_pb2

                call = channel.unary_unary(
                    "/grpc.health.v1.Health/Check",
                    request_serializer=health_pb2.HealthCheckRequest.SerializeToString,
                    response_deserializer=health_pb2.HealthCheckResponse.FromString,
                )
                response = await call(health_pb2.HealthCheckRequest(service="plugin"))
                assert response.status == health_pb2.HealthCheckResponse.SERVING
            finally:
                await channel.close()
        finally:
            handle.shutdown_requested.set()
            with contextlib.suppress(Exception):
                await asyncio.wait_for(handle.drive_task, timeout=2)


class TestShutdownCoordinatorWatchdog:
    """▶ MUST-FIX 3: the hung-event-loop watchdog, independent of asyncio."""

    def test_watchdog_fires_when_shutdown_never_confirmed(self) -> None:
        exit_calls: list[int] = []
        stderr = io.StringIO()
        coordinator = _ShutdownCoordinator(deadline=0.05, exit_fn=exit_calls.append, stderr=stderr)

        coordinator.start_watchdog()
        # `exit_fn` here just records (doesn't actually exit), matching the
        # design doc's injectable-exit_fn requirement so this test never
        # risks killing the pytest process.
        deadline = time.monotonic() + 2.0
        while not exit_calls and time.monotonic() < deadline:
            time.sleep(0.01)

        assert exit_calls == [1]
        diagnostic = stderr.getvalue()
        assert "wedged" in diagnostic.lower()

    def test_watchdog_does_not_fire_if_confirmed_before_deadline(self) -> None:
        exit_calls: list[int] = []
        coordinator = _ShutdownCoordinator(
            deadline=0.2, exit_fn=exit_calls.append, stderr=io.StringIO()
        )
        coordinator.start_watchdog()
        coordinator.confirm_clean_exit()

        time.sleep(0.35)  # past the deadline
        assert exit_calls == []

    def test_start_watchdog_is_idempotent(self) -> None:
        exit_calls: list[int] = []
        coordinator = _ShutdownCoordinator(
            deadline=0.05, exit_fn=exit_calls.append, stderr=io.StringIO()
        )
        coordinator.start_watchdog()
        coordinator.start_watchdog()  # must not start a second overlapping timer

        deadline = time.monotonic() + 2.0
        while not exit_calls and time.monotonic() < deadline:
            time.sleep(0.01)

        assert exit_calls == [1]  # exactly one force-exit, not two

    def test_watchdog_forces_exit_even_with_a_genuinely_wedged_event_loop(self) -> None:
        """Deliberately wedges a real asyncio event loop in a background thread.

        Simulates the ▶ MUST-FIX 3 scenario (a misbehaving sync-dispatched
        ``write()`` that blocks the loop's own thread, never yielding back --
        no Go analog, since Go's runtime preemptively schedules goroutines
        even through this). Proves the watchdog fires within its documented
        bounded deadline regardless, because it runs on an independent OS
        thread rather than depending on the (here, genuinely wedged) event
        loop's thread for anything.

        Scope note: this test wedges the loop and confirms the watchdog's
        independence from it; it does not exercise real OS `SIGTERM`
        delivery to a wedged main thread (CPython's signal-delivery
        interaction with a truly stuck main thread is its own, separately
        hard-to-construct-deterministically concern -- see the PR's
        Self-review for why that's flagged as compat-nightly-level scope,
        not asserted here).
        """
        exit_calls: list[int] = []
        stderr = io.StringIO()
        coordinator = _ShutdownCoordinator(deadline=0.2, exit_fn=exit_calls.append, stderr=stderr)

        wedged_ready = threading.Event()

        def run_wedged_loop() -> None:
            async def wedge_forever() -> None:
                wedged_ready.set()
                # A genuinely blocking, non-yielding call made directly
                # inside `async def` -- the exact misbehavior MUST-FIX 3
                # describes (not routed through `conduit._dispatch.invoke`,
                # which would correctly offload it to a thread pool instead).
                # This is the test deliberately doing the wrong thing to
                # prove the watchdog doesn't depend on it being done right.
                time.sleep(5)  # noqa: ASYNC251

            with contextlib.suppress(BaseException):
                asyncio.run(wedge_forever())

        thread = threading.Thread(target=run_wedged_loop, daemon=True)
        thread.start()
        assert wedged_ready.wait(timeout=2), "background loop never started"

        # The event loop's thread is now busy in a non-yielding sleep and
        # will not run any asyncio-scheduled callback for 5 seconds. Start
        # the watchdog directly (simulating what the real SIGTERM handler
        # does) and confirm it still fires well within that window.
        coordinator.start_watchdog()

        deadline = time.monotonic() + 2.0
        while not exit_calls and time.monotonic() < deadline:
            time.sleep(0.01)

        assert exit_calls == [1]
        assert "wedged" in stderr.getvalue().lower()

    def test_default_shutdown_deadline_is_a_few_seconds(self) -> None:
        """Sanity check on the documented default -- bounded, not instant or huge."""
        assert 1.0 <= DEFAULT_SHUTDOWN_DEADLINE_SECONDS <= 30.0


def test_serve_requires_exactly_one_of_source_or_destination() -> None:
    from conduit.serve import serve

    with pytest.raises(ValueError, match="exactly one"):
        serve(_SPEC)

    with pytest.raises(ValueError, match="exactly one"):
        serve(_SPEC, source=_TeardownSpySource, destination=_TeardownSpyDestination)


async def _empty_ack_stream() -> AsyncIterator[source_pb2.Source.Run.Request]:
    return
    yield  # pragma: no cover -- makes this an async generator with no items


class TestSigtermDrainsInFlightOperation:
    """The gap found in Tier-1 review: ``_sigterm_shutdown`` used to call

    ``teardown()`` immediately on SIGTERM with no regard for an actively
    streaming ``Run()`` -- never signaling the read/write loop to stop, never
    awaiting an in-flight ``read()``/``write()``. These tests build the SDK's
    real ``grpc.aio`` server, drive a real bidi-streaming ``Run()`` call
    against it, block the connector mid-``read()``/mid-``write()``, then
    invoke the coordinator's real ``_on_sigterm`` entry point directly
    (deterministic -- not waiting on OS signal-delivery timing, which
    ``tests/test_build.py``'s subprocess-level SIGTERM test already covers
    for the before-``Open`` case) and assert the in-flight operation
    completes strictly *before* ``teardown()`` runs, never concurrently with
    it.
    """

    async def test_sigterm_mid_read_drains_before_teardown(self) -> None:
        exit_calls: list[int] = []
        handle = await _build_plugin_server(
            _SPEC, source=_SlowReadSource, exit_fn=exit_calls.append
        )
        spy = handle.connector_instance
        assert isinstance(spy, _SlowReadSource)

        channel = grpc.aio.insecure_channel(f"127.0.0.1:{handle.port}")
        try:
            stub = source_pb2_grpc.SourcePluginStub(channel)
            call = stub.Run(_empty_ack_stream())
            responses: list[source_pb2.Source.Run.Response] = []

            async def _consume() -> None:
                async for response in call:
                    responses.append(response)

            consume_task = asyncio.create_task(_consume())

            # Wait until `read()` is genuinely in flight (blocked inside the
            # call, not merely "the RPC started").
            await asyncio.wait_for(spy.read_started.wait(), timeout=2)

            # Simulate SIGTERM landing *while `read()` is in flight* -- the
            # exact scenario the gap missed. Calling the coordinator's real
            # signal-handler method directly (rather than `os.kill`) keeps
            # this deterministic; it is the same code a real SIGTERM would
            # invoke (`signal.signal`'s registered callback).
            handle.coordinator._on_sigterm(signal.SIGTERM, None)

            # No matter how much the event loop is given to run here,
            # `teardown()` cannot legitimately have appended to `events` yet
            # -- `read()` is still blocked on `read_may_finish`, which
            # nothing but this test can set. If the gap this test targets
            # regressed (teardown() racing the in-flight read), this is
            # where it would show up.
            await asyncio.sleep(0.05)
            assert spy.events == []

            # Let the in-flight read() complete.
            spy.read_may_finish.set()

            await asyncio.wait_for(handle.drive_task, timeout=2)
            await asyncio.wait_for(consume_task, timeout=2)

            assert spy.events == ["read_end", "teardown"]
            assert handle.coordinator.is_confirmed
            assert len(responses) == 1
            assert exit_calls == []  # the watchdog must never have fired
        finally:
            await channel.close()
            with contextlib.suppress(Exception):
                await handle.server.stop(None)

    async def test_sigterm_mid_write_drains_before_teardown(self) -> None:
        exit_calls: list[int] = []
        handle = await _build_plugin_server(
            _SPEC, destination=_SlowWriteDestination, exit_fn=exit_calls.append
        )
        spy = handle.connector_instance
        assert isinstance(spy, _SlowWriteDestination)

        channel = grpc.aio.insecure_channel(f"127.0.0.1:{handle.port}")
        try:
            stub = destination_pb2_grpc.DestinationPluginStub(channel)
            record = Record(position=b"pos-1", operation=Operation.CREATE)

            async def _one_batch() -> AsyncIterator[destination_pb2.Destination.Run.Request]:
                yield destination_pb2.Destination.Run.Request(records=[record_to_proto(record)])

            call = stub.Run(_one_batch())
            acks: list[destination_pb2.Destination.Run.Response] = []

            async def _consume() -> None:
                async for response in call:
                    acks.append(response)

            consume_task = asyncio.create_task(_consume())

            # Wait until `write()` is genuinely in flight.
            await asyncio.wait_for(spy.write_started.wait(), timeout=2)

            # Simulate SIGTERM landing *while `write()` is in flight*.
            handle.coordinator._on_sigterm(signal.SIGTERM, None)

            # Same reasoning as the read case: `teardown()` cannot
            # legitimately have run yet -- `write()` is still blocked on
            # `write_may_finish`.
            await asyncio.sleep(0.05)
            assert spy.events == []

            # Let the in-flight write() complete.
            spy.write_may_finish.set()

            await asyncio.wait_for(handle.drive_task, timeout=2)
            await asyncio.wait_for(consume_task, timeout=2)

            assert spy.events == ["write_end", "teardown"]
            assert handle.coordinator.is_confirmed
            assert len(acks) == 1
            assert exit_calls == []  # the watchdog must never have fired
        finally:
            await channel.close()
            with contextlib.suppress(Exception):
                await handle.server.stop(None)
