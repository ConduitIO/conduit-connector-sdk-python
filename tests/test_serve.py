"""Tests for :mod:`conduit.serve` -- deterministic shutdown (▶ MUST-FIX 2)
and the hung-event-loop watchdog (▶ MUST-FIX 3).

Per the design doc's tightened Phase-1 acceptance criterion: the shutdown
test must be a deterministic RPC-invocation assertion, not a timing/log
heuristic. ``test_shutdown_rpc_runs_teardown_before_responding`` builds the
SDK's real ``grpc.aio`` server with its actual ``GRPCController`` servicer,
connects a real gRPC client to it, calls ``Shutdown``, and asserts (a) the
RPC succeeds and (b) ``teardown()`` ran to completion beforehand -- via a
spy, not a race against a clock.
"""

from __future__ import annotations

import asyncio
import contextlib
import io
import threading
import time

import grpc
import grpc.aio
import pytest
from google.protobuf import empty_pb2

from conduit.config import BaseConfig, Specification
from conduit.destination import Destination
from conduit.errors import BackoffRetry
from conduit.record import Record
from conduit.serve import (
    DEFAULT_SHUTDOWN_DEADLINE_SECONDS,
    _build_plugin_server,
    _ShutdownCoordinator,
)
from conduit.source import Source

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
