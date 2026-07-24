"""The ``serve()`` entry point: handshake + gRPC server bootstrap.

Implements ``docs/design/20260707-python-connector-sdk.md`` §1 end to end:
validates the go-plugin handshake (:mod:`conduit._handshake`, Lane A,
reused here, not reimplemented), starts a ``grpc.aio`` server, registers the
``SourcePlugin``/``DestinationPlugin`` servicer (:mod:`conduit.source`/
:mod:`conduit.destination`) plus ``SpecifierPlugin``, registers
``grpc.health.v1`` health (``SERVING`` for service ``"plugin"``, go-plugin's
``GRPCServiceName``), and registers the hand-written ``GRPCController``
(:mod:`conduit._grpc._controller`) so ``conduit pipelines stop`` tears the
subprocess down via go-plugin's graceful ``Shutdown`` RPC rather than its
2-second force-kill fallback (§1.1.5).

**▶ MUST-FIX 3 (hung-event-loop watchdog):** this module's
:class:`_ShutdownCoordinator` is the SDK-internal mitigation for a
genuinely wedged event loop -- see its docstring and the design doc's
"hung/deadlocked asyncio event loop mid-write" failure mode for the full
rationale. It is independent of asyncio by construction: the ``SIGTERM``
handler is installed with low-level ``signal.signal`` (not
``loop.add_signal_handler``), and the watchdog itself is a
``threading.Timer`` running on its own OS thread.
"""

from __future__ import annotations

import asyncio
import contextlib
import os
import signal
import sys
import threading
from collections.abc import Awaitable, Callable, Mapping
from dataclasses import dataclass
from typing import Any, NoReturn, TextIO

import grpc
import grpc.aio
from google.protobuf import empty_pb2
from grpc_health.v1 import health, health_pb2, health_pb2_grpc

import conduit._grpc  # noqa: F401  -- sets up sys.path, see conduit._grpc.__init__
from conduit._dispatch import invoke
from conduit._grpc._controller import build_controller_handler
from conduit._handshake import (
    HandshakeLine,
    check_magic_cookie,
    emit_handshake_line,
    negotiate_protocol_version,
)
from conduit.config import Specification, to_parameters
from conduit.destination import (
    Destination,
    _DestinationServicer,
    _resolve_destination_config_class,
)
from conduit.source import Source, _resolve_source_config_class, _SourceServicer
from connector.v2 import (
    destination_pb2_grpc,
    source_pb2_grpc,
    specifier_pb2,
    specifier_pb2_grpc,
)

DEFAULT_SHUTDOWN_DEADLINE_SECONDS = 5.0
"""Default bounded window (▶ MUST-FIX 3) for graceful shutdown to complete
after SIGTERM before the watchdog force-exits. Configurable per :func:`serve`
call; kept short enough that a genuinely wedged connector doesn't hang a
pipeline stop indefinitely, long enough not to truncate an ordinary
in-flight write/teardown under normal load."""

_HEALTH_SERVICE_NAME = "plugin"
"""go-plugin's own ``GRPCServiceName`` constant (`grpc_server.go:24-81`) --
the service name Conduit's health check probes, not a Conduit-side name."""


class _ShutdownCoordinator:
    """Independent-of-asyncio watchdog for the hung-event-loop failure mode.

    Per ▶ MUST-FIX 3 in the design doc: ``asyncio``'s signal handling has no
    equivalent to Go's preemptive goroutine scheduling -- if the event loop
    is genuinely wedged (blocked in a synchronous call that never yields),
    ``loop.add_signal_handler`` callbacks never run, because they're
    themselves scheduled on the same wedged loop's callback queue. This
    class never relies on that mechanism: ``SIGTERM`` is caught with
    low-level ``signal.signal`` (delivered via the interpreter's own
    signal-checking, independent of asyncio), and the watchdog itself is a
    ``threading.Timer`` on a separate OS thread -- it fires on schedule
    regardless of what the event loop's thread is doing.

    This does not make an in-flight write that's truly stuck mid-flight
    safe -- that record is lost either way, the same outcome invariant 1
    already tolerates for any never-acked record. What it bounds is how
    long Conduit (or an operator) waits on a connector that will never
    respond on its own, and it distinguishes, on stderr, "the loop was
    wedged" from "shutdown completed cleanly" before the process goes away
    either way.
    """

    def __init__(
        self,
        *,
        deadline: float,
        exit_fn: Callable[[int], NoReturn],
        stderr: TextIO,
        graceful_trigger: Callable[[], None] | None = None,
    ) -> None:
        """Initialize the coordinator.

        Args:
            deadline: seconds to wait, after the watchdog starts, before
                forcing an exit if clean shutdown hasn't been confirmed.
            exit_fn: called with exit code ``1`` if the deadline elapses
                without confirmation. Injectable so tests can observe the
                watchdog firing without killing the test process; defaults
                to ``os._exit`` in production (see :func:`serve`).
            stderr: stream the force-exit diagnostic is written to.
            graceful_trigger: optional callable invoked (in addition to
                starting the watchdog) when ``SIGTERM`` arrives, to kick
                off the SDK's own graceful-shutdown coroutine on the event
                loop. Must be safe to call from a signal handler (i.e.
                thread-safe scheduling only, e.g.
                ``asyncio.run_coroutine_threadsafe`` -- see
                :func:`_build_plugin_server`). If the loop is wedged, this
                callable's scheduled work simply never runs -- the
                watchdog, not this trigger, is what bounds that case.
        """
        self._deadline = deadline
        self._exit_fn = exit_fn
        self._stderr = stderr
        self._graceful_trigger = graceful_trigger
        self._confirmed = threading.Event()
        self._timer: threading.Timer | None = None
        self._timer_lock = threading.Lock()

    def install_sigterm_handler(self) -> None:
        """Install the low-level ``SIGTERM`` handler (main thread only).

        Uses ``signal.signal``, not ``loop.add_signal_handler`` -- see class
        docstring for why that distinction is the entire point of this
        class.
        """
        signal.signal(signal.SIGTERM, self._on_sigterm)

    def _on_sigterm(self, signum: int, frame: object) -> None:
        """Handle ``SIGTERM``: start the watchdog, best-effort-trigger graceful shutdown.

        Runs via the interpreter's own signal-checking mechanism, not
        scheduled on the event loop -- this is what lets it fire even if
        the loop is wedged (▶ MUST-FIX 3).
        """
        self.start_watchdog()
        if self._graceful_trigger is not None:
            with contextlib.suppress(RuntimeError):
                self._graceful_trigger()

    def start_watchdog(self) -> None:
        """Start the bounded force-exit timer, if not already running.

        Idempotent: a second call (a second ``SIGTERM``, or a direct test
        invocation) does not start an overlapping second timer.
        """
        with self._timer_lock:
            if self._timer is not None:
                return
            timer = threading.Timer(self._deadline, self._force_exit)
            timer.daemon = True
            self._timer = timer
            timer.start()

    def _force_exit(self) -> None:
        """Force-exit unconditionally, unless clean shutdown was confirmed first.

        Runs on the ``threading.Timer``'s own thread -- independent of
        whatever the event loop's thread is doing, wedged or not.
        """
        if self._confirmed.is_set():
            return
        print(
            f"conduit-sdk: graceful shutdown did not complete within "
            f"{self._deadline}s of SIGTERM -- forcing exit now. This means "
            "the event loop was wedged (blocked in a synchronous call that "
            "never yielded back), not a normal, if slow, drain -- a clean "
            "GRPCController.Shutdown never reaches this watchdog. See "
            "docs/design/20260707-python-connector-sdk.md, "
            "▶ MUST-FIX 3, for the failure mode this guards against.",
            file=self._stderr,
            flush=True,
        )
        self._exit_fn(1)

    def confirm_clean_exit(self) -> None:
        """Record that graceful shutdown completed; cancel the pending watchdog.

        Called once ``teardown()`` has run to completion and the server is
        stopping -- on the ordinary (non-wedged) path, this always wins the
        race against :meth:`_force_exit`, since it runs on the event loop's
        own thread as part of the same graceful-shutdown sequence that
        would otherwise have to reach this point anyway.
        """
        self._confirmed.set()
        with self._timer_lock:
            if self._timer is not None:
                self._timer.cancel()

    @property
    def is_confirmed(self) -> bool:
        """Whether :meth:`confirm_clean_exit` has been called."""
        return self._confirmed.is_set()


class _SpecifierServicer(specifier_pb2_grpc.SpecifierPluginServicer):
    """Adapts a :class:`~conduit.config.Specification` to ``SpecifierPluginServicer``."""

    def __init__(
        self,
        specification: Specification,
        source_params: Mapping[str, Any],
        destination_params: Mapping[str, Any],
    ) -> None:
        """Initialize with the static specification and pre-computed parameter maps.

        Args:
            specification: the author-supplied plugin metadata.
            source_params: ``config.Parameter`` map for the registered
                ``Source``'s config, empty if a ``Destination`` was
                registered instead.
            destination_params: same, for a registered ``Destination``.
        """
        self._specification = specification
        self._source_params = source_params
        self._destination_params = destination_params

    async def Specify(
        self, request: specifier_pb2.Specifier.Specify.Request, context: object
    ) -> specifier_pb2.Specifier.Specify.Response:
        """Return the plugin's static specification and parameter maps."""
        spec = specifier_pb2.Specification(
            name=self._specification.name,
            summary=self._specification.summary,
            description=self._specification.description,
            version=self._specification.version,
            author=self._specification.author,
            source_params=dict(self._source_params),
            destination_params=dict(self._destination_params),
        )
        return specifier_pb2.Specifier.Specify.Response(specification=spec)


@dataclass(slots=True)
class _ServerHandle:
    """Everything :func:`_serve_async` (or a test) needs to drive the server.

    Returned by :func:`_build_plugin_server`, which does all the wiring but
    deliberately does not itself block waiting for shutdown -- separating
    "build a fully working plugin server" from "run it to completion" is
    what lets the deterministic shutdown test (▶ MUST-FIX 2) connect a real
    gRPC client to a real, running server and drive ``GRPCController.
    Shutdown`` directly, without going through ``serve()``'s
    handshake/stdout/``asyncio.run`` machinery, none of which that test
    needs or should depend on.
    """

    server: grpc.aio.Server
    port: int
    coordinator: _ShutdownCoordinator
    connector_instance: Source[Any] | Destination[Any]
    shutdown_requested: asyncio.Event
    drive_task: asyncio.Task[None]
    drain: Callable[[], Awaitable[None]]
    """Bound ``_SourceServicer.drain``/``_DestinationServicer.drain`` for
    whichever of the two was registered -- stops the active read/write loop
    from accepting new work and awaits any operation already in flight. Used
    by ``_sigterm_shutdown`` (invariant 7) to drain the running connector
    before ``teardown()``; also usable directly by tests."""


async def _build_plugin_server(
    specification: Specification,
    *,
    source: type[Source[Any]] | None = None,
    destination: type[Destination[Any]] | None = None,
    shutdown_deadline: float = DEFAULT_SHUTDOWN_DEADLINE_SECONDS,
    exit_fn: Callable[[int], NoReturn] = os._exit,
    stderr: TextIO | None = None,
) -> _ServerHandle:
    """Build and start a fully wired, listening plugin gRPC server.

    Registers the connector servicer, the specifier servicer, gRPC health,
    and the ``GRPCController`` -- everything except the go-plugin handshake
    line and the top-level "wait for shutdown, then exit" drive loop (see
    :func:`_serve_async`), so this can be exercised directly by tests (and
    by :func:`serve`) against a real, running server on a real socket.

    Args:
        specification: static plugin metadata for the ``Specify`` RPC.
        source: a :class:`~conduit.source.Source` subclass to instantiate
            and serve. Exactly one of ``source``/``destination`` must be
            given.
        destination: a :class:`~conduit.destination.Destination` subclass
            to instantiate and serve.
        shutdown_deadline: seconds the hung-loop watchdog waits after
            ``SIGTERM`` before forcing an exit (▶ MUST-FIX 3).
        exit_fn: injectable process-exit function; see
            :class:`_ShutdownCoordinator`.
        stderr: stream for the watchdog's force-exit diagnostic; defaults
            to ``sys.stderr``.

    Returns:
        A handle exposing the running server, its port, the shutdown
        coordinator, the constructed connector instance, and the
        background task driving shutdown once requested.

    Raises:
        ValueError: if neither or both of ``source``/``destination`` are given.
    """
    if (source is None) == (destination is None):
        raise ValueError(
            "_build_plugin_server() requires exactly one of `source=` or "
            "`destination=`, not both/neither"
        )
    stderr = stderr if stderr is not None else sys.stderr

    server = grpc.aio.server()
    instance: Source[Any] | Destination[Any]
    source_params: Mapping[str, Any] = {}
    destination_params: Mapping[str, Any] = {}
    drain: Callable[[], Awaitable[None]]

    if source is not None:
        config_cls = _resolve_source_config_class(source)
        instance = source()
        source_servicer = _SourceServicer(instance, config_cls)
        # The generated `*_pb2_grpc.py` files carry no type annotations (no
        # companion `.pyi` for the service-registration helpers, only for
        # the message types) -- calling into them from this strict-mode
        # module is an intentional, vendored-codegen boundary, not a typing
        # gap in our own code (see pyproject.toml's mypy overrides comment).
        source_pb2_grpc.add_SourcePluginServicer_to_server(  # type: ignore[no-untyped-call]
            source_servicer, server
        )
        source_params = to_parameters(config_cls)
        drain = source_servicer.drain
    else:
        assert destination is not None  # narrowed by the xor check above
        config_cls = _resolve_destination_config_class(destination)
        instance = destination()
        destination_servicer = _DestinationServicer(instance, config_cls)
        destination_pb2_grpc.add_DestinationPluginServicer_to_server(  # type: ignore[no-untyped-call]
            destination_servicer, server
        )
        destination_params = to_parameters(config_cls)
        drain = destination_servicer.drain

    specifier_pb2_grpc.add_SpecifierPluginServicer_to_server(  # type: ignore[no-untyped-call]
        _SpecifierServicer(specification, source_params, destination_params), server
    )

    # `grpc_health-stubs`' bundled type stub for `grpc_health.v1.health` only
    # covers the sync `HealthServicer` and doesn't declare the `.aio`
    # submodule this runtime attribute access resolves at runtime (verified:
    # `grpc_health.v1.health.aio.HealthServicer` is `grpc_health.v1._async.
    # HealthServicer`) -- a third-party stub gap, not a bug in this module.
    health_servicer = health.aio.HealthServicer()  # type: ignore[attr-defined]
    health_pb2_grpc.add_HealthServicer_to_server(health_servicer, server)
    await health_servicer.set(_HEALTH_SERVICE_NAME, health_pb2.HealthCheckResponse.SERVING)

    loop = asyncio.get_running_loop()
    shutdown_requested = asyncio.Event()
    teardown_lock = asyncio.Lock()
    teardown_done = False

    async def run_teardown_once() -> None:
        nonlocal teardown_done
        async with teardown_lock:
            if not teardown_done:
                await invoke(instance.teardown)
                teardown_done = True

    async def on_shutdown_rpc(request: empty_pb2.Empty, context: object) -> empty_pb2.Empty:
        # Invariant 7 (graceful shutdown by default): teardown() is run to
        # completion HERE, before this RPC returns and before
        # `shutdown_requested` (which triggers server.stop()) is set. This
        # ordering -- not a timing assumption -- is what makes the
        # deterministic shutdown test (▶ MUST-FIX 2) valid: a passing RPC
        # call is itself proof teardown() already ran.
        await run_teardown_once()
        shutdown_requested.set()
        return empty_pb2.Empty()

    server.add_generic_rpc_handlers((build_controller_handler(on_shutdown_rpc),))

    port = server.add_insecure_port("127.0.0.1:0")
    await server.start()

    def graceful_trigger() -> None:
        """Schedule the same graceful-shutdown coroutine from a signal handler.

        Uses ``run_coroutine_threadsafe``, the correct primitive for
        scheduling coroutine work on the loop from outside it (a signal
        handler is not "another thread" in the OS sense, but per the
        ``asyncio`` docs it must be treated the same way: only
        thread-safe scheduling APIs are safe to call from it).
        """
        asyncio.run_coroutine_threadsafe(_sigterm_shutdown(), loop)

    async def _sigterm_shutdown() -> None:
        """Drain the active read/write loop, run teardown, then unblock ``drive_shutdown``.

        Runs unconditionally, regardless of whether draining or teardown
        raise -- see the "Bug this also still fixes" section below.

        **Invariant 7 (graceful shutdown by default) gap this closes:** this
        coroutine used to call ``run_teardown_once()`` immediately on
        ``SIGTERM``, with no regard for whether ``Run`` was actively
        streaming -- ``teardown()`` (e.g. closing a DB pool) could then run
        concurrently with an in-flight ``Source.read()``/``Destination.
        write()``, racing resource cleanup against live I/O. ``drain()``
        (``_SourceServicer.drain``/``_DestinationServicer.drain``, whichever
        was registered) is awaited first: it performs the same
        stop-the-loop-then-wait-for-it ordering the deterministic path
        already gets for free (Conduit's own ``Stop`` RPC → ``Run`` ends →
        ``Teardown`` RPC), so a SIGTERM-triggered shutdown drains an
        in-flight operation before teardown runs, not concurrently with it.
        This is not a data-loss fix -- a write raised mid-flight already
        nacks the whole batch (see ``_DestinationServicer._write_batch``)
        and Conduit redelivers on restart either way -- it is what makes
        this SDK's SIGTERM path actually graceful rather than merely
        forward-progressing. The hung-loop watchdog (▶ MUST-FIX 3) still
        bounds how long this can take: it was already started, on its own
        thread, before this coroutine was even scheduled (see
        ``_ShutdownCoordinator._on_sigterm``), so a ``drain()`` that never
        returns (a genuinely wedged read/write) still force-exits on
        schedule rather than hanging forever.

        **Bug this also still fixes, found while building/testing the
        ``build`` CLI command (item 5): if ``drain()`` or ``teardown()``
        itself raised (e.g. a connector's ``teardown()`` accessing a
        resource ``open()`` never got a chance to set up, because SIGTERM
        arrived before Conduit ever called ``Open``), the exception used to
        propagate out of this coroutine. Since it runs via
        ``run_coroutine_threadsafe`` with its returned ``Future`` never
        awaited/checked (a signal handler has nothing to await), that
        exception was silently swallowed -- ``shutdown_requested`` was
        never set, ``drive_shutdown`` waited forever, and the hung-loop
        watchdog fired and force-exited with a "wedged event loop"
        diagnostic that was actively misleading: the loop was never
        wedged, a plain exception was just never observed.
        ``shutdown_requested.set()`` in a ``finally`` block makes forward
        progress on shutdown unconditional -- a buggy ``drain()``/
        ``teardown()`` no longer blocks the *entire* SIGTERM-triggered
        graceful path; it only loses the (already-lost, since it raised)
        cleanup it was supposed to do, and this prints a clear diagnostic
        distinguishing "drain/teardown raised" from "loop genuinely
        wedged" rather than reaching the watchdog's generic message at all.
        """
        try:
            await drain()
        except Exception as exc:
            print(
                f"conduit-sdk: draining the in-flight read/write loop raised "
                f"during SIGTERM-triggered shutdown: {exc!r} -- running "
                "teardown() anyway rather than skipping it",
                file=stderr,
                flush=True,
            )
        try:
            await run_teardown_once()
        except Exception as exc:
            print(
                f"conduit-sdk: teardown() raised during SIGTERM-triggered "
                f"shutdown: {exc!r} -- shutting down anyway rather than "
                "hanging until the watchdog force-exits",
                file=stderr,
                flush=True,
            )
        finally:
            shutdown_requested.set()

    coordinator = _ShutdownCoordinator(
        deadline=shutdown_deadline,
        exit_fn=exit_fn,
        stderr=stderr,
        graceful_trigger=graceful_trigger,
    )

    async def drive_shutdown() -> None:
        await shutdown_requested.wait()
        await server.stop(grace=None)
        coordinator.confirm_clean_exit()

    drive_task = asyncio.create_task(drive_shutdown())

    return _ServerHandle(
        server=server,
        port=port,
        coordinator=coordinator,
        connector_instance=instance,
        shutdown_requested=shutdown_requested,
        drive_task=drive_task,
        drain=drain,
    )


async def _serve_async(
    specification: Specification,
    *,
    source: type[Source[Any]] | None,
    destination: type[Destination[Any]] | None,
    app_protocol_version: int,
    shutdown_deadline: float,
    exit_fn: Callable[[int], NoReturn],
    stdout: TextIO,
    stderr: TextIO,
) -> None:
    """Build the server, emit the handshake line, then run until shutdown.

    Separated from :func:`serve` only so ``asyncio.run`` has a single
    coroutine to drive; not part of the public API.
    """
    handle = await _build_plugin_server(
        specification,
        source=source,
        destination=destination,
        shutdown_deadline=shutdown_deadline,
        exit_fn=exit_fn,
        stderr=stderr,
    )
    handle.coordinator.install_sigterm_handler()

    # Failure modes (design doc): nothing may be written to stdout before
    # this line except this line itself -- it is the one channel
    # go-plugin's client parses byte-for-byte.
    address = f"127.0.0.1:{handle.port}"
    emit_handshake_line(
        HandshakeLine(app_protocol_version=app_protocol_version, address=address),
        stream=stdout,
    )

    await handle.drive_task
    # Mirrors the Go SDK's own clean-shutdown path (§1.1.5: "stop the
    # server / os._exit(0)"): an explicit, injectable process exit rather
    # than falling through Python's normal interpreter teardown, so this
    # goes through the exact same exit mechanism (and is exercised by the
    # exact same test seam) as the watchdog's forced path.
    exit_fn(0)


def serve(
    specification: Specification,
    *,
    source: type[Source[Any]] | None = None,
    destination: type[Destination[Any]] | None = None,
    env: Mapping[str, str] | None = None,
    shutdown_deadline: float = DEFAULT_SHUTDOWN_DEADLINE_SECONDS,
    exit_fn: Callable[[int], NoReturn] = os._exit,
    stdout: TextIO | None = None,
    stderr: TextIO | None = None,
) -> None:
    """Run a ``Source`` or ``Destination`` as a Conduit standalone plugin.

    Validates the go-plugin handshake, starts a ``grpc.aio`` server serving
    the registered connector, blocks until Conduit tears it down via
    ``GRPCController.Shutdown`` (or the bounded watchdog forces an exit --
    ▶ MUST-FIX 3), then returns (in practice, ``exit_fn`` -- ``os._exit`` by
    default -- ends the process before this function's caller ever resumes).

    Args:
        specification: static plugin metadata (name, version, author, ...).
        source: a :class:`~conduit.source.Source` subclass to serve.
            Exactly one of ``source``/``destination`` must be given.
        destination: a :class:`~conduit.destination.Destination` subclass
            to serve.
        env: environment mapping for handshake validation; defaults to
            ``os.environ``. Injectable for testing.
        shutdown_deadline: seconds the hung-loop watchdog waits after
            ``SIGTERM`` before forcing an exit; see
            :data:`DEFAULT_SHUTDOWN_DEADLINE_SECONDS`.
        exit_fn: injectable process-exit function, called with ``0`` on
            clean shutdown or ``1`` if the watchdog fires. Defaults to
            ``os._exit`` (never returns); tests may inject a non-exiting
            stand-in to observe calls.
        stdout: stream for the handshake line; defaults to ``sys.stdout``.
            Injectable for testing -- never write anything else here (see
            module docstring).
        stderr: stream for shutdown diagnostics; defaults to ``sys.stderr``.

    Raises:
        ValueError: if neither or both of ``source``/``destination`` are given.
        conduit._handshake.HandshakeError: if the go-plugin handshake
            cannot be validated (missing/wrong magic cookie, no compatible
            protocol version).
    """
    if (source is None) == (destination is None):
        raise ValueError(
            "serve() requires exactly one of `source=` or `destination=`, not both/neither"
        )

    env = env if env is not None else os.environ
    check_magic_cookie(env)
    app_protocol_version = negotiate_protocol_version(env)

    asyncio.run(
        _serve_async(
            specification,
            source=source,
            destination=destination,
            app_protocol_version=app_protocol_version,
            shutdown_deadline=shutdown_deadline,
            exit_fn=exit_fn,
            stdout=stdout if stdout is not None else sys.stdout,
            stderr=stderr if stderr is not None else sys.stderr,
        )
    )
