"""``conduit.local()``: spawn and supervise a `conduit` subprocess.

Mode 1 in the design doc's "Deployment modes -- framed honestly": engine
co-located with the host application, lifecycle tied to it. **Not a
production story** -- no independent lifecycle, no restart-without-the-host-
restarting, no fleet management. Fits dev, notebooks, one-off jobs,
single-host batch/ETL. See :func:`conduit.local`'s docstring (re-exported at
the package root) for the user-facing contract; this module is the
implementation.
"""

from __future__ import annotations

import atexit
import contextlib
import socket
import subprocess
import sys
import time
import warnings
from pathlib import Path
from types import TracebackType

import grpc

import conduit._grpc  # noqa: F401  -- sets up sys.path, see conduit._grpc.__init__
from api.v1 import api_pb2
from conduit._provision import ensure_binary, resolve_version
from conduit.client import Client
from conduit.errors import ConduitError
from conduit.pipeline import Pipeline
from conduit.run import Run

#: How long a graceful SIGTERM/terminate() gets before force-kill, per
#: Invariant 7 ("Shutdown is graceful by default... kill -9 at any instant
#: must be recoverable without loss"). This is the *client's* grace window for
#: its *own* supervised subprocess -- the engine's own drain behavior on
#: SIGTERM is unaffected by this constant.
_GRACEFUL_STOP_TIMEOUT = 10.0


def _free_port() -> int:
    """Reserve an OS-assigned loopback port, then release it immediately.

    Standard bind-then-release technique (used by e.g. pytest-xdist,
    testcontainers) to avoid parsing Conduit's startup log for its bound
    address. **Known, accepted race window**: another process could claim the
    port between release and the subprocess's own bind. If that happens, the
    readiness poll in :class:`LocalConduit._wait_ready` fails with a clear
    "engine did not become reachable" error rather than hanging -- it does
    not silently connect to the wrong process.
    """
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]  # type: ignore[no-any-return]


class LocalConduit:
    """Supervises one `conduit` subprocess and the :class:`Client` bound to it.

    Not constructed directly -- use :func:`conduit.local`.
    """

    def __init__(
        self,
        *,
        state_dir: Path,
        version: str | None,
        binary: Path | None,
        startup_timeout: float,
    ) -> None:
        """Provision (if needed), spawn, and wait for a `conduit` engine to become ready.

        Raises:
            ConduitError: if provisioning fails (see
                :func:`conduit._provision.ensure_binary`), the subprocess
                exits before becoming ready (code
                ``"client.engine_crashed"``, with captured output), or the
                engine never becomes reachable within ``startup_timeout``
                (code ``"client.engine_unreachable"``).
        """
        self.state_dir = state_dir
        self.binary_path = Path(binary) if binary is not None else ensure_binary(version)
        self._grpc_port = _free_port()
        self._http_port = _free_port()
        self._log_path = state_dir / "conduit.log"

        self._log_file = self._log_path.open("ab")
        self._process = subprocess.Popen(
            [
                str(self.binary_path),
                "run",
                "--api.enabled",
                f"--api.grpc.address=127.0.0.1:{self._grpc_port}",
                f"--api.http.address=127.0.0.1:{self._http_port}",
                "--db.type=badger",
                f"--db.badger.path={state_dir / 'conduit.db'}",
            ],
            stdout=self._log_file,
            stderr=subprocess.STDOUT,
            stdin=subprocess.DEVNULL,
        )
        self._stopped = False
        atexit.register(self._atexit_kill)

        try:
            self.client = self._wait_ready(startup_timeout)
        except BaseException:
            self._terminate()
            raise

    @property
    def addr(self) -> str:
        """The engine's ``host:port`` gRPC address."""
        return f"127.0.0.1:{self._grpc_port}"

    def _wait_ready(self, timeout: float) -> Client:
        """Poll until the engine's gRPC API answers ``GetInfo``, or fail fast.

        Distinguishes, per the design doc's failure mode 1, "the process died"
        (surfaces the captured log tail immediately -- never waits out the
        rest of ``timeout``) from "not ready yet" (keeps polling).
        """
        deadline = time.monotonic() + timeout
        channel = grpc.insecure_channel(self.addr)
        client = Client(channel, on_close=self._terminate)
        last_error: Exception | None = None

        while time.monotonic() < deadline:
            exit_code = self._process.poll()
            if exit_code is not None:
                channel.close()
                raise ConduitError(
                    f"conduit subprocess exited (code {exit_code}) before its API "
                    f"became ready. Log tail:\n{self._log_tail()}",
                    code="client.engine_crashed",
                )
            try:
                client.get_info(timeout=1.0)
            except ConduitError as e:
                last_error = e
                time.sleep(0.1)
                continue
            return client

        channel.close()
        raise ConduitError(
            f"conduit subprocess did not become reachable at {self.addr} within "
            f"{timeout}s (last error: {last_error}). Log tail:\n{self._log_tail()}",
            code="client.engine_unreachable",
        )

    def _log_tail(self, max_bytes: int = 4096) -> str:
        """Best-effort tail of the subprocess's combined stdout/stderr log."""
        try:
            with self._log_path.open("rb") as f:
                f.seek(0, 2)
                size = f.tell()
                f.seek(max(0, size - max_bytes))
                return f.read().decode("utf-8", errors="replace")
        except OSError:
            return "<log unavailable>"

    def _terminate(self) -> None:
        """Gracefully stop the subprocess (Invariant 7), force-killing if it won't."""
        if self._stopped:
            return
        self._stopped = True
        if self._process.poll() is None:
            self._process.terminate()  # SIGTERM on POSIX, TerminateProcess on Windows
            try:
                self._process.wait(timeout=_GRACEFUL_STOP_TIMEOUT)
            except subprocess.TimeoutExpired:
                self._process.kill()
                self._process.wait(timeout=_GRACEFUL_STOP_TIMEOUT)
        with contextlib.suppress(OSError):
            self._log_file.close()

    def _atexit_kill(self) -> None:
        """Best-effort backup teardown if the host process exits without calling close().

        Per the design doc: "killed on client close()/__exit__, or on host-
        process exit via an atexit/finalizer hook -- best-effort, not
        guaranteed." A hard `kill -9` of the *host* skips this entirely, same
        as any atexit hook.
        """
        with contextlib.suppress(Exception):
            self._terminate()

    # -- Client-like passthrough -------------------------------------------------
    # `conduit.local(...)` is usable exactly like `conduit.connect(...)`'s return
    # value -- `client.run(pipeline)`, `client.get_info()`, `client.close()` --
    # whether or not it's used as a context manager. Explicit passthrough
    # methods (not `__getattr__` delegation) so mypy strict sees real
    # signatures and a reader doesn't need to chase a magic-method proxy.

    def run(self, pipeline: Pipeline, *, start: bool = True, timeout: float = 10.0) -> Run:
        """See :meth:`conduit.client.Client.run`."""
        return self.client.run(pipeline, start=start, timeout=timeout)

    def get_info(self, *, timeout: float = 5.0) -> api_pb2.Info:
        """See :meth:`conduit.client.Client.get_info`."""
        return self.client.get_info(timeout=timeout)

    def close(self) -> None:
        """Close the channel and stop the supervised subprocess (graceful, then force)."""
        self.client.close()  # closes the channel, then calls self._terminate via on_close

    def __enter__(self) -> LocalConduit:
        """Return ``self`` for ``with conduit.local(...) as client: client.run(...)``."""
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        """Stop the subprocess and close the channel."""
        self.close()


def local(
    state_dir: str | Path | None = None,
    *,
    version: str | None = None,
    binary: str | Path | None = None,
    startup_timeout: float = 30.0,
) -> LocalConduit:
    """Spawn and supervise a `conduit` subprocess, returning a bound client.

    **Mode 1 -- dev/notebook/single-host shaped, not a production story**
    (design doc, "Deployment modes"): the engine's lifecycle is tied to this
    host process. Use :func:`conduit.connect` against an independently
    deployed engine for production.

    **`state_dir` is never an ephemeral temp directory.** If omitted, this
    defaults to ``./.conduit/state`` under the current working directory --
    stable across repeated runs in the same directory, not
    ``tempfile.mkdtemp()`` (which vanishes with the process and silently
    forfeits Invariant 2's crash-safe positions the moment the host exits).
    A runtime warning is emitted when the default is used; pass ``state_dir``
    explicitly for anything beyond a quick, throwaway experiment.

    Args:
        state_dir: directory Conduit's Badger state (positions, pipeline
            metadata) lives in. Created if it doesn't exist. Reusing the same
            path across runs resumes from where the last run left off.
        version: exact `conduit` version to provision (see
            :data:`conduit._provision.DEFAULT_CONDUIT_VERSION`); a leading
            `v` is stripped if present. Never resolved to "latest" --
            reproducible by default.
        binary: path to an already-present `conduit` executable, skipping
            download-on-first-use entirely. Use this to pin an exact local
            build/binary instead of what this library would otherwise
            provision.
        startup_timeout: seconds to wait for the engine's API to become
            reachable before raising.

    Returns:
        A :class:`LocalConduit` -- use as a context manager
        (``with conduit.local(...) as client:``) or access ``.client``
        directly; either way, call ``client.close()`` (or exit the ``with``
        block) to stop the subprocess.

    Raises:
        ConduitError: see :class:`LocalConduit`'s constructor.
    """
    resolved_state_dir = (
        Path(state_dir) if state_dir is not None else Path.cwd() / ".conduit" / "state"
    )
    if state_dir is None:
        warnings.warn(
            f"conduit.local(): no state_dir given, defaulting to "
            f"{resolved_state_dir} (stable across runs in this working "
            "directory -- never an ephemeral temp dir, which would silently "
            "lose pipeline positions on exit; see the design doc's Mode-1 "
            "caveat). Pass state_dir= explicitly for anything beyond a quick "
            "local experiment.",
            stacklevel=2,
        )
    resolved_state_dir.mkdir(parents=True, exist_ok=True)

    print(
        # real wall-clock time on first use (binary download) and this is a CLI-adjacent
        # library, not a silent background service
        f"conduit.local(): using engine v{resolve_version(version)}, "
        f"state_dir={resolved_state_dir}",
        file=sys.stderr,
    )

    return LocalConduit(
        state_dir=resolved_state_dir,
        version=version,
        binary=Path(binary) if binary is not None else None,
        startup_timeout=startup_timeout,
    )
