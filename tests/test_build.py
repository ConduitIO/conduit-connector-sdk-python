"""Tests for ``conduit-connector-sdk build`` (:mod:`conduit._build`/``_cli``).

Builds the real worked example connector (``examples/http-poll-source``)
into a self-contained artifact and **execs the resulting file directly**
(never via ``python <artifact>``) -- matching exactly how Conduit's
dispenser launches a standalone connector subprocess (design doc §1.1.6:
a clean environment, no inherited ``PATH``, so a plain shebang script
resolved via ``PATH`` cannot work). This must actually pass, not be
skipped: it's the closest thing this repo's CI has to proving the
packaging story the design doc calls out as a hard launch gate.
"""

from __future__ import annotations

import os
import stat
import subprocess
import sys
from pathlib import Path

import pytest

from conduit._build import BuildError, build_connector_artifact
from conduit._handshake import MAGIC_COOKIE_KEY, MAGIC_COOKIE_VALUE, PROTOCOL_VERSIONS_ENV

_EXAMPLE_PROJECT_DIR = Path(__file__).resolve().parent.parent / "examples" / "http-poll-source"


def _handshake_env(cache_dir: Path) -> dict[str, str]:
    env = dict(os.environ)
    env[MAGIC_COOKIE_KEY] = MAGIC_COOKIE_VALUE
    env[PROTOCOL_VERSIONS_ENV] = "2"
    # Isolate the extraction cache to this test run -- never touch the
    # real user cache directory, and never let one test's extraction leak
    # into another's.
    env["CONDUIT_CONNECTOR_BUILD_CACHE_DIR"] = str(cache_dir)
    return env


@pytest.fixture(scope="module")
def built_artifact(tmp_path_factory: pytest.TempPathFactory) -> Path:
    """Build the real example connector once, shared across this module's tests."""
    output_dir = tmp_path_factory.mktemp("build-output")
    output_path = output_dir / "http-poll-source.pyz"
    build_connector_artifact(_EXAMPLE_PROJECT_DIR, output_path)
    return output_path


class TestBuildConnectorArtifact:
    def test_output_file_exists_and_is_executable(self, built_artifact: Path) -> None:
        assert built_artifact.is_file()
        mode = built_artifact.stat().st_mode
        assert mode & stat.S_IXUSR, "artifact must have the executable bit set"

    def test_shebang_is_an_absolute_interpreter_path(self, built_artifact: Path) -> None:
        """Design doc §1.1.6: Conduit execs with no inherited PATH -- the

        shebang must be an absolute path, never `#!/usr/bin/env python3`
        (which would require PATH resolution at exec time).
        """
        with built_artifact.open("rb") as f:
            first_line = f.readline()
        assert first_line.startswith(b"#!")
        interpreter_path = first_line[2:].decode().strip()
        assert Path(interpreter_path).is_absolute()
        assert interpreter_path == sys.executable

    def test_directly_executed_artifact_prints_a_valid_handshake_line(
        self, built_artifact: Path, tmp_path: Path
    ) -> None:
        """Exec the artifact PATH itself -- not `python <artifact>` -- exactly

        how Conduit's dispenser launches a standalone connector subprocess.
        """
        cache_dir = tmp_path / "cache"
        proc = subprocess.Popen(
            [str(built_artifact)],  # <-- the artifact itself is the executable
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            env=_handshake_env(cache_dir),
            text=True,
        )
        try:
            line = proc.stdout.readline()
            parts = line.strip().split("|")
            assert len(parts) == 6, f"malformed handshake line: {line!r}"
            core_version, app_version, network, address, protocol, server_cert = parts
            assert core_version == "1"
            assert app_version == "2"
            assert network == "tcp"
            assert address  # non-empty listen address
            assert protocol == "grpc"
            assert server_cert == ""
        finally:
            proc.terminate()
            try:
                proc.wait(timeout=5)
            except subprocess.TimeoutExpired:
                proc.kill()
                proc.wait()

    def test_sigterm_triggers_prompt_graceful_shutdown(
        self, built_artifact: Path, tmp_path: Path
    ) -> None:
        """A real SIGTERM to the real exec'd artifact exits promptly, not after

        the watchdog's multi-second deadline -- the regression this test
        pins: found via this exact scenario while building this test
        (`HTTPPollSource.teardown()` used to crash with `AttributeError`
        when `SIGTERM` arrived before `Open` was ever called, silently
        swallowing the exception and hanging until the watchdog fired; see
        `conduit/serve.py`'s `_sigterm_shutdown` and the example's
        `teardown()` guard).
        """
        import signal
        import time

        cache_dir = tmp_path / "cache"
        proc = subprocess.Popen(
            [str(built_artifact)],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            env=_handshake_env(cache_dir),
            text=True,
        )
        try:
            proc.stdout.readline()  # wait for the handshake line
            start = time.monotonic()
            proc.send_signal(signal.SIGTERM)
            proc.wait(timeout=4)  # well under the default 5s watchdog deadline
            elapsed = time.monotonic() - start
            assert proc.returncode == 0
            assert elapsed < 2.0, (
                f"exited after {elapsed:.2f}s -- expected a prompt graceful "
                "shutdown, not a wait anywhere near the watchdog's deadline"
            )
        finally:
            if proc.poll() is None:
                proc.kill()
                proc.wait()

    def test_extraction_cache_is_reused_on_second_launch(
        self, built_artifact: Path, tmp_path: Path
    ) -> None:
        """The payload is extracted once per distinct cache dir, not on every launch."""
        cache_dir = tmp_path / "cache"
        env = _handshake_env(cache_dir)

        for _ in range(2):
            proc = subprocess.Popen(
                [str(built_artifact)],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                env=env,
                text=True,
            )
            try:
                line = proc.stdout.readline()
                assert line.strip().split("|")[4] == "grpc"
            finally:
                proc.terminate()
                try:
                    proc.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    proc.kill()
                    proc.wait()

        marker_dirs = list(cache_dir.glob("*/.extracted-ok"))
        assert len(marker_dirs) == 1, "expected exactly one cached extraction, reused twice"

    def test_extracted_payload_includes_compiled_extension_modules(
        self, built_artifact: Path, tmp_path: Path
    ) -> None:
        """Pins that this is NOT a plain zipapp: grpcio's and pydantic-core's

        compiled extensions must be present as real extracted files, not
        silently dropped or left unimportable inside the zip.
        """
        cache_dir = tmp_path / "cache"
        proc = subprocess.Popen(
            [str(built_artifact)],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            env=_handshake_env(cache_dir),
            text=True,
        )
        try:
            proc.stdout.readline()
        finally:
            proc.terminate()
            try:
                proc.wait(timeout=5)
            except subprocess.TimeoutExpired:
                proc.kill()
                proc.wait()

        extracted = list(cache_dir.glob("*/_payload"))
        assert len(extracted) == 1
        payload_dir = extracted[0]
        so_files = list(payload_dir.rglob("*.so")) + list(payload_dir.rglob("*.dylib"))
        assert so_files, "expected at least one compiled extension module to be vendored"
        assert (payload_dir / "conduit").is_dir()
        assert (payload_dir / "httpx").is_dir()
        assert (payload_dir / "__main__.py").is_file()


class TestBuildConnectorArtifactErrors:
    def test_missing_entry_point_raises_build_error(self, tmp_path: Path) -> None:
        empty_project = tmp_path / "empty-project"
        empty_project.mkdir()
        with pytest.raises(BuildError, match="does not exist"):
            build_connector_artifact(empty_project, tmp_path / "out.pyz")

    def test_uninstalled_dependency_raises_build_error(self, tmp_path: Path) -> None:
        project = tmp_path / "project-with-bad-dep"
        project.mkdir()
        (project / "main.py").write_text("print('hi')\n")
        (project / "pyproject.toml").write_text(
            '[project]\nname = "x"\nversion = "0.1.0"\n'
            'dependencies = ["definitely-not-a-real-installed-package-xyz"]\n'
        )
        with pytest.raises(BuildError, match="not installed"):
            build_connector_artifact(project, tmp_path / "out.pyz")
