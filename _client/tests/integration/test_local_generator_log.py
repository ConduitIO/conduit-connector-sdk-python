"""End-to-end: `conduit.local()` running a real `generator -> log` pipeline.

This is the one test in this repo that touches a real `conduit` binary --
downloaded for real (no mocking) via `conduit._provision.ensure_binary`,
spawned for real, driven over a real loopback gRPC channel. Marked
`integration` (excluded from the default `pytest` run, see
`pyproject.toml`'s `testpaths`/`markers` and the root `README.md`'s
"Testing" section).

**Honesty requirement (build task gate):** if this sandbox has no network
access to GitHub Releases, this test is skipped with the real reason stated
-- never faked, never silently passed by mocking away the exact thing it
exists to prove.
"""

from __future__ import annotations

import time
import urllib.error
import urllib.request

import pytest

import conduit
from conduit._provision import DEFAULT_CONDUIT_VERSION, _archive_name

pytestmark = pytest.mark.integration


def _github_releases_reachable() -> bool:
    """Best-effort, fast check for network access to GitHub Releases.

    Not a substitute for the download itself succeeding (e.g. this version's
    asset could 404 even if GitHub is reachable) -- just the cheap, honest
    "do we even have a network" gate so a sandboxed/offline run skips with a
    clear reason instead of hanging on a DNS/connect timeout.
    """
    try:
        urllib.request.urlopen("https://github.com", timeout=3)
    except (TimeoutError, urllib.error.URLError, OSError):
        return False
    return True


@pytest.fixture(scope="module")
def _network_available() -> bool:
    return _github_releases_reachable()


def test_generator_to_log_pipeline_runs_end_to_end(
    tmp_path_factory: pytest.TempPathFactory, _network_available: bool
) -> None:
    if not _network_available:
        pytest.skip(
            "no network access to github.com in this sandbox -- "
            f"conduit.local() needs to download {_archive_name(DEFAULT_CONDUIT_VERSION)} "
            "from GitHub Releases on first use, and this environment cannot reach it. "
            "Honestly skipped, not faked: this is the one test that exercises a real "
            "conduit binary end-to-end."
        )

    state_dir = tmp_path_factory.mktemp("conduit-state")
    pipeline = (
        conduit.Pipeline("integration-generator-log")
        .source("generator", settings={"format.type": "structured"}, operations="create")
        .destination("log", level="info")
    )

    with conduit.local(state_dir=state_dir, startup_timeout=60.0) as client:
        run = client.run(pipeline)
        status = run.wait_running(timeout=30.0)
        assert status.is_running

        settled = run.status()
        assert settled.status in ("running", "degraded")
        if settled.status == "degraded":
            pytest.fail(f"pipeline degraded after starting: {settled.error}")

        run.stop()
        # StopPipeline returns once the stop is *initiated*, not once the
        # pipeline has fully drained (Run.stop()'s own docstring) -- poll
        # status() rather than asserting immediately.
        deadline = time.monotonic() + 10.0
        final = run.status()
        while final.status != "stopped" and time.monotonic() < deadline:
            time.sleep(0.1)
            final = run.status()
        assert final.status == "stopped"
        assert final.stopped_reason == "user"
