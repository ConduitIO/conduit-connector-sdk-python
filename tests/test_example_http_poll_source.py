"""Runs the versioned acceptance suite against the Phase-1 worked example connector.

``examples/http-poll-source/main.py`` is exercised **unmodified** and
**in-process**, against a real local HTTP server (stdlib
``http.server.ThreadingHTTPServer``, backed by a small in-memory dataset --
no mocking of ``httpx`` internals). This is deliberately real HTTP, just not
a real Conduit binary or subprocess launch -- per
``docs/design/20260707-python-connector-sdk.md`` §3, that level of
end-to-end verification is ``compat-nightly.yml``/Conduit-repo-side scope,
not this repo's CI. This test must actually pass; it is not skipped or
stubbed.
"""

from __future__ import annotations

import http.server
import importlib.util
import json
import threading
from collections.abc import Iterator
from pathlib import Path
from types import ModuleType
from urllib.parse import parse_qs, urlparse

import pytest

from conduit.config import Specification
from conduit.testing.acceptance import AcceptanceTestSuite, ConfigurableAcceptanceTestDriver

_EXAMPLE_MAIN = Path(__file__).resolve().parent.parent / "examples" / "http-poll-source" / "main.py"

# 20 rows, ids 1..20 -- enough for the acceptance suite's resume-at-position
# tests (which read a couple of records, reopen, and expect a genuinely
# later one) without exhausting the dataset.
_ROWS = [{"id": i} for i in range(1, 21)]


def _load_example_module() -> ModuleType:
    """Load ``examples/http-poll-source/main.py`` by file path.

    The directory is hyphenated (matching a real connector repo's naming
    convention, per the design doc's proposed layout) and therefore not a
    valid Python package/module name to import with a plain ``import``
    statement -- loading by explicit file path sidesteps that without
    renaming the example to satisfy Python's import system instead of the
    org's naming convention.
    """
    spec = importlib.util.spec_from_file_location("http_poll_source_example", _EXAMPLE_MAIN)
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class _RowsHandler(http.server.BaseHTTPRequestHandler):
    """Serves ``_ROWS`` one at a time past ``?since=<id>``, oldest-first.

    Matches the contract ``HTTPPollSource.read()`` expects (design doc
    §2.7): a JSON list, empty when there's nothing newer than ``since``.
    """

    def do_GET(self) -> None:
        query = parse_qs(urlparse(self.path).query)
        since = int(query.get("since", ["0"])[0])
        remaining = [row for row in _ROWS if row["id"] > since]
        body = json.dumps(remaining[:1]).encode()
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, format_str: str, *args: object) -> None:
        """Silence the default stderr access log -- noisy for a test server."""
        return None


@pytest.fixture(scope="module")
def _rows_server() -> Iterator[http.server.ThreadingHTTPServer]:
    server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), _RowsHandler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield server
    finally:
        server.shutdown()
        thread.join(timeout=2)


class _HTTPPollSourceAcceptanceHelper(AcceptanceTestSuite):
    """The versioned acceptance suite, run against the real example file."""

    def __init__(self, rows_server: http.server.ThreadingHTTPServer) -> None:
        self._rows_server = rows_server

    def driver(self) -> ConfigurableAcceptanceTestDriver:
        module = _load_example_module()
        host, port = self._rows_server.server_address[:2]
        return ConfigurableAcceptanceTestDriver(
            spec=Specification(name="http-poll", version="0.1.0", author="you"),
            source_cls=module.HTTPPollSource,
            source_cfg={"url": f"http://{host}:{port}"},
        )


@pytest.fixture
def _suite(
    _rows_server: http.server.ThreadingHTTPServer,
) -> _HTTPPollSourceAcceptanceHelper:
    return _HTTPPollSourceAcceptanceHelper(_rows_server)


async def test_specifier_exists_and_is_valid(
    _suite: _HTTPPollSourceAcceptanceHelper,
) -> None:
    await _suite.test_specifier_exists_and_is_valid()


async def test_config_validation_succeeds_with_valid_config(
    _suite: _HTTPPollSourceAcceptanceHelper,
) -> None:
    await _suite.test_config_validation_succeeds_with_valid_config()


async def test_config_validation_fails_with_missing_required_param(
    _suite: _HTTPPollSourceAcceptanceHelper,
) -> None:
    await _suite.test_config_validation_fails_with_missing_required_param()


async def test_resume_at_position_snapshot(_suite: _HTTPPollSourceAcceptanceHelper) -> None:
    await _suite.test_resume_at_position_snapshot()


async def test_resume_at_position_cdc(_suite: _HTTPPollSourceAcceptanceHelper) -> None:
    await _suite.test_resume_at_position_cdc()


async def test_read_write_round_trip(_suite: _HTTPPollSourceAcceptanceHelper) -> None:
    await _suite.test_read_write_round_trip()


async def test_read_timeout_behavior(_suite: _HTTPPollSourceAcceptanceHelper) -> None:
    await _suite.test_read_timeout_behavior()


async def test_partial_batch_write_correctness(
    _suite: _HTTPPollSourceAcceptanceHelper,
) -> None:
    """SDK-level B1 guarantee -- applies identically even to this source-only connector."""
    await _suite.test_partial_batch_write_correctness()
