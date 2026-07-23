"""A minimal Conduit source connector: polls an HTTP endpoint for new rows.

The Phase-1 worked example from
``docs/design/20260707-python-connector-sdk.md`` §2.7 -- the SDK's own
"hello world," made fully runnable rather than illustrative-only. It expects
an HTTP endpoint that accepts ``?since=<cursor>`` and returns a JSON list of
rows (each with an ``id`` field) newer than that cursor, oldest-first.

Run it directly with ``python main.py`` under a real go-plugin host
(Conduit); it is not meant to be run as a plain script otherwise -- see
``conduit._handshake`` for why (this process expects
``CONDUIT_PLUGIN_MAGIC_COOKIE``/``PLUGIN_PROTOCOL_VERSIONS`` to be set by
the launching host).

See ``tests/test_example_http_poll_source.py`` (this repo's own test suite,
not this directory) for the versioned acceptance suite run against this
exact file, in-process, against a real local HTTP server -- proving this
example is a fully working connector, not just a doc snippet.
"""

from __future__ import annotations

from datetime import UTC, datetime

import httpx

from conduit import BackoffRetry, Change, Metadata, Operation, Record, Source, serve
from conduit.config import BaseConfig, Field, Specification


class Config(BaseConfig):
    """Configuration for :class:`HTTPPollSource`."""

    url: str = Field(description="HTTP endpoint to poll, expects ?since=<cursor>.")
    poll_interval_ms: int = Field(
        default=1000, ge=100, description="Delay between empty polls (paced by the SDK itself)."
    )


class HTTPPollSource(Source[Config]):
    """Polls ``config.url?since=<cursor>`` for new rows, oldest-first."""

    async def open(self, position: bytes | None) -> None:
        """Open the HTTP client and resume from ``position`` (or the beginning).

        Per invariant 2: ``position`` is the last cursor this connector (or
        a previous run of it) successfully emitted -- resuming means
        requesting strictly newer rows, never replaying it.
        """
        self._client = httpx.AsyncClient()
        self._since = position.decode() if position else "0"

    async def read(self) -> Record:
        """Fetch the next row past ``self._since``, or signal there's nothing yet.

        Raises:
            conduit.errors.BackoffRetry: the endpoint returned no new rows;
                the SDK's own read loop paces retries -- this does not
                sleep itself, avoiding a double backoff (design doc §2.7).
        """
        resp = await self._client.get(self.config.url, params={"since": self._since})
        rows = resp.json()
        if not rows:
            raise BackoffRetry()

        row = rows[0]
        self._since = str(row["id"])
        metadata: dict[str, str] = {}
        Metadata.set_read_at(metadata, int(datetime.now(UTC).timestamp() * 1e9))
        return Record(
            position=self._since.encode(),
            operation=Operation.CREATE,
            key={"id": row["id"]},
            payload=Change(after=row),
            metadata=metadata,
        )

    async def teardown(self) -> None:
        """Close the HTTP client."""
        await self._client.aclose()


if __name__ == "__main__":
    serve(Specification(name="http-poll", version="0.1.0", author="you"), source=HTTPPollSource)
