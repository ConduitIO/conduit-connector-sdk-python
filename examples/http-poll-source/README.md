# http-poll-source

The Phase-1 worked example connector from
`docs/design/20260707-python-connector-sdk.md` §2.7 — a minimal source that
polls an HTTP endpoint for new rows. Implemented in `main.py`, fully runnable
(`python main.py`, launched by a go-plugin host such as Conduit — it is not
meant to be run as a bare script; see `conduit._handshake`).

Exercised end to end, in-process, by this repo's own
`tests/test_example_http_poll_source.py`, which runs the versioned
acceptance suite (`conduit.testing.acceptance`) against this exact file
against a real local HTTP server — not a mock of `httpx`, not a stub of the
connector.

This is both the worked example *and* will become the source for
`conduit connector new --lang python`'s scaffolded template (Phase 3), per
the design doc's reasoning for reusing one connector for both rather than
building a separate template repo before the SDK API has stabilized.

## Notes for connector authors

- **You don't need to override `Source.ack()`** unless you're also
  acknowledging against the source system itself (e.g. committing a Kafka
  consumer offset, deleting a queue message, marking a row processed
  upstream). This example doesn't override it -- Conduit's own
  position-based resume (via `open(position)`) is enough for an HTTP
  polling source with no upstream ack concept.
- Build a standalone, directly-executable artifact for this connector with
  `conduit-connector-sdk build examples/http-poll-source -o http-poll-source`
  -- see the root [`README.md`](../../README.md#building-a-standalone-connector-artifact)
  for why this is required (not just convenient) for Conduit to launch it.
