# conduit.testing

The acceptance-test harness (`AcceptanceTestDriver` Protocol +
`ConfigurableAcceptanceTestDriver` convenience wrapper) and golden
record-shape fixtures, per `docs/design/20260707-python-connector-sdk.md`
§3.

- `acceptance.py` — `AcceptanceTestSuite`, the versioned (`CONTRACT_VERSION`)
  suite an author subclasses in their own `pytest` test module. See its
  module docstring for the exact usage shape.
- `fixtures.py` — golden OpenCDC record-shape factory functions
  (`snapshot_record`, `create_record`, `update_record`, `delete_record`).

See `tests/test_acceptance_harness.py` (synthetic driver) and
`tests/test_example_http_poll_source.py` (the real worked example) in this
repo for working usage examples.
