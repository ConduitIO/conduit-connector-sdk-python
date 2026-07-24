"""Testing utilities for Conduit connector authors.

``conduit.testing.acceptance`` provides the versioned acceptance-test suite
(:class:`~conduit.testing.acceptance.AcceptanceTestSuite`) a connector must
pass; ``conduit.testing.fixtures`` provides golden OpenCDC record-shape
fixtures. See each module's docstring for details, and
``docs/design/20260707-python-connector-sdk.md`` §3 for the design.
"""

from __future__ import annotations

from conduit.testing.acceptance import (
    CONTRACT_VERSION,
    AcceptanceTestDriver,
    AcceptanceTestSuite,
    ConfigurableAcceptanceTestDriver,
)

__all__ = [
    "CONTRACT_VERSION",
    "AcceptanceTestDriver",
    "AcceptanceTestSuite",
    "ConfigurableAcceptanceTestDriver",
]
