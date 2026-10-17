"""Python SDK for building Conduit source and destination connectors.

The connector SDK lives under the ``conduit.connector`` namespace, one of two
namespaces shipped by the single ``conduit`` distribution (the other is
``conduit.client``, the gRPC control client). ``conduit`` itself is a PEP 420
namespace package, so both namespaces install together without a top-level
``__init__.py`` collision — see
``docs/design/20260707-python-connector-sdk.md`` for the SDK design and
``docs/design/20261016-python-ga-merged-distribution.md`` for the
distribution shape.

Public author-facing surface, per
``docs/design/20260707-python-connector-sdk.md`` §2/§3: ``Source``,
``Destination``, ``Record``, ``Change``, ``Operation``, ``BaseConfig``,
``Field``, ``Specification``, ``serve``, and the connector-facing
exceptions (``BackoffRetry``, ``BatchWriteError``, ``ConnectorError``).

See ``CONTRIBUTING.md`` for the Tier-1 review bar this package is held to.
"""

from __future__ import annotations

from conduit._version import __version__
from conduit.connector.config import BaseConfig, Field, Specification, to_parameters
from conduit.connector.destination import Destination
from conduit.connector.errors import BackoffRetry, BatchWriteError, ConnectorError
from conduit.connector.record import Change, Data, Metadata, Operation, Record
from conduit.connector.serve import serve
from conduit.connector.source import Source

__all__ = [
    "BackoffRetry",
    "BaseConfig",
    "BatchWriteError",
    "Change",
    "ConnectorError",
    "Data",
    "Destination",
    "Field",
    "Metadata",
    "Operation",
    "Record",
    "Source",
    "Specification",
    "__version__",
    "serve",
    "to_parameters",
]
