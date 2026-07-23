"""Python SDK for building Conduit source and destination connectors.

Public author-facing surface, per
``docs/design/20260707-python-connector-sdk.md`` §2/§3: ``Source``,
``Destination``, ``Record``, ``Change``, ``Operation``, ``BaseConfig``,
``Field``, ``Specification``, ``serve``, and the connector-facing
exceptions (``BackoffRetry``, ``BatchWriteError``, ``ConnectorError``).

See ``CONTRIBUTING.md`` for the Tier-1 review bar this package is held to.
"""

from __future__ import annotations

from conduit.config import BaseConfig, Field, Specification, to_parameters
from conduit.destination import Destination
from conduit.errors import BackoffRetry, BatchWriteError, ConnectorError
from conduit.record import Change, Data, Metadata, Operation, Record
from conduit.serve import serve
from conduit.source import Source

__version__ = "0.1.0.dev0"

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
