"""The OpenCDC record model: ``Record``, ``Change``, ``Operation``, ``Data``.

See ``docs/design/20260707-python-connector-sdk.md`` §1.4 (wire shape,
confirmed against ``conduit-commons``' ``proto/opencdc/v1/opencdc.proto``)
and §2.3 (the Python API design and its B3 fidelity caveat). Proto
(de)serialization for these types lives in
:mod:`conduit._grpc.adapters`, not here -- this module is the plain,
dependency-free dataclass shape connector authors write against; the wire
boundary is a separate, internal concern.

Plain :mod:`dataclasses`, not pydantic, are used here deliberately: records
are produced/consumed at high frequency in the hot path, pydantic's
validation overhead isn't wanted there, and there is nothing to validate --
the wire format already constrains the shape (§2.3).
"""

from __future__ import annotations

import enum
from collections.abc import Mapping
from dataclasses import dataclass, field
from typing import Any

Data = bytes | Mapping[str, Any]
"""The two-way shape of an OpenCDC record's key/before/after payload.

Mirrors the wire-level ``oneof`` in ``opencdc.Data`` (§1.4): either raw bytes
or a JSON-like structured mapping. Go needs an interface (``Bytes()``,
``Clone()``, ``ToProto()``) to give ``RawData []byte`` and
``StructuredData map[string]interface{}`` a common contract; Python doesn't,
since ``bytes`` and ``dict``/``Mapping`` already have their own copy
semantics and ``isinstance()`` at the (de)serialization boundary is enough
to pick the right wire branch (§2.3).

**Known fidelity caveat (B3, design doc §2.3):** structured (mapping) data
crosses the wire as ``google.protobuf.Struct``, which only supports
JSON-like values. Integers are round-tripped as ``Struct``'s ``number_value``
(a double), so any integer beyond ``2**53`` loses precision silently -- see
:mod:`conduit._grpc.adapters` for exactly where this conversion happens and
``tests/test_record_codec.py`` for the property test pinning this as known,
not accidental, behavior.
"""


class Operation(enum.Enum):
    """The kind of change a record represents.

    Values match the wire enum ``opencdc.Operation`` exactly
    (``OPERATION_CREATE`` = 1 .. ``OPERATION_SNAPSHOT`` = 4), so
    :mod:`conduit._grpc.adapters` can convert with a plain ``.value``/
    ``Operation(...)`` round-trip -- no lookup table needed.
    """

    CREATE = 1
    UPDATE = 2
    DELETE = 3
    SNAPSHOT = 4


@dataclass(slots=True)
class Change:
    """The before/after payload of a record.

    Attributes:
        before: state prior to the change. Only meaningful for ``UPDATE``/
            ``DELETE``; ``None`` otherwise (or when the source doesn't
            capture prior state). On the wire, ``None`` is represented as an
            ``opencdc.Data`` message with neither ``oneof`` branch set --
            see :mod:`conduit._grpc.adapters`.
        after: state following the change. Meaningful for every operation
            except ``DELETE``.
    """

    before: Data | None = None
    after: Data | None = None


@dataclass(slots=True)
class Record:
    """A single OpenCDC record: one row/event flowing through a pipeline.

    Attributes:
        position: opaque, connector-defined cursor identifying this record's
            place in the source. Round-tripped byte-for-byte; the SDK never
            interprets its contents. See invariant 2 (positions are
            monotonic and crash-safe) -- resuming correctly from a
            previously emitted ``position`` is the source author's
            responsibility, enforced by the acceptance harness's
            resume-at-position tests (:mod:`conduit.testing.acceptance`).
        operation: the kind of change this record represents.
        metadata: string key/value pairs. See :class:`Metadata` for
            well-known keys and typed accessors.
        key: the record's key, used for partitioning/routing downstream.
            Defaults to empty bytes (not ``None`` -- unlike ``Change``'s
            fields, a record always has *some* key on the wire, even if
            empty).
        payload: the before/after change payload.
    """

    position: bytes
    operation: Operation
    metadata: dict[str, str] = field(default_factory=dict)
    key: Data = b""
    payload: Change = field(default_factory=Change)


class Metadata:
    """Well-known OpenCDC metadata keys, mirroring Go's ``opencdc`` constants.

    **Sourcing note (see the v0.19 build task's "wire contract facts"):**
    ``opencdc_pb2.pyi`` exposes these well-known keys as protobuf extension
    ``FieldDescriptor`` objects (e.g. ``metadata_created_at``) carrying the
    actual dotted key name as an extension option value on the message
    descriptor -- not as plain Python strings. Reflecting on those
    descriptors at runtime to recover the option value is possible but adds
    real fragility (undocumented internal protobuf API surface) for a value
    that is already public, stable, and documented on the Go side
    (``conduit-commons``' ``opencdc`` package constants). This module
    hardcodes the literal dotted strings instead, matching Go's naming
    convention (``opencdc.<name>``). If a future ``compat-nightly`` run
    against ``conduit-commons`` HEAD finds one of these has drifted, fix the
    literal here -- there is no runtime linkage to the extension descriptors
    that would otherwise catch drift automatically. This is a documented,
    accepted tradeoff, not an oversight.

    Only a representative subset gets typed accessors (§2.3: "don't need
    every Go helper, a representative subset is fine for v0.19 core") --
    ``created_at``/``read_at``/``collection``. Every well-known key still
    gets a string constant even without a matching typed accessor, so
    authors can always read/write via the plain ``dict`` if they need one
    this module doesn't wrap yet.
    """

    OPENCDC_VERSION = "opencdc.version"
    CREATED_AT = "opencdc.createdAt"
    READ_AT = "opencdc.readAt"
    COLLECTION = "opencdc.collection"
    KEY_SCHEMA_SUBJECT = "opencdc.key.schema.subject"
    KEY_SCHEMA_VERSION = "opencdc.key.schema.version"
    PAYLOAD_SCHEMA_SUBJECT = "opencdc.payload.schema.subject"
    PAYLOAD_SCHEMA_VERSION = "opencdc.payload.schema.version"
    FILE_NAME = "opencdc.file.name"
    FILE_SIZE = "opencdc.file.size"
    FILE_HASH = "opencdc.file.hash"
    FILE_CHUNKED = "opencdc.file.chunked"
    FILE_CHUNK_INDEX = "opencdc.file.chunk.index"
    FILE_CHUNK_COUNT = "opencdc.file.chunk.count"

    @staticmethod
    def set_created_at(metadata: dict[str, str], nanos_since_epoch: int) -> None:
        """Set the ``opencdc.createdAt`` key.

        Args:
            metadata: the ``Record.metadata`` dict to mutate in place.
            nanos_since_epoch: creation time as integer nanoseconds since the
                Unix epoch -- matches the design doc §2.7 worked example's
                convention (``str(int(datetime.now(UTC).timestamp() * 1e9))``).
        """
        metadata[Metadata.CREATED_AT] = str(nanos_since_epoch)

    @staticmethod
    def get_created_at(metadata: Mapping[str, str]) -> int | None:
        """Read the ``opencdc.createdAt`` key.

        Returns:
            Nanoseconds since the Unix epoch, or ``None`` if unset.
        """
        raw = metadata.get(Metadata.CREATED_AT)
        return int(raw) if raw is not None else None

    @staticmethod
    def set_read_at(metadata: dict[str, str], nanos_since_epoch: int) -> None:
        """Set the ``opencdc.readAt`` key.

        Args:
            metadata: the ``Record.metadata`` dict to mutate in place.
            nanos_since_epoch: read time as integer nanoseconds since the
                Unix epoch.
        """
        metadata[Metadata.READ_AT] = str(nanos_since_epoch)

    @staticmethod
    def get_read_at(metadata: Mapping[str, str]) -> int | None:
        """Read the ``opencdc.readAt`` key.

        Returns:
            Nanoseconds since the Unix epoch, or ``None`` if unset.
        """
        raw = metadata.get(Metadata.READ_AT)
        return int(raw) if raw is not None else None

    @staticmethod
    def set_collection(metadata: dict[str, str], collection: str) -> None:
        """Set the ``opencdc.collection`` key (table/topic/collection name).

        Args:
            metadata: the ``Record.metadata`` dict to mutate in place.
            collection: the source collection name.
        """
        metadata[Metadata.COLLECTION] = collection

    @staticmethod
    def get_collection(metadata: Mapping[str, str]) -> str | None:
        """Read the ``opencdc.collection`` key.

        Returns:
            The collection name, or ``None`` if unset.
        """
        return metadata.get(Metadata.COLLECTION)
