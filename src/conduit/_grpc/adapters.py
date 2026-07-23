"""Hand-written adapters between wire (proto) messages and Python dataclasses.

**Not generated.** This module lives inside ``_grpc/`` because it is
protocol glue tightly coupled to the generated stubs, but it is
hand-written, ordinary application code -- held to the repo's normal
lint/mypy-strict/docstring bar, unlike its siblings
(``*_pb2*.py``/``*.pyi``), which are vendored codegen output excluded from
those checks (see ``pyproject.toml`` and this package's own
``__init__.py``).

See ``docs/design/20260707-python-connector-sdk.md`` §1.4/§2.3 for the wire
shape this translates and the B3 ``google.protobuf.Struct`` int->float
fidelity caveat, which is implemented (and documented) at its one exact
conversion site below: :func:`_data_to_proto`.
"""

from __future__ import annotations

from collections.abc import Iterable, Mapping

from google.protobuf import struct_pb2
from google.protobuf.json_format import MessageToDict

import conduit._grpc  # noqa: F401  -- sets up sys.path, see conduit._grpc.__init__
from conduit.record import Change, Data, Operation, Record
from opencdc.v1 import opencdc_pb2

__all__ = [
    "config_map_from_proto",
    "record_from_proto",
    "record_to_proto",
    "records_from_proto",
    "records_to_proto",
]


def _data_to_proto(data: Data) -> opencdc_pb2.Data:
    """Encode a Python ``Data`` (``bytes | Mapping[str, Any]``) as wire ``Data``.

    **B3 fidelity boundary (design doc §2.3):** when ``data`` is a
    ``Mapping``, it is encoded via ``google.protobuf.Struct.update()``,
    which represents every JSON-like value including integers as a
    double-precision ``number_value``. Integers beyond ``2**53`` lose
    precision here -- **silently**, with no exception raised -- unlike
    passing raw ``bytes`` inside a structured mapping, which fails loudly
    with a ``ValueError`` at this exact call (``Struct`` has no bytes
    representation). ``tests/test_record_codec.py`` pins the integer case
    with a Hypothesis test that asserts exact round-trip identity for raw
    bytes and explicitly demonstrates (not merely tolerates) the int->float
    precision loss for large integers, per the design doc's B3 requirement.

    Args:
        data: raw bytes, or a JSON-like structured mapping.

    Returns:
        The wire ``opencdc.Data`` message with the corresponding ``oneof``
        branch set.
    """
    if isinstance(data, bytes):
        return opencdc_pb2.Data(raw_data=data)
    struct = struct_pb2.Struct()
    struct.update(data)  # <-- B3: int -> float precision loss happens here.
    return opencdc_pb2.Data(structured_data=struct)


def _data_from_proto(data: opencdc_pb2.Data) -> Data:
    """Decode a wire ``Data`` message to a Python ``Data``.

    Uses ``WhichOneof`` rather than checking field presence/truthiness, so
    an explicitly-set empty ``raw_data = b""`` correctly decodes to
    ``b""``, not to a structured empty dict.

    Args:
        data: the wire ``opencdc.Data`` message.

    Returns:
        ``bytes`` if the ``raw_data`` branch is set, otherwise a ``dict``
        decoded from the ``structured_data`` branch (via
        ``MessageToDict``, which recursively converts nested ``Struct``/
        ``ListValue`` to plain Python containers -- see :func:`_data_to_proto`
        for where the corresponding, one-directional precision loss is
        introduced on encode).
    """
    which = data.WhichOneof("data")
    if which == "raw_data":
        return data.raw_data
    if which == "structured_data":
        return MessageToDict(data.structured_data)
    # Neither oneof branch set -- an explicitly "empty" Data on the wire.
    # Records always carry *some* key (§2.3: `key: Data = b""`), so this
    # decodes to empty bytes, matching that default.
    return b""


def _data_to_proto_optional(data: Data | None) -> opencdc_pb2.Data:
    """Encode ``Data | None`` (``Change.before``/``after``) to wire ``Data``.

    ``None`` becomes a ``Data`` message with neither ``oneof`` branch set --
    the wire has no explicit "absent" representation for ``Change``'s
    fields beyond that, matching proto3 message-field-as-optional semantics.
    """
    if data is None:
        return opencdc_pb2.Data()
    return _data_to_proto(data)


def _data_from_proto_optional(data: opencdc_pb2.Data) -> Data | None:
    """Decode wire ``Data`` to ``Data | None`` for ``Change.before``/``after``.

    Returns ``None`` when neither ``oneof`` branch is set, distinguishing
    "absent" from an explicit empty payload -- see :func:`_data_from_proto`,
    which cannot make this distinction for ``Record.key`` (always present).
    """
    if data.WhichOneof("data") is None:
        return None
    return _data_from_proto(data)


def _change_to_proto(change: Change) -> opencdc_pb2.Change:
    """Encode a :class:`~conduit.record.Change` to wire ``opencdc.Change``."""
    return opencdc_pb2.Change(
        before=_data_to_proto_optional(change.before),
        after=_data_to_proto_optional(change.after),
    )


def _change_from_proto(change: opencdc_pb2.Change) -> Change:
    """Decode a wire ``opencdc.Change`` to :class:`~conduit.record.Change`."""
    return Change(
        before=_data_from_proto_optional(change.before),
        after=_data_from_proto_optional(change.after),
    )


def record_to_proto(record: Record) -> opencdc_pb2.Record:
    """Encode a :class:`~conduit.record.Record` to wire ``opencdc.Record``.

    Args:
        record: the Python-side record.

    Returns:
        The equivalent wire message.
    """
    return opencdc_pb2.Record(
        position=record.position,
        operation=record.operation.value,
        metadata=dict(record.metadata),
        key=_data_to_proto(record.key),
        payload=_change_to_proto(record.payload),
    )


def record_from_proto(record: opencdc_pb2.Record) -> Record:
    """Decode a wire ``opencdc.Record`` to :class:`~conduit.record.Record`.

    Args:
        record: the wire-side record.

    Returns:
        The equivalent Python dataclass.
    """
    return Record(
        position=record.position,
        operation=Operation(record.operation),
        metadata=dict(record.metadata),
        key=_data_from_proto(record.key),
        payload=_change_from_proto(record.payload),
    )


def records_to_proto(records: Iterable[Record]) -> list[opencdc_pb2.Record]:
    """Encode an iterable of :class:`~conduit.record.Record` to wire records."""
    return [record_to_proto(r) for r in records]


def records_from_proto(records: Iterable[opencdc_pb2.Record]) -> list[Record]:
    """Decode an iterable of wire records to :class:`~conduit.record.Record`."""
    return [record_from_proto(r) for r in records]


def config_map_from_proto(config: Mapping[str, str]) -> dict[str, str]:
    """Copy a wire ``map<string, string>`` (``ScalarMap``) into a plain ``dict``.

    A thin wrapper (rather than passing the ``ScalarMap`` proxy directly to
    ``BaseConfig.model_validate``) so callers never hold a reference into
    the proto message's internal map storage past the request's lifetime.
    """
    return dict(config)
