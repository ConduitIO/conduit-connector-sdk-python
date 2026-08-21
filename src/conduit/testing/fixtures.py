"""Golden OpenCDC record-shape fixtures for connector tests.

A small, representative set of record shapes -- one per :class:`~conduit.record.Operation`
-- so a connector's own tests (and :mod:`conduit.testing.acceptance`) exercise
the same shapes consistently, rather than every test author hand-rolling
slightly different ad hoc records. Per
``docs/design/20260707-python-connector-sdk.md`` §3/Phase 2: a full corpus
shared with the Go/other-language acceptance suites is fast-follow (v0.20)
scope; this module is the Phase-1 seed of that idea, scoped to what this
repo's own acceptance harness and example connector need today.
"""

from __future__ import annotations

from conduit.record import Change, Metadata, Operation, Record


def snapshot_record(position: bytes, key: str, value: dict[str, object]) -> Record:
    """A bulk/backfill-style record: ``OPERATION_SNAPSHOT``, no ``before``.

    Args:
        position: the record's position.
        key: the record's key (wrapped as ``{"id": key}``).
        value: the row's current value, used as ``payload.after``.
    """
    return Record(
        position=position,
        operation=Operation.SNAPSHOT,
        key={"id": key},
        payload=Change(after=value),
    )


def create_record(position: bytes, key: str, value: dict[str, object]) -> Record:
    """A newly-inserted-row record: ``OPERATION_CREATE``, no ``before``."""
    return Record(
        position=position,
        operation=Operation.CREATE,
        key={"id": key},
        payload=Change(after=value),
    )


def update_record(
    position: bytes,
    key: str,
    before: dict[str, object],
    after: dict[str, object],
) -> Record:
    """A modified-row record: ``OPERATION_UPDATE``, both ``before`` and ``after`` set."""
    return Record(
        position=position,
        operation=Operation.UPDATE,
        key={"id": key},
        payload=Change(before=before, after=after),
    )


def delete_record(position: bytes, key: str, before: dict[str, object]) -> Record:
    """A removed-row record: ``OPERATION_DELETE``, no ``after`` -- prior state is captured.

    Contrast with :func:`tombstone_record`: this shape is what most CDC
    sources actually emit for a delete (Debezium-style "before image"),
    carrying ``payload.before`` for downstream consumers that want it.
    """
    return Record(
        position=position,
        operation=Operation.DELETE,
        key={"id": key},
        payload=Change(before=before),
    )


def tombstone_record(position: bytes, key: str) -> Record:
    """A Kafka-Connect-style tombstone: ``OPERATION_DELETE``, key present, **no payload at all**.

    Distinct from :func:`delete_record` -- a tombstone carries neither
    ``before`` nor ``after`` (both ``None``, the wire's "absent" ``Data``
    representation -- see ``conduit._grpc.adapters._data_from_proto_optional``),
    matching the Kafka Connect/log-compaction convention of "key present,
    value entirely null" as the deletion marker, rather than a before-image
    a CDC-style delete carries. One of the three record shapes the
    acceptance suite's record-shape category exercises (raw, structured,
    tombstone) -- see ``conduit.testing.acceptance``.

    Args:
        position: the record's position.
        key: the record's key (wrapped as ``{"id": key}``, matching this
            module's other structured-key fixtures).
    """
    return Record(
        position=position,
        operation=Operation.DELETE,
        key={"id": key},
        payload=Change(before=None, after=None),
    )


def raw_record(
    position: bytes, key: bytes, value: bytes, operation: Operation = Operation.CREATE
) -> Record:
    """A record with a raw-``bytes`` key and payload, not the structured (``dict``) shape.

    Every other fixture in this module uses a structured (``dict``) key/
    value -- this one exists specifically to exercise the ``raw`` record
    shape (see ``conduit.record.Data``'s two-way ``bytes | Mapping``
    contract), the counterpart to :func:`create_record`/etc.'s structured
    shape in the acceptance suite's record-shape category.

    Args:
        position: the record's position.
        key: the record's raw-bytes key.
        value: the raw-bytes payload, used as ``payload.after``.
        operation: the record's operation; defaults to ``CREATE``.
    """
    return Record(
        position=position,
        operation=operation,
        key=key,
        payload=Change(after=value),
    )


def with_collection(record: Record, collection: str) -> Record:
    """Return a copy of ``record`` with ``opencdc.collection`` metadata set.

    Args:
        record: the record to annotate. Not mutated in place -- a shallow
            copy with a new ``metadata`` dict is returned, so callers reusing
            a shared fixture record across cases don't see cross-test
            mutation.
        collection: the source collection (table/topic) name.
    """
    metadata = dict(record.metadata)
    Metadata.set_collection(metadata, collection)
    return Record(
        position=record.position,
        operation=record.operation,
        metadata=metadata,
        key=record.key,
        payload=record.payload,
    )
