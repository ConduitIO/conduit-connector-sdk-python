"""SDK-level tests over the three record shapes: raw, structured, tombstone.

Complements `tests/test_record_codec.py` (which proves the wire codec
round-trips each shape byte-for-byte in isolation) by proving the full
`_SourceServicer`/`_DestinationServicer` adapter paths -- ack handling,
batching, write/nack accounting -- behave correctly for each shape, not
just the codec underneath them. `raw`/`structured` cover
`conduit.record.Data`'s two-way `bytes | Mapping` contract (design doc
§2.3); `tombstone` covers a `DELETE` record with no payload at all (see
`conduit.testing.fixtures.tombstone_record`), the shape most likely to
trip up code that assumes `payload.after` (or `before`) is always present.
"""

from __future__ import annotations

import asyncio

import conduit._grpc  # noqa: F401  -- sets up sys.path, see conduit._grpc.__init__
from conduit._grpc.adapters import record_from_proto
from conduit.config import BaseConfig
from conduit.destination import Destination, _DestinationServicer
from conduit.errors import BackoffRetry
from conduit.record import Record
from conduit.source import Source, _SourceServicer
from conduit.testing.fixtures import create_record, raw_record, tombstone_record

_RAW = raw_record(b"pos-raw", b"raw-key", b"raw-value")
_STRUCTURED = create_record(b"pos-structured", "1", {"id": "1", "name": "widget"})
_TOMBSTONE = tombstone_record(b"pos-tombstone", "1")


class _Config(BaseConfig):
    pass


class TestSourceEmitsAllThreeShapesCorrectly:
    """`_SourceServicer.Run` preserves each shape end-to-end through the wire codec."""

    async def test_raw_structured_and_tombstone_records_round_trip_through_run(self) -> None:
        emitted = [_RAW, _STRUCTURED, _TOMBSTONE]

        class _ShapeSource(Source[_Config]):
            def __init__(self) -> None:
                self._records = iter(emitted)

            async def read(self) -> Record:
                try:
                    return next(self._records)
                except StopIteration:
                    raise BackoffRetry() from None

        servicer = _SourceServicer(_ShapeSource(), _Config)

        async def empty_requests() -> object:
            return
            yield  # pragma: no cover

        received: list[Record] = []

        async def consume() -> None:
            async for response in servicer.Run(empty_requests(), object()):
                for proto_record in response.records:
                    received.append(record_from_proto(proto_record))

        # Consume on a background task -- `Stop()` below awaits the read
        # loop fully draining, which only happens once `Run()`'s generator
        # is itself driven to completion; calling `Stop()` from inside the
        # very loop consuming `Run()` would deadlock (see
        # `tests/test_source.py`'s tests for the same background-task
        # pattern).
        consume_task = asyncio.create_task(consume())
        while len(received) < len(emitted):  # noqa: ASYNC110 -- matches test_source.py's `_wait_until`
            await asyncio.sleep(0.005)
        await servicer.Stop(object(), object())
        await asyncio.wait_for(consume_task, timeout=2.0)

        assert len(received) == 3

        raw = received[0]
        assert raw.key == b"raw-key"
        assert isinstance(raw.key, bytes)
        assert raw.payload.after == b"raw-value"

        structured = received[1]
        assert isinstance(structured.key, dict)
        assert structured.key == {"id": "1"}
        assert structured.payload.after == {"id": "1", "name": "widget"}

        tombstone = received[2]
        assert tombstone.operation.name == "DELETE"
        assert tombstone.payload.before is None
        assert tombstone.payload.after is None


class TestDestinationWritesAllThreeShapesCorrectly:
    """`_DestinationServicer._write_batch` acks each shape without assuming payload presence."""

    async def test_raw_structured_and_tombstone_records_all_ack_cleanly(self) -> None:
        written: list[Record] = []

        class _ShapeDestination(Destination[_Config]):
            async def write(self, records: list[Record]) -> None:
                written.extend(records)

        servicer = _DestinationServicer(_ShapeDestination(), _Config)
        records = [_RAW, _STRUCTURED, _TOMBSTONE]
        acks = await servicer._write_batch(records)

        assert len(acks) == 3
        assert all(ack.error == "" for ack in acks)
        assert [ack.position for ack in acks] == [r.position for r in records]
        assert written == records

    async def test_a_destination_that_only_reads_key_and_after_does_not_crash_on_tombstone(
        self,
    ) -> None:
        """A tombstone's `payload.after`/`before` being `None` must not be treated as an error."""

        class _AssertsPayloadIsAbsent(Destination[_Config]):
            async def write(self, records: list[Record]) -> None:
                for record in records:
                    assert record.payload.after is None
                    assert record.payload.before is None

        servicer = _DestinationServicer(_AssertsPayloadIsAbsent(), _Config)
        acks = await servicer._write_batch([_TOMBSTONE])
        assert acks[0].error == ""
