"""Tests for `sdk.batch.size`/`sdk.batch.delay` on the destination side.

Exercises the real `_DestinationServicer.Run` path (not `collect_batches`
directly -- see `tests/test_batch.py` for that): batching must combine
several incoming `Destination.Run.Request` wire messages into one `write()`
call once `size`/`delay` is reached, and every ack still reaches the
corresponding record's position -- the B1 (`tests/test_destination.py`)
per-index accounting discipline applies identically whether the batch that
failed came from one incoming message or several combined by this SDK's own
batching middleware.
"""

from __future__ import annotations

import asyncio

import pytest

import conduit._grpc  # noqa: F401  -- sets up sys.path, see conduit._grpc.__init__
from conduit.config import BaseConfig
from conduit.destination import Destination, _DestinationServicer
from conduit.record import Operation, Record
from connector.v2 import destination_pb2
from opencdc.v1 import opencdc_pb2


class _Config(BaseConfig):
    pass


class _RecordingDestination(Destination[_Config]):
    def __init__(self) -> None:
        self.write_calls: list[list[bytes]] = []

    async def write(self, records: list[Record]) -> None:
        self.write_calls.append([r.position for r in records])


def _run_request(*positions: bytes) -> destination_pb2.Destination.Run.Request:
    records = [opencdc_pb2.Record(position=p, operation=Operation.CREATE.value) for p in positions]
    return destination_pb2.Destination.Run.Request(records=records)


async def _configure(servicer: _DestinationServicer, config: dict[str, str]) -> None:
    class _Ctx:
        async def abort(self, code: object, details: str) -> None:
            raise AssertionError(f"Configure aborted unexpectedly: {details}")

    await servicer.Configure(destination_pb2.Destination.Configure.Request(config=config), _Ctx())


async def _drive(
    servicer: _DestinationServicer, requests: list[destination_pb2.Destination.Run.Request]
) -> list[destination_pb2.Destination.Run.Response]:
    async def request_stream() -> object:
        for request in requests:
            yield request

    responses: list[destination_pb2.Destination.Run.Response] = []
    async for response in servicer.Run(request_stream(), object()):
        responses.append(response)
    return responses


class TestDestinationBatchingBySize:
    async def test_two_incoming_batches_combine_into_one_write_call(self) -> None:
        destination = _RecordingDestination()
        servicer = _DestinationServicer(destination, _Config)
        await _configure(servicer, {"sdk.batch.size": "4"})

        responses = await _drive(
            servicer,
            [_run_request(b"a", b"b"), _run_request(b"c", b"d")],
        )

        assert destination.write_calls == [[b"a", b"b", b"c", b"d"]]
        # One combined ack response covering all 4 records.
        assert len(responses) == 1
        assert [ack.position for ack in responses[0].acks] == [b"a", b"b", b"c", b"d"]
        assert all(ack.error == "" for ack in responses[0].acks)

    async def test_records_beyond_threshold_start_a_new_batch(self) -> None:
        destination = _RecordingDestination()
        servicer = _DestinationServicer(destination, _Config)
        await _configure(servicer, {"sdk.batch.size": "2"})

        responses = await _drive(
            servicer,
            [_run_request(b"a"), _run_request(b"b"), _run_request(b"c")],
        )

        assert destination.write_calls == [[b"a", b"b"], [b"c"]]
        assert len(responses) == 2

    async def test_partial_write_failure_nacks_correctly_across_a_combined_batch(self) -> None:
        """B1 applies identically when the failed batch spans two incoming wire messages."""
        from conduit.errors import BatchWriteError

        class _PartialWriteDestination(Destination[_Config]):
            async def write(self, records: list[Record]) -> None:
                raise BatchWriteError.partial(
                    len(records), written=2, cause=RuntimeError("destination unavailable")
                )

        servicer = _DestinationServicer(_PartialWriteDestination(), _Config)
        await _configure(servicer, {"sdk.batch.size": "4"})

        responses = await _drive(servicer, [_run_request(b"a", b"b"), _run_request(b"c", b"d")])
        acks = responses[0].acks
        assert acks[0].error == ""
        assert acks[1].error == ""
        assert acks[2].error != ""
        assert acks[3].error != ""


class TestDestinationBatchingByDelay:
    async def test_incomplete_batch_flushes_after_delay(self) -> None:
        destination = _RecordingDestination()
        servicer = _DestinationServicer(destination, _Config)
        await _configure(servicer, {"sdk.batch.size": "100", "sdk.batch.delay": "20ms"})

        released = asyncio.Event()

        async def request_stream() -> object:
            yield _run_request(b"a")
            await released.wait()

        responses: list[destination_pb2.Destination.Run.Response] = []

        async def consume() -> None:
            async for response in servicer.Run(request_stream(), object()):
                responses.append(response)
                released.set()  # let the (never-flushing) stream end after the first ack

        await asyncio.wait_for(consume(), timeout=2.0)
        assert destination.write_calls == [[b"a"]]
        assert responses[0].acks[0].position == b"a"


class TestDestinationBatchingFlushOnStop:
    async def test_buffered_remainder_is_flushed_when_the_stream_ends(self) -> None:
        """Invariant 3 (at-least-once): a below-threshold remainder must still be written+acked."""
        destination = _RecordingDestination()
        servicer = _DestinationServicer(destination, _Config)
        await _configure(servicer, {"sdk.batch.size": "10"})

        responses = await _drive(servicer, [_run_request(b"a"), _run_request(b"b")])

        assert destination.write_calls == [[b"a", b"b"]]
        assert len(responses) == 1
        assert [ack.position for ack in responses[0].acks] == [b"a", b"b"]


class TestDestinationBatchingPassthrough:
    """`sdk.batch.size`/`delay` absent, 0, or 1-with-no-delay: unchanged per-message writes."""

    @pytest.mark.parametrize(
        "batch_config",
        [
            {},
            {"sdk.batch.size": "0"},
            {"sdk.batch.size": "1"},
            {"sdk.batch.delay": "0s"},
        ],
    )
    async def test_each_incoming_message_is_written_and_acked_separately(
        self, batch_config: dict[str, str]
    ) -> None:
        destination = _RecordingDestination()
        servicer = _DestinationServicer(destination, _Config)
        await _configure(servicer, dict(batch_config))

        responses = await _drive(
            servicer, [_run_request(b"a"), _run_request(b"b"), _run_request(b"c")]
        )

        assert destination.write_calls == [[b"a"], [b"b"], [b"c"]]
        assert len(responses) == 3


class TestDestinationBatchConfigValidation:
    async def test_invalid_batch_delay_aborts_invalid_argument(self) -> None:
        import grpc

        servicer = _DestinationServicer(_RecordingDestination(), _Config)

        aborted: list[tuple[object, str]] = []

        class _Ctx:
            async def abort(self, code: object, details: str) -> None:
                aborted.append((code, details))
                raise RuntimeError("aborted")

        with pytest.raises(RuntimeError, match="aborted"):
            await servicer.Configure(
                destination_pb2.Destination.Configure.Request(
                    config={"sdk.batch.delay": "not-a-duration"}
                ),
                _Ctx(),
            )
        assert len(aborted) == 1
        code, details = aborted[0]
        assert code == grpc.StatusCode.INVALID_ARGUMENT
        assert "sdk.batch.delay" in details
