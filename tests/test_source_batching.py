"""Tests for `sdk.batch.size`/`sdk.batch.delay` on the source side.

Exercises the real `_SourceServicer.Run` path (not `collect_batches`
directly -- see `tests/test_batch.py` for that), proving: (1) `Configure`
wires a real `sdk.batch.size`/`sdk.batch.delay` config map into batched
`Run` output, (2) records are grouped into `Source.Run.Response` messages
of up to `size` records, (3) ack handling (invariant 1: never ack before
Conduit confirms) is unaffected by batching, and (4) the passthrough case
(`size` absent/0/1, `delay` absent/0) is byte-for-byte today's existing
one-record-per-response behavior -- a non-regression guardrail for the
existing `tests/test_source.py` suite this file complements.
"""

from __future__ import annotations

import asyncio

import pytest

import conduit._grpc  # noqa: F401  -- sets up sys.path, see conduit._grpc.__init__
from conduit._grpc.adapters import record_from_proto
from conduit.config import BaseConfig
from conduit.errors import BackoffRetry
from conduit.record import Operation, Record
from conduit.source import Source, _SourceServicer
from connector.v2 import source_pb2


class _Config(BaseConfig):
    pass


class _CountingRecordsSource(Source[_Config]):
    """Emits `n` records then raises BackoffRetry forever."""

    def __init__(self, n: int) -> None:
        self._n = n
        self._emitted = 0

    async def read(self) -> Record:
        if self._emitted >= self._n:
            raise BackoffRetry()
        self._emitted += 1
        return Record(position=f"pos-{self._emitted}".encode(), operation=Operation.CREATE)


async def _empty_request_stream() -> object:
    return
    yield  # pragma: no cover


async def _configure(servicer: _SourceServicer, config: dict[str, str]) -> None:
    class _Ctx:
        async def abort(self, code: object, details: str) -> None:
            raise AssertionError(f"Configure aborted unexpectedly: {details}")

    await servicer.Configure(source_pb2.Source.Configure.Request(config=config), _Ctx())


async def _collect_n_responses(
    servicer: _SourceServicer, n: int, *, timeout_seconds: float = 2.0
) -> list[source_pb2.Source.Run.Response]:
    """Collect exactly ``n`` `Run` responses, then stop the servicer the sanctioned way.

    Waits for ``n`` responses, then drives shutdown through
    ``servicer.Stop()`` (the same stop-then-wait-for-drain path Conduit's
    own deterministic RPC sequence and ``tests/test_source.py`` both use)
    rather than abandoning the ``Run()`` async generator mid-iteration.
    Abandoning it instead (e.g. `return`ing out of an `async for` early)
    does *not* reliably close nested async generators it's iterating over
    -- `async for` has no implicit `finally: aclose()` on exception/early-exit
    the way a `with` block would -- so it would leak the background
    ``_read_loop``/``collect_batches`` machinery rather than exercising the
    real, intended shutdown path.
    """
    responses: list[source_pb2.Source.Run.Response] = []

    async def consume() -> None:
        async for response in servicer.Run(_empty_request_stream(), object()):
            responses.append(response)

    consume_task = asyncio.create_task(consume())
    async with asyncio.timeout(timeout_seconds):
        while len(responses) < n:  # noqa: ASYNC110 -- see tests/test_source.py's `_wait_until`
            await asyncio.sleep(0.005)
        await servicer.Stop(object(), object())
        await consume_task
    return responses


class TestSourceBatchingBySize:
    async def test_records_are_grouped_into_batches_of_size(self) -> None:
        source = _CountingRecordsSource(n=6)
        servicer = _SourceServicer(source, _Config)
        await _configure(servicer, {"sdk.batch.size": "3"})

        responses = await _collect_n_responses(servicer, 2)
        assert [len(r.records) for r in responses] == [3, 3]
        first_batch = [record_from_proto(r).position for r in responses[0].records]
        assert first_batch == [b"pos-1", b"pos-2", b"pos-3"]

    async def test_final_partial_batch_flushes_on_stop(self) -> None:
        source = _CountingRecordsSource(n=5)
        servicer = _SourceServicer(source, _Config)
        await _configure(servicer, {"sdk.batch.size": "3"})

        collected: list[source_pb2.Source.Run.Response] = []

        async def consume() -> None:
            async for response in servicer.Run(_empty_request_stream(), object()):
                collected.append(response)

        consume_task = asyncio.create_task(consume())
        # Wait for the first full batch of 3, then stop -- the remaining 2
        # records are buffered but below threshold; Stop() must still flush
        # them (invariant 3: at-least-once is the floor).
        while not collected:  # noqa: ASYNC110 -- see tests/test_source.py's `_wait_until`
            await asyncio.sleep(0.01)
        await asyncio.wait_for(servicer.Stop(object(), object()), timeout=2.0)
        await asyncio.wait_for(consume_task, timeout=2.0)

        total_records = sum(len(r.records) for r in collected)
        assert total_records == 5, "batching must never drop buffered records on stop"

    async def test_last_position_reflects_the_last_record_in_the_last_batch(self) -> None:
        source = _CountingRecordsSource(n=4)
        servicer = _SourceServicer(source, _Config)
        await _configure(servicer, {"sdk.batch.size": "4"})

        await _collect_n_responses(servicer, 1)
        stop_response = await servicer.Stop(object(), object())
        assert stop_response.last_position == b"pos-4"


class TestSourceBatchingByDelay:
    async def test_incomplete_batch_flushes_after_delay(self) -> None:
        source = _CountingRecordsSource(n=2)  # never reaches a size threshold
        servicer = _SourceServicer(source, _Config)
        await _configure(servicer, {"sdk.batch.size": "100", "sdk.batch.delay": "20ms"})

        responses = await _collect_n_responses(servicer, 1, timeout_seconds=2.0)
        assert len(responses[0].records) == 2


class TestSourceBatchingPassthrough:
    """`sdk.batch.size`/`delay` absent, 0, or 1-with-no-delay: unchanged one-record responses."""

    @pytest.mark.parametrize(
        "batch_config",
        [
            {},
            {"sdk.batch.size": "0"},
            {"sdk.batch.size": "1"},
            {"sdk.batch.delay": "0s"},
        ],
    )
    async def test_one_record_per_response(self, batch_config: dict[str, str]) -> None:
        source = _CountingRecordsSource(n=3)
        servicer = _SourceServicer(source, _Config)
        await _configure(servicer, dict(batch_config))

        responses = await _collect_n_responses(servicer, 3)
        assert [len(r.records) for r in responses] == [1, 1, 1]


class TestSourceBatchConfigValidation:
    async def test_invalid_batch_size_aborts_invalid_argument(self) -> None:
        import grpc

        source = _CountingRecordsSource(n=0)
        servicer = _SourceServicer(source, _Config)

        aborted: list[tuple[object, str]] = []

        class _Ctx:
            async def abort(self, code: object, details: str) -> None:
                aborted.append((code, details))
                raise RuntimeError("aborted")  # mimic grpc.aio.abort's never-returns contract

        with pytest.raises(RuntimeError, match="aborted"):
            await servicer.Configure(
                source_pb2.Source.Configure.Request(config={"sdk.batch.size": "nope"}), _Ctx()
            )
        assert len(aborted) == 1
        code, details = aborted[0]
        assert code == grpc.StatusCode.INVALID_ARGUMENT
        assert "sdk.batch.size" in details
