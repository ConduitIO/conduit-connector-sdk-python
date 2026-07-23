"""Tests for :mod:`conduit.source` -- ack ordering (invariant 1) and backoff parity.

The central property under test: a source record's position is only ever
acknowledged to the connector (``Source.ack``) after Conduit's ``Run``
request stream sends it back via ``ack_positions`` -- never speculatively
when the record is merely produced by the read loop.
"""

from __future__ import annotations

import asyncio
from collections.abc import Callable

import pytest

from conduit._grpc.adapters import record_from_proto
from conduit.config import BaseConfig
from conduit.errors import BackoffRetry
from conduit.record import Operation, Record
from conduit.source import (
    BACKOFF_MAX_SECONDS,
    BACKOFF_MIN_SECONDS,
    Source,
    _Backoff,
    _SourceServicer,
)


class _Config(BaseConfig):
    pass


class _FakeRunContext:
    """Minimal stand-in for a grpc.aio ServicerContext -- unused by Run/Stop."""


async def _empty_request_stream() -> object:
    return
    yield  # pragma: no cover -- makes this an async generator with no items


class _AckPositionsRequest:
    def __init__(self, ack_positions: list[bytes]) -> None:
        self.ack_positions = ack_positions


class _CountingRecordsSource(Source[_Config]):
    """Emits ``n`` records then raises BackoffRetry forever."""

    def __init__(self, n: int) -> None:
        self._n = n
        self._emitted = 0
        self.acked: list[bytes] = []

    async def read(self) -> Record:
        if self._emitted >= self._n:
            raise BackoffRetry()
        self._emitted += 1
        return Record(position=f"pos-{self._emitted}".encode(), operation=Operation.CREATE)

    async def ack(self, position: bytes) -> None:
        self.acked.append(position)


async def _wait_until(predicate: Callable[[], bool], *, timeout_seconds: float = 2.0) -> None:
    """Poll an arbitrary boolean predicate until true or a timeout elapses.

    Deliberately polling, not waiting on a single ``asyncio.Event``: this
    helper is used against several different, unrelated conditions (record
    counts, ack lists) with no single event to await across all of them.
    """
    try:
        async with asyncio.timeout(timeout_seconds):
            while not predicate():  # noqa: ASYNC110 -- see docstring
                await asyncio.sleep(0.01)
    except TimeoutError:
        raise AssertionError(f"condition not met within {timeout_seconds}s") from None


class TestAckOnlyAfterConduitConfirms:
    async def test_ack_is_not_called_before_any_ack_positions_arrive(self) -> None:
        """Invariant 1: producing records must never itself call ack()."""
        source = _CountingRecordsSource(n=3)
        servicer = _SourceServicer(source, _Config)
        records: list[Record] = []

        async def consume() -> None:
            async for response in servicer.Run(_empty_request_stream(), _FakeRunContext()):
                for proto_record in response.records:
                    records.append(record_from_proto(proto_record))

        consume_task = asyncio.create_task(consume())
        await _wait_until(lambda: len(records) >= 3)

        # The read loop has produced 3 records; no ack_positions were ever
        # sent back (the request stream is empty), so ack() must not have
        # been called for any of them.
        assert source.acked == []

        await servicer.Stop(object(), _FakeRunContext())
        await asyncio.wait_for(consume_task, timeout=2)

    async def test_ack_is_called_only_for_positions_conduit_sends_back(self) -> None:
        source = _CountingRecordsSource(n=2)
        servicer = _SourceServicer(source, _Config)
        records: list[Record] = []
        ack_sent = asyncio.Event()

        async def request_stream() -> object:
            await ack_sent.wait()
            yield _AckPositionsRequest([b"pos-1"])

        async def consume() -> None:
            async for response in servicer.Run(request_stream(), _FakeRunContext()):
                for proto_record in response.records:
                    records.append(record_from_proto(proto_record))

        consume_task = asyncio.create_task(consume())
        await _wait_until(lambda: len(records) >= 2)
        assert source.acked == []  # still nothing acked before ack_positions arrives

        ack_sent.set()
        await _wait_until(lambda: source.acked == [b"pos-1"])

        await servicer.Stop(object(), _FakeRunContext())
        await asyncio.wait_for(consume_task, timeout=2)


class TestBackoff:
    def test_first_delay_is_min(self) -> None:
        backoff = _Backoff()
        assert backoff.duration() == BACKOFF_MIN_SECONDS

    def test_delay_doubles_each_attempt(self) -> None:
        backoff = _Backoff()
        first = backoff.duration()
        second = backoff.duration()
        third = backoff.duration()
        assert second == pytest.approx(first * 2)
        assert third == pytest.approx(first * 4)

    def test_delay_caps_at_max(self) -> None:
        backoff = _Backoff()
        delay = backoff.duration()
        for _ in range(20):
            delay = backoff.duration()
        assert delay == BACKOFF_MAX_SECONDS

    def test_reset_restarts_the_curve(self) -> None:
        backoff = _Backoff()
        backoff.duration()
        backoff.duration()
        backoff.reset()
        assert backoff.duration() == BACKOFF_MIN_SECONDS


async def test_stop_reports_last_emitted_position() -> None:
    source = _CountingRecordsSource(n=2)
    servicer = _SourceServicer(source, _Config)
    records: list[Record] = []

    async def consume() -> None:
        async for response in servicer.Run(_empty_request_stream(), _FakeRunContext()):
            for proto_record in response.records:
                records.append(record_from_proto(proto_record))

    consume_task = asyncio.create_task(consume())
    await _wait_until(lambda: len(records) >= 2)

    stop_response = await servicer.Stop(object(), _FakeRunContext())
    assert stop_response.last_position == b"pos-2"
    await asyncio.wait_for(consume_task, timeout=2)


def test_source_is_abstract_without_read() -> None:
    with pytest.raises(TypeError):
        Source()  # type: ignore[abstract]


async def test_configure_stores_config_by_default() -> None:
    class Config(BaseConfig):
        url: str

    class MySource(Source[Config]):
        async def read(self) -> Record:
            raise BackoffRetry()

    instance = MySource()
    await instance.configure(Config(url="https://example.com"))
    assert instance.config.url == "https://example.com"
