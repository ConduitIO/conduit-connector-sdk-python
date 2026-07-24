"""Tests for :mod:`conduit.testing.acceptance` itself -- run against a small
synthetic source and destination, proving the suite's categories actually
exercise real connector behavior (not stubs).
"""

from __future__ import annotations

from conduit.config import BaseConfig, Field
from conduit.destination import Destination
from conduit.errors import BackoffRetry
from conduit.record import Change, Operation, Record
from conduit.source import Source
from conduit.testing.acceptance import AcceptanceTestSuite, ConfigurableAcceptanceTestDriver


class _SourceConfig(BaseConfig):
    url: str = Field(description="required for the required-param-missing test")


class _DestinationConfig(BaseConfig):
    target: str = Field(description="required for the required-param-missing test")


class _InMemorySource(Source[_SourceConfig]):
    """A tiny in-memory source: emits integers 0, 1, 2, ... resuming after `position`."""

    async def open(self, position: bytes | None) -> None:
        self._next = int(position.decode()) + 1 if position else 0

    async def read(self) -> Record:
        if self._next > 100:
            raise BackoffRetry()
        value = self._next
        self._next += 1
        return Record(
            position=str(value).encode(),
            operation=Operation.CREATE if value > 0 else Operation.SNAPSHOT,
            key={"id": value},
            payload=Change(after={"id": value}),
        )


class _InMemoryDestination(Destination[_DestinationConfig]):
    def __init__(self) -> None:
        self.written: list[Record] = []

    async def write(self, records: list[Record]) -> None:
        self.written.extend(records)


class TestAcceptance(AcceptanceTestSuite):
    def driver(self) -> ConfigurableAcceptanceTestDriver:
        from conduit.config import Specification

        return ConfigurableAcceptanceTestDriver(
            spec=Specification(name="in-memory-test", version="0.0.0", author="test"),
            source_cls=_InMemorySource,
            destination_cls=_InMemoryDestination,
            source_cfg={"url": "https://example.com"},
            destination_cfg={"target": "table"},
        )
