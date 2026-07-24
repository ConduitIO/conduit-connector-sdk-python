"""Tests for :mod:`conduit.destination` -- the B1 partial-batch-write fix.

``test_destination_partial_write_nacks_all`` is the concrete, automated form
of the design doc's B1 fix (§2.5): an incomplete/absent per-index accounting
must nack the *entire* batch, never a silently-assumed-successful prefix.
"""

from __future__ import annotations

from typing import ClassVar

import pytest

from conduit.config import BaseConfig
from conduit.destination import Destination, _DestinationServicer
from conduit.errors import BatchWriteError
from conduit.record import Operation, Record


class _Config(BaseConfig):
    pass


def _records(n: int) -> list[Record]:
    return [Record(position=f"pos-{i}".encode(), operation=Operation.CREATE) for i in range(n)]


class _WholeBatchSucceeds(Destination[_Config]):
    async def write(self, records: list[Record]) -> None:
        return None


class _RaisesPlainException(Destination[_Config]):
    async def write(self, records: list[Record]) -> None:
        raise RuntimeError("connector bug: exploded mid-batch")


class _RaisesWrittenPrefix(Destination[_Config]):
    def __init__(self, written: int) -> None:
        self._written = written

    async def write(self, records: list[Record]) -> None:
        raise BatchWriteError(len(records), written=self._written)


class _RaisesExplicitAccounting(Destination[_Config]):
    def __init__(self, success: set[int], failures: dict[int, BaseException]) -> None:
        self._success = success
        self._failures = failures

    async def write(self, records: list[Record]) -> None:
        raise BatchWriteError(len(records), success=self._success, failures=self._failures)


class _RaisesNaiveIncompleteMapping(Destination[_Config]):
    """Simulates the exact bug B1 exists to prevent.

    A naive connector (or a hypothetical naive adapter) might construct an
    exception carrying only a partial map of *known* failures and expect
    "everything else succeeded" to be inferred. ``BatchWriteError`` refuses
    to construct that shape at all (see ``tests/test_errors.py``), so this
    double-checks the servicer's behavior specifically for a
    non-``BatchWriteError`` exception that merely *looks* like it might
    carry partial info (e.g. a custom exception with a ``.failures``
    attribute) -- the servicer must still nack the whole batch, because it
    only ever branches on ``isinstance(exc, BatchWriteError)``, never on
    duck-typed attributes.
    """

    class _LooksLikeBatchWriteErrorButIsnt(RuntimeError):
        failures: ClassVar[dict[int, str]] = {2: "only this one failed, allegedly"}

    async def write(self, records: list[Record]) -> None:
        raise self._LooksLikeBatchWriteErrorButIsnt("nope")


async def _write_batch(
    destination: Destination[_Config], n: int
) -> tuple[list[Record], list[object]]:
    servicer = _DestinationServicer(destination, _Config)
    records = _records(n)
    acks = await servicer._write_batch(records)
    return records, acks


class TestDestinationPartialWriteNacksAll:
    """AC #1 (design doc): the headline B1 regression-proof test."""

    async def test_incomplete_accounting_is_impossible_to_construct(self) -> None:
        """(a) An incomplete/absent accounting cannot even be constructed.

        This is the strongest possible version of "an incomplete accounting
        nacks the whole batch, not a silently-assumed-successful prefix":
        the adapter never gets the chance to make that mistake, because
        ``BatchWriteError`` itself refuses to exist in an incomplete state
        (see ``tests/test_errors.py`` for the exhaustive construction-time
        matrix). Demonstrated here in the exact shape a buggy connector
        might attempt: reporting only known failures with no explicit
        success set.
        """
        with pytest.raises(ValueError, match="must supply"):
            BatchWriteError(5, failures={2: RuntimeError("boom")})  # type: ignore[call-overload]

    async def test_non_batch_write_error_exception_nacks_everything(self) -> None:
        """(c) A plain (non-``BatchWriteError``) exception nacks the ENTIRE batch."""
        records, acks = await _write_batch(_RaisesPlainException(), 4)
        assert len(acks) == 4
        for record, ack in zip(records, acks, strict=True):
            assert ack.position == record.position
            assert ack.error != ""
            assert "exploded mid-batch" in ack.error

    async def test_exception_that_merely_resembles_batch_write_error_nacks_everything(
        self,
    ) -> None:
        """Duck-typing a ``.failures`` attribute does not grant partial credit.

        Only ``isinstance(exc, BatchWriteError)`` grants the partial-ack
        path -- anything else, however similar-looking, nacks the whole
        batch. This is what "banned by construction, not merely documented"
        means in practice at the adapter's actual branch point.
        """
        _records_arg, acks = await _write_batch(_RaisesNaiveIncompleteMapping(), 4)
        assert len(acks) == 4
        assert all(ack.error != "" for ack in acks)

    async def test_written_prefix_acks_exactly_that_prefix_nacks_the_rest(self) -> None:
        """(b) A well-formed partial success (``written=N``) acks ``[0, N)``, nacks the rest."""
        records, acks = await _write_batch(_RaisesWrittenPrefix(written=2), 5)
        assert len(acks) == 5
        for i, (record, ack) in enumerate(zip(records, acks, strict=True)):
            assert ack.position == record.position
            if i < 2:
                assert ack.error == ""
            else:
                assert ack.error != ""

    async def test_explicit_noncontiguous_accounting_acks_exactly_the_success_set(self) -> None:
        """A non-contiguous explicit ``success``/``failures`` split is honored precisely."""
        failures: dict[int, BaseException] = {1: ValueError("a"), 3: ValueError("b")}
        _records_arg, acks = await _write_batch(
            _RaisesExplicitAccounting(success={0, 2, 4}, failures=failures),
            5,
        )
        for i, ack in enumerate(acks):
            if i in (0, 2, 4):
                assert ack.error == "", f"index {i} should be acked"
            else:
                assert ack.error != "", f"index {i} should be nacked"
        assert "a" in acks[1].error
        assert "b" in acks[3].error

    async def test_full_batch_success_acks_everything(self) -> None:
        records, acks = await _write_batch(_WholeBatchSucceeds(), 3)
        assert len(acks) == 3
        for record, ack in zip(records, acks, strict=True):
            assert ack.position == record.position
            assert ack.error == ""


class TestDestinationConfigResolution:
    async def test_configure_validates_and_stores_config(self) -> None:
        class Config(BaseConfig):
            url: str

        class MyDestination(Destination[Config]):
            async def write(self, records: list[Record]) -> None:
                return None

        instance = MyDestination()
        await instance.configure(Config(url="https://example.com"))
        assert instance.config.url == "https://example.com"


async def test_stop_and_teardown_defaults_are_no_ops() -> None:
    class MyDestination(Destination[_Config]):
        async def write(self, records: list[Record]) -> None:
            return None

    instance = MyDestination()
    await instance.open()
    await instance.teardown()
    await instance.on_created({})
    await instance.on_updated({}, {})
    await instance.on_deleted({})


def test_destination_is_abstract_without_write() -> None:
    with pytest.raises(TypeError):
        Destination()  # type: ignore[abstract]
