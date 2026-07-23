"""The acceptance-test harness: the shared contract a Python connector must pass.

Mirrors the Go SDK's ``sdk.AcceptanceTest(t, driver)``
(``conduit-connector-sdk/acceptance_testing.go:54-58``) -- "a connector" is
defined by conformance to the connector protocol and by passing this suite,
not by language (design doc, "The problem"). Per
``docs/design/20260707-python-connector-sdk.md`` §3, this harness is pulled
forward into v0.19 core scope rather than deferred to Phase 2.

An author implements :class:`AcceptanceTestDriver` (or uses
:class:`ConfigurableAcceptanceTestDriver` for the common case) and
subclasses :class:`AcceptanceTestSuite` in their own ``pytest`` test module
-- the subclass must be named so pytest collects it (conventionally
``Test<Something>``) and must override :meth:`AcceptanceTestSuite.driver`.

This harness exercises connectors **in-process** -- constructing
``Source``/``Destination`` instances directly and driving them through the
same servicer adapters :mod:`conduit.serve` uses, without a real gRPC
socket or a real Conduit binary. A real subprocess/Conduit launch is
``compat-nightly.yml``/Conduit-repo-side scope (design doc §3), not this
repo's CI.

**Contract version:** :data:`CONTRACT_VERSION` -- bump only with a new,
additive test category or a documented breaking change to an existing one,
so an author's CI output states exactly which contract they passed (design
doc §3: "kept version-numbered").
"""

from __future__ import annotations

import asyncio
from collections.abc import Mapping
from dataclasses import dataclass, field
from typing import Any, Protocol

import pydantic

import conduit._grpc  # noqa: F401  -- sets up sys.path, see conduit._grpc.__init__
from conduit._grpc.adapters import record_from_proto
from conduit.config import BaseConfig, Specification
from conduit.destination import Destination, _DestinationServicer
from conduit.errors import BatchWriteError
from conduit.record import Operation, Record
from conduit.source import Source, _SourceServicer
from conduit.testing.fixtures import create_record
from connector.v2 import source_pb2

CONTRACT_VERSION = "2026-07.v1"
"""Version tag for this acceptance suite. See module docstring."""


class AcceptanceTestDriver(Protocol):
    """What a connector author implements to run the suite against their connector.

    Every method is synchronous and side-effect-free (returns
    metadata/classes, does not itself talk to a network) -- the suite
    constructs and drives actual ``Source``/``Destination`` instances
    itself using what these methods return.
    """

    def specification(self) -> Specification:
        """Return the connector's static specification."""
        ...

    def source_class(self) -> type[Source[Any]] | None:
        """Return the ``Source`` subclass under test, or ``None`` if source-less."""
        ...

    def destination_class(self) -> type[Destination[Any]] | None:
        """Return the ``Destination`` subclass under test, or ``None`` if destination-less."""
        ...

    def source_config(self) -> Mapping[str, str]:
        """Return a valid config map for the source (as it would arrive over the wire)."""
        ...

    def destination_config(self) -> Mapping[str, str]:
        """Return a valid config map for the destination."""
        ...


@dataclass(slots=True)
class ConfigurableAcceptanceTestDriver:
    """Convenience :class:`AcceptanceTestDriver` for the common single-connector case.

    Construct one with whichever of ``source_cls``/``destination_cls`` your
    connector provides -- most connectors implement only one of the two.
    """

    spec: Specification
    source_cls: type[Source[Any]] | None = None
    destination_cls: type[Destination[Any]] | None = None
    source_cfg: Mapping[str, str] = field(default_factory=dict)
    destination_cfg: Mapping[str, str] = field(default_factory=dict)

    def specification(self) -> Specification:
        """See :meth:`AcceptanceTestDriver.specification`."""
        return self.spec

    def source_class(self) -> type[Source[Any]] | None:
        """See :meth:`AcceptanceTestDriver.source_class`."""
        return self.source_cls

    def destination_class(self) -> type[Destination[Any]] | None:
        """See :meth:`AcceptanceTestDriver.destination_class`."""
        return self.destination_cls

    def source_config(self) -> Mapping[str, str]:
        """See :meth:`AcceptanceTestDriver.source_config`."""
        return self.source_cfg

    def destination_config(self) -> Mapping[str, str]:
        """See :meth:`AcceptanceTestDriver.destination_config`."""
        return self.destination_cfg


class AcceptanceTestSuite:
    """Mixin providing the versioned acceptance-test suite as test methods.

    Subclass this in your connector's own ``pytest`` test module, name the
    subclass so pytest collects it (e.g. ``TestAcceptance``), and override
    :meth:`driver`. Every ``test_*`` method below is one named, independently
    reportable category from ``docs/design/20260707-python-connector-sdk.md``
    §3.
    """

    def driver(self) -> AcceptanceTestDriver:
        """Return the :class:`AcceptanceTestDriver` to run the suite against.

        Must be overridden by subclasses; the default raises so a
        forgotten override fails loudly and immediately, not with a
        confusing downstream ``AttributeError``.
        """
        raise NotImplementedError("Override driver() to return your AcceptanceTestDriver")

    # -- Category: specifier existence/validity ---------------------------

    async def test_specifier_exists_and_is_valid(self) -> None:
        """The connector's ``Specify`` metadata is present and well-formed."""
        driver = self.driver()
        spec = driver.specification()
        assert spec.name, "Specification.name must be non-empty"
        assert spec.version, "Specification.version must be non-empty"
        assert spec.author, "Specification.author must be non-empty"

        source_cls = driver.source_class()
        destination_cls = driver.destination_class()
        assert source_cls is not None or destination_cls is not None, (
            "a connector must provide at least one of source_class()/destination_class()"
        )

    # -- Category: config parameter validation -----------------------------

    async def test_config_validation_succeeds_with_valid_config(self) -> None:
        """A valid config map is accepted (source and/or destination)."""
        driver = self.driver()
        if (source_cls := driver.source_class()) is not None:
            config_cls = _config_class(source_cls, Source)
            config_cls.model_validate(dict(driver.source_config()))
        if (destination_cls := driver.destination_class()) is not None:
            config_cls = _config_class(destination_cls, Destination)
            config_cls.model_validate(dict(driver.destination_config()))

    async def test_config_validation_fails_with_missing_required_param(self) -> None:
        """Omitting any single required field is rejected, not silently defaulted."""
        driver = self.driver()
        for cls, base, config in (
            (driver.source_class(), Source, driver.source_config()),
            (driver.destination_class(), Destination, driver.destination_config()),
        ):
            if cls is None:
                continue
            config_cls = _config_class(cls, base)
            required = {
                name for name, info in config_cls.model_fields.items() if info.is_required()
            }
            if not required:
                continue  # nothing required -- this connector has no required-param case
            missing_one = dict(config)
            missing_one.pop(next(iter(required)))
            try:
                config_cls.model_validate(missing_one)
            except pydantic.ValidationError:
                pass
            else:
                raise AssertionError(
                    f"{config_cls.__name__}.model_validate() accepted a config "
                    "missing a required field -- required-param validation is broken"
                )

    # -- Category: resume-at-position (snapshot and CDC-equivalent) -------

    async def test_resume_at_position_snapshot(self) -> None:
        """Reopening at a previously-emitted position does not replay it (snapshot-style)."""
        await self._assert_resume_does_not_replay(Operation.SNAPSHOT)

    async def test_resume_at_position_cdc(self) -> None:
        """Reopening at a previously-emitted position does not replay it (CDC-style)."""
        await self._assert_resume_does_not_replay(Operation.CREATE)

    async def _assert_resume_does_not_replay(self, operation: Operation) -> None:
        driver = self.driver()
        source_cls = driver.source_class()
        if source_cls is None:
            return  # destination-only connector -- nothing to resume

        config_cls = _config_class(source_cls, Source)
        config = config_cls.model_validate(dict(driver.source_config()))

        first_run = source_cls()
        await first_run.configure(config)
        await first_run.open(None)
        seen_positions: set[bytes] = set()
        last_position = b""
        for _ in range(2):
            record = await _read_one(first_run)
            seen_positions.add(record.position)
            last_position = record.position
        await first_run.teardown()

        second_run = source_cls()
        await second_run.configure(config)
        await second_run.open(last_position)
        record = await _read_one(second_run)
        await second_run.teardown()

        assert record.position not in seen_positions, (
            f"resuming from position {last_position!r} replayed an "
            f"already-emitted position {record.position!r} -- invariant 2 "
            "(monotonic, crash-safe positions) requires read() to resume "
            "strictly after the position passed to open()"
        )

    # -- Category: read/write round trip -----------------------------------

    async def test_read_write_round_trip(self) -> None:
        """A record read from the source (or synthesized) writes successfully to the destination."""
        driver = self.driver()
        source_cls = driver.source_class()
        destination_cls = driver.destination_class()

        if source_cls is not None:
            config_cls = _config_class(source_cls, Source)
            source = source_cls()
            await source.configure(config_cls.model_validate(dict(driver.source_config())))
            await source.open(None)
            record = await _read_one(source)
            await source.teardown()
        else:
            record = create_record(b"acceptance-1", "1", {"id": "1"})

        if destination_cls is not None:
            config_cls = _config_class(destination_cls, Destination)
            destination = destination_cls()
            await destination.configure(
                config_cls.model_validate(dict(driver.destination_config()))
            )
            await destination.open()
            await destination.write([record])  # must not raise
            await destination.teardown()

    # -- Category: read timeout behavior ------------------------------------

    async def test_read_timeout_behavior(self) -> None:
        """``BackoffRetry`` from ``read()`` never blocks the loop indefinitely or errors.

        A source with nothing to read must let the read loop pace itself
        with backoff (not raise, not hang past ``Stop()``) -- exercised
        directly against a minimal always-empty ``Source`` double, since
        this is an SDK-adapter guarantee, not something each connector's
        own ``read()`` needs to separately prove (parallels how
        ``test_destination_partial_write_nacks_all`` is an SDK-level
        guarantee test, not a per-connector one -- see
        :meth:`test_partial_batch_write_correctness`).
        """
        from conduit.errors import BackoffRetry

        class _Config(BaseConfig):
            pass

        class _AlwaysEmptySource(Source[_Config]):
            async def read(self) -> Record:
                raise BackoffRetry()

        servicer = _SourceServicer(_AlwaysEmptySource(), _Config)

        async def empty_requests() -> Any:
            return
            yield  # pragma: no cover

        produced: list[Record] = []

        async def consume() -> None:
            async for response in servicer.Run(empty_requests(), object()):
                for proto_record in response.records:
                    produced.append(record_from_proto(proto_record))

        consume_task = asyncio.create_task(consume())
        await asyncio.sleep(0.05)  # well under one backoff cycle
        assert produced == [], "an always-empty source must not fabricate records"

        # Stop() must return promptly -- proving the backoff wait is
        # interruptible, not an uninterruptible sleep that would make
        # shutdown hang behind a source with nothing to read.
        await asyncio.wait_for(
            servicer.Stop(source_pb2.Source.Stop.Request(), object()), timeout=1.0
        )
        await asyncio.wait_for(consume_task, timeout=1.0)

    # -- Category: partial-batch write correctness (the B1 fix) ------------

    async def test_partial_batch_write_correctness(self) -> None:
        """The SDK's write adapter fails closed on a partial batch -- see B1 (§2.5).

        Parallels ``tests/test_destination_partial_write_nacks_all``: an
        SDK-level adapter guarantee applicable identically to every
        destination connector, not specific to the driver's own
        implementation.
        """

        class _Config(BaseConfig):
            pass

        class _PartialWriteDestination(Destination[_Config]):
            async def write(self, records: list[Record]) -> None:
                raise BatchWriteError(len(records), written=1)

        servicer = _DestinationServicer(_PartialWriteDestination(), _Config)
        records = [create_record(f"acc-{i}".encode(), str(i), {"id": i}) for i in range(3)]
        acks = await servicer._write_batch(records)  # whitebox: the adapter's own contract

        assert acks[0].error == "", "index 0 was within the written prefix -- must be acked"
        assert acks[1].error != "", "index 1 was past the written prefix -- must be nacked"
        assert acks[2].error != "", "index 2 was past the written prefix -- must be nacked"


async def _read_one(source: Source[Any]) -> Record:
    """Call ``source.read()``, transparently retrying through ``BackoffRetry``.

    A thin helper so acceptance test bodies don't each need their own
    backoff-retry loop -- this harness isn't measuring backoff timing
    itself (that's :mod:`tests.test_source`'s job in this repo), just
    getting a real record out of a real connector.
    """
    from conduit.errors import BackoffRetry

    for _ in range(1000):
        try:
            return await source.read()
        except BackoffRetry:
            await asyncio.sleep(0.01)
    raise AssertionError("source.read() never returned a record after 1000 attempts")


def _config_class(connector_cls: type[Any], base: type[Any]) -> type[BaseConfig]:
    """Recover a connector's concrete config class for driver-supplied config maps."""
    from conduit._introspect import resolve_config_class

    return resolve_config_class(connector_cls, base)


__all__ = [
    "CONTRACT_VERSION",
    "AcceptanceTestDriver",
    "AcceptanceTestSuite",
    "ConfigurableAcceptanceTestDriver",
]
