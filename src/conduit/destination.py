"""Destination connector base class + the ``DestinationPlugin`` gRPC adapter.

See ``docs/design/20260707-python-connector-sdk.md`` §2.1 (async/dual-mode),
§2.4 (forward-compatible ABC), and **§2.5 (the B1 fix -- the single most
important correctness property in this repo)**. This module holds both
halves: the author-facing :class:`Destination` ABC, and the internal
:class:`_DestinationServicer` adapting it to the generated
``DestinationPluginServicer``.

**B1, restated precisely at its enforcement site (see
:meth:`_DestinationServicer._write_batch`):** a naive translation of "catch
the first exception, treat every index not present in the exception's map
as successful" is the exact bug Go's ``(n, err)`` contract has to defend
against at runtime (``destination.go:345-350``, re-verified ▶ MUST-FIX 1) --
"absence of an error entry" read as "acked" is a direct invariant 1/3
violation (a record never durably written gets acked). This module's write
adapter never does that: every ack decision is driven by
:class:`~conduit.errors.BatchWriteError`'s own exhaustive, construction-time-
validated accounting (see :mod:`conduit.errors`), or -- for any other
exception -- nacks the entire batch outright.
"""

from __future__ import annotations

import abc
from collections.abc import AsyncIterator, Sequence
from typing import Any, Generic, TypeVar

import conduit._grpc  # noqa: F401  -- sets up sys.path, see conduit._grpc.__init__
from conduit._dispatch import invoke
from conduit._grpc.adapters import config_map_from_proto, records_from_proto
from conduit._introspect import resolve_config_class
from conduit.config import BaseConfig
from conduit.errors import BatchWriteError
from conduit.record import Record
from connector.v2 import destination_pb2, destination_pb2_grpc

ConfigT = TypeVar("ConfigT", bound=BaseConfig)

Ack = destination_pb2.Destination.Run.Response.Ack


class Destination(abc.ABC, Generic[ConfigT]):
    """Base class for Conduit destination connectors.

    Subclass this, parameterized with your
    :class:`~conduit.config.BaseConfig` subclass, and override
    :meth:`write` at minimum. Every other method has a working default, per
    the same forward-compatible-ABC rationale as :class:`~conduit.source.Source`
    (§2.4).

    Methods are declared ``async def``; a sync ``def`` override runs in a
    thread-pool executor -- see :mod:`conduit._dispatch` (§2.1).
    """

    config: ConfigT
    """The validated config instance, set by :meth:`configure` (default
    implementation) before :meth:`open` is called."""

    async def configure(self, config: ConfigT) -> None:
        """Receive and store the validated config. Default: ``self.config = config``.

        Args:
            config: the parsed, pydantic-validated config instance.
        """
        self.config = config

    async def open(self) -> None:
        """Prepare to start writing records (e.g. open connections). Default: no-op."""
        return None

    @abc.abstractmethod
    async def write(self, records: list[Record]) -> None:
        """Durably write every record in ``records``, in order.

        Full success is "returns without raising." A partial-batch failure
        raises :class:`~conduit.errors.BatchWriteError` with an exhaustive
        accounting of which indices succeeded and which failed -- see that
        class's docstring for the exact construction contract (the B1 fix).
        Any other exception is treated by the SDK's adapter as a failure of
        the **entire** batch (see :meth:`_DestinationServicer._write_batch`)
        -- there is no partial-credit interpretation of a plain exception.

        The one genuinely required override (§2.4) -- ``abc.ABC`` refuses
        to instantiate a subclass that doesn't provide it.

        Args:
            records: the batch to write, in the order Conduit sent them.

        Raises:
            conduit.errors.BatchWriteError: on a partial-batch failure, with
                an exhaustive per-index accounting.
        """
        raise NotImplementedError("Destination subclasses must override write()")

    async def teardown(self) -> None:
        """Called once, after the write loop stops, before process exit. Default: no-op."""
        return None

    async def lifecycle_on_created(self, config: dict[str, str]) -> None:
        """Called once, the first time this connector instance is ever run. Default: no-op."""
        return None

    async def lifecycle_on_updated(
        self, config_before: dict[str, str], config_after: dict[str, str]
    ) -> None:
        """Called when the connector's configuration changed since the last run. Default: no-op."""
        return None

    async def lifecycle_on_deleted(self, config: dict[str, str]) -> None:
        """Called once, when this connector instance was deleted. Default: no-op."""
        return None


class _DestinationServicer(destination_pb2_grpc.DestinationPluginServicer):
    """Adapts a :class:`Destination` instance to the generated servicer.

    Internal; constructed by :func:`conduit.serve.serve`, never by
    connector authors directly.
    """

    def __init__(self, destination: Destination[Any], config_cls: type[BaseConfig]) -> None:
        """Wrap a connector instance for gRPC dispatch.

        Args:
            destination: the author's ``Destination`` instance. Typed
                ``Destination[Any]`` (not ``Destination[BaseConfig]``)
                deliberately: generics are invariant in Python's type
                system, so a concrete ``Destination[MyConfig]`` instance is
                not otherwise assignable here; ``config_cls`` (below) is
                what actually drives config validation, independent of
                this parameter's type.
            config_cls: the concrete :class:`~conduit.config.BaseConfig`
                subclass to validate ``Configure``'s config map against.
        """
        self._destination = destination
        self._config_cls = config_cls

    async def Configure(
        self, request: destination_pb2.Destination.Configure.Request, context: object
    ) -> destination_pb2.Destination.Configure.Response:
        """Validate and store the plugin's config."""
        config = self._config_cls.model_validate(config_map_from_proto(request.config))
        await invoke(self._destination.configure, config)
        return destination_pb2.Destination.Configure.Response()

    async def Open(
        self, request: destination_pb2.Destination.Open.Request, context: object
    ) -> destination_pb2.Destination.Open.Response:
        """Prepare the destination to start writing records."""
        await invoke(self._destination.open)
        return destination_pb2.Destination.Open.Response()

    async def Run(
        self,
        request_iterator: AsyncIterator[destination_pb2.Destination.Run.Request],
        context: object,
    ) -> AsyncIterator[destination_pb2.Destination.Run.Response]:
        """Bidirectional stream: consume record batches in, emit acks out.

        Each incoming ``Destination.Run.Request`` (a batch of records) is
        written via :meth:`_write_batch` and immediately followed by a
        ``Destination.Run.Response`` carrying one ack per record in that
        batch, in the same order -- there is no cross-batch buffering here,
        keeping the ack/write relationship for a given batch entirely
        local to one iteration of this loop.
        """
        async for request in request_iterator:
            records = records_from_proto(request.records)
            acks = await self._write_batch(records)
            yield destination_pb2.Destination.Run.Response(acks=acks)

    async def _write_batch(self, records: Sequence[Record]) -> list[Ack]:
        """Call ``write()`` and translate the outcome into per-record acks.

        This is the B1 enforcement site: every ack decision below is driven
        either by ``write()`` returning cleanly (full-batch success) or by
        :class:`~conduit.errors.BatchWriteError`'s own exhaustive, already-
        validated ``success``/``failures`` accounting -- never by assuming
        an unmentioned index succeeded.
        """
        try:
            await invoke(self._destination.write, list(records))
        except BatchWriteError as exc:
            return self._acks_from_batch_write_error(records, exc)
        except Exception as exc:
            # Invariant 1 / B1: any exception other than BatchWriteError
            # carries no per-index accounting at all, so the adapter cannot
            # assume *any* record in this batch was durably written. Nack
            # the entire batch rather than guessing a partial success.
            return [Ack(position=r.position, error=str(exc)) for r in records]
        # Invariant 1: ack only reached here, after `write()` returned
        # without raising for every record in this batch -- full-batch
        # success, the only case where every ack carries no error.
        return [Ack(position=r.position, error="") for r in records]

    def _acks_from_batch_write_error(
        self, records: Sequence[Record], exc: BatchWriteError
    ) -> list[Ack]:
        """Build one ack per record from a validated ``BatchWriteError``.

        ``exc.success``/``exc.failures`` were already checked exhaustive
        and disjoint at ``BatchWriteError.__init__`` time (see
        :mod:`conduit.errors`) -- this method has no code path that
        computes "ack everything not explicitly marked as failed": the
        ``else`` branch below only runs for the (should-be-impossible,
        defense-in-depth) case of an index outside both sets, and even then
        it nacks, it never acks.
        """
        acks: list[Ack] = []
        for i, record in enumerate(records):
            if i in exc.success:
                # Invariant 1: explicitly accounted as successfully,
                # durably written by write() -- ack.
                acks.append(Ack(position=record.position, error=""))
            else:
                # Invariant 1 / B1 fail-closed fix: every other index --
                # whether explicitly in `exc.failures`, or (defensively)
                # absent from both accountings entirely, which
                # BatchWriteError's own constructor should already have
                # rejected -- is nacked, never assumed successful. Mirrors
                # destination.go:345-350's defensive re-check.
                reason = exc.failures.get(
                    i, RuntimeError(f"index {i} unaccounted for in BatchWriteError")
                )
                acks.append(Ack(position=record.position, error=str(reason)))
        return acks

    async def Stop(
        self, request: destination_pb2.Destination.Stop.Request, context: object
    ) -> destination_pb2.Destination.Stop.Response:
        """Acknowledge the last record Conduit will send; nothing further to do here.

        The destination has no read-loop analog to halt -- ``Run``'s
        request stream simply ends after this record, which is Conduit's
        own responsibility, not this servicer's.
        """
        return destination_pb2.Destination.Stop.Response()

    async def Teardown(
        self, request: destination_pb2.Destination.Teardown.Request, context: object
    ) -> destination_pb2.Destination.Teardown.Response:
        """Run the connector's teardown hook to completion."""
        await invoke(self._destination.teardown)
        return destination_pb2.Destination.Teardown.Response()

    async def LifecycleOnCreated(
        self, request: destination_pb2.Destination.Lifecycle.OnCreated.Request, context: object
    ) -> destination_pb2.Destination.Lifecycle.OnCreated.Response:
        """Dispatch the connector's first-run lifecycle hook."""
        await invoke(self._destination.lifecycle_on_created, config_map_from_proto(request.config))
        return destination_pb2.Destination.Lifecycle.OnCreated.Response()

    async def LifecycleOnUpdated(
        self, request: destination_pb2.Destination.Lifecycle.OnUpdated.Request, context: object
    ) -> destination_pb2.Destination.Lifecycle.OnUpdated.Response:
        """Dispatch the connector's config-changed lifecycle hook."""
        await invoke(
            self._destination.lifecycle_on_updated,
            config_map_from_proto(request.config_before),
            config_map_from_proto(request.config_after),
        )
        return destination_pb2.Destination.Lifecycle.OnUpdated.Response()

    async def LifecycleOnDeleted(
        self, request: destination_pb2.Destination.Lifecycle.OnDeleted.Request, context: object
    ) -> destination_pb2.Destination.Lifecycle.OnDeleted.Response:
        """Dispatch the connector's deleted lifecycle hook."""
        await invoke(self._destination.lifecycle_on_deleted, config_map_from_proto(request.config))
        return destination_pb2.Destination.Lifecycle.OnDeleted.Response()


def _resolve_destination_config_class(
    destination_cls: type[Destination[BaseConfig]],
) -> type[BaseConfig]:
    """Recover the concrete ``BaseConfig`` subclass a ``Destination[Config]`` used.

    Thin, ``Destination``-specific wrapper over
    :func:`conduit._introspect.resolve_config_class`.
    """
    return resolve_config_class(destination_cls, Destination)
