"""Tests that ``Configure`` forwards pydantic validation detail over gRPC.

Per CLAUDE.md's "errors are API, actionable" standard: a config validation
failure must surface which field failed and why, not collapse to an opaque
blob. This exercises the real path: a real ``grpc.aio`` server, a real gRPC
client, a real ``Configure`` RPC call with an invalid config map -- not a
mock of ``pydantic.ValidationError`` or of the transport.
"""

from __future__ import annotations

import grpc
import grpc.aio
import pytest

import conduit._grpc  # noqa: F401  -- sets up sys.path, see conduit._grpc.__init__
from conduit.config import BaseConfig, Field, Specification
from conduit.destination import Destination
from conduit.errors import BackoffRetry
from conduit.record import Record
from conduit.serve import _build_plugin_server
from conduit.source import Source
from connector.v2 import destination_pb2, source_pb2

_SPEC = Specification(name="test-plugin", version="0.0.0", author="test")


class _Config(BaseConfig):
    url: str = Field(description="a required field")
    count: int = Field(default=1, description="an int field")


class _NoopSource(Source[_Config]):
    async def read(self) -> Record:
        raise BackoffRetry()


class _NoopDestination(Destination[_Config]):
    async def write(self, records: list[Record]) -> None:
        return None


async def _configure(port: int, service: str, config: dict[str, str]) -> None:
    channel = grpc.aio.insecure_channel(f"127.0.0.1:{port}")
    try:
        if service == "source":
            call = channel.unary_unary(
                "/connector.v2.SourcePlugin/Configure",
                request_serializer=source_pb2.Source.Configure.Request.SerializeToString,
                response_deserializer=source_pb2.Source.Configure.Response.FromString,
            )
            await call(source_pb2.Source.Configure.Request(config=config))
        else:
            call = channel.unary_unary(
                "/connector.v2.DestinationPlugin/Configure",
                request_serializer=destination_pb2.Destination.Configure.Request.SerializeToString,
                response_deserializer=destination_pb2.Destination.Configure.Response.FromString,
            )
            await call(destination_pb2.Destination.Configure.Request(config=config))
    finally:
        await channel.close()


class TestConfigureValidationErrorDetail:
    async def test_source_missing_required_field_is_invalid_argument_with_field_detail(
        self,
    ) -> None:
        handle = await _build_plugin_server(_SPEC, source=_NoopSource)
        try:
            with pytest.raises(grpc.aio.AioRpcError) as exc_info:
                await _configure(handle.port, "source", {})
            error = exc_info.value
            assert error.code() == grpc.StatusCode.INVALID_ARGUMENT
            details = error.details()
            assert details is not None
            assert "url" in details
            assert "required" in details.lower() or "missing" in details.lower()
        finally:
            handle.shutdown_requested.set()
            await handle.drive_task

    async def test_source_wrong_type_is_invalid_argument_with_field_detail(self) -> None:
        handle = await _build_plugin_server(_SPEC, source=_NoopSource)
        try:
            with pytest.raises(grpc.aio.AioRpcError) as exc_info:
                await _configure(handle.port, "source", {"url": "x", "count": "not-an-int"})
            error = exc_info.value
            assert error.code() == grpc.StatusCode.INVALID_ARGUMENT
            details = error.details()
            assert details is not None
            assert "count" in details
        finally:
            handle.shutdown_requested.set()
            await handle.drive_task

    async def test_destination_missing_required_field_is_invalid_argument_with_field_detail(
        self,
    ) -> None:
        handle = await _build_plugin_server(_SPEC, destination=_NoopDestination)
        try:
            with pytest.raises(grpc.aio.AioRpcError) as exc_info:
                await _configure(handle.port, "destination", {})
            error = exc_info.value
            assert error.code() == grpc.StatusCode.INVALID_ARGUMENT
            details = error.details()
            assert details is not None
            assert "url" in details
        finally:
            handle.shutdown_requested.set()
            await handle.drive_task

    async def test_valid_config_does_not_raise(self) -> None:
        handle = await _build_plugin_server(_SPEC, source=_NoopSource)
        try:
            await _configure(handle.port, "source", {"url": "https://example.com"})
        finally:
            handle.shutdown_requested.set()
            await handle.drive_task
