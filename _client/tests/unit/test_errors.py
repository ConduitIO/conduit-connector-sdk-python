"""grpc.RpcError -> ConduitError translation.

Spins up a tiny real in-process gRPC server/channel (loopback, no external
process) so the translation is exercised against a genuine wire-encoded
``google.rpc.Status``/``ErrorInfo`` detail -- the exact shape Conduit's Go
server emits (``pkg/foundation/cerrors/conduiterr/status.go``'s ``ToStatus``),
not a hand-constructed fake.
"""

from __future__ import annotations

from collections.abc import Iterator
from concurrent import futures

import grpc
import pytest
from google.rpc import error_details_pb2, status_pb2
from grpc_status import rpc_status

import conduit._grpc  # noqa: F401  -- sets up sys.path, see conduit._grpc.__init__
from api.v1 import api_pb2, api_pb2_grpc
from conduit.errors import ConduitError


class _Servicer(api_pb2_grpc.PipelineServiceServicer):
    """Serves exactly one canned failure per test, mirroring conduiterr's wire shape."""

    def __init__(
        self, status: status_pb2.Status | None, plain_code: grpc.StatusCode | None
    ) -> None:
        self._status = status
        self._plain_code = plain_code

    def GetPipeline(
        self, request: api_pb2.GetPipelineRequest, context: grpc.ServicerContext
    ) -> api_pb2.GetPipelineResponse:
        if self._status is not None:
            context.abort_with_status(rpc_status.to_status(self._status))
        assert self._plain_code is not None
        context.abort(self._plain_code, "not found, no structured detail")
        raise AssertionError("unreachable -- context.abort always raises")


def _serve(
    status: status_pb2.Status | None = None,
    plain_code: grpc.StatusCode | None = None,
) -> Iterator[api_pb2_grpc.PipelineServiceStub]:
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=2))
    api_pb2_grpc.add_PipelineServiceServicer_to_server(_Servicer(status, plain_code), server)
    port = server.add_insecure_port("127.0.0.1:0")
    server.start()
    channel = grpc.insecure_channel(f"127.0.0.1:{port}")
    try:
        yield api_pb2_grpc.PipelineServiceStub(channel)
    finally:
        channel.close()
        server.stop(None)


@pytest.fixture
def conduit_error_info_stub() -> Iterator[api_pb2_grpc.PipelineServiceStub]:
    """A server that fails GetPipeline with a full conduiterr-shaped ErrorInfo detail."""
    info = error_details_pb2.ErrorInfo(
        reason="common.not_found",
        domain="conduit",
        metadata={"configPath": "/id", "suggestion": "check the pipeline id"},
    )
    status = status_pb2.Status(
        code=grpc.StatusCode.NOT_FOUND.value[0], message="pipeline not found: xyz"
    )
    status.details.add().Pack(info)
    yield from _serve(status=status)


@pytest.fixture
def plain_not_found_stub() -> Iterator[api_pb2_grpc.PipelineServiceStub]:
    """A server that fails with a bare gRPC status, no ErrorInfo detail at all."""
    yield from _serve(plain_code=grpc.StatusCode.NOT_FOUND)


def test_decodes_conduit_error_info_detail(
    conduit_error_info_stub: api_pb2_grpc.PipelineServiceStub,
) -> None:
    with pytest.raises(grpc.RpcError) as exc_info:
        conduit_error_info_stub.GetPipeline(api_pb2.GetPipelineRequest(id="xyz"))

    err = ConduitError.from_rpc_error(exc_info.value)

    assert err.code == "common.not_found"
    assert "pipeline not found: xyz" in str(err)
    assert err.config_path == "/id"
    assert err.suggestion == "check the pipeline id"
    assert err.grpc_status_code == grpc.StatusCode.NOT_FOUND


def test_falls_back_to_bare_status_without_error_info(
    plain_not_found_stub: api_pb2_grpc.PipelineServiceStub,
) -> None:
    with pytest.raises(grpc.RpcError) as exc_info:
        plain_not_found_stub.GetPipeline(api_pb2.GetPipelineRequest(id="xyz"))

    err = ConduitError.from_rpc_error(exc_info.value)

    assert err.code == "common.not_found"  # synthesized from the gRPC category
    assert err.grpc_status_code == grpc.StatusCode.NOT_FOUND
    assert err.config_path == ""
    assert err.suggestion == ""


def test_unreachable_server_never_leaks_raw_traceback() -> None:
    # Deliberately no server listening on this port.
    channel = grpc.insecure_channel("127.0.0.1:1")
    stub = api_pb2_grpc.PipelineServiceStub(channel)
    try:
        with pytest.raises(grpc.RpcError) as exc_info:
            stub.GetPipeline(api_pb2.GetPipelineRequest(id="xyz"), timeout=2.0)
        err = ConduitError.from_rpc_error(exc_info.value)
        assert isinstance(err, ConduitError)
        assert err.grpc_status_code in (
            grpc.StatusCode.UNAVAILABLE,
            grpc.StatusCode.DEADLINE_EXCEEDED,
        )
    finally:
        channel.close()


def test_str_renders_structured_fields() -> None:
    err = ConduitError(
        "boom",
        code="common.invalid_argument",
        config_path="/settings/url",
        suggestion="set a valid URL",
        docs_url="https://conduit.io/docs/errors/common.invalid_argument",
    )
    rendered = str(err)
    assert "[common.invalid_argument] boom" in rendered
    assert "config path: /settings/url" in rendered
    assert "suggestion: set a valid URL" in rendered
    assert "docs: https://conduit.io/docs/errors/common.invalid_argument" in rendered


def test_pipeline_id_defaults_empty_and_renders_when_set() -> None:
    assert ConduitError("boom").pipeline_id == ""

    err = ConduitError("boom", pipeline_id="pipeline-1")
    assert err.pipeline_id == "pipeline-1"
    assert "pipeline-1" in str(err)
