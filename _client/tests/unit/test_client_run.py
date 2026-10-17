"""``Client.run()``/``Run`` against a small fake control-plane server.

An in-process fake `PipelineService`/`ConnectorService`/`ProcessorService`
(loopback gRPC, no external process) lets these tests verify the *sequence
and shape* of RPCs `Client.run()` issues, and `Run.status()`/`wait_running()`/
`stop()`'s behavior, without a real `conduit` binary. The integration test
(`tests/integration/test_local_generator_log.py`) covers the real binary.
"""

from __future__ import annotations

from collections.abc import Iterator
from concurrent import futures

import grpc
import pytest

import conduit._grpc  # noqa: F401  -- sets up sys.path, see conduit._grpc.__init__
from api.v1 import api_pb2, api_pb2_grpc
from conduit.client import Client
from conduit.errors import ConduitError
from conduit.pipeline import Pipeline


class _FakeControlPlane(
    api_pb2_grpc.PipelineServiceServicer,
    api_pb2_grpc.ConnectorServiceServicer,
    api_pb2_grpc.ProcessorServiceServicer,
):
    """Enough of the control-plane API to exercise `Client.run()`/`Run` end-to-end.

    Tracks every request it receives (`self.calls`) so tests can assert on
    the exact sequence `Client.run()` issues, and lets a test script a
    sequence of `Pipeline.State.Status` values `GetPipeline` returns (for
    `wait_running()`'s polling behavior).
    """

    def __init__(self) -> None:
        self.calls: list[str] = []
        self._next_connector_id = 0
        self._next_processor_id = 0
        self.status_sequence: list[api_pb2.Pipeline.Status.ValueType] = [
            api_pb2.Pipeline.STATUS_RUNNING
        ]
        self.error_message = ""
        self.stopped_reason: api_pb2.Pipeline.State.StoppedReason.ValueType = (
            api_pb2.Pipeline.State.STOPPED_REASON_UNSPECIFIED
        )
        # When set, CreateConnector aborts instead of succeeding -- lets tests
        # simulate the mid-sequence failure Client.run() deliberately doesn't
        # roll back from (pipeline already created, a later step fails).
        self.fail_create_connector = False

    def CreatePipeline(
        self, request: api_pb2.CreatePipelineRequest, context: grpc.ServicerContext
    ) -> api_pb2.CreatePipelineResponse:
        self.calls.append("CreatePipeline")
        pipeline = api_pb2.Pipeline(id="pipeline-1", config=request.config)
        return api_pb2.CreatePipelineResponse(pipeline=pipeline)

    def CreateConnector(
        self, request: api_pb2.CreateConnectorRequest, context: grpc.ServicerContext
    ) -> api_pb2.CreateConnectorResponse:
        self.calls.append(f"CreateConnector({request.plugin}, pipeline_id={request.pipeline_id})")
        if self.fail_create_connector:
            context.abort(grpc.StatusCode.INTERNAL, "simulated connector creation failure")
        self._next_connector_id += 1
        connector = api_pb2.Connector(
            id=f"connector-{self._next_connector_id}",
            type=request.type,
            plugin=request.plugin,
            pipeline_id=request.pipeline_id,
            config=request.config,
        )
        return api_pb2.CreateConnectorResponse(connector=connector)

    def CreateProcessor(
        self, request: api_pb2.CreateProcessorRequest, context: grpc.ServicerContext
    ) -> api_pb2.CreateProcessorResponse:
        self.calls.append(
            f"CreateProcessor({request.plugin}, parent={request.parent.type}/{request.parent.id})"
        )
        self._next_processor_id += 1
        processor = api_pb2.Processor(
            id=f"processor-{self._next_processor_id}",
            plugin=request.plugin,
            parent=request.parent,
            config=request.config,
            condition=request.condition,
        )
        return api_pb2.CreateProcessorResponse(processor=processor)

    def UpdateDLQ(
        self, request: api_pb2.UpdateDLQRequest, context: grpc.ServicerContext
    ) -> api_pb2.UpdateDLQResponse:
        self.calls.append(f"UpdateDLQ(id={request.id})")
        return api_pb2.UpdateDLQResponse(dlq=request.dlq)

    def StartPipeline(
        self, request: api_pb2.StartPipelineRequest, context: grpc.ServicerContext
    ) -> api_pb2.StartPipelineResponse:
        self.calls.append(f"StartPipeline(id={request.id})")
        return api_pb2.StartPipelineResponse()

    def StopPipeline(
        self, request: api_pb2.StopPipelineRequest, context: grpc.ServicerContext
    ) -> api_pb2.StopPipelineResponse:
        self.calls.append(f"StopPipeline(id={request.id}, force={request.force})")
        return api_pb2.StopPipelineResponse()

    def GetPipeline(
        self, request: api_pb2.GetPipelineRequest, context: grpc.ServicerContext
    ) -> api_pb2.GetPipelineResponse:
        status = (
            self.status_sequence.pop(0)
            if len(self.status_sequence) > 1
            else self.status_sequence[0]
        )
        state = api_pb2.Pipeline.State(
            status=status, error=self.error_message, stopped_reason=self.stopped_reason
        )
        return api_pb2.GetPipelineResponse(pipeline=api_pb2.Pipeline(id=request.id, state=state))


@pytest.fixture
def fake_server() -> Iterator[tuple[_FakeControlPlane, Client]]:
    servicer = _FakeControlPlane()
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=4))
    api_pb2_grpc.add_PipelineServiceServicer_to_server(servicer, server)
    api_pb2_grpc.add_ConnectorServiceServicer_to_server(servicer, server)
    api_pb2_grpc.add_ProcessorServiceServicer_to_server(servicer, server)
    port = server.add_insecure_port("127.0.0.1:0")
    server.start()
    channel = grpc.insecure_channel(f"127.0.0.1:{port}")
    client = Client(channel)
    try:
        yield servicer, client
    finally:
        client.close()
        server.stop(None)


def test_run_creates_pipeline_connectors_processors_dlq_then_starts(
    fake_server: tuple[_FakeControlPlane, Client],
) -> None:
    servicer, client = fake_server
    pipeline = (
        Pipeline("orders-sync")
        .source("generator", format_type="structured")
        .destination("log", level="info")
        .process("filter.field", condition="x")
        .dlq("builtin:log", window_size=1)
    )

    run = client.run(pipeline)

    assert run.pipeline_id == "pipeline-1"
    assert servicer.calls == [
        "CreatePipeline",
        "CreateConnector(generator, pipeline_id=pipeline-1)",
        "CreateConnector(log, pipeline_id=pipeline-1)",
        "CreateProcessor(filter.field, parent=2/pipeline-1)",  # Parent.TYPE_PIPELINE == 2
        "UpdateDLQ(id=pipeline-1)",
        "StartPipeline(id=pipeline-1)",
    ]


def test_run_with_start_false_skips_start_pipeline(
    fake_server: tuple[_FakeControlPlane, Client],
) -> None:
    servicer, client = fake_server
    pipeline = Pipeline("p").source("generator").destination("log")

    client.run(pipeline, start=False)

    assert "StartPipeline(id=pipeline-1)" not in servicer.calls
    assert servicer.calls[0] == "CreatePipeline"


def test_run_without_dlq_skips_update_dlq(fake_server: tuple[_FakeControlPlane, Client]) -> None:
    servicer, client = fake_server
    client.run(Pipeline("p").source("generator").destination("log"))
    assert not any(call.startswith("UpdateDLQ") for call in servicer.calls)


def test_status_maps_enum_to_lowercase_strings(
    fake_server: tuple[_FakeControlPlane, Client],
) -> None:
    servicer, client = fake_server
    servicer.status_sequence = [api_pb2.Pipeline.STATUS_DEGRADED]
    servicer.error_message = "destination unreachable"
    run = client.run(Pipeline("p").source("generator").destination("log"), start=False)

    status = run.status()

    assert status.status == "degraded"
    assert status.error == "destination unreachable"
    assert status.is_degraded is True
    assert status.is_running is False


def test_wait_running_polls_until_running(fake_server: tuple[_FakeControlPlane, Client]) -> None:
    servicer, client = fake_server
    servicer.status_sequence = [
        api_pb2.Pipeline.STATUS_RECOVERING,
        api_pb2.Pipeline.STATUS_RECOVERING,
        api_pb2.Pipeline.STATUS_RUNNING,
    ]
    run = client.run(Pipeline("p").source("generator").destination("log"), start=False)

    status = run.wait_running(timeout=5.0, poll_interval=0.01)

    assert status.is_running is True


def test_wait_running_raises_on_degraded(fake_server: tuple[_FakeControlPlane, Client]) -> None:
    servicer, client = fake_server
    servicer.status_sequence = [api_pb2.Pipeline.STATUS_DEGRADED]
    servicer.error_message = "boom"
    run = client.run(Pipeline("p").source("generator").destination("log"), start=False)

    with pytest.raises(ConduitError) as exc_info:
        run.wait_running(timeout=5.0, poll_interval=0.01)
    assert exc_info.value.code == "client.pipeline_degraded"
    assert "boom" in str(exc_info.value)


def test_wait_running_times_out(fake_server: tuple[_FakeControlPlane, Client]) -> None:
    servicer, client = fake_server
    servicer.status_sequence = [api_pb2.Pipeline.STATUS_RECOVERING]
    run = client.run(Pipeline("p").source("generator").destination("log"), start=False)

    with pytest.raises(ConduitError) as exc_info:
        run.wait_running(timeout=0.2, poll_interval=0.05)
    assert exc_info.value.code == "client.wait_timeout"


def test_stop_issues_stop_pipeline_graceful_by_default(
    fake_server: tuple[_FakeControlPlane, Client],
) -> None:
    servicer, client = fake_server
    run = client.run(Pipeline("p").source("generator").destination("log"), start=False)

    run.stop()

    assert "StopPipeline(id=pipeline-1, force=False)" in servicer.calls


def test_run_mid_sequence_failure_carries_pipeline_id(
    fake_server: tuple[_FakeControlPlane, Client],
) -> None:
    """Regression test: run() deliberately never rolls back a partially
    created pipeline, but the raised ConduitError must carry the
    already-created pipeline_id -- otherwise the caller has no way to find
    and clean up the orphaned pipeline.
    """
    servicer, client = fake_server
    servicer.fail_create_connector = True
    pipeline = Pipeline("orders-sync").source("generator").destination("log")

    with pytest.raises(ConduitError) as exc_info:
        client.run(pipeline)

    assert exc_info.value.pipeline_id == "pipeline-1"
    assert "pipeline-1" in str(exc_info.value)


def test_stop_force_true_is_explicit() -> None:
    """force=True is available but never the default -- graceful is (Invariant 7)."""
    import inspect

    from conduit.run import Run

    sig = inspect.signature(Run.stop)
    assert sig.parameters["force"].default is False
