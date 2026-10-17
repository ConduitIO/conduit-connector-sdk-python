"""Builder -> exact RPC payload mapping.

Exercises :meth:`conduit.pipeline.Pipeline.build_requests` directly -- pure,
no gRPC channel, no mock server -- per the build task's unit-test gate.
"""

from __future__ import annotations

import conduit._grpc  # noqa: F401  -- sets up sys.path, see conduit._grpc.__init__
from api.v1 import api_pb2
from conduit.pipeline import Pipeline


def test_id_becomes_default_name() -> None:
    plan = Pipeline("orders-sync").build_requests()
    assert plan.create_pipeline == api_pb2.CreatePipelineRequest(
        config=api_pb2.Pipeline.Config(name="orders-sync", description="")
    )


def test_explicit_name_overrides_id() -> None:
    plan = Pipeline("orders-sync", name="Orders Sync", description="syncs orders").build_requests()
    assert plan.create_pipeline.config.name == "Orders Sync"
    assert plan.create_pipeline.config.description == "syncs orders"


def test_source_produces_create_connector_request() -> None:
    plan = Pipeline("p").source("postgres", url="postgres://x", tables="orders").build_requests()
    assert len(plan.create_connectors) == 1
    req = plan.create_connectors[0]
    assert req.type == api_pb2.Connector.TYPE_SOURCE
    assert req.plugin == "postgres"
    assert req.config.name == "postgres"
    assert dict(req.config.settings) == {"url": "postgres://x", "tables": "orders"}
    # pipeline_id is filled in by Client.run() after CreatePipeline responds,
    # not by the builder -- it doesn't exist yet at build_requests() time.
    assert req.pipeline_id == ""


def test_destination_produces_create_connector_request() -> None:
    plan = (
        Pipeline("p")
        .destination("kafka", brokers="localhost:9092", topic="orders")
        .build_requests()
    )
    assert len(plan.create_connectors) == 1
    req = plan.create_connectors[0]
    assert req.type == api_pb2.Connector.TYPE_DESTINATION
    assert req.plugin == "kafka"
    assert dict(req.config.settings) == {"brokers": "localhost:9092", "topic": "orders"}


def test_source_and_destination_order_preserved() -> None:
    plan = (
        Pipeline("p")
        .source("generator", format_type="structured")
        .destination("log", level="info")
        .build_requests()
    )
    assert [c.plugin for c in plan.create_connectors] == ["generator", "log"]
    assert [c.type for c in plan.create_connectors] == [
        api_pb2.Connector.TYPE_SOURCE,
        api_pb2.Connector.TYPE_DESTINATION,
    ]


def test_connector_name_override() -> None:
    plan = Pipeline("p").source("postgres", name="orders-db", url="x").build_requests()
    assert plan.create_connectors[0].config.name == "orders-db"


def test_process_produces_create_processor_request() -> None:
    plan = (
        Pipeline("p")
        .process("filter.field", condition="orders.deleted == false", workers=4, some_key="v")
        .build_requests()
    )
    assert len(plan.create_processors) == 1
    req = plan.create_processors[0]
    assert req.plugin == "filter.field"
    assert req.condition == "orders.deleted == false"
    assert req.config.workers == 4
    assert dict(req.config.settings) == {"some_key": "v"}
    # parent is filled in by Client.run() (pipeline id doesn't exist yet).
    assert req.parent.id == ""


def test_process_without_workers_leaves_field_unset() -> None:
    plan = Pipeline("p").process("filter.field").build_requests()
    assert plan.create_processors[0].config.workers == 0


def test_multiple_processors_preserve_order() -> None:
    plan = Pipeline("p").process("a").process("b").process("c").build_requests()
    assert [p.plugin for p in plan.create_processors] == ["a", "b", "c"]


def test_no_dlq_by_default() -> None:
    plan = Pipeline("p").build_requests()
    assert plan.dlq is None


def test_dlq_produces_update_dlq_request() -> None:
    plan = (
        Pipeline("p")
        .dlq("builtin:log", window_size=5, window_nack_threshold=2, level="warn")
        .build_requests()
    )
    assert plan.dlq is not None
    assert plan.dlq.dlq.plugin == "builtin:log"
    assert plan.dlq.dlq.window_size == 5
    assert plan.dlq.dlq.window_nack_threshold == 2
    assert dict(plan.dlq.dlq.settings) == {"level": "warn"}
    # id is filled in by Client.run() after CreatePipeline responds.
    assert plan.dlq.id == ""


def test_dlq_defaults_plugin_to_builtin_log() -> None:
    plan = Pipeline("p").dlq().build_requests()
    assert plan.dlq is not None
    assert plan.dlq.dlq.plugin == "builtin:log"


def test_dlq_defaults_window_size_to_one_when_not_given() -> None:
    """Regression test: `UpdateDLQ` has no server-side defaulting, unlike
    pipeline creation's `DefaultDLQ` (window_size=1). Leaving `window_size`
    unset on the wire sends the protobuf zero value, which *disables* the
    nack-window stop-safety -- worse than never calling `.dlq()` at all. The
    builder must default `window_size=1`/`window_nack_threshold=0` itself
    whenever `.dlq()` is called without explicit window args, matching the
    engine's own `DefaultDLQ`. This bug shipped invisibly once (no test
    covered the DLQ payload's window fields) -- do not regress it.
    """
    plan = Pipeline("p").dlq("builtin:log").build_requests()
    assert plan.dlq is not None
    assert plan.dlq.dlq.window_size == 1
    assert plan.dlq.dlq.window_nack_threshold == 0


def test_dlq_explicit_window_size_zero_is_respected() -> None:
    """An explicit `window_size=0` is a deliberate override, not the same as
    omitting the argument -- the builder must not clobber an intentional
    opt-out of the nack window.
    """
    plan = Pipeline("p").dlq("builtin:log", window_size=0).build_requests()
    assert plan.dlq is not None
    assert plan.dlq.dlq.window_size == 0


def test_settings_coerce_bool_to_lowercase_string() -> None:
    plan = Pipeline("p").source("x", recreate=True, dry_run=False).build_requests()
    assert dict(plan.create_connectors[0].config.settings) == {
        "recreate": "true",
        "dry_run": "false",
    }


def test_settings_coerce_int_to_string() -> None:
    plan = Pipeline("p").source("x", batch_size=100).build_requests()
    assert dict(plan.create_connectors[0].config.settings) == {"batch_size": "100"}


def test_settings_dict_supports_dotted_keys_not_valid_as_kwargs() -> None:
    """Real connector config (e.g. generator's `format.type`) can't be a **kwarg."""
    plan = (
        Pipeline("p")
        .source(
            "generator",
            settings={"format.type": "structured", "sdk.batch.size": 10},
            operations="create",
        )
        .build_requests()
    )
    assert dict(plan.create_connectors[0].config.settings) == {
        "format.type": "structured",
        "sdk.batch.size": "10",
        "operations": "create",
    }


def test_settings_dict_and_kwargs_kwargs_wins_on_overlap() -> None:
    plan = Pipeline("p").source("x", settings={"level": "warn"}, level="info").build_requests()
    assert dict(plan.create_connectors[0].config.settings) == {"level": "info"}


def test_chaining_returns_same_builder() -> None:
    pipeline = Pipeline("p")
    assert pipeline.source("a") is pipeline
    assert pipeline.destination("b") is pipeline
    assert pipeline.process("c") is pipeline
    assert pipeline.dlq() is pipeline


def test_full_quickstart_shape_matches_design_doc() -> None:
    """The frozen ≤15-line quickstart shape end-to-end (builder side only)."""
    pipeline = (
        Pipeline("orders-sync")
        .source("generator", format_type="structured", operations="create")
        .destination("log", level="info")
    )
    plan = pipeline.build_requests()

    assert plan.create_pipeline.config.name == "orders-sync"
    assert len(plan.create_connectors) == 2
    assert plan.create_connectors[0].type == api_pb2.Connector.TYPE_SOURCE
    assert plan.create_connectors[0].plugin == "generator"
    assert plan.create_connectors[1].type == api_pb2.Connector.TYPE_DESTINATION
    assert plan.create_connectors[1].plugin == "log"
    assert len(plan.create_processors) == 0
    assert plan.dlq is None
