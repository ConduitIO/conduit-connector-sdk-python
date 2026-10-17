"""``Pipeline``: the fluent builder that produces control-plane API payloads.

Per the design doc (``docs/design/20260724-embed-grpc-client-libraries.md``,
"Decision -- client-library API design" / "Pipeline builder") and the task
spec's frozen surface:

    pipeline = (
        conduit.Pipeline("orders-sync")
        .source("postgres", url="postgres://...", tables="orders")
        .destination("kafka", brokers="localhost:9092", topic="orders")
        .process("filter.field", condition="orders.deleted == false")
    )

This is **Slice 1, Case A only**: every ``source``/``destination``/``process``
call takes a *plugin name string* (``"postgres"``, ``"builtin:log"``, ...),
never a Python object -- ``inline_source``/``inline_destination`` (Case B,
external connectors) is out of scope for this build (design doc, "Build
slices").

``Settings`` on the wire is a flat ``map<string, string>``
(``Connector.Config.settings``, ``Processor.Config.settings`` in
``proto/api/v1/api.proto``) -- every keyword argument is coerced to ``str``
here. Typed, per-connector config (real parameter types, IDE-visible) is a
documented fast-follow generated from connector param specs, not hand-written
per connector (design doc, "Fast-follow -- typed-config codegen"); see the
README.

**Real connector config keys are frequently not valid Python identifiers**
(e.g. the builtin generator connector's ``format.type``, ``sdk.batch.size``,
``collections.*.operations``) -- a dotted or wildcarded key can never be
written as a Python ``**kwargs`` keyword. Every ``.source()``/
``.destination()``/``.process()``/``.dlq()`` call therefore also accepts an
explicit ``settings: Mapping[str, object]`` dict for exactly those keys,
merged with (and overridden by, on overlap) any ``**kwargs`` given at the
same call: ``.source("generator", settings={"format.type": "structured"},
operations="create")``. Plain identifier-shaped keys can use whichever form
reads better at the call site.

This module never touches gRPC. :meth:`Pipeline.build_requests` is a pure
function of the builder's accumulated state -- deliberately so it can be
unit-tested (builder -> exact RPC payload) without a mock channel or a live
server. :class:`conduit.client.Client` is the only thing that sends the
requests this module builds.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass

import conduit._grpc  # noqa: F401  -- sets up sys.path, see conduit._grpc.__init__
from api.v1 import api_pb2


def _settings(
    settings: Mapping[str, object] | None, kwargs: Mapping[str, object]
) -> dict[str, str]:
    """Merge the explicit ``settings=`` mapping with ``**kwargs``, coerced to strings.

    ``kwargs`` wins on key overlap (it's the more specific, call-site-local
    form). Booleans render as Conduit's config parser expects
    (``"true"``/``"false"``, not Python's ``"True"``/``"False"``); everything
    else uses ``str()``.
    """
    merged: dict[str, object] = dict(settings or {})
    merged.update(kwargs)
    out: dict[str, str] = {}
    for k, v in merged.items():
        out[k] = "true" if v is True else "false" if v is False else str(v)
    return out


@dataclass(frozen=True)
class _ConnectorSpec:
    """One accumulated ``.source()``/``.destination()`` call."""

    type: api_pb2.Connector.Type
    plugin: str
    name: str
    settings: dict[str, str]


@dataclass(frozen=True)
class _ProcessorSpec:
    """One accumulated ``.process()`` call. Attaches to the pipeline itself.

    Attaching a processor to a specific connector (``Processor.Parent.TYPE_CONNECTOR``)
    is supported by the wire protocol but not exposed by this builder yet --
    Slice 1 only needs pipeline-level processors to cover the frozen quickstart
    surface; a per-connector attachment point is a natural, additive follow-up.
    """

    plugin: str
    condition: str
    workers: int | None
    settings: dict[str, str]


@dataclass(frozen=True)
class _DLQSpec:
    """One ``.dlq()`` call -- pipeline-level dead-letter-queue config."""

    plugin: str
    settings: dict[str, str]
    window_size: int | None
    window_nack_threshold: int | None


@dataclass(frozen=True)
class BuildPlan:
    """The exact, ordered set of RPC request payloads a :class:`Pipeline` produces.

    Pure data -- constructing one performs no I/O. :class:`conduit.client.Client`
    sends ``create_pipeline`` first (the response carries the server-assigned
    pipeline id every other request needs), then ``create_connectors`` and
    ``create_processors`` in the order they were declared, then ``dlq`` (if
    set) last, before starting the pipeline.
    """

    create_pipeline: api_pb2.CreatePipelineRequest
    create_connectors: tuple[api_pb2.CreateConnectorRequest, ...]
    create_processors: tuple[api_pb2.CreateProcessorRequest, ...]
    dlq: api_pb2.UpdateDLQRequest | None


class Pipeline:
    """Fluent builder for a pipeline's create-RPC payloads.

    ``id`` is a local, display-only label (Conduit assigns the real pipeline
    ID server-side -- ``Pipeline.id`` is ``OUTPUT_ONLY`` on the wire,
    ``proto/api/v1/api.proto:100``). It becomes the pipeline's ``name`` unless
    ``name=`` is given explicitly.
    """

    def __init__(self, id: str, *, name: str | None = None, description: str = "") -> None:
        """Start building a pipeline.

        Args:
            id: local, display-only label; also the default ``name`` sent to
                ``CreatePipeline`` if ``name`` is not given.
            name: pipeline name on the wire (``Pipeline.Config.name``).
                Defaults to ``id``.
            description: pipeline description (``Pipeline.Config.description``).
        """
        self.id = id
        self.name = name if name is not None else id
        self.description = description
        self._connectors: list[_ConnectorSpec] = []
        self._processors: list[_ProcessorSpec] = []
        self._dlq: _DLQSpec | None = None

    def source(
        self,
        plugin: str,
        *,
        name: str = "",
        settings: Mapping[str, object] | None = None,
        **kwargs: object,
    ) -> Pipeline:
        """Add a source connector.

        Args:
            plugin: connector plugin reference, e.g. ``"postgres"``,
                ``"builtin:generator"``, ``"standalone:s3@v0.2.0"`` (see
                ``proto/api/v1/api.proto``'s ``CreateConnectorRequest.plugin``
                doc comment for the full ``[TYPE:]NAME[@VERSION]`` grammar).
            name: connector name (``Connector.Config.name``); defaults to
                ``plugin`` if omitted.
            settings: connector config for keys that aren't valid Python
                identifiers (e.g. ``"format.type"``); merged with ``**kwargs``
                (see module docstring).
            **kwargs: connector config for identifier-shaped keys, coerced to
                the wire's flat ``map<string, string>``.

        Returns:
            ``self``, for chaining.
        """
        self._connectors.append(
            _ConnectorSpec(
                type=api_pb2.Connector.TYPE_SOURCE,
                plugin=plugin,
                name=name or plugin,
                settings=_settings(settings, kwargs),
            )
        )
        return self

    def destination(
        self,
        plugin: str,
        *,
        name: str = "",
        settings: Mapping[str, object] | None = None,
        **kwargs: object,
    ) -> Pipeline:
        """Add a destination connector. See :meth:`source` for argument semantics."""
        self._connectors.append(
            _ConnectorSpec(
                type=api_pb2.Connector.TYPE_DESTINATION,
                plugin=plugin,
                name=name or plugin,
                settings=_settings(settings, kwargs),
            )
        )
        return self

    def process(
        self,
        plugin: str,
        *,
        condition: str = "",
        workers: int | None = None,
        settings: Mapping[str, object] | None = None,
        **kwargs: object,
    ) -> Pipeline:
        """Add a pipeline-level processor.

        Args:
            plugin: processor plugin reference, e.g. ``"builtin:field.filter"``.
            condition: goTemplate-formatted condition string
                (``Processor.condition``); empty means "always run".
            workers: number of concurrent workers
                (``Processor.Config.workers``); ``None`` leaves the field
                unset (server default).
            settings: processor config for keys that aren't valid Python
                identifiers; merged with ``**kwargs`` (see module docstring).
            **kwargs: processor config for identifier-shaped keys, coerced to
                the wire's flat ``map<string, string>``.

        Returns:
            ``self``, for chaining.
        """
        self._processors.append(
            _ProcessorSpec(
                plugin=plugin,
                condition=condition,
                workers=workers,
                settings=_settings(settings, kwargs),
            )
        )
        return self

    def dlq(
        self,
        plugin: str = "builtin:log",
        *,
        window_size: int | None = None,
        window_nack_threshold: int | None = None,
        settings: Mapping[str, object] | None = None,
        **kwargs: object,
    ) -> Pipeline:
        """Configure the pipeline-level dead-letter queue (``Pipeline.DLQ``).

        **Defaulting note (data-integrity):** ``UpdateDLQ`` takes
        ``window_size``/``window_nack_threshold`` at face value with no
        server-side defaulting -- unlike pipeline *creation*, where the engine
        applies ``DefaultDLQ`` (``window_size=1``) if ``.dlq()`` is never
        called at all. Leaving these ``None`` here and sending the protobuf
        zero value would silently *disable* the nack-window stop-safety
        (``window_size=0`` means "don't monitor"), which is strictly worse
        than never calling ``.dlq()``. So this builder mirrors the engine's
        own ``DefaultDLQ`` client-side: omitting ``window_size``/
        ``window_nack_threshold`` defaults them to ``1``/``0`` on the emitted
        ``UpdateDLQ`` payload; pass explicit values (including ``0``) to
        override.

        Args:
            plugin: DLQ connector plugin; Conduit's own default is
                ``builtin:log`` at ``WARN`` level (``proto/api/v1/api.proto``'s
                ``Pipeline.DLQ.plugin`` doc comment) if never configured.
            window_size: how many recent acks/nacks are monitored
                (``0`` disables the window). Defaults to ``1`` (matching the
                engine's ``DefaultDLQ``) if not given -- see note above.
            window_nack_threshold: nacks tolerated within the window before
                the pipeline stops. Defaults to ``0`` if not given.
            settings: DLQ connector config for keys that aren't valid Python
                identifiers; merged with ``**kwargs`` (see module docstring).
            **kwargs: DLQ connector config for identifier-shaped keys, coerced
                to ``map<string, string>``.

        Returns:
            ``self``, for chaining.
        """
        self._dlq = _DLQSpec(
            plugin=plugin,
            settings=_settings(settings, kwargs),
            window_size=window_size if window_size is not None else 1,
            window_nack_threshold=(
                window_nack_threshold if window_nack_threshold is not None else 0
            ),
        )
        return self

    def build_requests(self) -> BuildPlan:
        """Render the accumulated builder state into exact RPC request payloads.

        Pure and side-effect-free -- this is the seam unit tests exercise
        directly (builder calls in, exact protobuf messages out) without any
        gRPC channel involved.

        Returns:
            A :class:`BuildPlan` with every request :class:`conduit.client.Client`
            needs to send, in send order (minus pipeline id, which only
            exists after ``CreatePipeline`` responds -- connector/processor/
            DLQ requests are returned with ``pipeline_id``/``id`` left unset;
            the client fills it in after creating the pipeline).
        """
        create_pipeline = api_pb2.CreatePipelineRequest(
            config=api_pb2.Pipeline.Config(name=self.name, description=self.description)
        )
        create_connectors = tuple(
            api_pb2.CreateConnectorRequest(
                type=c.type,
                plugin=c.plugin,
                config=api_pb2.Connector.Config(name=c.name, settings=c.settings),
            )
            for c in self._connectors
        )
        create_processors = tuple(
            api_pb2.CreateProcessorRequest(
                plugin=p.plugin,
                condition=p.condition,
                config=api_pb2.Processor.Config(
                    settings=p.settings,
                    **({"workers": p.workers} if p.workers is not None else {}),
                ),
            )
            for p in self._processors
        )
        dlq_request: api_pb2.UpdateDLQRequest | None = None
        if self._dlq is not None:
            dlq_request = api_pb2.UpdateDLQRequest(
                dlq=api_pb2.Pipeline.DLQ(
                    plugin=self._dlq.plugin,
                    settings=self._dlq.settings,
                    **(
                        {"window_size": self._dlq.window_size}
                        if self._dlq.window_size is not None
                        else {}
                    ),
                    **(
                        {"window_nack_threshold": self._dlq.window_nack_threshold}
                        if self._dlq.window_nack_threshold is not None
                        else {}
                    ),
                )
            )
        return BuildPlan(
            create_pipeline=create_pipeline,
            create_connectors=create_connectors,
            create_processors=create_processors,
            dlq=dlq_request,
        )
