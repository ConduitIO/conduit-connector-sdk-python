from config.v1 import parameter_pb2 as _parameter_pb2
from google.api import annotations_pb2 as _annotations_pb2
from google.api import field_behavior_pb2 as _field_behavior_pb2
from google.protobuf import timestamp_pb2 as _timestamp_pb2
from opencdc.v1 import opencdc_pb2 as _opencdc_pb2
from protoc_gen_openapiv2.options import annotations_pb2 as _annotations_pb2_1
from google.protobuf.internal import containers as _containers
from google.protobuf.internal import enum_type_wrapper as _enum_type_wrapper
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from typing import ClassVar as _ClassVar, Iterable as _Iterable, Mapping as _Mapping, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class Pipeline(_message.Message):
    __slots__ = ("id", "state", "config", "connector_ids", "processor_ids", "created_at", "updated_at")
    class Status(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
        __slots__ = ()
        STATUS_UNSPECIFIED: _ClassVar[Pipeline.Status]
        STATUS_RUNNING: _ClassVar[Pipeline.Status]
        STATUS_STOPPED: _ClassVar[Pipeline.Status]
        STATUS_DEGRADED: _ClassVar[Pipeline.Status]
        STATUS_RECOVERING: _ClassVar[Pipeline.Status]
    STATUS_UNSPECIFIED: Pipeline.Status
    STATUS_RUNNING: Pipeline.Status
    STATUS_STOPPED: Pipeline.Status
    STATUS_DEGRADED: Pipeline.Status
    STATUS_RECOVERING: Pipeline.Status
    class State(_message.Message):
        __slots__ = ("status", "error", "stopped_reason")
        class StoppedReason(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
            __slots__ = ()
            STOPPED_REASON_UNSPECIFIED: _ClassVar[Pipeline.State.StoppedReason]
            STOPPED_REASON_USER: _ClassVar[Pipeline.State.StoppedReason]
            STOPPED_REASON_SYSTEM: _ClassVar[Pipeline.State.StoppedReason]
        STOPPED_REASON_UNSPECIFIED: Pipeline.State.StoppedReason
        STOPPED_REASON_USER: Pipeline.State.StoppedReason
        STOPPED_REASON_SYSTEM: Pipeline.State.StoppedReason
        STATUS_FIELD_NUMBER: _ClassVar[int]
        ERROR_FIELD_NUMBER: _ClassVar[int]
        STOPPED_REASON_FIELD_NUMBER: _ClassVar[int]
        status: Pipeline.Status
        error: str
        stopped_reason: Pipeline.State.StoppedReason
        def __init__(self, status: _Optional[_Union[Pipeline.Status, str]] = ..., error: _Optional[str] = ..., stopped_reason: _Optional[_Union[Pipeline.State.StoppedReason, str]] = ...) -> None: ...
    class Config(_message.Message):
        __slots__ = ("name", "description")
        NAME_FIELD_NUMBER: _ClassVar[int]
        DESCRIPTION_FIELD_NUMBER: _ClassVar[int]
        name: str
        description: str
        def __init__(self, name: _Optional[str] = ..., description: _Optional[str] = ...) -> None: ...
    class DLQ(_message.Message):
        __slots__ = ("plugin", "settings", "window_size", "window_nack_threshold")
        class SettingsEntry(_message.Message):
            __slots__ = ("key", "value")
            KEY_FIELD_NUMBER: _ClassVar[int]
            VALUE_FIELD_NUMBER: _ClassVar[int]
            key: str
            value: str
            def __init__(self, key: _Optional[str] = ..., value: _Optional[str] = ...) -> None: ...
        PLUGIN_FIELD_NUMBER: _ClassVar[int]
        SETTINGS_FIELD_NUMBER: _ClassVar[int]
        WINDOW_SIZE_FIELD_NUMBER: _ClassVar[int]
        WINDOW_NACK_THRESHOLD_FIELD_NUMBER: _ClassVar[int]
        plugin: str
        settings: _containers.ScalarMap[str, str]
        window_size: int
        window_nack_threshold: int
        def __init__(self, plugin: _Optional[str] = ..., settings: _Optional[_Mapping[str, str]] = ..., window_size: _Optional[int] = ..., window_nack_threshold: _Optional[int] = ...) -> None: ...
    ID_FIELD_NUMBER: _ClassVar[int]
    STATE_FIELD_NUMBER: _ClassVar[int]
    CONFIG_FIELD_NUMBER: _ClassVar[int]
    CONNECTOR_IDS_FIELD_NUMBER: _ClassVar[int]
    PROCESSOR_IDS_FIELD_NUMBER: _ClassVar[int]
    CREATED_AT_FIELD_NUMBER: _ClassVar[int]
    UPDATED_AT_FIELD_NUMBER: _ClassVar[int]
    id: str
    state: Pipeline.State
    config: Pipeline.Config
    connector_ids: _containers.RepeatedScalarFieldContainer[str]
    processor_ids: _containers.RepeatedScalarFieldContainer[str]
    created_at: _timestamp_pb2.Timestamp
    updated_at: _timestamp_pb2.Timestamp
    def __init__(self, id: _Optional[str] = ..., state: _Optional[_Union[Pipeline.State, _Mapping]] = ..., config: _Optional[_Union[Pipeline.Config, _Mapping]] = ..., connector_ids: _Optional[_Iterable[str]] = ..., processor_ids: _Optional[_Iterable[str]] = ..., created_at: _Optional[_Union[_timestamp_pb2.Timestamp, _Mapping]] = ..., updated_at: _Optional[_Union[_timestamp_pb2.Timestamp, _Mapping]] = ...) -> None: ...

class Connector(_message.Message):
    __slots__ = ("id", "destination_state", "source_state", "config", "type", "plugin", "pipeline_id", "processor_ids", "created_at", "updated_at")
    class Type(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
        __slots__ = ()
        TYPE_UNSPECIFIED: _ClassVar[Connector.Type]
        TYPE_SOURCE: _ClassVar[Connector.Type]
        TYPE_DESTINATION: _ClassVar[Connector.Type]
    TYPE_UNSPECIFIED: Connector.Type
    TYPE_SOURCE: Connector.Type
    TYPE_DESTINATION: Connector.Type
    class SourceState(_message.Message):
        __slots__ = ("position",)
        POSITION_FIELD_NUMBER: _ClassVar[int]
        position: bytes
        def __init__(self, position: _Optional[bytes] = ...) -> None: ...
    class DestinationState(_message.Message):
        __slots__ = ("positions",)
        class PositionsEntry(_message.Message):
            __slots__ = ("key", "value")
            KEY_FIELD_NUMBER: _ClassVar[int]
            VALUE_FIELD_NUMBER: _ClassVar[int]
            key: str
            value: bytes
            def __init__(self, key: _Optional[str] = ..., value: _Optional[bytes] = ...) -> None: ...
        POSITIONS_FIELD_NUMBER: _ClassVar[int]
        positions: _containers.ScalarMap[str, bytes]
        def __init__(self, positions: _Optional[_Mapping[str, bytes]] = ...) -> None: ...
    class Config(_message.Message):
        __slots__ = ("name", "settings")
        class SettingsEntry(_message.Message):
            __slots__ = ("key", "value")
            KEY_FIELD_NUMBER: _ClassVar[int]
            VALUE_FIELD_NUMBER: _ClassVar[int]
            key: str
            value: str
            def __init__(self, key: _Optional[str] = ..., value: _Optional[str] = ...) -> None: ...
        NAME_FIELD_NUMBER: _ClassVar[int]
        SETTINGS_FIELD_NUMBER: _ClassVar[int]
        name: str
        settings: _containers.ScalarMap[str, str]
        def __init__(self, name: _Optional[str] = ..., settings: _Optional[_Mapping[str, str]] = ...) -> None: ...
    ID_FIELD_NUMBER: _ClassVar[int]
    DESTINATION_STATE_FIELD_NUMBER: _ClassVar[int]
    SOURCE_STATE_FIELD_NUMBER: _ClassVar[int]
    CONFIG_FIELD_NUMBER: _ClassVar[int]
    TYPE_FIELD_NUMBER: _ClassVar[int]
    PLUGIN_FIELD_NUMBER: _ClassVar[int]
    PIPELINE_ID_FIELD_NUMBER: _ClassVar[int]
    PROCESSOR_IDS_FIELD_NUMBER: _ClassVar[int]
    CREATED_AT_FIELD_NUMBER: _ClassVar[int]
    UPDATED_AT_FIELD_NUMBER: _ClassVar[int]
    id: str
    destination_state: Connector.DestinationState
    source_state: Connector.SourceState
    config: Connector.Config
    type: Connector.Type
    plugin: str
    pipeline_id: str
    processor_ids: _containers.RepeatedScalarFieldContainer[str]
    created_at: _timestamp_pb2.Timestamp
    updated_at: _timestamp_pb2.Timestamp
    def __init__(self, id: _Optional[str] = ..., destination_state: _Optional[_Union[Connector.DestinationState, _Mapping]] = ..., source_state: _Optional[_Union[Connector.SourceState, _Mapping]] = ..., config: _Optional[_Union[Connector.Config, _Mapping]] = ..., type: _Optional[_Union[Connector.Type, str]] = ..., plugin: _Optional[str] = ..., pipeline_id: _Optional[str] = ..., processor_ids: _Optional[_Iterable[str]] = ..., created_at: _Optional[_Union[_timestamp_pb2.Timestamp, _Mapping]] = ..., updated_at: _Optional[_Union[_timestamp_pb2.Timestamp, _Mapping]] = ...) -> None: ...

class Processor(_message.Message):
    __slots__ = ("id", "config", "condition", "plugin", "parent", "created_at", "updated_at")
    class Parent(_message.Message):
        __slots__ = ("type", "id")
        class Type(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
            __slots__ = ()
            TYPE_UNSPECIFIED: _ClassVar[Processor.Parent.Type]
            TYPE_CONNECTOR: _ClassVar[Processor.Parent.Type]
            TYPE_PIPELINE: _ClassVar[Processor.Parent.Type]
        TYPE_UNSPECIFIED: Processor.Parent.Type
        TYPE_CONNECTOR: Processor.Parent.Type
        TYPE_PIPELINE: Processor.Parent.Type
        TYPE_FIELD_NUMBER: _ClassVar[int]
        ID_FIELD_NUMBER: _ClassVar[int]
        type: Processor.Parent.Type
        id: str
        def __init__(self, type: _Optional[_Union[Processor.Parent.Type, str]] = ..., id: _Optional[str] = ...) -> None: ...
    class Config(_message.Message):
        __slots__ = ("settings", "workers")
        class SettingsEntry(_message.Message):
            __slots__ = ("key", "value")
            KEY_FIELD_NUMBER: _ClassVar[int]
            VALUE_FIELD_NUMBER: _ClassVar[int]
            key: str
            value: str
            def __init__(self, key: _Optional[str] = ..., value: _Optional[str] = ...) -> None: ...
        SETTINGS_FIELD_NUMBER: _ClassVar[int]
        WORKERS_FIELD_NUMBER: _ClassVar[int]
        settings: _containers.ScalarMap[str, str]
        workers: int
        def __init__(self, settings: _Optional[_Mapping[str, str]] = ..., workers: _Optional[int] = ...) -> None: ...
    ID_FIELD_NUMBER: _ClassVar[int]
    CONFIG_FIELD_NUMBER: _ClassVar[int]
    CONDITION_FIELD_NUMBER: _ClassVar[int]
    PLUGIN_FIELD_NUMBER: _ClassVar[int]
    PARENT_FIELD_NUMBER: _ClassVar[int]
    CREATED_AT_FIELD_NUMBER: _ClassVar[int]
    UPDATED_AT_FIELD_NUMBER: _ClassVar[int]
    id: str
    config: Processor.Config
    condition: str
    plugin: str
    parent: Processor.Parent
    created_at: _timestamp_pb2.Timestamp
    updated_at: _timestamp_pb2.Timestamp
    def __init__(self, id: _Optional[str] = ..., config: _Optional[_Union[Processor.Config, _Mapping]] = ..., condition: _Optional[str] = ..., plugin: _Optional[str] = ..., parent: _Optional[_Union[Processor.Parent, _Mapping]] = ..., created_at: _Optional[_Union[_timestamp_pb2.Timestamp, _Mapping]] = ..., updated_at: _Optional[_Union[_timestamp_pb2.Timestamp, _Mapping]] = ...) -> None: ...

class ConnectorPluginSpecifications(_message.Message):
    __slots__ = ("name", "summary", "description", "version", "author", "destination_params", "source_params")
    class DestinationParamsEntry(_message.Message):
        __slots__ = ("key", "value")
        KEY_FIELD_NUMBER: _ClassVar[int]
        VALUE_FIELD_NUMBER: _ClassVar[int]
        key: str
        value: _parameter_pb2.Parameter
        def __init__(self, key: _Optional[str] = ..., value: _Optional[_Union[_parameter_pb2.Parameter, _Mapping]] = ...) -> None: ...
    class SourceParamsEntry(_message.Message):
        __slots__ = ("key", "value")
        KEY_FIELD_NUMBER: _ClassVar[int]
        VALUE_FIELD_NUMBER: _ClassVar[int]
        key: str
        value: _parameter_pb2.Parameter
        def __init__(self, key: _Optional[str] = ..., value: _Optional[_Union[_parameter_pb2.Parameter, _Mapping]] = ...) -> None: ...
    NAME_FIELD_NUMBER: _ClassVar[int]
    SUMMARY_FIELD_NUMBER: _ClassVar[int]
    DESCRIPTION_FIELD_NUMBER: _ClassVar[int]
    VERSION_FIELD_NUMBER: _ClassVar[int]
    AUTHOR_FIELD_NUMBER: _ClassVar[int]
    DESTINATION_PARAMS_FIELD_NUMBER: _ClassVar[int]
    SOURCE_PARAMS_FIELD_NUMBER: _ClassVar[int]
    name: str
    summary: str
    description: str
    version: str
    author: str
    destination_params: _containers.MessageMap[str, _parameter_pb2.Parameter]
    source_params: _containers.MessageMap[str, _parameter_pb2.Parameter]
    def __init__(self, name: _Optional[str] = ..., summary: _Optional[str] = ..., description: _Optional[str] = ..., version: _Optional[str] = ..., author: _Optional[str] = ..., destination_params: _Optional[_Mapping[str, _parameter_pb2.Parameter]] = ..., source_params: _Optional[_Mapping[str, _parameter_pb2.Parameter]] = ...) -> None: ...

class ProcessorPluginSpecifications(_message.Message):
    __slots__ = ("name", "summary", "description", "version", "author", "parameters")
    class ParametersEntry(_message.Message):
        __slots__ = ("key", "value")
        KEY_FIELD_NUMBER: _ClassVar[int]
        VALUE_FIELD_NUMBER: _ClassVar[int]
        key: str
        value: _parameter_pb2.Parameter
        def __init__(self, key: _Optional[str] = ..., value: _Optional[_Union[_parameter_pb2.Parameter, _Mapping]] = ...) -> None: ...
    NAME_FIELD_NUMBER: _ClassVar[int]
    SUMMARY_FIELD_NUMBER: _ClassVar[int]
    DESCRIPTION_FIELD_NUMBER: _ClassVar[int]
    VERSION_FIELD_NUMBER: _ClassVar[int]
    AUTHOR_FIELD_NUMBER: _ClassVar[int]
    PARAMETERS_FIELD_NUMBER: _ClassVar[int]
    name: str
    summary: str
    description: str
    version: str
    author: str
    parameters: _containers.MessageMap[str, _parameter_pb2.Parameter]
    def __init__(self, name: _Optional[str] = ..., summary: _Optional[str] = ..., description: _Optional[str] = ..., version: _Optional[str] = ..., author: _Optional[str] = ..., parameters: _Optional[_Mapping[str, _parameter_pb2.Parameter]] = ...) -> None: ...

class PluginSpecifications(_message.Message):
    __slots__ = ("name", "summary", "description", "version", "author", "destination_params", "source_params")
    class Parameter(_message.Message):
        __slots__ = ("description", "default", "type", "validations")
        class Type(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
            __slots__ = ()
            TYPE_UNSPECIFIED: _ClassVar[PluginSpecifications.Parameter.Type]
            TYPE_STRING: _ClassVar[PluginSpecifications.Parameter.Type]
            TYPE_INT: _ClassVar[PluginSpecifications.Parameter.Type]
            TYPE_FLOAT: _ClassVar[PluginSpecifications.Parameter.Type]
            TYPE_BOOL: _ClassVar[PluginSpecifications.Parameter.Type]
            TYPE_FILE: _ClassVar[PluginSpecifications.Parameter.Type]
            TYPE_DURATION: _ClassVar[PluginSpecifications.Parameter.Type]
        TYPE_UNSPECIFIED: PluginSpecifications.Parameter.Type
        TYPE_STRING: PluginSpecifications.Parameter.Type
        TYPE_INT: PluginSpecifications.Parameter.Type
        TYPE_FLOAT: PluginSpecifications.Parameter.Type
        TYPE_BOOL: PluginSpecifications.Parameter.Type
        TYPE_FILE: PluginSpecifications.Parameter.Type
        TYPE_DURATION: PluginSpecifications.Parameter.Type
        class Validation(_message.Message):
            __slots__ = ("type", "value")
            class Type(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
                __slots__ = ()
                TYPE_UNSPECIFIED: _ClassVar[PluginSpecifications.Parameter.Validation.Type]
                TYPE_REQUIRED: _ClassVar[PluginSpecifications.Parameter.Validation.Type]
                TYPE_GREATER_THAN: _ClassVar[PluginSpecifications.Parameter.Validation.Type]
                TYPE_LESS_THAN: _ClassVar[PluginSpecifications.Parameter.Validation.Type]
                TYPE_INCLUSION: _ClassVar[PluginSpecifications.Parameter.Validation.Type]
                TYPE_EXCLUSION: _ClassVar[PluginSpecifications.Parameter.Validation.Type]
                TYPE_REGEX: _ClassVar[PluginSpecifications.Parameter.Validation.Type]
            TYPE_UNSPECIFIED: PluginSpecifications.Parameter.Validation.Type
            TYPE_REQUIRED: PluginSpecifications.Parameter.Validation.Type
            TYPE_GREATER_THAN: PluginSpecifications.Parameter.Validation.Type
            TYPE_LESS_THAN: PluginSpecifications.Parameter.Validation.Type
            TYPE_INCLUSION: PluginSpecifications.Parameter.Validation.Type
            TYPE_EXCLUSION: PluginSpecifications.Parameter.Validation.Type
            TYPE_REGEX: PluginSpecifications.Parameter.Validation.Type
            TYPE_FIELD_NUMBER: _ClassVar[int]
            VALUE_FIELD_NUMBER: _ClassVar[int]
            type: PluginSpecifications.Parameter.Validation.Type
            value: str
            def __init__(self, type: _Optional[_Union[PluginSpecifications.Parameter.Validation.Type, str]] = ..., value: _Optional[str] = ...) -> None: ...
        DESCRIPTION_FIELD_NUMBER: _ClassVar[int]
        DEFAULT_FIELD_NUMBER: _ClassVar[int]
        TYPE_FIELD_NUMBER: _ClassVar[int]
        VALIDATIONS_FIELD_NUMBER: _ClassVar[int]
        description: str
        default: str
        type: PluginSpecifications.Parameter.Type
        validations: _containers.RepeatedCompositeFieldContainer[PluginSpecifications.Parameter.Validation]
        def __init__(self, description: _Optional[str] = ..., default: _Optional[str] = ..., type: _Optional[_Union[PluginSpecifications.Parameter.Type, str]] = ..., validations: _Optional[_Iterable[_Union[PluginSpecifications.Parameter.Validation, _Mapping]]] = ...) -> None: ...
    class DestinationParamsEntry(_message.Message):
        __slots__ = ("key", "value")
        KEY_FIELD_NUMBER: _ClassVar[int]
        VALUE_FIELD_NUMBER: _ClassVar[int]
        key: str
        value: PluginSpecifications.Parameter
        def __init__(self, key: _Optional[str] = ..., value: _Optional[_Union[PluginSpecifications.Parameter, _Mapping]] = ...) -> None: ...
    class SourceParamsEntry(_message.Message):
        __slots__ = ("key", "value")
        KEY_FIELD_NUMBER: _ClassVar[int]
        VALUE_FIELD_NUMBER: _ClassVar[int]
        key: str
        value: PluginSpecifications.Parameter
        def __init__(self, key: _Optional[str] = ..., value: _Optional[_Union[PluginSpecifications.Parameter, _Mapping]] = ...) -> None: ...
    NAME_FIELD_NUMBER: _ClassVar[int]
    SUMMARY_FIELD_NUMBER: _ClassVar[int]
    DESCRIPTION_FIELD_NUMBER: _ClassVar[int]
    VERSION_FIELD_NUMBER: _ClassVar[int]
    AUTHOR_FIELD_NUMBER: _ClassVar[int]
    DESTINATION_PARAMS_FIELD_NUMBER: _ClassVar[int]
    SOURCE_PARAMS_FIELD_NUMBER: _ClassVar[int]
    name: str
    summary: str
    description: str
    version: str
    author: str
    destination_params: _containers.MessageMap[str, PluginSpecifications.Parameter]
    source_params: _containers.MessageMap[str, PluginSpecifications.Parameter]
    def __init__(self, name: _Optional[str] = ..., summary: _Optional[str] = ..., description: _Optional[str] = ..., version: _Optional[str] = ..., author: _Optional[str] = ..., destination_params: _Optional[_Mapping[str, PluginSpecifications.Parameter]] = ..., source_params: _Optional[_Mapping[str, PluginSpecifications.Parameter]] = ...) -> None: ...

class ListPipelinesRequest(_message.Message):
    __slots__ = ("name",)
    NAME_FIELD_NUMBER: _ClassVar[int]
    name: str
    def __init__(self, name: _Optional[str] = ...) -> None: ...

class ListPipelinesResponse(_message.Message):
    __slots__ = ("pipelines",)
    PIPELINES_FIELD_NUMBER: _ClassVar[int]
    pipelines: _containers.RepeatedCompositeFieldContainer[Pipeline]
    def __init__(self, pipelines: _Optional[_Iterable[_Union[Pipeline, _Mapping]]] = ...) -> None: ...

class CreatePipelineRequest(_message.Message):
    __slots__ = ("config",)
    CONFIG_FIELD_NUMBER: _ClassVar[int]
    config: Pipeline.Config
    def __init__(self, config: _Optional[_Union[Pipeline.Config, _Mapping]] = ...) -> None: ...

class CreatePipelineResponse(_message.Message):
    __slots__ = ("pipeline",)
    PIPELINE_FIELD_NUMBER: _ClassVar[int]
    pipeline: Pipeline
    def __init__(self, pipeline: _Optional[_Union[Pipeline, _Mapping]] = ...) -> None: ...

class GetPipelineRequest(_message.Message):
    __slots__ = ("id",)
    ID_FIELD_NUMBER: _ClassVar[int]
    id: str
    def __init__(self, id: _Optional[str] = ...) -> None: ...

class GetPipelineResponse(_message.Message):
    __slots__ = ("pipeline",)
    PIPELINE_FIELD_NUMBER: _ClassVar[int]
    pipeline: Pipeline
    def __init__(self, pipeline: _Optional[_Union[Pipeline, _Mapping]] = ...) -> None: ...

class UpdatePipelineRequest(_message.Message):
    __slots__ = ("id", "config")
    ID_FIELD_NUMBER: _ClassVar[int]
    CONFIG_FIELD_NUMBER: _ClassVar[int]
    id: str
    config: Pipeline.Config
    def __init__(self, id: _Optional[str] = ..., config: _Optional[_Union[Pipeline.Config, _Mapping]] = ...) -> None: ...

class UpdatePipelineResponse(_message.Message):
    __slots__ = ("pipeline",)
    PIPELINE_FIELD_NUMBER: _ClassVar[int]
    pipeline: Pipeline
    def __init__(self, pipeline: _Optional[_Union[Pipeline, _Mapping]] = ...) -> None: ...

class DeletePipelineRequest(_message.Message):
    __slots__ = ("id",)
    ID_FIELD_NUMBER: _ClassVar[int]
    id: str
    def __init__(self, id: _Optional[str] = ...) -> None: ...

class DeletePipelineResponse(_message.Message):
    __slots__ = ()
    def __init__(self) -> None: ...

class StartPipelineRequest(_message.Message):
    __slots__ = ("id",)
    ID_FIELD_NUMBER: _ClassVar[int]
    id: str
    def __init__(self, id: _Optional[str] = ...) -> None: ...

class StartPipelineResponse(_message.Message):
    __slots__ = ()
    def __init__(self) -> None: ...

class StopPipelineRequest(_message.Message):
    __slots__ = ("id", "force")
    ID_FIELD_NUMBER: _ClassVar[int]
    FORCE_FIELD_NUMBER: _ClassVar[int]
    id: str
    force: bool
    def __init__(self, id: _Optional[str] = ..., force: bool = ...) -> None: ...

class StopPipelineResponse(_message.Message):
    __slots__ = ()
    def __init__(self) -> None: ...

class GetDLQRequest(_message.Message):
    __slots__ = ("id",)
    ID_FIELD_NUMBER: _ClassVar[int]
    id: str
    def __init__(self, id: _Optional[str] = ...) -> None: ...

class GetDLQResponse(_message.Message):
    __slots__ = ("dlq",)
    DLQ_FIELD_NUMBER: _ClassVar[int]
    dlq: Pipeline.DLQ
    def __init__(self, dlq: _Optional[_Union[Pipeline.DLQ, _Mapping]] = ...) -> None: ...

class UpdateDLQRequest(_message.Message):
    __slots__ = ("id", "dlq")
    ID_FIELD_NUMBER: _ClassVar[int]
    DLQ_FIELD_NUMBER: _ClassVar[int]
    id: str
    dlq: Pipeline.DLQ
    def __init__(self, id: _Optional[str] = ..., dlq: _Optional[_Union[Pipeline.DLQ, _Mapping]] = ...) -> None: ...

class UpdateDLQResponse(_message.Message):
    __slots__ = ("dlq",)
    DLQ_FIELD_NUMBER: _ClassVar[int]
    dlq: Pipeline.DLQ
    def __init__(self, dlq: _Optional[_Union[Pipeline.DLQ, _Mapping]] = ...) -> None: ...

class ExportPipelineRequest(_message.Message):
    __slots__ = ("id",)
    ID_FIELD_NUMBER: _ClassVar[int]
    id: str
    def __init__(self, id: _Optional[str] = ...) -> None: ...

class ExportPipelineResponse(_message.Message):
    __slots__ = ("pipeline",)
    PIPELINE_FIELD_NUMBER: _ClassVar[int]
    pipeline: Pipeline
    def __init__(self, pipeline: _Optional[_Union[Pipeline, _Mapping]] = ...) -> None: ...

class ImportPipelineRequest(_message.Message):
    __slots__ = ("pipeline",)
    PIPELINE_FIELD_NUMBER: _ClassVar[int]
    pipeline: Pipeline
    def __init__(self, pipeline: _Optional[_Union[Pipeline, _Mapping]] = ...) -> None: ...

class ImportPipelineResponse(_message.Message):
    __slots__ = ("pipeline",)
    PIPELINE_FIELD_NUMBER: _ClassVar[int]
    pipeline: Pipeline
    def __init__(self, pipeline: _Optional[_Union[Pipeline, _Mapping]] = ...) -> None: ...

class PipelineDocument(_message.Message):
    __slots__ = ("id", "status", "name", "description", "connectors", "processors", "dlq")
    class Connector(_message.Message):
        __slots__ = ("id", "type", "plugin", "name", "settings", "processors")
        class SettingsEntry(_message.Message):
            __slots__ = ("key", "value")
            KEY_FIELD_NUMBER: _ClassVar[int]
            VALUE_FIELD_NUMBER: _ClassVar[int]
            key: str
            value: str
            def __init__(self, key: _Optional[str] = ..., value: _Optional[str] = ...) -> None: ...
        ID_FIELD_NUMBER: _ClassVar[int]
        TYPE_FIELD_NUMBER: _ClassVar[int]
        PLUGIN_FIELD_NUMBER: _ClassVar[int]
        NAME_FIELD_NUMBER: _ClassVar[int]
        SETTINGS_FIELD_NUMBER: _ClassVar[int]
        PROCESSORS_FIELD_NUMBER: _ClassVar[int]
        id: str
        type: str
        plugin: str
        name: str
        settings: _containers.ScalarMap[str, str]
        processors: _containers.RepeatedCompositeFieldContainer[PipelineDocument.Processor]
        def __init__(self, id: _Optional[str] = ..., type: _Optional[str] = ..., plugin: _Optional[str] = ..., name: _Optional[str] = ..., settings: _Optional[_Mapping[str, str]] = ..., processors: _Optional[_Iterable[_Union[PipelineDocument.Processor, _Mapping]]] = ...) -> None: ...
    class Processor(_message.Message):
        __slots__ = ("id", "plugin", "settings", "workers", "condition")
        class SettingsEntry(_message.Message):
            __slots__ = ("key", "value")
            KEY_FIELD_NUMBER: _ClassVar[int]
            VALUE_FIELD_NUMBER: _ClassVar[int]
            key: str
            value: str
            def __init__(self, key: _Optional[str] = ..., value: _Optional[str] = ...) -> None: ...
        ID_FIELD_NUMBER: _ClassVar[int]
        PLUGIN_FIELD_NUMBER: _ClassVar[int]
        SETTINGS_FIELD_NUMBER: _ClassVar[int]
        WORKERS_FIELD_NUMBER: _ClassVar[int]
        CONDITION_FIELD_NUMBER: _ClassVar[int]
        id: str
        plugin: str
        settings: _containers.ScalarMap[str, str]
        workers: int
        condition: str
        def __init__(self, id: _Optional[str] = ..., plugin: _Optional[str] = ..., settings: _Optional[_Mapping[str, str]] = ..., workers: _Optional[int] = ..., condition: _Optional[str] = ...) -> None: ...
    class DLQ(_message.Message):
        __slots__ = ("plugin", "settings", "window_size", "window_nack_threshold")
        class SettingsEntry(_message.Message):
            __slots__ = ("key", "value")
            KEY_FIELD_NUMBER: _ClassVar[int]
            VALUE_FIELD_NUMBER: _ClassVar[int]
            key: str
            value: str
            def __init__(self, key: _Optional[str] = ..., value: _Optional[str] = ...) -> None: ...
        PLUGIN_FIELD_NUMBER: _ClassVar[int]
        SETTINGS_FIELD_NUMBER: _ClassVar[int]
        WINDOW_SIZE_FIELD_NUMBER: _ClassVar[int]
        WINDOW_NACK_THRESHOLD_FIELD_NUMBER: _ClassVar[int]
        plugin: str
        settings: _containers.ScalarMap[str, str]
        window_size: int
        window_nack_threshold: int
        def __init__(self, plugin: _Optional[str] = ..., settings: _Optional[_Mapping[str, str]] = ..., window_size: _Optional[int] = ..., window_nack_threshold: _Optional[int] = ...) -> None: ...
    ID_FIELD_NUMBER: _ClassVar[int]
    STATUS_FIELD_NUMBER: _ClassVar[int]
    NAME_FIELD_NUMBER: _ClassVar[int]
    DESCRIPTION_FIELD_NUMBER: _ClassVar[int]
    CONNECTORS_FIELD_NUMBER: _ClassVar[int]
    PROCESSORS_FIELD_NUMBER: _ClassVar[int]
    DLQ_FIELD_NUMBER: _ClassVar[int]
    id: str
    status: str
    name: str
    description: str
    connectors: _containers.RepeatedCompositeFieldContainer[PipelineDocument.Connector]
    processors: _containers.RepeatedCompositeFieldContainer[PipelineDocument.Processor]
    dlq: PipelineDocument.DLQ
    def __init__(self, id: _Optional[str] = ..., status: _Optional[str] = ..., name: _Optional[str] = ..., description: _Optional[str] = ..., connectors: _Optional[_Iterable[_Union[PipelineDocument.Connector, _Mapping]]] = ..., processors: _Optional[_Iterable[_Union[PipelineDocument.Processor, _Mapping]]] = ..., dlq: _Optional[_Union[PipelineDocument.DLQ, _Mapping]] = ...) -> None: ...

class Diff(_message.Message):
    __slots__ = ("pipeline_id", "changes", "hash")
    class Change(_message.Message):
        __slots__ = ("resource", "id", "action", "effect", "config_paths", "code")
        RESOURCE_FIELD_NUMBER: _ClassVar[int]
        ID_FIELD_NUMBER: _ClassVar[int]
        ACTION_FIELD_NUMBER: _ClassVar[int]
        EFFECT_FIELD_NUMBER: _ClassVar[int]
        CONFIG_PATHS_FIELD_NUMBER: _ClassVar[int]
        CODE_FIELD_NUMBER: _ClassVar[int]
        resource: str
        id: str
        action: str
        effect: str
        config_paths: _containers.RepeatedScalarFieldContainer[str]
        code: str
        def __init__(self, resource: _Optional[str] = ..., id: _Optional[str] = ..., action: _Optional[str] = ..., effect: _Optional[str] = ..., config_paths: _Optional[_Iterable[str]] = ..., code: _Optional[str] = ...) -> None: ...
    PIPELINE_ID_FIELD_NUMBER: _ClassVar[int]
    CHANGES_FIELD_NUMBER: _ClassVar[int]
    HASH_FIELD_NUMBER: _ClassVar[int]
    pipeline_id: str
    changes: _containers.RepeatedCompositeFieldContainer[Diff.Change]
    hash: str
    def __init__(self, pipeline_id: _Optional[str] = ..., changes: _Optional[_Iterable[_Union[Diff.Change, _Mapping]]] = ..., hash: _Optional[str] = ...) -> None: ...

class PlanPipelineRequest(_message.Message):
    __slots__ = ("config",)
    CONFIG_FIELD_NUMBER: _ClassVar[int]
    config: PipelineDocument
    def __init__(self, config: _Optional[_Union[PipelineDocument, _Mapping]] = ...) -> None: ...

class PlanPipelineResponse(_message.Message):
    __slots__ = ("diff",)
    DIFF_FIELD_NUMBER: _ClassVar[int]
    diff: Diff
    def __init__(self, diff: _Optional[_Union[Diff, _Mapping]] = ...) -> None: ...

class ApplyPipelineRequest(_message.Message):
    __slots__ = ("config", "hash")
    CONFIG_FIELD_NUMBER: _ClassVar[int]
    HASH_FIELD_NUMBER: _ClassVar[int]
    config: PipelineDocument
    hash: str
    def __init__(self, config: _Optional[_Union[PipelineDocument, _Mapping]] = ..., hash: _Optional[str] = ...) -> None: ...

class ApplyPipelineResponse(_message.Message):
    __slots__ = ("diff",)
    DIFF_FIELD_NUMBER: _ClassVar[int]
    diff: Diff
    def __init__(self, diff: _Optional[_Union[Diff, _Mapping]] = ...) -> None: ...

class CreateConnectorRequest(_message.Message):
    __slots__ = ("type", "plugin", "pipeline_id", "config")
    TYPE_FIELD_NUMBER: _ClassVar[int]
    PLUGIN_FIELD_NUMBER: _ClassVar[int]
    PIPELINE_ID_FIELD_NUMBER: _ClassVar[int]
    CONFIG_FIELD_NUMBER: _ClassVar[int]
    type: Connector.Type
    plugin: str
    pipeline_id: str
    config: Connector.Config
    def __init__(self, type: _Optional[_Union[Connector.Type, str]] = ..., plugin: _Optional[str] = ..., pipeline_id: _Optional[str] = ..., config: _Optional[_Union[Connector.Config, _Mapping]] = ...) -> None: ...

class CreateConnectorResponse(_message.Message):
    __slots__ = ("connector",)
    CONNECTOR_FIELD_NUMBER: _ClassVar[int]
    connector: Connector
    def __init__(self, connector: _Optional[_Union[Connector, _Mapping]] = ...) -> None: ...

class ValidateConnectorRequest(_message.Message):
    __slots__ = ("type", "plugin", "config")
    TYPE_FIELD_NUMBER: _ClassVar[int]
    PLUGIN_FIELD_NUMBER: _ClassVar[int]
    CONFIG_FIELD_NUMBER: _ClassVar[int]
    type: Connector.Type
    plugin: str
    config: Connector.Config
    def __init__(self, type: _Optional[_Union[Connector.Type, str]] = ..., plugin: _Optional[str] = ..., config: _Optional[_Union[Connector.Config, _Mapping]] = ...) -> None: ...

class ValidateConnectorResponse(_message.Message):
    __slots__ = ()
    def __init__(self) -> None: ...

class ListConnectorsRequest(_message.Message):
    __slots__ = ("pipeline_id",)
    PIPELINE_ID_FIELD_NUMBER: _ClassVar[int]
    pipeline_id: str
    def __init__(self, pipeline_id: _Optional[str] = ...) -> None: ...

class ListConnectorsResponse(_message.Message):
    __slots__ = ("connectors",)
    CONNECTORS_FIELD_NUMBER: _ClassVar[int]
    connectors: _containers.RepeatedCompositeFieldContainer[Connector]
    def __init__(self, connectors: _Optional[_Iterable[_Union[Connector, _Mapping]]] = ...) -> None: ...

class InspectConnectorRequest(_message.Message):
    __slots__ = ("id",)
    ID_FIELD_NUMBER: _ClassVar[int]
    id: str
    def __init__(self, id: _Optional[str] = ...) -> None: ...

class InspectConnectorResponse(_message.Message):
    __slots__ = ("record",)
    RECORD_FIELD_NUMBER: _ClassVar[int]
    record: _opencdc_pb2.Record
    def __init__(self, record: _Optional[_Union[_opencdc_pb2.Record, _Mapping]] = ...) -> None: ...

class GetConnectorRequest(_message.Message):
    __slots__ = ("id",)
    ID_FIELD_NUMBER: _ClassVar[int]
    id: str
    def __init__(self, id: _Optional[str] = ...) -> None: ...

class GetConnectorResponse(_message.Message):
    __slots__ = ("connector",)
    CONNECTOR_FIELD_NUMBER: _ClassVar[int]
    connector: Connector
    def __init__(self, connector: _Optional[_Union[Connector, _Mapping]] = ...) -> None: ...

class UpdateConnectorRequest(_message.Message):
    __slots__ = ("id", "config", "plugin")
    ID_FIELD_NUMBER: _ClassVar[int]
    CONFIG_FIELD_NUMBER: _ClassVar[int]
    PLUGIN_FIELD_NUMBER: _ClassVar[int]
    id: str
    config: Connector.Config
    plugin: str
    def __init__(self, id: _Optional[str] = ..., config: _Optional[_Union[Connector.Config, _Mapping]] = ..., plugin: _Optional[str] = ...) -> None: ...

class UpdateConnectorResponse(_message.Message):
    __slots__ = ("connector",)
    CONNECTOR_FIELD_NUMBER: _ClassVar[int]
    connector: Connector
    def __init__(self, connector: _Optional[_Union[Connector, _Mapping]] = ...) -> None: ...

class DeleteConnectorRequest(_message.Message):
    __slots__ = ("id",)
    ID_FIELD_NUMBER: _ClassVar[int]
    id: str
    def __init__(self, id: _Optional[str] = ...) -> None: ...

class DeleteConnectorResponse(_message.Message):
    __slots__ = ()
    def __init__(self) -> None: ...

class ListConnectorPluginsRequest(_message.Message):
    __slots__ = ("name",)
    NAME_FIELD_NUMBER: _ClassVar[int]
    name: str
    def __init__(self, name: _Optional[str] = ...) -> None: ...

class ListConnectorPluginsResponse(_message.Message):
    __slots__ = ("plugins",)
    PLUGINS_FIELD_NUMBER: _ClassVar[int]
    plugins: _containers.RepeatedCompositeFieldContainer[ConnectorPluginSpecifications]
    def __init__(self, plugins: _Optional[_Iterable[_Union[ConnectorPluginSpecifications, _Mapping]]] = ...) -> None: ...

class ListProcessorsRequest(_message.Message):
    __slots__ = ("parent_ids",)
    PARENT_IDS_FIELD_NUMBER: _ClassVar[int]
    parent_ids: _containers.RepeatedScalarFieldContainer[str]
    def __init__(self, parent_ids: _Optional[_Iterable[str]] = ...) -> None: ...

class ListProcessorsResponse(_message.Message):
    __slots__ = ("processors",)
    PROCESSORS_FIELD_NUMBER: _ClassVar[int]
    processors: _containers.RepeatedCompositeFieldContainer[Processor]
    def __init__(self, processors: _Optional[_Iterable[_Union[Processor, _Mapping]]] = ...) -> None: ...

class InspectProcessorInRequest(_message.Message):
    __slots__ = ("id",)
    ID_FIELD_NUMBER: _ClassVar[int]
    id: str
    def __init__(self, id: _Optional[str] = ...) -> None: ...

class InspectProcessorInResponse(_message.Message):
    __slots__ = ("record",)
    RECORD_FIELD_NUMBER: _ClassVar[int]
    record: _opencdc_pb2.Record
    def __init__(self, record: _Optional[_Union[_opencdc_pb2.Record, _Mapping]] = ...) -> None: ...

class InspectProcessorOutRequest(_message.Message):
    __slots__ = ("id",)
    ID_FIELD_NUMBER: _ClassVar[int]
    id: str
    def __init__(self, id: _Optional[str] = ...) -> None: ...

class InspectProcessorOutResponse(_message.Message):
    __slots__ = ("record",)
    RECORD_FIELD_NUMBER: _ClassVar[int]
    record: _opencdc_pb2.Record
    def __init__(self, record: _Optional[_Union[_opencdc_pb2.Record, _Mapping]] = ...) -> None: ...

class CreateProcessorRequest(_message.Message):
    __slots__ = ("type", "parent", "config", "condition", "plugin")
    TYPE_FIELD_NUMBER: _ClassVar[int]
    PARENT_FIELD_NUMBER: _ClassVar[int]
    CONFIG_FIELD_NUMBER: _ClassVar[int]
    CONDITION_FIELD_NUMBER: _ClassVar[int]
    PLUGIN_FIELD_NUMBER: _ClassVar[int]
    type: str
    parent: Processor.Parent
    config: Processor.Config
    condition: str
    plugin: str
    def __init__(self, type: _Optional[str] = ..., parent: _Optional[_Union[Processor.Parent, _Mapping]] = ..., config: _Optional[_Union[Processor.Config, _Mapping]] = ..., condition: _Optional[str] = ..., plugin: _Optional[str] = ...) -> None: ...

class CreateProcessorResponse(_message.Message):
    __slots__ = ("processor",)
    PROCESSOR_FIELD_NUMBER: _ClassVar[int]
    processor: Processor
    def __init__(self, processor: _Optional[_Union[Processor, _Mapping]] = ...) -> None: ...

class GetProcessorRequest(_message.Message):
    __slots__ = ("id",)
    ID_FIELD_NUMBER: _ClassVar[int]
    id: str
    def __init__(self, id: _Optional[str] = ...) -> None: ...

class GetProcessorResponse(_message.Message):
    __slots__ = ("processor",)
    PROCESSOR_FIELD_NUMBER: _ClassVar[int]
    processor: Processor
    def __init__(self, processor: _Optional[_Union[Processor, _Mapping]] = ...) -> None: ...

class UpdateProcessorRequest(_message.Message):
    __slots__ = ("id", "config", "plugin")
    ID_FIELD_NUMBER: _ClassVar[int]
    CONFIG_FIELD_NUMBER: _ClassVar[int]
    PLUGIN_FIELD_NUMBER: _ClassVar[int]
    id: str
    config: Processor.Config
    plugin: str
    def __init__(self, id: _Optional[str] = ..., config: _Optional[_Union[Processor.Config, _Mapping]] = ..., plugin: _Optional[str] = ...) -> None: ...

class UpdateProcessorResponse(_message.Message):
    __slots__ = ("processor",)
    PROCESSOR_FIELD_NUMBER: _ClassVar[int]
    processor: Processor
    def __init__(self, processor: _Optional[_Union[Processor, _Mapping]] = ...) -> None: ...

class DeleteProcessorRequest(_message.Message):
    __slots__ = ("id",)
    ID_FIELD_NUMBER: _ClassVar[int]
    id: str
    def __init__(self, id: _Optional[str] = ...) -> None: ...

class DeleteProcessorResponse(_message.Message):
    __slots__ = ()
    def __init__(self) -> None: ...

class ListProcessorPluginsRequest(_message.Message):
    __slots__ = ("name",)
    NAME_FIELD_NUMBER: _ClassVar[int]
    name: str
    def __init__(self, name: _Optional[str] = ...) -> None: ...

class ListProcessorPluginsResponse(_message.Message):
    __slots__ = ("plugins",)
    PLUGINS_FIELD_NUMBER: _ClassVar[int]
    plugins: _containers.RepeatedCompositeFieldContainer[ProcessorPluginSpecifications]
    def __init__(self, plugins: _Optional[_Iterable[_Union[ProcessorPluginSpecifications, _Mapping]]] = ...) -> None: ...

class GetInfoRequest(_message.Message):
    __slots__ = ()
    def __init__(self) -> None: ...

class GetInfoResponse(_message.Message):
    __slots__ = ("info",)
    INFO_FIELD_NUMBER: _ClassVar[int]
    info: Info
    def __init__(self, info: _Optional[_Union[Info, _Mapping]] = ...) -> None: ...

class Info(_message.Message):
    __slots__ = ("version", "os", "arch")
    VERSION_FIELD_NUMBER: _ClassVar[int]
    OS_FIELD_NUMBER: _ClassVar[int]
    ARCH_FIELD_NUMBER: _ClassVar[int]
    version: str
    os: str
    arch: str
    def __init__(self, version: _Optional[str] = ..., os: _Optional[str] = ..., arch: _Optional[str] = ...) -> None: ...

class ListPluginsRequest(_message.Message):
    __slots__ = ("name",)
    NAME_FIELD_NUMBER: _ClassVar[int]
    name: str
    def __init__(self, name: _Optional[str] = ...) -> None: ...

class ListPluginsResponse(_message.Message):
    __slots__ = ("plugins",)
    PLUGINS_FIELD_NUMBER: _ClassVar[int]
    plugins: _containers.RepeatedCompositeFieldContainer[PluginSpecifications]
    def __init__(self, plugins: _Optional[_Iterable[_Union[PluginSpecifications, _Mapping]]] = ...) -> None: ...
