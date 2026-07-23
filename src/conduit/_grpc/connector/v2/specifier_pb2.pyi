from google.protobuf import descriptor_pb2 as _descriptor_pb2
from config.v1 import parameter_pb2 as _parameter_pb2
from google.protobuf.internal import containers as _containers
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from typing import ClassVar as _ClassVar, Mapping as _Mapping, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class Specification(_message.Message):
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

class Specifier(_message.Message):
    __slots__ = ()
    class Specify(_message.Message):
        __slots__ = ()
        class Request(_message.Message):
            __slots__ = ()
            def __init__(self) -> None: ...
        class Response(_message.Message):
            __slots__ = ("specification",)
            SPECIFICATION_FIELD_NUMBER: _ClassVar[int]
            specification: Specification
            def __init__(self, specification: _Optional[_Union[Specification, _Mapping]] = ...) -> None: ...
        def __init__(self) -> None: ...
    def __init__(self) -> None: ...
