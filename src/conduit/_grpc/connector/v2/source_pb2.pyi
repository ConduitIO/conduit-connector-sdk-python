from google.protobuf import descriptor_pb2 as _descriptor_pb2
from opencdc.v1 import opencdc_pb2 as _opencdc_pb2
from google.protobuf.internal import containers as _containers
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from typing import ClassVar as _ClassVar, Iterable as _Iterable, Mapping as _Mapping, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class Source(_message.Message):
    __slots__ = ()
    class Configure(_message.Message):
        __slots__ = ()
        class Request(_message.Message):
            __slots__ = ("config",)
            class ConfigEntry(_message.Message):
                __slots__ = ("key", "value")
                KEY_FIELD_NUMBER: _ClassVar[int]
                VALUE_FIELD_NUMBER: _ClassVar[int]
                key: str
                value: str
                def __init__(self, key: _Optional[str] = ..., value: _Optional[str] = ...) -> None: ...
            CONFIG_FIELD_NUMBER: _ClassVar[int]
            config: _containers.ScalarMap[str, str]
            def __init__(self, config: _Optional[_Mapping[str, str]] = ...) -> None: ...
        class Response(_message.Message):
            __slots__ = ()
            def __init__(self) -> None: ...
        def __init__(self) -> None: ...
    class Open(_message.Message):
        __slots__ = ()
        class Request(_message.Message):
            __slots__ = ("position",)
            POSITION_FIELD_NUMBER: _ClassVar[int]
            position: bytes
            def __init__(self, position: _Optional[bytes] = ...) -> None: ...
        class Response(_message.Message):
            __slots__ = ()
            def __init__(self) -> None: ...
        def __init__(self) -> None: ...
    class Run(_message.Message):
        __slots__ = ()
        class Request(_message.Message):
            __slots__ = ("ack_positions",)
            ACK_POSITIONS_FIELD_NUMBER: _ClassVar[int]
            ack_positions: _containers.RepeatedScalarFieldContainer[bytes]
            def __init__(self, ack_positions: _Optional[_Iterable[bytes]] = ...) -> None: ...
        class Response(_message.Message):
            __slots__ = ("records",)
            RECORDS_FIELD_NUMBER: _ClassVar[int]
            records: _containers.RepeatedCompositeFieldContainer[_opencdc_pb2.Record]
            def __init__(self, records: _Optional[_Iterable[_Union[_opencdc_pb2.Record, _Mapping]]] = ...) -> None: ...
        def __init__(self) -> None: ...
    class Stop(_message.Message):
        __slots__ = ()
        class Request(_message.Message):
            __slots__ = ()
            def __init__(self) -> None: ...
        class Response(_message.Message):
            __slots__ = ("last_position",)
            LAST_POSITION_FIELD_NUMBER: _ClassVar[int]
            last_position: bytes
            def __init__(self, last_position: _Optional[bytes] = ...) -> None: ...
        def __init__(self) -> None: ...
    class Teardown(_message.Message):
        __slots__ = ()
        class Request(_message.Message):
            __slots__ = ()
            def __init__(self) -> None: ...
        class Response(_message.Message):
            __slots__ = ()
            def __init__(self) -> None: ...
        def __init__(self) -> None: ...
    class Lifecycle(_message.Message):
        __slots__ = ()
        class OnCreated(_message.Message):
            __slots__ = ()
            class Request(_message.Message):
                __slots__ = ("config",)
                class ConfigEntry(_message.Message):
                    __slots__ = ("key", "value")
                    KEY_FIELD_NUMBER: _ClassVar[int]
                    VALUE_FIELD_NUMBER: _ClassVar[int]
                    key: str
                    value: str
                    def __init__(self, key: _Optional[str] = ..., value: _Optional[str] = ...) -> None: ...
                CONFIG_FIELD_NUMBER: _ClassVar[int]
                config: _containers.ScalarMap[str, str]
                def __init__(self, config: _Optional[_Mapping[str, str]] = ...) -> None: ...
            class Response(_message.Message):
                __slots__ = ()
                def __init__(self) -> None: ...
            def __init__(self) -> None: ...
        class OnUpdated(_message.Message):
            __slots__ = ()
            class Request(_message.Message):
                __slots__ = ("config_before", "config_after")
                class ConfigBeforeEntry(_message.Message):
                    __slots__ = ("key", "value")
                    KEY_FIELD_NUMBER: _ClassVar[int]
                    VALUE_FIELD_NUMBER: _ClassVar[int]
                    key: str
                    value: str
                    def __init__(self, key: _Optional[str] = ..., value: _Optional[str] = ...) -> None: ...
                class ConfigAfterEntry(_message.Message):
                    __slots__ = ("key", "value")
                    KEY_FIELD_NUMBER: _ClassVar[int]
                    VALUE_FIELD_NUMBER: _ClassVar[int]
                    key: str
                    value: str
                    def __init__(self, key: _Optional[str] = ..., value: _Optional[str] = ...) -> None: ...
                CONFIG_BEFORE_FIELD_NUMBER: _ClassVar[int]
                CONFIG_AFTER_FIELD_NUMBER: _ClassVar[int]
                config_before: _containers.ScalarMap[str, str]
                config_after: _containers.ScalarMap[str, str]
                def __init__(self, config_before: _Optional[_Mapping[str, str]] = ..., config_after: _Optional[_Mapping[str, str]] = ...) -> None: ...
            class Response(_message.Message):
                __slots__ = ()
                def __init__(self) -> None: ...
            def __init__(self) -> None: ...
        class OnDeleted(_message.Message):
            __slots__ = ()
            class Request(_message.Message):
                __slots__ = ("config",)
                class ConfigEntry(_message.Message):
                    __slots__ = ("key", "value")
                    KEY_FIELD_NUMBER: _ClassVar[int]
                    VALUE_FIELD_NUMBER: _ClassVar[int]
                    key: str
                    value: str
                    def __init__(self, key: _Optional[str] = ..., value: _Optional[str] = ...) -> None: ...
                CONFIG_FIELD_NUMBER: _ClassVar[int]
                config: _containers.ScalarMap[str, str]
                def __init__(self, config: _Optional[_Mapping[str, str]] = ...) -> None: ...
            class Response(_message.Message):
                __slots__ = ()
                def __init__(self) -> None: ...
            def __init__(self) -> None: ...
        def __init__(self) -> None: ...
    def __init__(self) -> None: ...
