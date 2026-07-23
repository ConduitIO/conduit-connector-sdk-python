from google.protobuf.internal import containers as _containers
from google.protobuf.internal import enum_type_wrapper as _enum_type_wrapper
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from typing import ClassVar as _ClassVar, Iterable as _Iterable, Mapping as _Mapping, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class Parameter(_message.Message):
    __slots__ = ("default", "description", "type", "validations")
    class Type(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
        __slots__ = ()
        TYPE_UNSPECIFIED: _ClassVar[Parameter.Type]
        TYPE_STRING: _ClassVar[Parameter.Type]
        TYPE_INT: _ClassVar[Parameter.Type]
        TYPE_FLOAT: _ClassVar[Parameter.Type]
        TYPE_BOOL: _ClassVar[Parameter.Type]
        TYPE_FILE: _ClassVar[Parameter.Type]
        TYPE_DURATION: _ClassVar[Parameter.Type]
    TYPE_UNSPECIFIED: Parameter.Type
    TYPE_STRING: Parameter.Type
    TYPE_INT: Parameter.Type
    TYPE_FLOAT: Parameter.Type
    TYPE_BOOL: Parameter.Type
    TYPE_FILE: Parameter.Type
    TYPE_DURATION: Parameter.Type
    DEFAULT_FIELD_NUMBER: _ClassVar[int]
    DESCRIPTION_FIELD_NUMBER: _ClassVar[int]
    TYPE_FIELD_NUMBER: _ClassVar[int]
    VALIDATIONS_FIELD_NUMBER: _ClassVar[int]
    default: str
    description: str
    type: Parameter.Type
    validations: _containers.RepeatedCompositeFieldContainer[Validation]
    def __init__(self, default: _Optional[str] = ..., description: _Optional[str] = ..., type: _Optional[_Union[Parameter.Type, str]] = ..., validations: _Optional[_Iterable[_Union[Validation, _Mapping]]] = ...) -> None: ...

class Validation(_message.Message):
    __slots__ = ("type", "value")
    class Type(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
        __slots__ = ()
        TYPE_UNSPECIFIED: _ClassVar[Validation.Type]
        TYPE_REQUIRED: _ClassVar[Validation.Type]
        TYPE_GREATER_THAN: _ClassVar[Validation.Type]
        TYPE_LESS_THAN: _ClassVar[Validation.Type]
        TYPE_INCLUSION: _ClassVar[Validation.Type]
        TYPE_EXCLUSION: _ClassVar[Validation.Type]
        TYPE_REGEX: _ClassVar[Validation.Type]
    TYPE_UNSPECIFIED: Validation.Type
    TYPE_REQUIRED: Validation.Type
    TYPE_GREATER_THAN: Validation.Type
    TYPE_LESS_THAN: Validation.Type
    TYPE_INCLUSION: Validation.Type
    TYPE_EXCLUSION: Validation.Type
    TYPE_REGEX: Validation.Type
    TYPE_FIELD_NUMBER: _ClassVar[int]
    VALUE_FIELD_NUMBER: _ClassVar[int]
    type: Validation.Type
    value: str
    def __init__(self, type: _Optional[_Union[Validation.Type, str]] = ..., value: _Optional[str] = ...) -> None: ...
