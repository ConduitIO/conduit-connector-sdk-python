from google.protobuf import descriptor_pb2 as _descriptor_pb2
from google.protobuf import struct_pb2 as _struct_pb2
from google.protobuf.internal import containers as _containers
from google.protobuf.internal import enum_type_wrapper as _enum_type_wrapper
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from typing import ClassVar as _ClassVar, Mapping as _Mapping, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class Operation(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    OPERATION_UNSPECIFIED: _ClassVar[Operation]
    OPERATION_CREATE: _ClassVar[Operation]
    OPERATION_UPDATE: _ClassVar[Operation]
    OPERATION_DELETE: _ClassVar[Operation]
    OPERATION_SNAPSHOT: _ClassVar[Operation]
OPERATION_UNSPECIFIED: Operation
OPERATION_CREATE: Operation
OPERATION_UPDATE: Operation
OPERATION_DELETE: Operation
OPERATION_SNAPSHOT: Operation
OPENCDC_VERSION_FIELD_NUMBER: _ClassVar[int]
opencdc_version: _descriptor.FieldDescriptor
METADATA_VERSION_FIELD_NUMBER: _ClassVar[int]
metadata_version: _descriptor.FieldDescriptor
METADATA_CREATED_AT_FIELD_NUMBER: _ClassVar[int]
metadata_created_at: _descriptor.FieldDescriptor
METADATA_READ_AT_FIELD_NUMBER: _ClassVar[int]
metadata_read_at: _descriptor.FieldDescriptor
METADATA_COLLECTION_FIELD_NUMBER: _ClassVar[int]
metadata_collection: _descriptor.FieldDescriptor
METADATA_KEY_SCHEMA_SUBJECT_FIELD_NUMBER: _ClassVar[int]
metadata_key_schema_subject: _descriptor.FieldDescriptor
METADATA_KEY_SCHEMA_VERSION_FIELD_NUMBER: _ClassVar[int]
metadata_key_schema_version: _descriptor.FieldDescriptor
METADATA_PAYLOAD_SCHEMA_SUBJECT_FIELD_NUMBER: _ClassVar[int]
metadata_payload_schema_subject: _descriptor.FieldDescriptor
METADATA_PAYLOAD_SCHEMA_VERSION_FIELD_NUMBER: _ClassVar[int]
metadata_payload_schema_version: _descriptor.FieldDescriptor
METADATA_FILE_NAME_FIELD_NUMBER: _ClassVar[int]
metadata_file_name: _descriptor.FieldDescriptor
METADATA_FILE_SIZE_FIELD_NUMBER: _ClassVar[int]
metadata_file_size: _descriptor.FieldDescriptor
METADATA_FILE_HASH_FIELD_NUMBER: _ClassVar[int]
metadata_file_hash: _descriptor.FieldDescriptor
METADATA_FILE_CHUNKED_FIELD_NUMBER: _ClassVar[int]
metadata_file_chunked: _descriptor.FieldDescriptor
METADATA_FILE_CHUNK_INDEX_FIELD_NUMBER: _ClassVar[int]
metadata_file_chunk_index: _descriptor.FieldDescriptor
METADATA_FILE_CHUNK_COUNT_FIELD_NUMBER: _ClassVar[int]
metadata_file_chunk_count: _descriptor.FieldDescriptor

class Record(_message.Message):
    __slots__ = ("position", "operation", "metadata", "key", "payload")
    class MetadataEntry(_message.Message):
        __slots__ = ("key", "value")
        KEY_FIELD_NUMBER: _ClassVar[int]
        VALUE_FIELD_NUMBER: _ClassVar[int]
        key: str
        value: str
        def __init__(self, key: _Optional[str] = ..., value: _Optional[str] = ...) -> None: ...
    POSITION_FIELD_NUMBER: _ClassVar[int]
    OPERATION_FIELD_NUMBER: _ClassVar[int]
    METADATA_FIELD_NUMBER: _ClassVar[int]
    KEY_FIELD_NUMBER: _ClassVar[int]
    PAYLOAD_FIELD_NUMBER: _ClassVar[int]
    position: bytes
    operation: Operation
    metadata: _containers.ScalarMap[str, str]
    key: Data
    payload: Change
    def __init__(self, position: _Optional[bytes] = ..., operation: _Optional[_Union[Operation, str]] = ..., metadata: _Optional[_Mapping[str, str]] = ..., key: _Optional[_Union[Data, _Mapping]] = ..., payload: _Optional[_Union[Change, _Mapping]] = ...) -> None: ...

class Change(_message.Message):
    __slots__ = ("before", "after")
    BEFORE_FIELD_NUMBER: _ClassVar[int]
    AFTER_FIELD_NUMBER: _ClassVar[int]
    before: Data
    after: Data
    def __init__(self, before: _Optional[_Union[Data, _Mapping]] = ..., after: _Optional[_Union[Data, _Mapping]] = ...) -> None: ...

class Data(_message.Message):
    __slots__ = ("raw_data", "structured_data")
    RAW_DATA_FIELD_NUMBER: _ClassVar[int]
    STRUCTURED_DATA_FIELD_NUMBER: _ClassVar[int]
    raw_data: bytes
    structured_data: _struct_pb2.Struct
    def __init__(self, raw_data: _Optional[bytes] = ..., structured_data: _Optional[_Union[_struct_pb2.Struct, _Mapping]] = ...) -> None: ...
