import datetime

from google.protobuf import timestamp_pb2 as _timestamp_pb2
from google.protobuf.internal import containers as _containers
from google.protobuf.internal import enum_type_wrapper as _enum_type_wrapper
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from collections.abc import Iterable as _Iterable, Mapping as _Mapping
from typing import ClassVar as _ClassVar, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class JobState(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    JOB_STATE_UNSPECIFIED: _ClassVar[JobState]
    JOB_STATE_QUEUED: _ClassVar[JobState]
    JOB_STATE_SCHEDULED: _ClassVar[JobState]
    JOB_STATE_RUNNING: _ClassVar[JobState]
    JOB_STATE_CHECKPOINTING: _ClassVar[JobState]
    JOB_STATE_SUCCEEDED: _ClassVar[JobState]
    JOB_STATE_FAILED: _ClassVar[JobState]
    JOB_STATE_RETRYING: _ClassVar[JobState]
    JOB_STATE_CANCEL_REQUESTED: _ClassVar[JobState]
    JOB_STATE_CANCELLED: _ClassVar[JobState]
    JOB_STATE_PREEMPTED: _ClassVar[JobState]
    JOB_STATE_LOST: _ClassVar[JobState]
JOB_STATE_UNSPECIFIED: JobState
JOB_STATE_QUEUED: JobState
JOB_STATE_SCHEDULED: JobState
JOB_STATE_RUNNING: JobState
JOB_STATE_CHECKPOINTING: JobState
JOB_STATE_SUCCEEDED: JobState
JOB_STATE_FAILED: JobState
JOB_STATE_RETRYING: JobState
JOB_STATE_CANCEL_REQUESTED: JobState
JOB_STATE_CANCELLED: JobState
JOB_STATE_PREEMPTED: JobState
JOB_STATE_LOST: JobState

class ResourceRequest(_message.Message):
    __slots__ = ("gpu_count", "min_gpu_memory_bytes", "cpu_cores", "memory_bytes")
    GPU_COUNT_FIELD_NUMBER: _ClassVar[int]
    MIN_GPU_MEMORY_BYTES_FIELD_NUMBER: _ClassVar[int]
    CPU_CORES_FIELD_NUMBER: _ClassVar[int]
    MEMORY_BYTES_FIELD_NUMBER: _ClassVar[int]
    gpu_count: int
    min_gpu_memory_bytes: int
    cpu_cores: float
    memory_bytes: int
    def __init__(self, gpu_count: _Optional[int] = ..., min_gpu_memory_bytes: _Optional[int] = ..., cpu_cores: _Optional[float] = ..., memory_bytes: _Optional[int] = ...) -> None: ...

class Job(_message.Message):
    __slots__ = ("id", "name", "owner", "image", "command", "resources", "priority", "preemptible", "retry_limit", "current_attempt", "timeout_seconds", "checkpoint_interval_seconds", "state", "assigned_worker_ids", "failure_reason", "created_at", "started_at", "completed_at", "failed_at")
    ID_FIELD_NUMBER: _ClassVar[int]
    NAME_FIELD_NUMBER: _ClassVar[int]
    OWNER_FIELD_NUMBER: _ClassVar[int]
    IMAGE_FIELD_NUMBER: _ClassVar[int]
    COMMAND_FIELD_NUMBER: _ClassVar[int]
    RESOURCES_FIELD_NUMBER: _ClassVar[int]
    PRIORITY_FIELD_NUMBER: _ClassVar[int]
    PREEMPTIBLE_FIELD_NUMBER: _ClassVar[int]
    RETRY_LIMIT_FIELD_NUMBER: _ClassVar[int]
    CURRENT_ATTEMPT_FIELD_NUMBER: _ClassVar[int]
    TIMEOUT_SECONDS_FIELD_NUMBER: _ClassVar[int]
    CHECKPOINT_INTERVAL_SECONDS_FIELD_NUMBER: _ClassVar[int]
    STATE_FIELD_NUMBER: _ClassVar[int]
    ASSIGNED_WORKER_IDS_FIELD_NUMBER: _ClassVar[int]
    FAILURE_REASON_FIELD_NUMBER: _ClassVar[int]
    CREATED_AT_FIELD_NUMBER: _ClassVar[int]
    STARTED_AT_FIELD_NUMBER: _ClassVar[int]
    COMPLETED_AT_FIELD_NUMBER: _ClassVar[int]
    FAILED_AT_FIELD_NUMBER: _ClassVar[int]
    id: str
    name: str
    owner: str
    image: str
    command: _containers.RepeatedScalarFieldContainer[str]
    resources: ResourceRequest
    priority: int
    preemptible: bool
    retry_limit: int
    current_attempt: int
    timeout_seconds: int
    checkpoint_interval_seconds: int
    state: JobState
    assigned_worker_ids: _containers.RepeatedScalarFieldContainer[str]
    failure_reason: str
    created_at: _timestamp_pb2.Timestamp
    started_at: _timestamp_pb2.Timestamp
    completed_at: _timestamp_pb2.Timestamp
    failed_at: _timestamp_pb2.Timestamp
    def __init__(self, id: _Optional[str] = ..., name: _Optional[str] = ..., owner: _Optional[str] = ..., image: _Optional[str] = ..., command: _Optional[_Iterable[str]] = ..., resources: _Optional[_Union[ResourceRequest, _Mapping]] = ..., priority: _Optional[int] = ..., preemptible: _Optional[bool] = ..., retry_limit: _Optional[int] = ..., current_attempt: _Optional[int] = ..., timeout_seconds: _Optional[int] = ..., checkpoint_interval_seconds: _Optional[int] = ..., state: _Optional[_Union[JobState, str]] = ..., assigned_worker_ids: _Optional[_Iterable[str]] = ..., failure_reason: _Optional[str] = ..., created_at: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ..., started_at: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ..., completed_at: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ..., failed_at: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ...) -> None: ...
