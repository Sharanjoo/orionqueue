import datetime

from google.protobuf import timestamp_pb2 as _timestamp_pb2
from google.protobuf.internal import containers as _containers
from google.protobuf.internal import enum_type_wrapper as _enum_type_wrapper
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from collections.abc import Iterable as _Iterable, Mapping as _Mapping
from typing import ClassVar as _ClassVar, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class WorkerStatus(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    WORKER_STATUS_UNSPECIFIED: _ClassVar[WorkerStatus]
    WORKER_STATUS_ACTIVE: _ClassVar[WorkerStatus]
    WORKER_STATUS_LOST: _ClassVar[WorkerStatus]

class GPUHealthState(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    GPU_HEALTH_STATE_UNSPECIFIED: _ClassVar[GPUHealthState]
    GPU_HEALTH_STATE_HEALTHY: _ClassVar[GPUHealthState]
    GPU_HEALTH_STATE_DEGRADED: _ClassVar[GPUHealthState]
    GPU_HEALTH_STATE_UNHEALTHY: _ClassVar[GPUHealthState]
WORKER_STATUS_UNSPECIFIED: WorkerStatus
WORKER_STATUS_ACTIVE: WorkerStatus
WORKER_STATUS_LOST: WorkerStatus
GPU_HEALTH_STATE_UNSPECIFIED: GPUHealthState
GPU_HEALTH_STATE_HEALTHY: GPUHealthState
GPU_HEALTH_STATE_DEGRADED: GPUHealthState
GPU_HEALTH_STATE_UNHEALTHY: GPUHealthState

class GPU(_message.Message):
    __slots__ = ("id", "device_index", "uuid", "total_memory_bytes", "allocated_memory_bytes", "utilization_percent", "temperature_celsius", "mig_profile", "health_state")
    ID_FIELD_NUMBER: _ClassVar[int]
    DEVICE_INDEX_FIELD_NUMBER: _ClassVar[int]
    UUID_FIELD_NUMBER: _ClassVar[int]
    TOTAL_MEMORY_BYTES_FIELD_NUMBER: _ClassVar[int]
    ALLOCATED_MEMORY_BYTES_FIELD_NUMBER: _ClassVar[int]
    UTILIZATION_PERCENT_FIELD_NUMBER: _ClassVar[int]
    TEMPERATURE_CELSIUS_FIELD_NUMBER: _ClassVar[int]
    MIG_PROFILE_FIELD_NUMBER: _ClassVar[int]
    HEALTH_STATE_FIELD_NUMBER: _ClassVar[int]
    id: str
    device_index: int
    uuid: str
    total_memory_bytes: int
    allocated_memory_bytes: int
    utilization_percent: float
    temperature_celsius: float
    mig_profile: str
    health_state: GPUHealthState
    def __init__(self, id: _Optional[str] = ..., device_index: _Optional[int] = ..., uuid: _Optional[str] = ..., total_memory_bytes: _Optional[int] = ..., allocated_memory_bytes: _Optional[int] = ..., utilization_percent: _Optional[float] = ..., temperature_celsius: _Optional[float] = ..., mig_profile: _Optional[str] = ..., health_state: _Optional[_Union[GPUHealthState, str]] = ...) -> None: ...

class Worker(_message.Message):
    __slots__ = ("id", "hostname", "status", "cpu_capacity", "memory_capacity_bytes", "gpus", "software_version", "labels", "running_job_ids", "registered_at", "last_heartbeat_at")
    class LabelsEntry(_message.Message):
        __slots__ = ("key", "value")
        KEY_FIELD_NUMBER: _ClassVar[int]
        VALUE_FIELD_NUMBER: _ClassVar[int]
        key: str
        value: str
        def __init__(self, key: _Optional[str] = ..., value: _Optional[str] = ...) -> None: ...
    ID_FIELD_NUMBER: _ClassVar[int]
    HOSTNAME_FIELD_NUMBER: _ClassVar[int]
    STATUS_FIELD_NUMBER: _ClassVar[int]
    CPU_CAPACITY_FIELD_NUMBER: _ClassVar[int]
    MEMORY_CAPACITY_BYTES_FIELD_NUMBER: _ClassVar[int]
    GPUS_FIELD_NUMBER: _ClassVar[int]
    SOFTWARE_VERSION_FIELD_NUMBER: _ClassVar[int]
    LABELS_FIELD_NUMBER: _ClassVar[int]
    RUNNING_JOB_IDS_FIELD_NUMBER: _ClassVar[int]
    REGISTERED_AT_FIELD_NUMBER: _ClassVar[int]
    LAST_HEARTBEAT_AT_FIELD_NUMBER: _ClassVar[int]
    id: str
    hostname: str
    status: WorkerStatus
    cpu_capacity: float
    memory_capacity_bytes: int
    gpus: _containers.RepeatedCompositeFieldContainer[GPU]
    software_version: str
    labels: _containers.ScalarMap[str, str]
    running_job_ids: _containers.RepeatedScalarFieldContainer[str]
    registered_at: _timestamp_pb2.Timestamp
    last_heartbeat_at: _timestamp_pb2.Timestamp
    def __init__(self, id: _Optional[str] = ..., hostname: _Optional[str] = ..., status: _Optional[_Union[WorkerStatus, str]] = ..., cpu_capacity: _Optional[float] = ..., memory_capacity_bytes: _Optional[int] = ..., gpus: _Optional[_Iterable[_Union[GPU, _Mapping]]] = ..., software_version: _Optional[str] = ..., labels: _Optional[_Mapping[str, str]] = ..., running_job_ids: _Optional[_Iterable[str]] = ..., registered_at: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ..., last_heartbeat_at: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ...) -> None: ...
