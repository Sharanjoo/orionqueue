from google.api import annotations_pb2 as _annotations_pb2
from orionqueue.v1 import job_pb2 as _job_pb2
from orionqueue.v1 import worker_pb2 as _worker_pb2
from google.protobuf.internal import containers as _containers
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from collections.abc import Iterable as _Iterable, Mapping as _Mapping
from typing import ClassVar as _ClassVar, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class RegisterWorkerRequest(_message.Message):
    __slots__ = ("hostname", "cpu_capacity", "memory_capacity_bytes", "gpus", "software_version", "labels")
    class LabelsEntry(_message.Message):
        __slots__ = ("key", "value")
        KEY_FIELD_NUMBER: _ClassVar[int]
        VALUE_FIELD_NUMBER: _ClassVar[int]
        key: str
        value: str
        def __init__(self, key: _Optional[str] = ..., value: _Optional[str] = ...) -> None: ...
    HOSTNAME_FIELD_NUMBER: _ClassVar[int]
    CPU_CAPACITY_FIELD_NUMBER: _ClassVar[int]
    MEMORY_CAPACITY_BYTES_FIELD_NUMBER: _ClassVar[int]
    GPUS_FIELD_NUMBER: _ClassVar[int]
    SOFTWARE_VERSION_FIELD_NUMBER: _ClassVar[int]
    LABELS_FIELD_NUMBER: _ClassVar[int]
    hostname: str
    cpu_capacity: float
    memory_capacity_bytes: int
    gpus: _containers.RepeatedCompositeFieldContainer[_worker_pb2.GPU]
    software_version: str
    labels: _containers.ScalarMap[str, str]
    def __init__(self, hostname: _Optional[str] = ..., cpu_capacity: _Optional[float] = ..., memory_capacity_bytes: _Optional[int] = ..., gpus: _Optional[_Iterable[_Union[_worker_pb2.GPU, _Mapping]]] = ..., software_version: _Optional[str] = ..., labels: _Optional[_Mapping[str, str]] = ...) -> None: ...

class RegisterWorkerResponse(_message.Message):
    __slots__ = ("worker", "heartbeat_interval_seconds")
    WORKER_FIELD_NUMBER: _ClassVar[int]
    HEARTBEAT_INTERVAL_SECONDS_FIELD_NUMBER: _ClassVar[int]
    worker: _worker_pb2.Worker
    heartbeat_interval_seconds: int
    def __init__(self, worker: _Optional[_Union[_worker_pb2.Worker, _Mapping]] = ..., heartbeat_interval_seconds: _Optional[int] = ...) -> None: ...

class WorkerHeartbeatRequest(_message.Message):
    __slots__ = ("worker_id", "gpus", "running_job_ids")
    WORKER_ID_FIELD_NUMBER: _ClassVar[int]
    GPUS_FIELD_NUMBER: _ClassVar[int]
    RUNNING_JOB_IDS_FIELD_NUMBER: _ClassVar[int]
    worker_id: str
    gpus: _containers.RepeatedCompositeFieldContainer[_worker_pb2.GPU]
    running_job_ids: _containers.RepeatedScalarFieldContainer[str]
    def __init__(self, worker_id: _Optional[str] = ..., gpus: _Optional[_Iterable[_Union[_worker_pb2.GPU, _Mapping]]] = ..., running_job_ids: _Optional[_Iterable[str]] = ...) -> None: ...

class WorkerHeartbeatResponse(_message.Message):
    __slots__ = ("worker", "assigned_jobs", "stop_job_ids")
    WORKER_FIELD_NUMBER: _ClassVar[int]
    ASSIGNED_JOBS_FIELD_NUMBER: _ClassVar[int]
    STOP_JOB_IDS_FIELD_NUMBER: _ClassVar[int]
    worker: _worker_pb2.Worker
    assigned_jobs: _containers.RepeatedCompositeFieldContainer[_job_pb2.Job]
    stop_job_ids: _containers.RepeatedScalarFieldContainer[str]
    def __init__(self, worker: _Optional[_Union[_worker_pb2.Worker, _Mapping]] = ..., assigned_jobs: _Optional[_Iterable[_Union[_job_pb2.Job, _Mapping]]] = ..., stop_job_ids: _Optional[_Iterable[str]] = ...) -> None: ...

class ListWorkersRequest(_message.Message):
    __slots__ = ("page_size", "page_token", "status_filter")
    PAGE_SIZE_FIELD_NUMBER: _ClassVar[int]
    PAGE_TOKEN_FIELD_NUMBER: _ClassVar[int]
    STATUS_FILTER_FIELD_NUMBER: _ClassVar[int]
    page_size: int
    page_token: str
    status_filter: _worker_pb2.WorkerStatus
    def __init__(self, page_size: _Optional[int] = ..., page_token: _Optional[str] = ..., status_filter: _Optional[_Union[_worker_pb2.WorkerStatus, str]] = ...) -> None: ...

class ListWorkersResponse(_message.Message):
    __slots__ = ("workers", "next_page_token")
    WORKERS_FIELD_NUMBER: _ClassVar[int]
    NEXT_PAGE_TOKEN_FIELD_NUMBER: _ClassVar[int]
    workers: _containers.RepeatedCompositeFieldContainer[_worker_pb2.Worker]
    next_page_token: str
    def __init__(self, workers: _Optional[_Iterable[_Union[_worker_pb2.Worker, _Mapping]]] = ..., next_page_token: _Optional[str] = ...) -> None: ...
