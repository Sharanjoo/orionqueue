"""gRPC client for the OrionQueue worker agent, talking to the control
plane's WorkerService (RegisterWorker, WorkerHeartbeat — see
proto/orionqueue/v1/worker_service.proto). All protobuf/grpc-specific
code lives here, kept out of main.py and gpu/fake.py so those stay easy
to read and test independently of a live server.
"""

from __future__ import annotations

import grpc
from orionqueue.v1 import worker_pb2, worker_service_pb2, worker_service_pb2_grpc

from gpu.fake import GPU


class RegistrationResult:
    """What the caller (agent/main.py) needs from a successful
    RegisterWorker call, decoupled from the raw protobuf response type.
    """

    def __init__(self, worker_id: str, heartbeat_interval_seconds: float) -> None:
        self.worker_id = worker_id
        self.heartbeat_interval_seconds = heartbeat_interval_seconds


class WorkerClient:
    """Wraps a gRPC channel to the control plane's WorkerService."""

    def __init__(self, api_grpc_addr: str) -> None:
        self._channel = grpc.insecure_channel(api_grpc_addr)
        self._stub = worker_service_pb2_grpc.WorkerServiceStub(self._channel)

    def close(self) -> None:
        self._channel.close()

    def register(
        self,
        hostname: str,
        cpu_capacity: float,
        memory_capacity_bytes: int,
        gpus: list[GPU],
        software_version: str,
        labels: dict[str, str],
    ) -> RegistrationResult:
        request = worker_service_pb2.RegisterWorkerRequest(
            hostname=hostname,
            cpu_capacity=cpu_capacity,
            memory_capacity_bytes=memory_capacity_bytes,
            gpus=[_gpu_to_proto(g) for g in gpus],
            software_version=software_version,
            labels=labels,
        )
        response = self._stub.RegisterWorker(request)
        return RegistrationResult(
            worker_id=response.worker.id,
            heartbeat_interval_seconds=response.heartbeat_interval_seconds,
        )

    def heartbeat(self, worker_id: str, gpus: list[GPU], running_job_ids: list[str]) -> None:
        request = worker_service_pb2.WorkerHeartbeatRequest(
            worker_id=worker_id,
            gpus=[_gpu_to_proto(g) for g in gpus],
            running_job_ids=running_job_ids,
        )
        self._stub.WorkerHeartbeat(request)


def _gpu_to_proto(g: GPU):
    kwargs = dict(
        device_index=g.device_index,
        uuid=g.uuid,
        total_memory_bytes=g.total_memory_bytes,
        allocated_memory_bytes=g.allocated_memory_bytes,
        utilization_percent=g.utilization_percent,
        mig_profile=g.mig_profile,
        health_state=worker_pb2.GPU_HEALTH_STATE_HEALTHY
        if g.health_state == "HEALTHY"
        else worker_pb2.GPU_HEALTH_STATE_UNHEALTHY,
    )
    if g.temperature_celsius is not None:
        kwargs["temperature_celsius"] = g.temperature_celsius
    return worker_pb2.GPU(**kwargs)
