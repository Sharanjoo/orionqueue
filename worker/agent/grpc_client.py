"""gRPC client for the OrionQueue worker agent, talking to the control
plane's WorkerService (register/heartbeat) and JobService (lifecycle
reporting) — see proto/orionqueue/v1/{worker_service,job_service}.proto.
All protobuf/grpc-specific code lives here, kept out of main.py,
gpu/fake.py, and executors/fake.py so those stay easy to read and test
independently of a live server.
"""

from __future__ import annotations

import grpc
from orionqueue.v1 import (
    job_service_pb2,
    job_service_pb2_grpc,
    worker_pb2,
    worker_service_pb2,
    worker_service_pb2_grpc,
)

from gpu.fake import GPU


class RegistrationResult:
    """What the caller (agent/main.py) needs from a successful
    RegisterWorker call, decoupled from the raw protobuf response type.
    """

    def __init__(self, worker_id: str, heartbeat_interval_seconds: float) -> None:
        self.worker_id = worker_id
        self.heartbeat_interval_seconds = heartbeat_interval_seconds


class AssignedJob:
    """A job the control plane wants this worker to start, decoupled from
    the raw protobuf Job message — main.py's execution loop only ever
    needs these three fields.
    """

    def __init__(self, job_id: str, command: list[str], timeout_seconds: float) -> None:
        self.job_id = job_id
        self.command = command
        self.timeout_seconds = timeout_seconds


class HeartbeatResult:
    """What the caller needs from a successful WorkerHeartbeat call."""

    def __init__(self, assigned_jobs: list[AssignedJob], stop_job_ids: list[str]) -> None:
        self.assigned_jobs = assigned_jobs
        # stop_job_ids are jobs this worker should cooperatively stop
        # (cancellation or preemption — Phase 7). Only IDs, since the
        # worker already has each job's details from assigned_jobs — see
        # worker_service.proto's WorkerHeartbeatResponse.stop_job_ids.
        self.stop_job_ids = stop_job_ids


class WorkerClient:
    """Wraps gRPC channels to the control plane's WorkerService and
    JobService (the latter only for the lifecycle-reporting RPCs a worker
    calls — job submission/lookup/etc. are client-facing, not called from
    here).
    """

    def __init__(self, api_grpc_addr: str) -> None:
        self._channel = grpc.insecure_channel(api_grpc_addr)
        self._worker_stub = worker_service_pb2_grpc.WorkerServiceStub(self._channel)
        self._job_stub = job_service_pb2_grpc.JobServiceStub(self._channel)

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
        response = self._worker_stub.RegisterWorker(request)
        return RegistrationResult(
            worker_id=response.worker.id,
            heartbeat_interval_seconds=response.heartbeat_interval_seconds,
        )

    def heartbeat(
        self, worker_id: str, gpus: list[GPU], running_job_ids: list[str]
    ) -> HeartbeatResult:
        request = worker_service_pb2.WorkerHeartbeatRequest(
            worker_id=worker_id,
            gpus=[_gpu_to_proto(g) for g in gpus],
            running_job_ids=running_job_ids,
        )
        response = self._worker_stub.WorkerHeartbeat(request)
        assigned = [
            AssignedJob(job_id=j.id, command=list(j.command), timeout_seconds=j.timeout_seconds)
            for j in response.assigned_jobs
        ]
        return HeartbeatResult(assigned_jobs=assigned, stop_job_ids=list(response.stop_job_ids))

    def report_started(self, job_id: str, worker_id: str) -> None:
        self._job_stub.ReportJobStarted(
            job_service_pb2.ReportJobStartedRequest(job_id=job_id, worker_id=worker_id)
        )

    def report_completed(self, job_id: str, worker_id: str, exit_code: int = 0) -> None:
        self._job_stub.ReportJobCompleted(
            job_service_pb2.ReportJobCompletedRequest(
                job_id=job_id, worker_id=worker_id, exit_code=exit_code
            )
        )

    def report_failed(self, job_id: str, worker_id: str, failure_reason: str) -> None:
        self._job_stub.ReportJobFailed(
            job_service_pb2.ReportJobFailedRequest(
                job_id=job_id, worker_id=worker_id, failure_reason=failure_reason
            )
        )

    def report_stopped(self, job_id: str, worker_id: str) -> None:
        """Confirms a job's execution was cooperatively stopped in
        response to a stop signal (see HeartbeatResult.stop_job_ids). The
        server decides what this means (cancellation reaching terminal
        CANCELLED, or a preemption requeue to QUEUED) based on the job's
        own current state — this client, like main.py, doesn't need to
        know or care which.
        """
        self._job_stub.ReportJobStopped(
            job_service_pb2.ReportJobStoppedRequest(job_id=job_id, worker_id=worker_id)
        )


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
