"""Tests for agent/grpc_client.py.

_gpu_to_proto is a pure function and tested directly. The RPC-calling
methods (heartbeat's assigned_jobs parsing, report_started/completed/
failed) are tested against a real, minimal, in-process gRPC server
implementing WorkerServiceServicer/JobServiceServicer — this exercises
actual protobuf (de)serialization over a real localhost socket rather
than mocking the stub, catching request/response field-mapping bugs a
mock would hide, while still needing no control-plane binary or Docker.
"""

from __future__ import annotations

from concurrent import futures

import grpc
import pytest
from orionqueue.v1 import (
    job_pb2,
    job_service_pb2,
    job_service_pb2_grpc,
    worker_pb2,
    worker_service_pb2,
    worker_service_pb2_grpc,
)

from agent.grpc_client import WorkerClient, _gpu_to_proto
from gpu.fake import GPU


def test_gpu_to_proto_maps_all_fields():
    g = GPU(
        device_index=1,
        uuid="GPU-fake-abc",
        total_memory_bytes=1000,
        allocated_memory_bytes=200,
        utilization_percent=42.5,
        temperature_celsius=65.0,
        mig_profile="1g.5gb",
        health_state="HEALTHY",
    )
    pb = _gpu_to_proto(g)

    assert pb.device_index == 1
    assert pb.uuid == "GPU-fake-abc"
    assert pb.total_memory_bytes == 1000
    assert pb.allocated_memory_bytes == 200
    assert pb.utilization_percent == 42.5
    assert pb.temperature_celsius == 65.0
    assert pb.mig_profile == "1g.5gb"


def test_gpu_to_proto_omits_temperature_when_none():
    g = GPU(device_index=0, uuid="GPU-fake-1", total_memory_bytes=1000, temperature_celsius=None)
    pb = _gpu_to_proto(g)
    assert not pb.HasField("temperature_celsius")


def test_gpu_to_proto_maps_unhealthy_state():
    g = GPU(device_index=0, uuid="GPU-fake-1", total_memory_bytes=1000, health_state="UNHEALTHY")
    pb = _gpu_to_proto(g)
    assert pb.health_state == worker_pb2.GPU_HEALTH_STATE_UNHEALTHY


class _FakeWorkerServicer(worker_service_pb2_grpc.WorkerServiceServicer):
    def __init__(self, assigned_jobs=(), stop_job_ids=()):
        self.heartbeat_requests: list[worker_service_pb2.WorkerHeartbeatRequest] = []
        self._assigned_jobs = list(assigned_jobs)
        self._stop_job_ids = list(stop_job_ids)

    def RegisterWorker(self, request, context):
        return worker_service_pb2.RegisterWorkerResponse(
            worker=worker_pb2.Worker(id="worker-fake-1", hostname=request.hostname),
            heartbeat_interval_seconds=5.0,
        )

    def WorkerHeartbeat(self, request, context):
        self.heartbeat_requests.append(request)
        return worker_service_pb2.WorkerHeartbeatResponse(
            worker=worker_pb2.Worker(id=request.worker_id),
            assigned_jobs=self._assigned_jobs,
            stop_job_ids=self._stop_job_ids,
        )


class _FakeJobServicer(job_service_pb2_grpc.JobServiceServicer):
    def __init__(self):
        self.started_requests: list[job_service_pb2.ReportJobStartedRequest] = []
        self.completed_requests: list[job_service_pb2.ReportJobCompletedRequest] = []
        self.failed_requests: list[job_service_pb2.ReportJobFailedRequest] = []
        self.stopped_requests: list[job_service_pb2.ReportJobStoppedRequest] = []

    def ReportJobStarted(self, request, context):
        self.started_requests.append(request)
        return job_service_pb2.ReportJobStartedResponse(job=job_pb2.Job(id=request.job_id))

    def ReportJobCompleted(self, request, context):
        self.completed_requests.append(request)
        return job_service_pb2.ReportJobCompletedResponse(job=job_pb2.Job(id=request.job_id))

    def ReportJobFailed(self, request, context):
        self.failed_requests.append(request)
        return job_service_pb2.ReportJobFailedResponse(job=job_pb2.Job(id=request.job_id))

    def ReportJobStopped(self, request, context):
        self.stopped_requests.append(request)
        return job_service_pb2.ReportJobStoppedResponse(job=job_pb2.Job(id=request.job_id))


@pytest.fixture
def fake_server():
    """Starts a real gRPC server on an OS-assigned localhost port with
    fake Worker/Job servicers, yields (worker_servicer, job_servicer,
    address), and tears the server down afterward.
    """
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=4))
    worker_servicer = _FakeWorkerServicer()
    job_servicer = _FakeJobServicer()
    worker_service_pb2_grpc.add_WorkerServiceServicer_to_server(worker_servicer, server)
    job_service_pb2_grpc.add_JobServiceServicer_to_server(job_servicer, server)
    port = server.add_insecure_port("127.0.0.1:0")
    server.start()
    try:
        yield worker_servicer, job_servicer, f"127.0.0.1:{port}"
    finally:
        server.stop(grace=None)


def test_heartbeat_returns_no_assigned_jobs_when_none_pending(fake_server):
    worker_servicer, _, addr = fake_server
    client = WorkerClient(addr)
    try:
        result = client.heartbeat("worker-fake-1", gpus=[], running_job_ids=[])
    finally:
        client.close()

    assert result.assigned_jobs == []
    assert len(worker_servicer.heartbeat_requests) == 1
    assert worker_servicer.heartbeat_requests[0].worker_id == "worker-fake-1"


def test_heartbeat_parses_assigned_jobs_into_plain_objects():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=4))
    worker_servicer = _FakeWorkerServicer(
        assigned_jobs=[
            job_pb2.Job(id="job-1", command=["--steps=3", "--step-seconds=0"], timeout_seconds=60),
        ]
    )
    worker_service_pb2_grpc.add_WorkerServiceServicer_to_server(worker_servicer, server)
    port = server.add_insecure_port("127.0.0.1:0")
    server.start()
    try:
        client = WorkerClient(f"127.0.0.1:{port}")
        try:
            result = client.heartbeat("worker-fake-1", gpus=[], running_job_ids=[])
        finally:
            client.close()
    finally:
        server.stop(grace=None)

    assert len(result.assigned_jobs) == 1
    job = result.assigned_jobs[0]
    assert job.job_id == "job-1"
    assert job.command == ["--steps=3", "--step-seconds=0"]
    assert job.timeout_seconds == 60


def test_heartbeat_sends_running_job_ids(fake_server):
    worker_servicer, _, addr = fake_server
    client = WorkerClient(addr)
    try:
        client.heartbeat("worker-fake-1", gpus=[], running_job_ids=["job-a", "job-b"])
    finally:
        client.close()

    assert list(worker_servicer.heartbeat_requests[0].running_job_ids) == ["job-a", "job-b"]


def test_report_started_sends_job_and_worker_id(fake_server):
    _, job_servicer, addr = fake_server
    client = WorkerClient(addr)
    try:
        client.report_started("job-1", "worker-fake-1")
    finally:
        client.close()

    assert len(job_servicer.started_requests) == 1
    assert job_servicer.started_requests[0].job_id == "job-1"
    assert job_servicer.started_requests[0].worker_id == "worker-fake-1"


def test_report_completed_sends_exit_code(fake_server):
    _, job_servicer, addr = fake_server
    client = WorkerClient(addr)
    try:
        client.report_completed("job-1", "worker-fake-1", exit_code=0)
    finally:
        client.close()

    assert len(job_servicer.completed_requests) == 1
    assert job_servicer.completed_requests[0].job_id == "job-1"
    assert job_servicer.completed_requests[0].exit_code == 0


def test_report_failed_sends_failure_reason(fake_server):
    _, job_servicer, addr = fake_server
    client = WorkerClient(addr)
    try:
        client.report_failed("job-1", "worker-fake-1", "simulated failure at step 2 of 3")
    finally:
        client.close()

    assert len(job_servicer.failed_requests) == 1
    assert job_servicer.failed_requests[0].job_id == "job-1"
    assert job_servicer.failed_requests[0].failure_reason == "simulated failure at step 2 of 3"


def test_report_stopped_sends_job_and_worker_id(fake_server):
    _, job_servicer, addr = fake_server
    client = WorkerClient(addr)
    try:
        client.report_stopped("job-1", "worker-fake-1")
    finally:
        client.close()

    assert len(job_servicer.stopped_requests) == 1
    assert job_servicer.stopped_requests[0].job_id == "job-1"
    assert job_servicer.stopped_requests[0].worker_id == "worker-fake-1"


def test_heartbeat_returns_no_stop_job_ids_when_none_pending(fake_server):
    _, _, addr = fake_server
    client = WorkerClient(addr)
    try:
        result = client.heartbeat("worker-fake-1", gpus=[], running_job_ids=[])
    finally:
        client.close()

    assert result.stop_job_ids == []


def test_heartbeat_parses_stop_job_ids():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=4))
    worker_servicer = _FakeWorkerServicer(stop_job_ids=["job-1", "job-2"])
    worker_service_pb2_grpc.add_WorkerServiceServicer_to_server(worker_servicer, server)
    port = server.add_insecure_port("127.0.0.1:0")
    server.start()
    try:
        client = WorkerClient(f"127.0.0.1:{port}")
        try:
            result = client.heartbeat("worker-fake-1", gpus=[], running_job_ids=[])
        finally:
            client.close()
    finally:
        server.stop(grace=None)

    assert result.stop_job_ids == ["job-1", "job-2"]
